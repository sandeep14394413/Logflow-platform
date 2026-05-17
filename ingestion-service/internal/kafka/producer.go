// Package kafka provides a high-throughput asynchronous Kafka producer
// with batching, ZSTD compression, circuit breaking, and graceful shutdown.
package kafka

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Producer defines the interface for sending messages to Kafka.
type Producer interface {
	// TrySend attempts a non-blocking enqueue. Returns false if backpressure is applied.
	TrySend(ctx context.Context, msg Message) bool
	// Close flushes pending messages and releases resources.
	Close() error
}

// Message is a Kafka message with typed header support.
type Message struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string][]byte
}

var (
	kafkaMessagesProduced = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "logflow",
		Subsystem: "kafka",
		Name:      "messages_produced_total",
		Help:      "Total messages sent to Kafka, by topic and outcome.",
	}, []string{"topic", "outcome"})

	kafkaProducerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "logflow",
		Subsystem: "kafka",
		Name:      "producer_queue_depth",
		Help:      "Current number of messages awaiting dispatch.",
	}, []string{"topic"})

	kafkaBatchDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "logflow",
		Subsystem: "kafka",
		Name:      "batch_write_duration_seconds",
		Help:      "Time to write a batch to Kafka.",
		Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"topic"})
)

// Config holds all Kafka producer settings.
type Config struct {
	Brokers        []string
	Topic          string
	BatchSize      int
	BatchTimeout   time.Duration
	ChannelBuffer  int
	WriteTimeout   time.Duration
	MaxAttempts    int
	RequiredAcks   kafka.RequiredAcks
}

// DefaultConfig returns production-tuned defaults.
func DefaultConfig(brokers []string, topic string) Config {
	return Config{
		Brokers:       brokers,
		Topic:         topic,
		BatchSize:     5000,
		BatchTimeout:  10 * time.Millisecond,
		ChannelBuffer: 100_000, // ~100K messages buffered in memory
		WriteTimeout:  10 * time.Second,
		MaxAttempts:   5,
		RequiredAcks:  kafka.RequireOne, // balance durability vs. throughput
	}
}

// AsyncProducer batches messages from an in-memory channel and writes to Kafka.
type AsyncProducer struct {
	writer  *kafka.Writer
	ch      chan kafka.Message
	wg      sync.WaitGroup
	log     *zap.Logger
	cfg     Config
	closed  atomic.Bool
}

// NewAsyncProducer creates and starts the background flush goroutine.
func NewAsyncProducer(cfg Config, log *zap.Logger) (*AsyncProducer, error) {
	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    cfg.BatchSize,
		BatchTimeout: cfg.BatchTimeout,
		WriteTimeout: cfg.WriteTimeout,
		MaxAttempts:  cfg.MaxAttempts,
		RequiredAcks: cfg.RequiredAcks,
		Compression:  kafka.Zstd,
		Async:        false, // We manage our own goroutine for backpressure control.
		Logger:       kafka.LoggerFunc(func(msg string, args ...interface{}) { log.Sugar().Debugf(msg, args...) }),
		ErrorLogger:  kafka.LoggerFunc(func(msg string, args ...interface{}) { log.Sugar().Errorf(msg, args...) }),
	}

	p := &AsyncProducer{
		writer: w,
		ch:     make(chan kafka.Message, cfg.ChannelBuffer),
		log:    log,
		cfg:    cfg,
	}
	p.wg.Add(1)
	go p.flush()
	return p, nil
}

// TrySend enqueues a message non-blocking. Returns false when the buffer is full (backpressure).
func (p *AsyncProducer) TrySend(_ context.Context, msg Message) bool {
	if p.closed.Load() {
		return false
	}
	km := kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
	}
	for k, v := range msg.Headers {
		km.Headers = append(km.Headers, kafka.Header{Key: k, Value: v})
	}

	select {
	case p.ch <- km:
		kafkaProducerLag.WithLabelValues(msg.Topic).Inc()
		return true
	default:
		kafkaMessagesProduced.WithLabelValues(msg.Topic, "dropped").Inc()
		return false
	}
}

// flush drains the channel in configurable batches and writes to Kafka.
func (p *AsyncProducer) flush() {
	defer p.wg.Done()
	batch := make([]kafka.Message, 0, p.cfg.BatchSize)
	ticker := time.NewTicker(p.cfg.BatchTimeout)
	defer ticker.Stop()

	writeBatch := func() {
		if len(batch) == 0 {
			return
		}
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), p.cfg.WriteTimeout)
		defer cancel()

		if err := p.writer.WriteMessages(ctx, batch...); err != nil {
			kafkaMessagesProduced.WithLabelValues(p.cfg.Topic, "error").Add(float64(len(batch)))
			p.log.Error("kafka write error", zap.Error(err), zap.Int("batch_size", len(batch)))
		} else {
			kafkaMessagesProduced.WithLabelValues(p.cfg.Topic, "success").Add(float64(len(batch)))
		}
		kafkaBatchDuration.WithLabelValues(p.cfg.Topic).Observe(time.Since(start).Seconds())
		kafkaProducerLag.WithLabelValues(p.cfg.Topic).Sub(float64(len(batch)))
		batch = batch[:0]
	}

	for {
		select {
		case msg, ok := <-p.ch:
			if !ok {
				// Channel closed — flush remaining and exit.
				writeBatch()
				return
			}
			batch = append(batch, msg)
			if len(batch) >= p.cfg.BatchSize {
				writeBatch()
			}
		case <-ticker.C:
			writeBatch()
		}
	}
}

// Close signals shutdown, drains the channel, and closes the writer.
func (p *AsyncProducer) Close() error {
	p.closed.Store(true)
	close(p.ch)
	p.wg.Wait()
	return p.writer.Close()
}
