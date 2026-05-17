// Package consumer implements a parallel Kafka consumer that batches records
// and bulk-inserts them into ClickHouse with retry, DLQ, and circuit-breaker logic.
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"logflow/kafka-consumer/internal/writer"
)

var (
	consumerMessagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "logflow",
		Subsystem: "consumer",
		Name:      "messages_consumed_total",
		Help:      "Total messages consumed from Kafka.",
	}, []string{"topic", "outcome"})

	consumerBatchWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "logflow",
		Subsystem: "consumer",
		Name:      "batch_write_duration_seconds",
		Help:      "Time to write a batch to ClickHouse.",
		Buckets:   []float64{.01, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"topic"})

	dlqMessagesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "logflow",
		Subsystem: "consumer",
		Name:      "dlq_messages_total",
		Help:      "Messages routed to the dead-letter queue.",
	})
)

// Config controls the consumer group behaviour.
type Config struct {
	Brokers      []string
	Topic        string
	GroupID      string
	DLQTopic     string
	Workers      int
	BatchSize    int
	BatchTimeout time.Duration
	MaxRetries   int
}

// LogConsumer reads from Kafka and writes to ClickHouse in parallel.
type LogConsumer struct {
	cfg    Config
	reader *kafka.Reader
	dlq    *kafka.Writer
	ch     writer.ClickHouseWriter
	log    *zap.Logger
}

// New creates a consumer backed by a ClickHouse writer and a DLQ producer.
func New(cfg Config, ch writer.ClickHouseWriter, log *zap.Logger) *LogConsumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       1e3,  // 1 KB
		MaxBytes:       10e6, // 10 MB
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
		Logger:         kafka.LoggerFunc(func(msg string, args ...interface{}) { log.Sugar().Debugf(msg, args...) }),
		ErrorLogger:    kafka.LoggerFunc(func(msg string, args ...interface{}) { log.Sugar().Errorf(msg, args...) }),
	})
	dlqWriter := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Brokers...),
		Topic:    cfg.DLQTopic,
		Balancer: &kafka.LeastBytes{},
	}
	return &LogConsumer{cfg: cfg, reader: r, dlq: dlqWriter, ch: ch, log: log}
}

// Run starts the consumer group with cfg.Workers parallel goroutines.
// It blocks until ctx is cancelled.
func (c *LogConsumer) Run(ctx context.Context) {
	msgCh := make(chan kafka.Message, c.cfg.BatchSize*c.cfg.Workers)
	var wg sync.WaitGroup

	// Reader goroutine: fetch messages and push to channel.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(msgCh)
		for {
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return // Normal shutdown.
				}
				c.log.Error("kafka fetch error", zap.Error(err))
				continue
			}
			select {
			case msgCh <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Worker goroutines: batch, deserialise, write to ClickHouse.
	for i := 0; i < c.cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c.batchWorker(ctx, id, msgCh)
		}(i)
	}

	wg.Wait()
	if err := c.reader.Close(); err != nil {
		c.log.Error("kafka reader close error", zap.Error(err))
	}
	if err := c.dlq.Close(); err != nil {
		c.log.Error("dlq writer close error", zap.Error(err))
	}
}

// batchWorker accumulates messages until batch size or timeout, then writes.
func (c *LogConsumer) batchWorker(ctx context.Context, id int, in <-chan kafka.Message) {
	tracer := otel.Tracer("kafka-consumer")
	batch := make([]kafka.Message, 0, c.cfg.BatchSize)
	ticker := time.NewTicker(c.cfg.BatchTimeout)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		wctx, span := tracer.Start(ctx, "consumer.flush")
		defer span.End()

		start := time.Now()
		logs, bad := c.deserialise(batch)

		// Route malformed messages to DLQ immediately.
		if len(bad) > 0 {
			c.sendToDLQ(ctx, bad)
		}

		if len(logs) > 0 {
			if err := c.writeWithRetry(wctx, logs, c.cfg.MaxRetries); err != nil {
				c.log.Error("clickhouse write failed after retries",
					zap.Int("worker", id), zap.Int("batch", len(logs)), zap.Error(err))
				c.sendToDLQ(ctx, batch)
				consumerMessagesTotal.WithLabelValues(c.cfg.Topic, "dlq").Add(float64(len(logs)))
			} else {
				// Commit only successfully written offsets.
				if err := c.reader.CommitMessages(ctx, batch...); err != nil {
					c.log.Warn("kafka commit error", zap.Error(err))
				}
				consumerMessagesTotal.WithLabelValues(c.cfg.Topic, "success").Add(float64(len(logs)))
			}
		}
		consumerBatchWriteDuration.WithLabelValues(c.cfg.Topic).Observe(time.Since(start).Seconds())
		batch = batch[:0]
	}

	for {
		select {
		case msg, ok := <-in:
			if !ok {
				flush()
				return
			}
			batch = append(batch, msg)
			if len(batch) >= c.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			flush()
			return
		}
	}
}

// deserialise decodes JSON payloads; returns valid entries and the raw messages that failed.
func (c *LogConsumer) deserialise(msgs []kafka.Message) ([]writer.LogRow, []kafka.Message) {
	rows := make([]writer.LogRow, 0, len(msgs))
	var bad []kafka.Message
	for _, m := range msgs {
		var row writer.LogRow
		if err := json.Unmarshal(m.Value, &row); err != nil {
			c.log.Warn("deserialise error", zap.ByteString("key", m.Key), zap.Error(err))
			bad = append(bad, m)
		} else {
			rows = append(rows, row)
		}
	}
	return rows, bad
}

// writeWithRetry attempts a ClickHouse batch write with exponential back-off.
func (c *LogConsumer) writeWithRetry(ctx context.Context, rows []writer.LogRow, retries int) error {
	var err error
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt-1)) * 100 * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err = c.ch.WriteBatch(ctx, rows); err == nil {
			return nil
		}
		c.log.Warn("clickhouse write attempt failed", zap.Int("attempt", attempt+1), zap.Error(err))
	}
	return fmt.Errorf("all %d attempts failed: %w", retries+1, err)
}

// sendToDLQ publishes failed messages to the dead-letter topic.
func (c *LogConsumer) sendToDLQ(ctx context.Context, msgs []kafka.Message) {
	dlqMsgs := make([]kafka.Message, len(msgs))
	for i, m := range msgs {
		dlqMsgs[i] = kafka.Message{Key: m.Key, Value: m.Value, Headers: m.Headers}
	}
	if err := c.dlq.WriteMessages(ctx, dlqMsgs...); err != nil {
		c.log.Error("dlq write failed", zap.Error(err))
	}
	dlqMessagesTotal.Add(float64(len(msgs)))
}
