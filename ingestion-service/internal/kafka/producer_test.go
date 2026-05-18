package kafka

import (
	"context"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// newTestProducer creates an AsyncProducer without a real Kafka connection.
// It bypasses NewAsyncProducer (which dials the broker) by directly setting
// the internal channel — valid because we're in the same package.
func newTestProducer(bufSize int) *AsyncProducer {
	return &AsyncProducer{
		// writer is nil — tests must never call flush() or Close().
		ch: make(chan kafkago.Message, bufSize),
	}
}

func TestTrySend_WhenClosed_ReturnsFalse(t *testing.T) {
	p := newTestProducer(10)
	p.closed.Store(true)

	sent := p.TrySend(context.Background(), Message{
		Topic: "test",
		Key:   []byte("k"),
		Value: []byte("v"),
	})
	if sent {
		t.Error("expected TrySend to return false when producer is closed")
	}
	if len(p.ch) != 0 {
		t.Error("expected no messages enqueued when closed")
	}
}

func TestTrySend_WhenBufferFull_ReturnsFalse(t *testing.T) {
	p := newTestProducer(1)
	// Fill the single-slot buffer with a raw kafka.Message.
	p.ch <- kafkago.Message{Value: []byte("existing")}

	sent := p.TrySend(context.Background(), Message{
		Topic: "test",
		Value: []byte("overflow"),
	})
	if sent {
		t.Error("expected TrySend to return false when buffer is full")
	}
}

func TestTrySend_Success_ReturnsTrue(t *testing.T) {
	p := newTestProducer(10)

	sent := p.TrySend(context.Background(), Message{
		Topic: "test",
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	if !sent {
		t.Error("expected TrySend to return true with available buffer space")
	}
	if len(p.ch) != 1 {
		t.Errorf("expected 1 message in channel, got %d", len(p.ch))
	}
}

func TestTrySend_MessageFields_EncodedCorrectly(t *testing.T) {
	p := newTestProducer(10)

	msg := Message{
		Topic: "logs-raw",
		Key:   []byte("tenant-acme"),
		Value: []byte(`{"level":"INFO","message":"hello"}`),
		Headers: map[string][]byte{
			"tenant_id": []byte("tenant-acme"),
		},
	}
	p.TrySend(context.Background(), msg)

	// Drain the channel and inspect the kafka.Message that was enqueued.
	km := <-p.ch
	if string(km.Key) != "tenant-acme" {
		t.Errorf("expected key 'tenant-acme', got %q", string(km.Key))
	}
	if string(km.Value) != `{"level":"INFO","message":"hello"}` {
		t.Errorf("unexpected value: %q", string(km.Value))
	}
	// Headers should be set.
	found := false
	for _, h := range km.Headers {
		if h.Key == "tenant_id" && string(h.Value) == "tenant-acme" {
			found = true
		}
	}
	if !found {
		t.Error("expected tenant_id header to be set on kafka message")
	}
}

func TestTrySend_MultipleSends_AllEnqueued(t *testing.T) {
	p := newTestProducer(100)
	count := 50
	for i := 0; i < count; i++ {
		sent := p.TrySend(context.Background(), Message{
			Topic: "test",
			Value: []byte("msg"),
		})
		if !sent {
			t.Errorf("send %d failed unexpectedly", i)
		}
	}
	if len(p.ch) != count {
		t.Errorf("expected %d messages in channel, got %d", count, len(p.ch))
	}
}

func TestDefaultConfig_Values(t *testing.T) {
	brokers := []string{"broker1:9092", "broker2:9092"}
	cfg := DefaultConfig(brokers, "my-topic")

	if cfg.Topic != "my-topic" {
		t.Errorf("expected topic 'my-topic', got %q", cfg.Topic)
	}
	if len(cfg.Brokers) != 2 {
		t.Errorf("expected 2 brokers, got %d", len(cfg.Brokers))
	}
	if cfg.BatchSize <= 0 {
		t.Errorf("expected positive BatchSize, got %d", cfg.BatchSize)
	}
	if cfg.ChannelBuffer <= 0 {
		t.Errorf("expected positive ChannelBuffer, got %d", cfg.ChannelBuffer)
	}
	if cfg.BatchTimeout <= 0 {
		t.Errorf("expected positive BatchTimeout, got %v", cfg.BatchTimeout)
	}
	if cfg.WriteTimeout <= 0 {
		t.Errorf("expected positive WriteTimeout, got %v", cfg.WriteTimeout)
	}
	if cfg.MaxAttempts <= 0 {
		t.Errorf("expected positive MaxAttempts, got %d", cfg.MaxAttempts)
	}
}

func TestDefaultConfig_ReasonableDefaults(t *testing.T) {
	cfg := DefaultConfig([]string{"localhost:9092"}, "topic")

	// BatchSize should be large enough for high throughput.
	if cfg.BatchSize < 1000 {
		t.Errorf("expected BatchSize >= 1000 for high throughput, got %d", cfg.BatchSize)
	}
	// ChannelBuffer should be large enough for burst absorption.
	if cfg.ChannelBuffer < 10_000 {
		t.Errorf("expected ChannelBuffer >= 10000, got %d", cfg.ChannelBuffer)
	}
	// BatchTimeout should be very short for low-latency ingestion.
	if cfg.BatchTimeout > 100*time.Millisecond {
		t.Errorf("expected BatchTimeout <= 100ms, got %v", cfg.BatchTimeout)
	}
}
