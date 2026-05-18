package kafka

import (
	"context"
	"testing"
	"time"
)

func TestAsyncProducer_TrySend_Closed(t *testing.T) {
	cfg := Config{
		Brokers:       []string{"localhost:9092"},
		Topic:         "test",
		BatchSize:     10,
		BatchTimeout:  10 * time.Millisecond,
		ChannelBuffer: 100,
		WriteTimeout:  time.Second,
		MaxAttempts:   1,
	}
	// We don't start an actual Kafka connection in unit tests —
	// we test the producer interface and channel behaviour in isolation.
	p := &AsyncProducer{
		ch:  make(chan Message, cfg.ChannelBuffer),
		cfg: cfg,
	}
	p.closed.Store(true)

	sent := p.TrySend(context.Background(), Message{
		Topic: "test",
		Key:   []byte("k"),
		Value: []byte("v"),
	})
	if sent {
		t.Error("expected TrySend to return false when producer is closed")
	}
}

func TestAsyncProducer_TrySend_BufferFull(t *testing.T) {
	p := &AsyncProducer{
		ch:  make(chan Message, 1), // buffer of 1
		cfg: Config{Topic: "test"},
	}

	// Fill the buffer.
	p.ch <- Message{}

	// Next send should be dropped.
	sent := p.TrySend(context.Background(), Message{
		Topic: "test",
		Value: []byte("overflow"),
	})
	if sent {
		t.Error("expected TrySend to return false when buffer is full")
	}
}

func TestAsyncProducer_TrySend_Success(t *testing.T) {
	p := &AsyncProducer{
		ch:  make(chan Message, 10),
		cfg: Config{Topic: "test"},
	}

	sent := p.TrySend(context.Background(), Message{
		Topic: "test",
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	if !sent {
		t.Error("expected TrySend to return true with available buffer")
	}
	if len(p.ch) != 1 {
		t.Errorf("expected 1 message in channel, got %d", len(p.ch))
	}
}

func TestDefaultConfig(t *testing.T) {
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
}
