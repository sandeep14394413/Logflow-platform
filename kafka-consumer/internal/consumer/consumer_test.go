package consumer

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"logflow/kafka-consumer/internal/writer"
)

// mockWriter records rows written.
type mockWriter struct {
	rows    []writer.LogRow
	callCnt int
}

func (m *mockWriter) WriteBatch(_ context.Context, rows []writer.LogRow) error {
	m.callCnt++
	m.rows = append(m.rows, rows...)
	return nil
}

func newTestConsumer(mw writer.ClickHouseWriter) *LogConsumer {
	log, _ := zap.NewDevelopment()
	cfg := Config{
		Brokers:    []string{"localhost:9092"},
		Topic:      "test",
		GroupID:    "g",
		DLQTopic:   "dlq",
		MaxRetries: 3,
	}
	return NewForTesting(cfg, mw, log)
}

func makeKafkaMsg(t *testing.T, row writer.LogRow) kafka.Message {
	t.Helper()
	b, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("failed to marshal LogRow: %v", err)
	}
	return kafka.Message{Key: []byte(row.TenantID), Value: b}
}

func TestDeserialise_Valid(t *testing.T) {
	c := newTestConsumer(&mockWriter{})
	rows := []writer.LogRow{
		{TenantID: "t1", Service: "svc", Message: "hello"},
		{TenantID: "t1", Service: "svc", Message: "world"},
	}
	msgs := []kafka.Message{makeKafkaMsg(t, rows[0]), makeKafkaMsg(t, rows[1])}

	valid, bad := c.deserialise(msgs)
	if len(valid) != 2 {
		t.Errorf("expected 2 valid rows, got %d", len(valid))
	}
	if len(bad) != 0 {
		t.Errorf("expected 0 bad messages, got %d", len(bad))
	}
}

func TestDeserialise_InvalidJSON(t *testing.T) {
	c := newTestConsumer(&mockWriter{})
	msgs := []kafka.Message{
		{Key: []byte("k"), Value: []byte("not-json")},
		makeKafkaMsg(t, writer.LogRow{TenantID: "t", Message: "ok"}),
	}

	valid, bad := c.deserialise(msgs)
	if len(valid) != 1 {
		t.Errorf("expected 1 valid row, got %d", len(valid))
	}
	if len(bad) != 1 {
		t.Errorf("expected 1 bad message, got %d", len(bad))
	}
}

func TestDeserialise_Empty(t *testing.T) {
	c := newTestConsumer(&mockWriter{})
	valid, bad := c.deserialise(nil)
	if len(valid) != 0 || len(bad) != 0 {
		t.Error("expected empty result for nil input")
	}
}

func TestDeserialise_AllInvalid(t *testing.T) {
	c := newTestConsumer(&mockWriter{})
	msgs := []kafka.Message{
		{Value: []byte("{invalid}")},
		{Value: []byte("null")},
	}
	valid, bad := c.deserialise(msgs)
	if len(valid) != 0 {
		t.Errorf("expected 0 valid rows, got %d", len(valid))
	}
	if len(bad) != 2 {
		t.Errorf("expected 2 bad messages, got %d", len(bad))
	}
}

func TestWriteWithRetry_Success(t *testing.T) {
	mw := &mockWriter{}
	c := newTestConsumer(mw)

	rows := []writer.LogRow{{TenantID: "t", Message: "ok"}}
	err := c.writeWithRetry(context.Background(), rows, 3)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if mw.callCnt != 1 {
		t.Errorf("expected 1 write call, got %d", mw.callCnt)
	}
}

func TestWriteWithRetry_ContextCancelled(t *testing.T) {
	c := newTestConsumer(&mockWriter{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rows := []writer.LogRow{{TenantID: "t", Message: "ctx-cancelled"}}
	// Must not hang — context already cancelled.
	_ = c.writeWithRetry(ctx, rows, 5)
}
