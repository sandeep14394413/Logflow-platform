package handler

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"logflow/ingestion-service/internal/kafka"
)

// mockProducer is a test double for kafka.Producer.
type mockProducer struct {
	sent    []kafka.Message
	dropAll bool
}

func (m *mockProducer) TrySend(_ context.Context, msg kafka.Message) bool {
	if m.dropAll {
		return false
	}
	m.sent = append(m.sent, msg)
	return true
}

func (m *mockProducer) Close() error { return nil }

func newTestRouter(p kafka.Producer) *gin.Engine {
	gin.SetMode(gin.TestMode)
	log, _ := zap.NewDevelopment()
	h := NewIngestHandler(p, log)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Request.Header.Set("X-Tenant-ID", "test-tenant")
		c.Request.Header.Set("X-Request-ID", "req-123")
		c.Next()
	})
	r.POST("/api/v1/logs", h.Ingest)
	r.GET("/health", Health)
	return r
}

func TestIngest_ValidBatch(t *testing.T) {
	mp := &mockProducer{}
	r := newTestRouter(mp)

	body := IngestRequest{
		Logs: []LogEntry{
			{Level: "INFO", Service: "svc", Namespace: "ns", Message: "hello"},
			{Level: "ERROR", Service: "svc", Namespace: "ns", Message: "boom"},
		},
	}
	b, _ := json.Marshal(body)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/logs", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "test-tenant")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	var resp IngestResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Accepted != 2 {
		t.Errorf("expected 2 accepted, got %d", resp.Accepted)
	}
	if len(mp.sent) != 2 {
		t.Errorf("expected 2 kafka messages sent, got %d", len(mp.sent))
	}
}

func TestIngest_GzipBody(t *testing.T) {
	mp := &mockProducer{}
	r := newTestRouter(mp)

	body := IngestRequest{
		Logs: []LogEntry{{Level: "WARN", Service: "svc", Message: "compressed log"}},
	}
	b, _ := json.Marshal(body)

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write(b) //nolint:errcheck
	gz.Close()

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/logs", &buf)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("X-Tenant-ID", "test-tenant")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202 for gzip body, got %d: %s", w.Code, w.Body.String())
	}
}

func TestIngest_EmptyBatch(t *testing.T) {
	mp := &mockProducer{}
	r := newTestRouter(mp)

	body := `{"logs":[]}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/logs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "test-tenant")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for empty batch, got %d", w.Code)
	}
}

func TestIngest_InvalidJSON(t *testing.T) {
	mp := &mockProducer{}
	r := newTestRouter(mp)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/logs", strings.NewReader(`not json`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "test-tenant")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("expected 422, got %d", w.Code)
	}
}

func TestIngest_BackpressureDrop(t *testing.T) {
	mp := &mockProducer{dropAll: true}
	r := newTestRouter(mp)

	body := IngestRequest{
		Logs: []LogEntry{
			{Level: "INFO", Service: "svc", Message: "msg1"},
			{Level: "INFO", Service: "svc", Message: "msg2"},
		},
	}
	b, _ := json.Marshal(body)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/logs", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "test-tenant")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202 even with drops, got %d", w.Code)
	}
	var resp IngestResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Dropped != 2 {
		t.Errorf("expected 2 dropped, got %d", resp.Dropped)
	}
}

func TestIngest_TimestampDefaulted(t *testing.T) {
	mp := &mockProducer{}
	r := newTestRouter(mp)

	body := IngestRequest{
		Logs: []LogEntry{{Level: "INFO", Service: "svc", Message: "no ts"}},
	}
	b, _ := json.Marshal(body)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/logs", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "test-tenant")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}

	// Verify Kafka message has tenant set.
	if len(mp.sent) != 1 {
		t.Fatalf("expected 1 message, got %d", len(mp.sent))
	}
	var entry LogEntry
	json.Unmarshal(mp.sent[0].Value, &entry) //nolint:errcheck
	if entry.TenantID != "test-tenant" {
		t.Errorf("expected TenantID='test-tenant', got %q", entry.TenantID)
	}
	if entry.Timestamp.IsZero() {
		t.Error("expected Timestamp to be defaulted, got zero")
	}
}

func TestIngest_IDAssigned(t *testing.T) {
	mp := &mockProducer{}
	r := newTestRouter(mp)

	body := IngestRequest{
		Logs: []LogEntry{{Level: "INFO", Service: "svc", Message: "no id"}},
	}
	b, _ := json.Marshal(body)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/logs", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "test-tenant")
	r.ServeHTTP(w, req)

	var entry LogEntry
	json.Unmarshal(mp.sent[0].Value, &entry) //nolint:errcheck
	if entry.ID == "" {
		t.Error("expected ID to be auto-assigned")
	}
}

func TestHealth(t *testing.T) {
	mp := &mockProducer{}
	r := newTestRouter(mp)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

// Ensure LogEntry zero value is safe.
func TestLogEntry_ZeroValue(t *testing.T) {
	e := LogEntry{}
	if !e.Timestamp.IsZero() {
		t.Error("zero LogEntry should have zero Timestamp")
	}
	_ = time.Now() // keep time import used
}
