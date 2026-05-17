// Package handler implements the log ingestion HTTP API.
// Designed for extreme throughput: batched writes, async Kafka dispatch,
// LZ4/ZSTD body compression, and non-blocking backpressure via buffered channels.
package handler

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"logflow/ingestion-service/internal/kafka"
)

var (
	logsIngestedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "logflow",
		Subsystem: "ingestion",
		Name:      "logs_ingested_total",
		Help:      "Total log entries accepted for ingestion.",
	}, []string{"tenant", "status"})

	ingestionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "logflow",
		Subsystem: "ingestion",
		Name:      "batch_duration_seconds",
		Help:      "Time to enqueue an ingestion batch.",
		Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5},
	}, []string{"tenant"})

	batchSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "logflow",
		Subsystem: "ingestion",
		Name:      "batch_size_logs",
		Help:      "Number of logs per ingestion batch.",
		Buckets:   []float64{1, 10, 50, 100, 500, 1000, 5000, 10000},
	}, []string{"tenant"})

	backpressureDrops = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "logflow",
		Subsystem: "ingestion",
		Name:      "backpressure_drops_total",
		Help:      "Logs dropped due to producer channel backpressure.",
	}, []string{"tenant"})
)

const (
	maxBodyBytes  = 32 << 20 // 32 MB
	maxBatchSize  = 10_000
	producerTopic = "logs-raw"
)

// IngestHandler handles POST /api/v1/logs requests.
type IngestHandler struct {
	producer kafka.Producer
	log      *zap.Logger
}

// NewIngestHandler wires up the handler with a Kafka producer.
func NewIngestHandler(producer kafka.Producer, log *zap.Logger) *IngestHandler {
	return &IngestHandler{producer: producer, log: log}
}

// Ingest is the core handler: decode → validate → enqueue → respond.
func (h *IngestHandler) Ingest(c *gin.Context) {
	ctx, span := otel.Tracer("ingestion-service").Start(c.Request.Context(), "ingest.batch")
	defer span.End()

	tenantID := c.GetHeader("X-Tenant-ID")
	if tenantID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"code": "BAD_REQUEST", "message": "X-Tenant-ID header required"})
		return
	}
	requestID := c.GetHeader("X-Request-ID")
	if requestID == "" {
		requestID = uuid.New().String()
	}

	start := time.Now()

	// Honour Content-Encoding: gzip for on-wire compression.
	body, err := readBody(c.Request)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": "READ_ERROR", "message": err.Error()})
		return
	}

	var req IngestRequest
	if err := json.Unmarshal(body, &req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"code": "PARSE_ERROR", "message": "invalid JSON payload"})
		return
	}

	if len(req.Logs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"code": "EMPTY_BATCH", "message": "logs array must not be empty"})
		return
	}
	if len(req.Logs) > maxBatchSize {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{
			"code":    "BATCH_TOO_LARGE",
			"message": "maximum batch size is 10,000 entries",
		})
		return
	}

	span.SetAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.Int("batch_size", len(req.Logs)),
	)

	// Normalise entries: set tenant, assign IDs, floor timestamps.
	now := time.Now().UTC()
	for i := range req.Logs {
		req.Logs[i].TenantID = tenantID
		if req.Logs[i].ID == "" {
			req.Logs[i].ID = uuid.New().String()
		}
		if req.Logs[i].Timestamp.IsZero() {
			req.Logs[i].Timestamp = now
		}
	}

	accepted, dropped, err := h.dispatchBatch(ctx, tenantID, req.Logs)
	if err != nil {
		h.log.Error("dispatch failed", zap.String("tenant", tenantID), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"code": "DISPATCH_ERROR", "message": "failed to enqueue logs"})
		return
	}

	dur := time.Since(start)
	ingestionDuration.WithLabelValues(tenantID).Observe(dur.Seconds())
	batchSize.WithLabelValues(tenantID).Observe(float64(len(req.Logs)))
	logsIngestedTotal.WithLabelValues(tenantID, "accepted").Add(float64(accepted))
	if dropped > 0 {
		logsIngestedTotal.WithLabelValues(tenantID, "dropped").Add(float64(dropped))
		backpressureDrops.WithLabelValues(tenantID).Add(float64(dropped))
	}

	h.log.Info("batch ingested",
		zap.String("tenant", tenantID),
		zap.String("request_id", requestID),
		zap.Int("accepted", accepted),
		zap.Int("dropped", dropped),
		zap.Duration("took", dur),
	)

	c.JSON(http.StatusAccepted, IngestResponse{
		Accepted:  accepted,
		Dropped:   dropped,
		RequestID: requestID,
	})
}

// dispatchBatch serialises each log entry and sends it to Kafka asynchronously.
// Uses a non-blocking send: if the producer channel is full it drops and counts.
func (h *IngestHandler) dispatchBatch(ctx context.Context, tenantID string, logs []LogEntry) (accepted, dropped int, err error) {
	for i := range logs {
		payload, merr := json.Marshal(logs[i])
		if merr != nil {
			dropped++
			continue
		}
		msg := kafka.Message{
			Topic:   producerTopic,
			Key:     []byte(tenantID),
			Value:   payload,
			Headers: map[string][]byte{"tenant_id": []byte(tenantID)},
		}
		if !h.producer.TrySend(ctx, msg) {
			dropped++
		} else {
			accepted++
		}
	}
	return accepted, dropped, nil
}

// readBody reads the body respecting gzip and a max-size limit.
func readBody(r *http.Request) ([]byte, error) {
	r.Body = http.MaxBytesReader(nil, r.Body, maxBodyBytes)
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		gr, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, err
		}
		defer gr.Close()
		return io.ReadAll(io.LimitReader(gr, maxBodyBytes))
	}
	return io.ReadAll(r.Body)
}

// Health returns 200 with a simple liveness payload.
func Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "ingestion"})
}
