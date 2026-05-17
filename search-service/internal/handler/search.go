// Package handler implements the log search API backed by ClickHouse + Redis.
// Query results are cached with tenant-scoped keys and LRU eviction.
package handler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

var (
	searchQueryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "logflow",
		Subsystem: "search",
		Name:      "query_duration_seconds",
		Help:      "Search query execution time distribution.",
		Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"tenant", "cached"})

	searchCacheHits = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "logflow",
		Subsystem: "search",
		Name:      "cache_hits_total",
		Help:      "Redis cache hit/miss counters.",
	}, []string{"outcome"})

	searchResultsReturned = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "logflow",
		Subsystem: "search",
		Name:      "results_returned",
		Help:      "Number of log entries returned per search.",
		Buckets:   []float64{0, 10, 50, 100, 500, 1000, 5000},
	}, []string{"tenant"})
)

const (
	defaultPageSize = 100
	maxPageSize     = 1000
	cacheDefaultTTL = 30 * time.Second
	cacheLongTTL    = 5 * time.Minute // used for queries ending >1h ago
)

// SearchHandler serves GET /api/v1/logs/search.
type SearchHandler struct {
	ch    driver.Conn
	redis *redis.Client
	log   *zap.Logger
}

// NewSearchHandler wires the handler to ClickHouse and Redis.
func NewSearchHandler(ch driver.Conn, rdb *redis.Client, log *zap.Logger) *SearchHandler {
	return &SearchHandler{ch: ch, redis: rdb, log: log}
}

// SearchRequest is parsed from query parameters.
type SearchRequest struct {
	Query     string    `form:"q"`
	Regex     string    `form:"regex"`
	Level     string    `form:"level"`
	Service   string    `form:"service"`
	Namespace string    `form:"namespace"`
	PodName   string    `form:"pod_name"`
	TraceID   string    `form:"trace_id"`
	StartTime time.Time `form:"start_time" time_format:"2006-01-02T15:04:05Z07:00"`
	EndTime   time.Time `form:"end_time"   time_format:"2006-01-02T15:04:05Z07:00"`
	Page      int       `form:"page"`
	PageSize  int       `form:"page_size"`
	OrderDir  string    `form:"order_dir"` // asc | desc
}

// LogEntry mirrors the ClickHouse row for JSON serialisation.
type LogEntry struct {
	ID          string            `json:"id"`
	TenantID    string            `json:"tenant_id"`
	Timestamp   time.Time         `json:"timestamp"`
	Level       string            `json:"level"`
	Service     string            `json:"service"`
	Namespace   string            `json:"namespace"`
	PodName     string            `json:"pod_name"`
	TraceID     string            `json:"trace_id"`
	Message     string            `json:"message"`
	Labels      map[string]string `json:"labels"`
	Attributes  map[string]string `json:"attributes"`
}

// Search is the main query handler.
func (h *SearchHandler) Search(c *gin.Context) {
	ctx, span := otel.Tracer("search-service").Start(c.Request.Context(), "search.query")
	defer span.End()

	tenantID := c.GetHeader("X-Tenant-ID")
	if tenantID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"code": "BAD_REQUEST", "message": "X-Tenant-ID required"})
		return
	}

	var req SearchRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": "INVALID_PARAMS", "message": err.Error()})
		return
	}

	// Validate time range.
	if req.StartTime.IsZero() || req.EndTime.IsZero() {
		c.JSON(http.StatusBadRequest, gin.H{"code": "MISSING_TIME_RANGE", "message": "start_time and end_time are required"})
		return
	}
	if req.EndTime.Before(req.StartTime) {
		c.JSON(http.StatusBadRequest, gin.H{"code": "INVALID_TIME_RANGE", "message": "end_time must be after start_time"})
		return
	}
	if req.EndTime.Sub(req.StartTime) > 7*24*time.Hour {
		c.JSON(http.StatusBadRequest, gin.H{"code": "RANGE_TOO_LARGE", "message": "max query range is 7 days"})
		return
	}

	// Validate regex if provided.
	if req.Regex != "" {
		if _, err := regexp.Compile(req.Regex); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"code": "INVALID_REGEX", "message": "invalid regex: " + err.Error()})
			return
		}
	}

	// Apply defaults and bounds.
	if req.Page < 1 {
		req.Page = 1
	}
	if req.PageSize < 1 {
		req.PageSize = defaultPageSize
	}
	if req.PageSize > maxPageSize {
		req.PageSize = maxPageSize
	}
	if req.OrderDir != "asc" {
		req.OrderDir = "desc"
	}

	start := time.Now()
	cacheKey := buildCacheKey(tenantID, req)
	cacheTTL := cacheDefaultTTL
	if time.Since(req.EndTime) > time.Hour {
		cacheTTL = cacheLongTTL // Historical queries can be cached longer.
	}

	// ── Redis cache lookup ──────────────────────────────────────────────────
	if cached, err := h.redis.Get(ctx, cacheKey).Bytes(); err == nil {
		var resp SearchResponse
		if json.Unmarshal(cached, &resp) == nil {
			resp.Cached = true
			resp.Took = time.Since(start).String()
			searchCacheHits.WithLabelValues("hit").Inc()
			searchQueryDuration.WithLabelValues(tenantID, "true").Observe(time.Since(start).Seconds())
			c.JSON(http.StatusOK, resp)
			return
		}
	}
	searchCacheHits.WithLabelValues("miss").Inc()

	// ── Build and execute ClickHouse query ──────────────────────────────────
	query, countQuery, args := buildQuery(tenantID, req)

	var total uint64
	if err := h.ch.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		h.log.Error("clickhouse count error", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"code": "QUERY_ERROR", "message": "count query failed"})
		return
	}

	rows, err := h.ch.Query(ctx, query, args...)
	if err != nil {
		h.log.Error("clickhouse search error", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"code": "QUERY_ERROR", "message": "search query failed"})
		return
	}
	defer rows.Close()

	logs := make([]LogEntry, 0, req.PageSize)
	for rows.Next() {
		var e LogEntry
		if err := rows.Scan(
			&e.ID, &e.TenantID, &e.Timestamp, &e.Level, &e.Service,
			&e.Namespace, &e.PodName, &e.TraceID, &e.Message, &e.Labels, &e.Attributes,
		); err != nil {
			h.log.Warn("row scan error", zap.Error(err))
			continue
		}
		logs = append(logs, e)
	}
	if err := rows.Err(); err != nil {
		h.log.Error("rows iteration error", zap.Error(err))
	}

	pageCount := int((total + uint64(req.PageSize) - 1) / uint64(req.PageSize))
	resp := SearchResponse{
		Logs:      logs,
		Total:     total,
		Page:      req.Page,
		PageSize:  req.PageSize,
		PageCount: pageCount,
		Took:      time.Since(start).String(),
		Cached:    false,
	}

	searchResultsReturned.WithLabelValues(tenantID).Observe(float64(len(logs)))
	searchQueryDuration.WithLabelValues(tenantID, "false").Observe(time.Since(start).Seconds())

	h.log.Info("search completed",
		zap.String("tenant", tenantID),
		zap.Uint64("total", total),
		zap.Int("returned", len(logs)),
		zap.Duration("took", time.Since(start)),
	)

	// ── Cache the result ────────────────────────────────────────────────────
	if b, err := json.Marshal(resp); err == nil {
		h.redis.Set(ctx, cacheKey, b, cacheTTL)
	}

	c.JSON(http.StatusOK, resp)
}

// SearchResponse wraps paginated results.
type SearchResponse struct {
	Logs      []LogEntry `json:"logs"`
	Total     uint64     `json:"total"`
	Page      int        `json:"page"`
	PageSize  int        `json:"page_size"`
	PageCount int        `json:"page_count"`
	Took      string     `json:"took"`
	Cached    bool       `json:"cached"`
}

// buildQuery generates the parameterised ClickHouse SQL from the SearchRequest.
// Uses ClickHouse-native full-text (hasToken) and regex functions for maximum speed.
func buildQuery(tenantID string, req SearchRequest) (query, countQuery string, args []interface{}) {
	where := []string{"tenant_id = ?", "timestamp >= ? AND timestamp < ?"}
	args = []interface{}{tenantID, req.StartTime.UTC(), req.EndTime.UTC()}

	addFilter := func(col, val string) {
		if val != "" {
			where = append(where, col+" = ?")
			args = append(args, val)
		}
	}
	addFilter("level", req.Level)
	addFilter("service", req.Service)
	addFilter("namespace", req.Namespace)
	addFilter("pod_name", req.PodName)
	addFilter("trace_id", req.TraceID)

	// Full-text search via ClickHouse tokenbf index.
	if req.Query != "" {
		for _, token := range strings.Fields(req.Query) {
			where = append(where, "hasToken(message, ?)")
			args = append(args, token)
		}
	}
	// Regex match (re2 syntax supported natively).
	if req.Regex != "" {
		where = append(where, "match(message, ?)")
		args = append(args, req.Regex)
	}

	whereClause := strings.Join(where, " AND ")
	orderDir := "DESC"
	if req.OrderDir == "asc" {
		orderDir = "ASC"
	}
	offset := (req.Page - 1) * req.PageSize

	countQuery = fmt.Sprintf(`
		SELECT count()
		FROM logflow.logs_distributed
		WHERE %s`, whereClause)

	query = fmt.Sprintf(`
		SELECT
			id, tenant_id, timestamp, level, service,
			namespace, pod_name, trace_id, message,
			labels, attributes
		FROM logflow.logs_distributed
		WHERE %s
		ORDER BY timestamp %s
		LIMIT %d
		OFFSET %d
		SETTINGS max_execution_time = 30`, whereClause, orderDir, req.PageSize, offset)

	return query, countQuery, args
}

// buildCacheKey creates a deterministic SHA-256 key scoped to the tenant.
func buildCacheKey(tenantID string, req SearchRequest) string {
	raw := fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s|%d|%d|%s",
		tenantID, req.Query, req.Regex, req.Level, req.Service, req.Namespace,
		req.PodName, req.TraceID,
		req.StartTime.UTC().Format(time.RFC3339),
		req.Page, req.PageSize, req.OrderDir,
	)
	sum := sha256.Sum256([]byte(raw))
	return "search:" + tenantID + ":" + hex.EncodeToString(sum[:])
}
