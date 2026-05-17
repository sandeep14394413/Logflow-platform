// Package middleware — Prometheus instrumentation and request-ID injection.
package middleware

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "logflow",
		Subsystem: "gateway",
		Name:      "http_requests_total",
		Help:      "Total HTTP requests processed by the API gateway.",
	}, []string{"method", "path", "status", "tenant"})

	httpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "logflow",
		Subsystem: "gateway",
		Name:      "http_request_duration_seconds",
		Help:      "HTTP request latency distribution.",
		Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"method", "path", "status"})

	httpRequestSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "logflow",
		Subsystem: "gateway",
		Name:      "http_request_size_bytes",
		Help:      "HTTP request body size distribution.",
		Buckets:   prometheus.ExponentialBuckets(100, 10, 7),
	}, []string{"method", "path"})
)

// Metrics returns a Gin middleware that records Prometheus counters and histograms.
func Metrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.FullPath()
		if path == "" {
			path = "unknown"
		}
		size := c.Request.ContentLength

		c.Next()

		status := strconv.Itoa(c.Writer.Status())
		tenant, _ := c.Get(claimsTenantKey)
		tenantStr, _ := tenant.(string)
		if tenantStr == "" {
			tenantStr = "anon"
		}

		dur := time.Since(start).Seconds()
		httpRequestsTotal.WithLabelValues(c.Request.Method, path, status, tenantStr).Inc()
		httpRequestDuration.WithLabelValues(c.Request.Method, path, status).Observe(dur)
		if size > 0 {
			httpRequestSize.WithLabelValues(c.Request.Method, path).Observe(float64(size))
		}
	}
}

// RequestID injects a unique X-Request-ID header for distributed tracing correlation.
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		rid := c.GetHeader("X-Request-ID")
		if rid == "" {
			rid = uuid.New().String()
		}
		c.Set("request_id", rid)
		c.Header("X-Request-ID", rid)
		c.Next()
	}
}

// StructuredLogger logs every request with structured fields.
func StructuredLogger(log *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		rid, _ := c.Get("request_id")
		log.Info("request",
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("latency", time.Since(start)),
			zap.String("client_ip", c.ClientIP()),
			zap.String("request_id", rid.(string)),
			zap.Int64("bytes_in", c.Request.ContentLength),
			zap.Int("bytes_out", c.Writer.Size()),
		)
	}
}

// CORS sets permissive (but configurable) cross-origin headers.
func CORS(origins []string) gin.HandlerFunc {
	allowed := make(map[string]bool, len(origins))
	for _, o := range origins {
		allowed[o] = true
	}
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if allowed[origin] || allowed["*"] {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Request-ID")
			c.Header("Access-Control-Expose-Headers", "X-Request-ID")
			c.Header("Access-Control-Max-Age", "86400")
		}
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	}
}
