// cmd/main.go — Search Service entrypoint.
package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.uber.org/zap"

	"logflow/search-service/internal/handler"
)

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync() //nolint:errcheck

	port := getEnv("PORT", "8082")
	metricsPort := getEnv("METRICS_PORT", "9092")

	// ── ClickHouse connection ───────────────────────────────────────────────
	chHosts := splitEnv("CLICKHOUSE_HOSTS", "localhost:9000")
	chConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: chHosts,
		Auth: clickhouse.Auth{
			Database: getEnv("CLICKHOUSE_DB", "logflow"),
			Username: getEnv("CLICKHOUSE_USER", "default"),
			Password: getEnv("CLICKHOUSE_PASSWORD", ""),
		},
		MaxOpenConns:    20,
		MaxIdleConns:    10,
		ConnMaxLifetime: time.Hour,
		Compression:     &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		log.Fatal("clickhouse connect failed", zap.Error(err))
	}
	if err := chConn.Ping(context.Background()); err != nil {
		log.Fatal("clickhouse ping failed", zap.Error(err))
	}

	// ── Redis client ────────────────────────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{
		Addr:         getEnv("REDIS_ADDR", "localhost:6379"),
		Password:     getEnv("REDIS_PASSWORD", ""),
		DB:           0,
		PoolSize:     50,
		MinIdleConns: 10,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	// ── HTTP server ─────────────────────────────────────────────────────────
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery(), otelgin.Middleware("search-service"))

	searchHandler := handler.NewSearchHandler(chConn, rdb, log)

	v1 := engine.Group("/api/v1")
	v1.GET("/logs/search", searchHandler.Search)
	v1.DELETE("/admin/logs", searchHandler.DeleteByTenant)
	engine.GET("/health", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	engine.GET("/ready", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ready"}) })

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:              ":" + metricsPort,
		Handler:           metricsMux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           engine,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		log.Info("search-service starting", zap.String("port", port))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server error", zap.Error(err))
		}
	}()
	go func() { metricsServer.ListenAndServe() }() //nolint:errcheck

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutting down search-service")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	server.Shutdown(ctx) //nolint:errcheck
	if err := chConn.Close(); err != nil {
		log.Error("clickhouse close error", zap.Error(err))
	}
	if err := rdb.Close(); err != nil {
		log.Error("redis close error", zap.Error(err))
	}
	log.Info("search-service stopped")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitEnv(key, fallback string) []string {
	v := getEnv(key, fallback)
	out := []string{}
	for _, s := range splitStr(v, ",") {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func splitStr(s, sep string) []string {
	var result []string
	start := 0
	for i := 0; i < len(s); i++ {
		if string(s[i]) == sep {
			result = append(result, s[start:i])
			start = i + 1
		}
	}
	result = append(result, s[start:])
	return result
}
