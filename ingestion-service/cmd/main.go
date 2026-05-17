// cmd/main.go — Ingestion Service entrypoint.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.uber.org/zap"

	"logflow/ingestion-service/internal/handler"
	"logflow/ingestion-service/internal/kafka"
)

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync() //nolint:errcheck

	port := getEnv("PORT", "8081")
	metricsPort := getEnv("METRICS_PORT", "9091")
	kafkaBrokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	kafkaTopic := getEnv("KAFKA_TOPIC", "logs-raw")
	batchSize := getEnvInt("KAFKA_BATCH_SIZE", 5000)
	batchTimeout := time.Duration(getEnvInt("KAFKA_BATCH_TIMEOUT_MS", 10)) * time.Millisecond
	channelBuffer := getEnvInt("PRODUCER_CHANNEL_BUFFER", 100_000)

	// ── Kafka producer ──────────────────────────────────────────────────────
	producerCfg := kafka.Config{
		Brokers:       kafkaBrokers,
		Topic:         kafkaTopic,
		BatchSize:     batchSize,
		BatchTimeout:  batchTimeout,
		ChannelBuffer: channelBuffer,
		WriteTimeout:  10 * time.Second,
		MaxAttempts:   5,
	}
	producer, err := kafka.NewAsyncProducer(producerCfg, log)
	if err != nil {
		log.Fatal("failed to create kafka producer", zap.Error(err))
	}

	// ── HTTP server ─────────────────────────────────────────────────────────
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery(), otelgin.Middleware("ingestion-service"))

	ingestHandler := handler.NewIngestHandler(producer, log)

	v1 := engine.Group("/api/v1")
	v1.POST("/logs", ingestHandler.Ingest)
	v1.POST("/logs/batch", ingestHandler.Ingest)
	engine.GET("/health", handler.Health)
	engine.GET("/ready", handler.Health)

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{Addr: ":" + metricsPort, Handler: metricsMux}

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           engine,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		log.Info("ingestion-service starting", zap.String("port", port))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server error", zap.Error(err))
		}
	}()
	go func() { metricsServer.ListenAndServe() }() //nolint:errcheck

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutting down ingestion-service")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	server.Shutdown(ctx)   //nolint:errcheck
	producer.Close()        //nolint:errcheck
	log.Info("ingestion-service stopped")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

var _ = fmt.Sprintf // keep fmt imported
