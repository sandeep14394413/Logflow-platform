// cmd/main.go — WebSocket Service entrypoint.
// Runs a Kafka consumer that fans out log events to WebSocket subscribers in real time.
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"logflow/websocket-service/internal/hub"
)

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync() //nolint:errcheck

	port := getEnv("PORT", "8083")
	metricsPort := getEnv("METRICS_PORT", "9093")
	kafkaBrokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	kafkaTopic := getEnv("KAFKA_TOPIC", "logs-raw")
	groupID := getEnv("KAFKA_GROUP_ID", "logflow-ws-consumer")

	streamHub := hub.New(log)

	// ── Kafka reader — forwards messages to the WebSocket hub ──────────────
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        kafkaBrokers,
		Topic:          kafkaTopic,
		GroupID:        groupID,
		MinBytes:       1e3,
		MaxBytes:       1e6,
		CommitInterval: time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for {
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Error("ws kafka fetch error", zap.Error(err))
				continue
			}
			// Extract tenant from Kafka header for routing.
			tenantID := ""
			for _, h := range msg.Headers {
				if h.Key == "tenant_id" {
					tenantID = string(h.Value)
				}
			}
			if tenantID == "" {
				// Try JSON body.
				var m map[string]interface{}
				if json.Unmarshal(msg.Value, &m) == nil {
					tenantID, _ = m["tenant_id"].(string)
				}
			}
			if tenantID != "" {
				streamHub.Broadcast(tenantID, msg.Value)
			}
			reader.CommitMessages(ctx, msg) //nolint:errcheck
		}
	}()

	// ── HTTP / WebSocket server ─────────────────────────────────────────────
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())

	engine.GET("/api/v1/stream/tail", streamHub.Handle)
	engine.GET("/health", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })

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
		WriteTimeout:      0, // Disable for WebSocket long-lived connections.
		IdleTimeout:       0,
	}

	go func() {
		log.Info("websocket-service starting", zap.String("port", port))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server error", zap.Error(err))
		}
	}()
	go func() { metricsServer.ListenAndServe() }() //nolint:errcheck

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	cancel()
	log.Info("shutting down websocket-service")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutCancel()
	server.Shutdown(shutCtx) //nolint:errcheck
	reader.Close()            //nolint:errcheck
	log.Info("websocket-service stopped")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
