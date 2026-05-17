// cmd/main.go — API Gateway service entrypoint.
// Handles TLS termination, JWT validation, rate limiting, and request routing.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.uber.org/zap"

	"logflow/api-gateway/internal/config"
	"logflow/api-gateway/internal/middleware"
	"logflow/api-gateway/internal/router"
)

func main() {
	// ── Structured logger ──────────────────────────────────────────────────────
	log, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Sync() //nolint:errcheck

	// ── Config ─────────────────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("failed to load config", zap.Error(err))
	}

	// ── Gin engine ─────────────────────────────────────────────────────────────
	if cfg.Env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}
	engine := gin.New()
	engine.Use(
		gin.Recovery(),
		otelgin.Middleware("api-gateway"),
		middleware.RequestID(),
		middleware.StructuredLogger(log),
		middleware.CORS(cfg.AllowedOrigins),
		middleware.RateLimiter(cfg.RateLimit),
		middleware.JWT(cfg.JWTSecret),
		middleware.Metrics(),
	)

	// ── Route registration ─────────────────────────────────────────────────────
	router.Register(engine, cfg, log)

	// ── Prometheus metrics endpoint (internal port) ────────────────────────────
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.MetricsPort),
		Handler:      metricsMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	// ── Main server ────────────────────────────────────────────────────────────
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           engine,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1 MB
	}

	// ── Start ──────────────────────────────────────────────────────────────────
	go func() {
		log.Info("api-gateway starting", zap.Int("port", cfg.Port))
		if cfg.TLSEnabled {
			if err := server.ListenAndServeTLS(cfg.TLSCert, cfg.TLSKey); err != nil && err != http.ErrServerClosed {
				log.Fatal("server error", zap.Error(err))
			}
		} else {
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatal("server error", zap.Error(err))
			}
		}
	}()

	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server error", zap.Error(err))
		}
	}()

	// ── Graceful shutdown ──────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutting down api-gateway")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Error("graceful shutdown failed", zap.Error(err))
	}
	if err := metricsServer.Shutdown(ctx); err != nil {
		log.Error("metrics server shutdown failed", zap.Error(err))
	}
	log.Info("api-gateway stopped")
}
