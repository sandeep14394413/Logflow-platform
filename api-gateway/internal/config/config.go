// Package config loads and validates the API Gateway configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration for the API Gateway.
type Config struct {
	Env            string
	Port           int
	MetricsPort    int
	TLSEnabled     bool
	TLSCert        string
	TLSKey         string
	JWTSecret      string
	AllowedOrigins []string

	// Upstream service URLs
	IngestionURL string
	SearchURL    string
	WebsocketURL string
	AuthURL      string

	// Rate limiting
	RateLimit RateLimitConfig

	// Timeouts
	UpstreamTimeout time.Duration
}

// RateLimitConfig configures the token-bucket rate limiter.
type RateLimitConfig struct {
	RequestsPerSecond int
	BurstSize         int
	PerTenant         bool
}

// Load reads configuration from environment variables, applies defaults, and validates.
func Load() (*Config, error) {
	cfg := &Config{
		Env:         getEnv("ENV", "development"),
		Port:        getEnvInt("PORT", 8080),
		MetricsPort: getEnvInt("METRICS_PORT", 9090),
		TLSEnabled:  getEnvBool("TLS_ENABLED", false),
		TLSCert:     getEnv("TLS_CERT_PATH", "/etc/tls/tls.crt"),
		TLSKey:      getEnv("TLS_KEY_PATH", "/etc/tls/tls.key"),
		JWTSecret:   getEnv("JWT_SECRET", ""),
		AllowedOrigins: strings.Split(
			getEnv("ALLOWED_ORIGINS", "http://localhost:3000"), ",",
		),
		IngestionURL: getEnv("INGESTION_SERVICE_URL", "http://ingestion-service:8081"),
		SearchURL:    getEnv("SEARCH_SERVICE_URL", "http://search-service:8082"),
		WebsocketURL: getEnv("WEBSOCKET_SERVICE_URL", "http://websocket-service:8083"),
		AuthURL:      getEnv("AUTH_SERVICE_URL", "http://auth-service:8084"),
		UpstreamTimeout: time.Duration(getEnvInt("UPSTREAM_TIMEOUT_MS", 5000)) * time.Millisecond,
		RateLimit: RateLimitConfig{
			RequestsPerSecond: getEnvInt("RATE_LIMIT_RPS", 10000),
			BurstSize:         getEnvInt("RATE_LIMIT_BURST", 20000),
			PerTenant:         getEnvBool("RATE_LIMIT_PER_TENANT", true),
		},
	}

	if cfg.JWTSecret == "" {
		return nil, fmt.Errorf("JWT_SECRET must be set")
	}
	if cfg.Port < 1 || cfg.Port > 65535 {
		return nil, fmt.Errorf("PORT %d is out of range", cfg.Port)
	}
	return cfg, nil
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

func getEnvBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}
