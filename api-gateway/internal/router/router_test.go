package router

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"logflow/api-gateway/internal/config"
)

func TestRegister_HealthEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{
		IngestionURL:  "http://localhost:8081",
		SearchURL:     "http://localhost:8082",
		WebsocketURL:  "http://localhost:8083",
		AuthURL:       "http://localhost:8084",
	}
	log, _ := zap.NewDevelopment()
	engine := gin.New()
	Register(engine, cfg, log)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for /health, got %d", w.Code)
	}
}

func TestRegister_ReadyEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{
		IngestionURL: "http://localhost:8081",
		SearchURL:    "http://localhost:8082",
		WebsocketURL: "http://localhost:8083",
		AuthURL:      "http://localhost:8084",
	}
	log, _ := zap.NewDevelopment()
	engine := gin.New()
	Register(engine, cfg, log)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for /ready, got %d", w.Code)
	}
}
