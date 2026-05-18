package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"logflow/api-gateway/internal/config"
)

func setupRateLimitRouter(rps, burst int, perTenant bool) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	cfg := config.RateLimitConfig{
		RequestsPerSecond: rps,
		BurstSize:         burst,
		PerTenant:         perTenant,
	}
	r.Use(RateLimiter(cfg))
	r.GET("/ping", func(c *gin.Context) { c.Status(http.StatusOK) })
	return r
}

func TestRateLimiter_AllowsUnderLimit(t *testing.T) {
	r := setupRateLimitRouter(100, 10, false)
	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/ping", nil)
		req.RemoteAddr = "127.0.0.1:1234"
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("request %d: expected 200, got %d", i+1, w.Code)
		}
	}
}

func TestRateLimiter_BlocksOverLimit(t *testing.T) {
	// 1 RPS, burst 1 — second request must be blocked.
	r := setupRateLimitRouter(1, 1, false)
	pass := 0
	blocked := 0
	for i := 0; i < 10; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/ping", nil)
		req.RemoteAddr = "10.0.0.1:9999"
		r.ServeHTTP(w, req)
		if w.Code == http.StatusOK {
			pass++
		} else if w.Code == http.StatusTooManyRequests {
			blocked++
		}
	}
	if pass == 0 {
		t.Error("expected at least one request to pass")
	}
	if blocked == 0 {
		t.Error("expected at least one request to be rate-limited")
	}
}

func TestRateLimiter_PerTenantIsolation(t *testing.T) {
	// Burst=1: tenant-A should not consume tenant-B's quota.
	r := setupRateLimitRouter(1, 1, true)
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/ping", nil)
		req.Header.Set("X-Tenant-ID", tenant)
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("first request for %s should pass, got %d", tenant, w.Code)
		}
	}
}

func TestRateLimiter_RetryAfterHeader(t *testing.T) {
	r := setupRateLimitRouter(1, 1, false)
	// Exhaust burst.
	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/ping", nil)
		req.RemoteAddr = "192.168.1.1:5555"
		r.ServeHTTP(w, req)
		if w.Code == http.StatusTooManyRequests {
			if w.Header().Get("Retry-After") == "" {
				t.Error("expected Retry-After header on 429 response")
			}
			return
		}
	}
}
