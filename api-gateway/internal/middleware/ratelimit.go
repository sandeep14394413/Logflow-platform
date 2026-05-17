// Package middleware — rate limiter using token-bucket algorithm.
// Supports global and per-tenant limiting backed by an in-memory shard map.
package middleware

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"

	"logflow/api-gateway/internal/config"
)

// tenantLimiter holds a rate.Limiter plus the last access time for cleanup.
type tenantLimiter struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// rateLimiterStore maintains per-tenant limiters with TTL-based eviction.
type rateLimiterStore struct {
	mu       sync.RWMutex
	limiters map[string]*tenantLimiter
	rps      rate.Limit
	burst    int
}

func newStore(rps, burst int) *rateLimiterStore {
	s := &rateLimiterStore{
		limiters: make(map[string]*tenantLimiter, 512),
		rps:      rate.Limit(rps),
		burst:    burst,
	}
	go s.cleanup()
	return s
}

// getLimiter retrieves or creates a rate.Limiter for the given key.
func (s *rateLimiterStore) getLimiter(key string) *rate.Limiter {
	s.mu.RLock()
	if tl, ok := s.limiters[key]; ok {
		tl.lastSeen = time.Now()
		s.mu.RUnlock()
		return tl.limiter
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	// Double-check under write lock.
	if tl, ok := s.limiters[key]; ok {
		return tl.limiter
	}
	tl := &tenantLimiter{
		limiter:  rate.NewLimiter(s.rps, s.burst),
		lastSeen: time.Now(),
	}
	s.limiters[key] = tl
	return tl.limiter
}

// cleanup removes stale limiters every 10 minutes to prevent unbounded growth.
func (s *rateLimiterStore) cleanup() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-30 * time.Minute)
		s.mu.Lock()
		for k, tl := range s.limiters {
			if tl.lastSeen.Before(cutoff) {
				delete(s.limiters, k)
			}
		}
		s.mu.Unlock()
	}
}

// RateLimiter returns a Gin middleware implementing per-tenant token-bucket limiting.
// Falls back to IP-based limiting when per-tenant mode is disabled or the tenant header is absent.
func RateLimiter(cfg config.RateLimitConfig) gin.HandlerFunc {
	store := newStore(cfg.RequestsPerSecond, cfg.BurstSize)

	return func(c *gin.Context) {
		var key string
		if cfg.PerTenant {
			if tid := c.GetHeader("X-Tenant-ID"); tid != "" {
				key = "tenant:" + tid
			}
		}
		if key == "" {
			// Fallback: use client IP (supports X-Forwarded-For via trusted proxy).
			key = "ip:" + c.ClientIP()
		}

		limiter := store.getLimiter(key)
		if !limiter.Allow() {
			c.Header("Retry-After", "1")
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code":    "RATE_LIMITED",
				"message": "too many requests — please slow down",
			})
			return
		}
		c.Next()
	}
}
