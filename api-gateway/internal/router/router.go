// Package router registers all API Gateway routes and wires upstream proxies.
package router

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"logflow/api-gateway/internal/config"
	"logflow/api-gateway/internal/middleware"
)

// Register mounts all routes on the Gin engine.
func Register(e *gin.Engine, cfg *config.Config, log *zap.Logger) {
	// ── Health / readiness ────────────────────────────────────────────────────
	e.GET("/health", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	e.GET("/ready", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ready"}) })

	transport := &http.Transport{
		MaxIdleConns:        500,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
	}

	v1 := e.Group("/api/v1")
	{
		// ── Auth (no JWT required) ────────────────────────────────────────────
		auth := v1.Group("/auth")
		auth.POST("/login", reverseProxy(cfg.AuthURL+"/api/v1/auth/login", transport))
		auth.POST("/refresh", reverseProxy(cfg.AuthURL+"/api/v1/auth/refresh", transport))
		auth.POST("/introspect", reverseProxy(cfg.AuthURL+"/api/v1/auth/introspect", transport))

		// ── Log ingestion ──────────────────────────────────────────────────────
		logs := v1.Group("/logs")
		logs.POST("", reverseProxy(cfg.IngestionURL+"/api/v1/logs", transport))
		logs.POST("/batch", reverseProxy(cfg.IngestionURL+"/api/v1/logs/batch", transport))

		// ── Search ─────────────────────────────────────────────────────────────
		logs.GET("/search", reverseProxy(cfg.SearchURL+"/api/v1/logs/search", transport))
		logs.GET("/count", reverseProxy(cfg.SearchURL+"/api/v1/logs/count", transport))
		logs.GET("/histogram", reverseProxy(cfg.SearchURL+"/api/v1/logs/histogram", transport))
		logs.GET("/:id", reverseProxy(cfg.SearchURL+"/api/v1/logs/:id", transport))

		// ── WebSocket streaming ────────────────────────────────────────────────
		stream := v1.Group("/stream")
		stream.GET("/tail", reverseProxy(cfg.WebsocketURL+"/api/v1/stream/tail", transport))

		// ── Admin (require admin role) ─────────────────────────────────────────
		admin := v1.Group("/admin", middleware.RequireRole("admin"))
		admin.GET("/tenants", reverseProxy(cfg.AuthURL+"/api/v1/admin/tenants", transport))
		admin.DELETE("/logs", reverseProxy(cfg.SearchURL+"/api/v1/admin/logs", transport))
	}
}

// reverseProxy builds a single-target reverse proxy handler.
func reverseProxy(target string, transport http.RoundTripper) gin.HandlerFunc {
	u, err := url.Parse(target)
	if err != nil {
		panic("invalid upstream URL: " + target)
	}
	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.Transport = transport
	proxy.ModifyResponse = func(resp *http.Response) error {
		resp.Header.Set("X-Served-By", "logflow-gateway")
		return nil
	}
	return func(c *gin.Context) {
		c.Request.URL.Path = u.Path
		proxy.ServeHTTP(c.Writer, c.Request)
	}
}
