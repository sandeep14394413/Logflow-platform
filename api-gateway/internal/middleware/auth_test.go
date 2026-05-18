package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

const testSecret = "test-secret-at-least-32-characters!!"

func makeToken(t *testing.T, tenantID, userID string, roles []string, exp time.Time) string {
	t.Helper()
	claims := JWTClaims{
		TenantID: tenantID,
		UserID:   userID,
		Roles:    roles,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(exp),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(testSecret))
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}
	return token
}

func setupRouter(secret string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(JWT(secret))
	r.GET("/protected", func(c *gin.Context) {
		tid, _ := c.Get(claimsTenantKey)
		c.JSON(http.StatusOK, gin.H{"tenant": tid})
	})
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	return r
}

func TestJWT_MissingHeader(t *testing.T) {
	r := setupRouter(testSecret)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}
}

func TestJWT_ValidToken(t *testing.T) {
	r := setupRouter(testSecret)
	token := makeToken(t, "tenant-acme", "user-1", []string{"admin"}, time.Now().Add(time.Hour))
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestJWT_ExpiredToken(t *testing.T) {
	r := setupRouter(testSecret)
	token := makeToken(t, "tenant-acme", "user-1", []string{"admin"}, time.Now().Add(-time.Hour))
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for expired token, got %d", w.Code)
	}
}

func TestJWT_WrongSecret(t *testing.T) {
	token := makeToken(t, "tenant-acme", "user-1", []string{"admin"}, time.Now().Add(time.Hour))
	// Set up router with different secret
	r2 := gin.New()
	r2.Use(JWT("completely-different-secret-32-chars"))
	r2.GET("/protected", func(c *gin.Context) { c.Status(http.StatusOK) })
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	r2.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for wrong secret, got %d", w.Code)
	}
}

func TestJWT_HealthExempt(t *testing.T) {
	r := setupRouter(testSecret)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	// No Authorization header — health should pass through.
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for health (exempt), got %d", w.Code)
	}
}

func TestJWT_MalformedBearer(t *testing.T) {
	r := setupRouter(testSecret)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "NotBearer token123")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for malformed header, got %d", w.Code)
	}
}

func TestRequireRole_Allowed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(claimsRolesKey, []string{"admin", "viewer"})
		c.Next()
	})
	r.GET("/admin", RequireRole("admin"), func(c *gin.Context) {
		c.Status(http.StatusOK)
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for admin role, got %d", w.Code)
	}
}

func TestRequireRole_Forbidden(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(claimsRolesKey, []string{"viewer"})
		c.Next()
	})
	r.GET("/admin", RequireRole("admin"), func(c *gin.Context) {
		c.Status(http.StatusOK)
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("expected 403 for missing role, got %d", w.Code)
	}
}
