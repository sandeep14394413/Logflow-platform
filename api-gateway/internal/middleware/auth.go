// Package middleware provides Gin middleware for the API Gateway.
package middleware

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

const (
	claimsTenantKey = "tenant_id"
	claimsUserKey   = "user_id"
	claimsRolesKey  = "roles"
	authHeaderKey   = "Authorization"
	bearerPrefix    = "Bearer "
)

// JWTClaims is the payload embedded in every issued token.
type JWTClaims struct {
	TenantID string   `json:"tenant_id"`
	UserID   string   `json:"user_id"`
	Roles    []string `json:"roles"`
	jwt.RegisteredClaims
}

// JWT returns a Gin middleware that validates Bearer tokens and injects claims into the context.
// Public endpoints (health, metrics, /auth/*) are exempted from validation.
func JWT(secret string) gin.HandlerFunc {
	signingKey := []byte(secret)
	exemptPrefixes := []string{"/health", "/ready", "/metrics", "/api/v1/auth/"}

	return func(c *gin.Context) {
		for _, prefix := range exemptPrefixes {
			if strings.HasPrefix(c.Request.URL.Path, prefix) {
				c.Next()
				return
			}
		}

		raw := c.GetHeader(authHeaderKey)
		if !strings.HasPrefix(raw, bearerPrefix) {
			abortUnauthorized(c, "missing or malformed Authorization header")
			return
		}
		tokenStr := strings.TrimPrefix(raw, bearerPrefix)

		claims := &JWTClaims{}
		token, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, errors.New("unexpected signing method")
			}
			return signingKey, nil
		}, jwt.WithLeeway(30*time.Second))

		if err != nil || !token.Valid {
			abortUnauthorized(c, "invalid or expired token")
			return
		}

		// Inject tenant context — consumed by downstream services.
		c.Set(claimsTenantKey, claims.TenantID)
		c.Set(claimsUserKey, claims.UserID)
		c.Set(claimsRolesKey, claims.Roles)
		c.Request.Header.Set("X-Tenant-ID", claims.TenantID)
		c.Request.Header.Set("X-User-ID", claims.UserID)

		c.Next()
	}
}

func abortUnauthorized(c *gin.Context, msg string) {
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
		"code":    "UNAUTHORIZED",
		"message": msg,
	})
}

// RequireRole checks that the authenticated user holds at least one of the required roles.
func RequireRole(roles ...string) gin.HandlerFunc {
	required := make(map[string]bool, len(roles))
	for _, r := range roles {
		required[r] = true
	}
	return func(c *gin.Context) {
		userRolesRaw, exists := c.Get(claimsRolesKey)
		if !exists {
			abortUnauthorized(c, "no roles in token")
			return
		}
		userRoles, ok := userRolesRaw.([]string)
		if !ok {
			abortUnauthorized(c, "malformed roles claim")
			return
		}
		for _, r := range userRoles {
			if required[r] {
				c.Next()
				return
			}
		}
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
			"code":    "FORBIDDEN",
			"message": "insufficient permissions",
		})
	}
}
