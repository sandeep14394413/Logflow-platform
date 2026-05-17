// Package handler implements the authentication API: login, token refresh, introspect.
package handler

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

// JWTClaims is the token payload.
type JWTClaims struct {
	TenantID string   `json:"tenant_id"`
	UserID   string   `json:"user_id"`
	Roles    []string `json:"roles"`
	jwt.RegisteredClaims
}

// AuthHandler handles /api/v1/auth/* endpoints.
type AuthHandler struct {
	secret          []byte
	accessTokenTTL  time.Duration
	refreshTokenTTL time.Duration
	log             *zap.Logger
	// In production: inject a UserRepository and RefreshTokenStore.
}

// NewAuthHandler creates a handler with the signing key.
func NewAuthHandler(secret string, log *zap.Logger) *AuthHandler {
	return &AuthHandler{
		secret:          []byte(secret),
		accessTokenTTL:  15 * time.Minute,
		refreshTokenTTL: 7 * 24 * time.Hour,
		log:             log,
	}
}

// LoginRequest is the credentials payload.
type LoginRequest struct {
	Email    string `json:"email"    binding:"required,email"`
	Password string `json:"password" binding:"required"`
}

// TokenResponse is issued after successful authentication.
type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"` // seconds
	RefreshToken string `json:"refresh_token"`
}

// Login validates credentials and issues a JWT access token + refresh token.
// In production wire to your user store; here we stub a single demo user.
func (h *AuthHandler) Login(c *gin.Context) {
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": "INVALID_BODY", "message": err.Error()})
		return
	}

	// ── Stub user lookup (replace with real DB query) ────────────────────────
	user, ok := stubLookupUser(req.Email)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"code": "INVALID_CREDENTIALS", "message": "invalid email or password"})
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		h.log.Warn("failed login attempt", zap.String("email", req.Email))
		c.JSON(http.StatusUnauthorized, gin.H{"code": "INVALID_CREDENTIALS", "message": "invalid email or password"})
		return
	}

	now := time.Now()
	accessToken, err := h.issueToken(user, now, h.accessTokenTTL)
	if err != nil {
		h.log.Error("token issuance failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"code": "TOKEN_ERROR", "message": "could not issue token"})
		return
	}
	refreshToken, err := h.issueToken(user, now, h.refreshTokenTTL)
	if err != nil {
		h.log.Error("refresh token issuance failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"code": "TOKEN_ERROR", "message": "could not issue refresh token"})
		return
	}

	h.log.Info("user logged in", zap.String("user_id", user.ID), zap.String("tenant", user.TenantID))
	c.JSON(http.StatusOK, TokenResponse{
		AccessToken:  accessToken,
		TokenType:    "Bearer",
		ExpiresIn:    int(h.accessTokenTTL.Seconds()),
		RefreshToken: refreshToken,
	})
}

// Refresh validates a refresh token and issues a new access token.
func (h *AuthHandler) Refresh(c *gin.Context) {
	var body struct {
		RefreshToken string `json:"refresh_token" binding:"required"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": "INVALID_BODY", "message": err.Error()})
		return
	}

	claims := &JWTClaims{}
	token, err := jwt.ParseWithClaims(body.RefreshToken, claims, func(t *jwt.Token) (interface{}, error) {
		return h.secret, nil
	})
	if err != nil || !token.Valid {
		c.JSON(http.StatusUnauthorized, gin.H{"code": "INVALID_REFRESH_TOKEN", "message": "token is invalid or expired"})
		return
	}

	now := time.Now()
	user := stubUser{ID: claims.UserID, TenantID: claims.TenantID, Roles: claims.Roles}
	accessToken, err := h.issueToken(user, now, h.accessTokenTTL)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": "TOKEN_ERROR", "message": "could not issue token"})
		return
	}
	c.JSON(http.StatusOK, TokenResponse{
		AccessToken: accessToken,
		TokenType:   "Bearer",
		ExpiresIn:   int(h.accessTokenTTL.Seconds()),
	})
}

// Introspect validates a token and returns its claims (for service-to-service use).
func (h *AuthHandler) Introspect(c *gin.Context) {
	var body struct {
		Token string `json:"token" binding:"required"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": "INVALID_BODY", "message": err.Error()})
		return
	}

	claims := &JWTClaims{}
	token, err := jwt.ParseWithClaims(body.Token, claims, func(t *jwt.Token) (interface{}, error) {
		return h.secret, nil
	})
	if err != nil || !token.Valid {
		c.JSON(http.StatusOK, gin.H{"active": false})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"active":    true,
		"tenant_id": claims.TenantID,
		"user_id":   claims.UserID,
		"roles":     claims.Roles,
		"exp":       claims.ExpiresAt.Unix(),
	})
}

func (h *AuthHandler) issueToken(u stubUser, now time.Time, ttl time.Duration) (string, error) {
	claims := JWTClaims{
		TenantID: u.TenantID,
		UserID:   u.ID,
		Roles:    u.Roles,
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
			Issuer:    "logflow-auth",
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(h.secret)
}

// ── Stub user store (replace with real DB) ───────────────────────────────────

type stubUser struct {
	ID           string
	TenantID     string
	Email        string
	PasswordHash string
	Roles        []string
}

func stubLookupUser(email string) (stubUser, bool) {
	// bcrypt hash of "changeme" for demonstration only.
	const demoHash = "$2a$10$wJ3MnBVmPF5UM/nD8C/iH.dFZFT5JgZ4mQw1yL9TYU0LwZDwSkPKC"
	users := map[string]stubUser{
		"admin@logflow.dev": {
			ID:           "usr-0001",
			TenantID:     "tenant-acme",
			Email:        "admin@logflow.dev",
			PasswordHash: demoHash,
			Roles:        []string{"admin", "viewer"},
		},
	}
	u, ok := users[email]
	return u, ok
}
