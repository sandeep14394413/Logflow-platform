package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const authTestSecret = "test-jwt-secret-minimum-32-chars!!"

func newAuthRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	log, _ := zap.NewDevelopment()
	h := NewAuthHandler(authTestSecret, log)
	r := gin.New()
	r.POST("/api/v1/auth/login", h.Login)
	r.POST("/api/v1/auth/refresh", h.Refresh)
	r.POST("/api/v1/auth/introspect", h.Introspect)
	return r
}

func TestLogin_ValidCredentials(t *testing.T) {
	r := newAuthRouter()
	body, _ := json.Marshal(LoginRequest{
		Email:    "admin@logflow.dev",
		Password: "changeme",
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp TokenResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.AccessToken == "" {
		t.Error("expected access_token in response")
	}
	if resp.RefreshToken == "" {
		t.Error("expected refresh_token in response")
	}
	if resp.TokenType != "Bearer" {
		t.Errorf("expected token_type='Bearer', got %q", resp.TokenType)
	}
	if resp.ExpiresIn <= 0 {
		t.Errorf("expected positive expires_in, got %d", resp.ExpiresIn)
	}
}

func TestLogin_WrongPassword(t *testing.T) {
	r := newAuthRouter()
	body, _ := json.Marshal(LoginRequest{
		Email:    "admin@logflow.dev",
		Password: "wrongpassword",
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for wrong password, got %d", w.Code)
	}
}

func TestLogin_UnknownEmail(t *testing.T) {
	r := newAuthRouter()
	body, _ := json.Marshal(LoginRequest{
		Email:    "nobody@nowhere.com",
		Password: "anypassword",
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for unknown email, got %d", w.Code)
	}
}

func TestLogin_MissingFields(t *testing.T) {
	r := newAuthRouter()
	body := `{"email":"notanemail"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing fields, got %d", w.Code)
	}
}

func TestRefresh_ValidToken(t *testing.T) {
	r := newAuthRouter()

	// First login to get a refresh token.
	body, _ := json.Marshal(LoginRequest{Email: "admin@logflow.dev", Password: "changeme"})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	var loginResp TokenResponse
	json.NewDecoder(w.Body).Decode(&loginResp)

	// Now refresh.
	refreshBody, _ := json.Marshal(map[string]string{"refresh_token": loginResp.RefreshToken})
	w2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/auth/refresh", bytes.NewReader(refreshBody))
	req2.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Errorf("expected 200 on refresh, got %d: %s", w2.Code, w2.Body.String())
	}
	var refreshResp TokenResponse
	json.NewDecoder(w2.Body).Decode(&refreshResp)
	if refreshResp.AccessToken == "" {
		t.Error("expected new access_token from refresh")
	}
}

func TestRefresh_InvalidToken(t *testing.T) {
	r := newAuthRouter()
	body, _ := json.Marshal(map[string]string{"refresh_token": "not.a.valid.jwt"})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/refresh", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for invalid refresh token, got %d", w.Code)
	}
}

func TestIntrospect_ActiveToken(t *testing.T) {
	r := newAuthRouter()

	// Login first.
	loginBody, _ := json.Marshal(LoginRequest{Email: "admin@logflow.dev", Password: "changeme"})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewReader(loginBody))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	var loginResp TokenResponse
	json.NewDecoder(w.Body).Decode(&loginResp)

	// Introspect the access token.
	body, _ := json.Marshal(map[string]string{"token": loginResp.AccessToken})
	w2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/auth/introspect", bytes.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w2.Code)
	}
	var result map[string]interface{}
	json.NewDecoder(w2.Body).Decode(&result)
	if result["active"] != true {
		t.Errorf("expected active=true, got %v", result["active"])
	}
	if result["tenant_id"] != "tenant-acme" {
		t.Errorf("expected tenant_id='tenant-acme', got %v", result["tenant_id"])
	}
}

func TestIntrospect_InvalidToken(t *testing.T) {
	r := newAuthRouter()
	body, _ := json.Marshal(map[string]string{"token": "invalid.jwt.here"})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/introspect", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 (with active=false), got %d", w.Code)
	}
	var result map[string]interface{}
	json.NewDecoder(w.Body).Decode(&result)
	if result["active"] != false {
		t.Errorf("expected active=false for invalid token, got %v", result["active"])
	}
}
