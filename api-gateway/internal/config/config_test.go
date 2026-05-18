package config

import (
	"os"
	"testing"
)

func TestLoad_MissingJWTSecret(t *testing.T) {
	os.Unsetenv("JWT_SECRET")
	_, err := Load()
	if err == nil {
		t.Fatal("expected error when JWT_SECRET is missing, got nil")
	}
}

func TestLoad_Defaults(t *testing.T) {
	os.Setenv("JWT_SECRET", "test-secret-at-least-32-characters-long")
	defer os.Unsetenv("JWT_SECRET")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Port != 8080 {
		t.Errorf("expected default port 8080, got %d", cfg.Port)
	}
	if cfg.MetricsPort != 9090 {
		t.Errorf("expected default metrics port 9090, got %d", cfg.MetricsPort)
	}
	if cfg.RateLimit.RequestsPerSecond != 10000 {
		t.Errorf("expected default RPS 10000, got %d", cfg.RateLimit.RequestsPerSecond)
	}
}

func TestLoad_CustomPort(t *testing.T) {
	os.Setenv("JWT_SECRET", "test-secret-at-least-32-characters-long")
	os.Setenv("PORT", "9999")
	defer func() {
		os.Unsetenv("JWT_SECRET")
		os.Unsetenv("PORT")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Port != 9999 {
		t.Errorf("expected port 9999, got %d", cfg.Port)
	}
}

func TestLoad_TLSEnabled(t *testing.T) {
	os.Setenv("JWT_SECRET", "test-secret-at-least-32-characters-long")
	os.Setenv("TLS_ENABLED", "true")
	defer func() {
		os.Unsetenv("JWT_SECRET")
		os.Unsetenv("TLS_ENABLED")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.TLSEnabled {
		t.Error("expected TLS to be enabled")
	}
}

func TestGetEnv_Fallback(t *testing.T) {
	os.Unsetenv("__TEST_KEY__")
	val := getEnv("__TEST_KEY__", "fallback")
	if val != "fallback" {
		t.Errorf("expected 'fallback', got %q", val)
	}
}

func TestGetEnv_Override(t *testing.T) {
	os.Setenv("__TEST_KEY__", "override")
	defer os.Unsetenv("__TEST_KEY__")
	val := getEnv("__TEST_KEY__", "fallback")
	if val != "override" {
		t.Errorf("expected 'override', got %q", val)
	}
}

func TestGetEnvInt_Fallback(t *testing.T) {
	os.Unsetenv("__TEST_INT__")
	val := getEnvInt("__TEST_INT__", 42)
	if val != 42 {
		t.Errorf("expected 42, got %d", val)
	}
}

func TestGetEnvInt_Invalid(t *testing.T) {
	os.Setenv("__TEST_INT__", "not-a-number")
	defer os.Unsetenv("__TEST_INT__")
	val := getEnvInt("__TEST_INT__", 42)
	if val != 42 {
		t.Errorf("expected fallback 42, got %d", val)
	}
}
