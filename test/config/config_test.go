package config_test

import (
	"github.com/fishstick/internal/config"
	"os"
	"strings"
	"testing"
	"time"
)

func cleanDatabaseEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"DATABASE_URL", "DATABASE_MAX_OPEN_CONNS", "DATABASE_MAX_IDLE_CONNS",
		"DATABASE_CONN_MAX_LIFETIME", "DATABASE_CONN_MAX_IDLE_TIME", "DATABASE_CONNECT_TIMEOUT",
	} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNewConfigDefaults(t *testing.T) {
	cleanDatabaseEnv(t)
	t.Setenv("DATABASE_URL", "postgres://localhost/fishstick")
	cfg, err := config.NewConfig()
	if err != nil {
		t.Fatal(err)
	}
	want := config.Config{
		DatabaseURL:             "postgres://localhost/fishstick",
		DatabaseMaxOpenConns:    25,
		DatabaseMaxIdleConns:    5,
		DatabaseConnMaxLifetime: 30 * time.Minute,
		DatabaseConnMaxIdleTime: 5 * time.Minute,
		DatabaseConnectTimeout:  5 * time.Second,
	}
	if *cfg != want {
		t.Fatalf("configuration = %+v, want %+v", cfg, want)
	}
}

func TestNewConfigOverrides(t *testing.T) {
	cleanDatabaseEnv(t)
	for key, value := range map[string]string{
		"DATABASE_URL":                "postgres://localhost/custom",
		"DATABASE_MAX_OPEN_CONNS":     "12",
		"DATABASE_MAX_IDLE_CONNS":     "3",
		"DATABASE_CONN_MAX_LIFETIME":  "1h",
		"DATABASE_CONN_MAX_IDLE_TIME": "90s",
		"DATABASE_CONNECT_TIMEOUT":    "250ms",
	} {
		t.Setenv(key, value)
	}
	cfg, err := config.NewConfig()
	if err != nil {
		t.Fatal(err)
	}
	want := config.Config{
		DatabaseURL:             "postgres://localhost/custom",
		DatabaseMaxOpenConns:    12,
		DatabaseMaxIdleConns:    3,
		DatabaseConnMaxLifetime: time.Hour,
		DatabaseConnMaxIdleTime: 90 * time.Second,
		DatabaseConnectTimeout:  250 * time.Millisecond,
	}
	if *cfg != want {
		t.Fatalf("configuration = %+v, want %+v", cfg, want)
	}
}

func TestNewConfigRejectsInvalidValues(t *testing.T) {
	for _, tt := range []struct{ key, value, message string }{
		{"DATABASE_URL", " ", "DATABASE_URL"},
		{"DATABASE_MAX_OPEN_CONNS", "many", "decode configuration"},
		{"DATABASE_MAX_OPEN_CONNS", "", "decode configuration"},
		{"DATABASE_MAX_IDLE_CONNS", "-1", "must not be negative"},
		{"DATABASE_MAX_IDLE_CONNS", "26", "must not exceed"},
		{"DATABASE_CONN_MAX_LIFETIME", "forever", "decode configuration"},
		{"DATABASE_CONN_MAX_LIFETIME", "-1s", "DATABASE_CONN_MAX_LIFETIME"},
		{"DATABASE_CONN_MAX_IDLE_TIME", "-1s", "DATABASE_CONN_MAX_IDLE_TIME"},
		{"DATABASE_CONNECT_TIMEOUT", "0s", "DATABASE_CONNECT_TIMEOUT"},
		{"DATABASE_CONNECT_TIMEOUT", "-1s", "DATABASE_CONNECT_TIMEOUT"},
	} {
		t.Run(tt.key+"="+tt.value, func(t *testing.T) {
			cleanDatabaseEnv(t)
			t.Setenv("DATABASE_URL", "postgres://localhost/fishstick")
			t.Setenv(tt.key, tt.value)
			cfg, err := config.NewConfig()
			if err == nil || !strings.Contains(err.Error(), tt.message) || cfg != nil {
				t.Fatalf("config.NewConfig() = (%+v, %v), want nil config and %q error", cfg, err, tt.message)
			}
		})
	}
}

func TestValidateZeroPoolSettings(t *testing.T) {
	cfg := config.Config{DatabaseURL: "postgres://localhost/fishstick", DatabaseConnectTimeout: time.Second}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("zero pool settings should preserve database/sql semantics: %v", err)
	}
	var absent *config.Config
	if err := absent.Validate(); err == nil {
		t.Fatal("nil config must be rejected")
	}
}
