package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/v2"
)

type Config struct {
	DatabaseURL             string        `koanf:"database_url"`
	DatabaseMaxOpenConns    int           `koanf:"database_max_open_conns"`
	DatabaseMaxIdleConns    int           `koanf:"database_max_idle_conns"`
	DatabaseConnMaxLifetime time.Duration `koanf:"database_conn_max_lifetime"`
	DatabaseConnMaxIdleTime time.Duration `koanf:"database_conn_max_idle_time"`
	DatabaseConnectTimeout  time.Duration `koanf:"database_connect_timeout"`
}

// NewConfig loads defaults followed by DATABASE_* environment overrides.
func NewConfig() (*Config, error) {
	k := koanf.New(".")
	if err := k.Load(confmap.Provider(map[string]interface{}{
		"database_max_open_conns":     25,
		"database_max_idle_conns":     5,
		"database_conn_max_lifetime":  "30m",
		"database_conn_max_idle_time": "5m",
		"database_connect_timeout":    "5s",
	}, "."), nil); err != nil {
		return nil, fmt.Errorf("load configuration defaults: %w", err)
	}
	if err := k.Load(env.Provider("DATABASE_", ".", strings.ToLower), nil); err != nil {
		return nil, fmt.Errorf("load environment configuration: %w", err)
	}
	// Reject empty overrides before weak decoding can turn them into zero limits.
	for _, key := range []string{
		"database_max_open_conns", "database_max_idle_conns",
		"database_conn_max_lifetime", "database_conn_max_idle_time", "database_connect_timeout",
	} {
		if value, ok := k.Get(key).(string); ok && strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("decode configuration: %s must not be empty", strings.ToUpper(key))
		}
	}
	var cfg Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, fmt.Errorf("decode configuration: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate also checks configurations constructed directly by callers.
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("configuration is required")
	}
	if strings.TrimSpace(c.DatabaseURL) == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if c.DatabaseMaxOpenConns < 0 || c.DatabaseMaxIdleConns < 0 {
		return fmt.Errorf("database connection limits must not be negative")
	}
	if c.DatabaseMaxOpenConns > 0 && c.DatabaseMaxIdleConns > c.DatabaseMaxOpenConns {
		return fmt.Errorf("DATABASE_MAX_IDLE_CONNS must not exceed DATABASE_MAX_OPEN_CONNS")
	}
	if c.DatabaseConnMaxLifetime < 0 {
		return fmt.Errorf("DATABASE_CONN_MAX_LIFETIME must not be negative")
	}
	if c.DatabaseConnMaxIdleTime < 0 {
		return fmt.Errorf("DATABASE_CONN_MAX_IDLE_TIME must not be negative")
	}
	if c.DatabaseConnectTimeout <= 0 {
		return fmt.Errorf("DATABASE_CONNECT_TIMEOUT must be positive")
	}
	return nil
}
