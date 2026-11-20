package config

import (
	"fmt"
	"math"
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

// SchedulerConfig contains settings used only by cmd/schedular. Keeping it
// separate lets the API validate database settings without requiring scheduler
// configuration to be relevant to that process.
type SchedulerConfig struct {
	Interval      time.Duration `koanf:"interval"`
	BatchSize     int           `koanf:"batch_size"`
	ShutdownGrace time.Duration `koanf:"shutdown_grace"`
	HealthAddress string        `koanf:"health_address"`
}

type RelayConfig struct {
	Interval      time.Duration `koanf:"interval"`
	BatchSize     int           `koanf:"batch_size"`
	LockTTL       time.Duration `koanf:"lock_ttl"`
	ShutdownGrace time.Duration `koanf:"shutdown_grace"`
	RabbitURL     string        `koanf:"rabbit_url"`
	Exchange      string        `koanf:"exchange"`
	Queue         string        `koanf:"queue"`
	RoutingKey    string        `koanf:"routing_key"`
	WorkerID      string        `koanf:"worker_id"`
}
type WorkerConfig struct {
	LeaseDuration     time.Duration `koanf:"lease_duration"`
	HeartbeatInterval time.Duration `koanf:"heartbeat_interval"`
	ShutdownGrace     time.Duration `koanf:"shutdown_grace"`
	StubDelay         time.Duration `koanf:"stub_delay"`
	RabbitURL         string        `koanf:"rabbit_url"`
	Exchange          string        `koanf:"exchange"`
	Queue             string        `koanf:"queue"`
	RoutingKey        string        `koanf:"routing_key"`
	Prefetch          int           `koanf:"prefetch"`
	WorkerID          string        `koanf:"worker_id"`
}

func loadProcessConfig(prefix string, defaults map[string]any, out any) error {
	k := koanf.New(".")
	if err := k.Load(confmap.Provider(defaults, "."), nil); err != nil {
		return err
	}
	if err := k.Load(env.Provider(prefix, ".", func(key string) string { return strings.ToLower(strings.TrimPrefix(key, prefix)) }), nil); err != nil {
		return err
	}
	for key := range defaults {
		if value, ok := k.Get(key).(string); ok && strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s%s must not be empty", prefix, strings.ToUpper(key))
		}
	}
	return k.Unmarshal("", out)
}

func NewRelayConfig() (*RelayConfig, error) {
	c := &RelayConfig{}
	err := loadProcessConfig("RELAY_", map[string]any{"interval": "1s", "batch_size": 100, "lock_ttl": "30s", "shutdown_grace": "10s", "rabbit_url": "amqp://fishstick:fishstick@localhost:5672/", "exchange": "fishstick", "queue": "fishstick.worker", "routing_key": "job.dispatch", "worker_id": "relay-1"}, c)
	if err == nil && (c.Interval <= 0 || c.BatchSize <= 0 || c.LockTTL <= 0 || c.ShutdownGrace <= 0 || c.Exchange == "" || c.Queue == "" || c.RoutingKey == "" || c.WorkerID == "") {
		err = fmt.Errorf("invalid relay configuration")
	}
	return c, err
}

func NewWorkerConfig() (*WorkerConfig, error) {
	c := &WorkerConfig{}
	err := loadProcessConfig("WORKER_", map[string]any{"lease_duration": "60s", "heartbeat_interval": "15s", "shutdown_grace": "30s", "stub_delay": "0s", "rabbit_url": "amqp://fishstick:fishstick@localhost:5672/", "exchange": "fishstick", "queue": "fishstick.worker", "routing_key": "job.dispatch", "prefetch": 1, "worker_id": "worker-1"}, c)
	if err == nil && (c.LeaseDuration <= 0 || c.HeartbeatInterval <= 0 || c.HeartbeatInterval >= c.LeaseDuration || c.ShutdownGrace <= 0 || c.StubDelay < 0 || c.Queue == "" || c.Exchange == "" || c.RoutingKey == "" || c.Prefetch <= 0 || c.WorkerID == "") {
		err = fmt.Errorf("invalid worker configuration")
	}
	return c, err
}

// defaultValues are the only defaults; the empty-override check below iterates
// the same keys so adding a setting cannot forget one.
var defaultValues = map[string]any{
	"database_max_open_conns":     25,
	"database_max_idle_conns":     5,
	"database_conn_max_lifetime":  "30m",
	"database_conn_max_idle_time": "5m",
	"database_connect_timeout":    "5s",
}

var schedulerDefaultValues = map[string]any{
	"interval":       "5s",
	"batch_size":     100,
	"shutdown_grace": "10s",
	"health_address": ":8081",
}

// NewSchedulerConfig loads scheduler defaults and overrides them with
// SCHEDULER_* environment variables through Koanf.
func NewSchedulerConfig() (*SchedulerConfig, error) {
	k := koanf.New(".")
	if err := k.Load(confmap.Provider(schedulerDefaultValues, "."), nil); err != nil {
		return nil, fmt.Errorf("load scheduler configuration defaults: %w", err)
	}
	if err := k.Load(env.Provider("SCHEDULER_", ".", func(key string) string {
		return strings.ToLower(strings.TrimPrefix(key, "SCHEDULER_"))
	}), nil); err != nil {
		return nil, fmt.Errorf("load scheduler environment configuration: %w", err)
	}
	for key := range schedulerDefaultValues {
		if value, ok := k.Get(key).(string); ok && strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("decode scheduler configuration: SCHEDULER_%s must not be empty", strings.ToUpper(key))
		}
	}
	var cfg SchedulerConfig
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, fmt.Errorf("decode scheduler configuration: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate also checks scheduler configurations constructed by callers.
func (c *SchedulerConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("scheduler configuration is required")
	}
	if c.Interval <= 0 {
		return fmt.Errorf("SCHEDULER_INTERVAL must be positive")
	}
	if c.BatchSize <= 0 || int64(c.BatchSize) > math.MaxInt32 {
		return fmt.Errorf("SCHEDULER_BATCH_SIZE must be a positive 32-bit integer")
	}
	if c.ShutdownGrace <= 0 {
		return fmt.Errorf("SCHEDULER_SHUTDOWN_GRACE must be positive")
	}
	if strings.TrimSpace(c.HealthAddress) == "" {
		return fmt.Errorf("SCHEDULER_HEALTH_ADDRESS must not be empty")
	}
	return nil
}

// NewConfig loads defaults followed by DATABASE_* environment overrides.
func NewConfig() (*Config, error) {
	k := koanf.New(".")
	if err := k.Load(confmap.Provider(defaultValues, "."), nil); err != nil {
		return nil, fmt.Errorf("load configuration defaults: %w", err)
	}
	if err := k.Load(env.Provider("DATABASE_", ".", strings.ToLower), nil); err != nil {
		return nil, fmt.Errorf("load environment configuration: %w", err)
	}
	// Reject empty overrides before weak decoding can turn them into zero limits.
	for key := range defaultValues {
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
