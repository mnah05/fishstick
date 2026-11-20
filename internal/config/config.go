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

// PollingConfig contains the settings shared by database polling processes.
type PollingConfig struct {
	Interval      time.Duration `koanf:"interval"`
	BatchSize     int           `koanf:"batch_size"`
	ShutdownGrace time.Duration `koanf:"shutdown_grace"`
	HealthAddress string        `koanf:"health_address"`
}

// SchedulerConfig and WatcherConfig keep independent process configuration
// types while sharing the same polling settings.
type SchedulerConfig PollingConfig
type WatcherConfig PollingConfig

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
	return loadConfig(prefix,
		func(key string) string { return strings.ToLower(strings.TrimPrefix(key, prefix)) },
		func(key string) string { return prefix + strings.ToUpper(key) },
		defaults, out)
}

func loadConfig(prefix string, transformKey, environmentName func(string) string, defaults map[string]any, out any) error {
	k := koanf.New(".")
	if err := k.Load(confmap.Provider(defaults, "."), nil); err != nil {
		return fmt.Errorf("load %s configuration defaults: %w", strings.TrimSuffix(prefix, "_"), err)
	}
	if err := k.Load(env.Provider(prefix, ".", transformKey), nil); err != nil {
		return fmt.Errorf("load %s environment configuration: %w", strings.TrimSuffix(prefix, "_"), err)
	}
	for key := range defaults {
		if value, ok := k.Get(key).(string); ok && strings.TrimSpace(value) == "" {
			return fmt.Errorf("decode configuration: %s must not be empty", environmentName(key))
		}
	}
	if err := k.Unmarshal("", out); err != nil {
		return fmt.Errorf("decode configuration: %w", err)
	}
	return nil
}

func rabbitDefaults() map[string]any {
	return map[string]any{
		"rabbit_url":  "amqp://fishstick:fishstick@localhost:5672/",
		"exchange":    "fishstick",
		"queue":       "fishstick.worker",
		"routing_key": "job.dispatch",
	}
}

func NewRelayConfig() (*RelayConfig, error) {
	c := &RelayConfig{}
	defaults := rabbitDefaults()
	defaults["interval"] = "1s"
	defaults["batch_size"] = 100
	defaults["lock_ttl"] = "30s"
	defaults["shutdown_grace"] = "10s"
	defaults["worker_id"] = "relay-1"
	err := loadProcessConfig("RELAY_", defaults, c)
	if err == nil && (c.Interval <= 0 || c.BatchSize <= 0 || c.LockTTL <= 0 || c.ShutdownGrace <= 0 || c.Exchange == "" || c.Queue == "" || c.RoutingKey == "" || c.WorkerID == "") {
		err = fmt.Errorf("invalid relay configuration")
	}
	return c, err
}

func NewWorkerConfig() (*WorkerConfig, error) {
	c := &WorkerConfig{}
	defaults := rabbitDefaults()
	defaults["lease_duration"] = "60s"
	defaults["heartbeat_interval"] = "15s"
	defaults["shutdown_grace"] = "30s"
	defaults["stub_delay"] = "0s"
	defaults["prefetch"] = 1
	defaults["worker_id"] = "worker-1"
	err := loadProcessConfig("WORKER_", defaults, c)
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

func pollingDefaults(healthAddress string) map[string]any {
	return map[string]any{
		"interval":       "5s",
		"batch_size":     100,
		"shutdown_grace": "10s",
		"health_address": healthAddress,
	}
}

var schedulerDefaultValues = pollingDefaults(":8081")

// NewSchedulerConfig loads scheduler defaults and overrides them with
// SCHEDULER_* environment variables through Koanf.
func NewSchedulerConfig() (*SchedulerConfig, error) {
	cfg := &SchedulerConfig{}
	err := loadProcessConfig("SCHEDULER_", schedulerDefaultValues, cfg)
	if err == nil {
		err = cfg.Validate()
	}
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate also checks scheduler configurations constructed by callers.
func (c *SchedulerConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("scheduler configuration is required")
	}
	return validatePollConfig("SCHEDULER", c.Interval, c.BatchSize, c.ShutdownGrace, c.HealthAddress)
}

var watcherDefaultValues = pollingDefaults(":8082")

// NewWatcherConfig loads watcher defaults followed by WATCHER_* overrides.
func NewWatcherConfig() (*WatcherConfig, error) {
	cfg := &WatcherConfig{}
	err := loadProcessConfig("WATCHER_", watcherDefaultValues, cfg)
	if err == nil {
		err = cfg.Validate()
	}
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate also checks watcher configurations constructed directly by callers.
func (c *WatcherConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("watcher configuration is required")
	}
	return validatePollConfig("WATCHER", c.Interval, c.BatchSize, c.ShutdownGrace, c.HealthAddress)
}

func validatePollConfig(prefix string, interval time.Duration, batchSize int, shutdownGrace time.Duration, healthAddress string) error {
	if interval <= 0 {
		return fmt.Errorf("%s_INTERVAL must be positive", prefix)
	}
	if batchSize <= 0 || int64(batchSize) > math.MaxInt32 {
		return fmt.Errorf("%s_BATCH_SIZE must be a positive 32-bit integer", prefix)
	}
	if shutdownGrace <= 0 {
		return fmt.Errorf("%s_SHUTDOWN_GRACE must be positive", prefix)
	}
	if strings.TrimSpace(healthAddress) == "" {
		return fmt.Errorf("%s_HEALTH_ADDRESS must not be empty", prefix)
	}
	return nil
}

// NewConfig loads defaults followed by DATABASE_* environment overrides.
func NewConfig() (*Config, error) {
	cfg := &Config{}
	if err := loadConfig("DATABASE_", strings.ToLower, strings.ToUpper, defaultValues, cfg); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
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
