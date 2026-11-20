package config_test

import (
	"os"
	"testing"
	"time"

	"github.com/fishstick/internal/config"
)

func clearEnv(t *testing.T, keys ...string) {
	t.Helper()
	for _, key := range keys {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRelayConfigDefaultsAndValidation(t *testing.T) {
	clearEnv(t, "RELAY_INTERVAL", "RELAY_BATCH_SIZE", "RELAY_LOCK_TTL", "RELAY_SHUTDOWN_GRACE", "RELAY_RABBIT_URL", "RELAY_EXCHANGE", "RELAY_QUEUE", "RELAY_ROUTING_KEY", "RELAY_WORKER_ID")
	cfg, err := config.NewRelayConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Interval != time.Second || cfg.BatchSize != 100 || cfg.LockTTL != 30*time.Second || cfg.Exchange != "fishstick" {
		t.Fatalf("unexpected relay defaults: %+v", cfg)
	}
	t.Setenv("RELAY_BATCH_SIZE", "0")
	if _, err = config.NewRelayConfig(); err == nil {
		t.Fatal("zero relay batch size was accepted")
	}
}

func TestWorkerConfigLeaseAndHeartbeatRules(t *testing.T) {
	clearEnv(t, "WORKER_LEASE_DURATION", "WORKER_HEARTBEAT_INTERVAL", "WORKER_SHUTDOWN_GRACE", "WORKER_STUB_DELAY", "WORKER_RABBIT_URL", "WORKER_EXCHANGE", "WORKER_QUEUE", "WORKER_ROUTING_KEY", "WORKER_PREFETCH", "WORKER_WORKER_ID")
	cfg, err := config.NewWorkerConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LeaseDuration != time.Minute || cfg.HeartbeatInterval != 15*time.Second || cfg.Prefetch != 1 {
		t.Fatalf("unexpected worker defaults: %+v", cfg)
	}
	t.Setenv("WORKER_HEARTBEAT_INTERVAL", "60s")
	if _, err = config.NewWorkerConfig(); err == nil {
		t.Fatal("heartbeat interval equal to lease was accepted")
	}
}
