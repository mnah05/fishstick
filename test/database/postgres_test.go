package database_test

import (
	"context"
	"errors"
	"github.com/fishstick/internal/database"
	"os"
	"testing"
	"time"

	"github.com/fishstick/internal/config"
	"github.com/fishstick/internal/database/sqlc"
)

func TestNewPostgresRejectsMissingConfig(t *testing.T) {
	db, err := database.NewPostgres(context.Background(), nil)
	if err == nil || db != nil {
		t.Fatalf("expected error and no pool, got pool %v, error %v", db, err)
	}
}

func TestPostgresIntegration(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run against PostgreSQL")
	}
	cfg := &config.Config{
		DatabaseURL:             url,
		DatabaseMaxOpenConns:    2,
		DatabaseMaxIdleConns:    1,
		DatabaseConnMaxLifetime: time.Minute,
		DatabaseConnMaxIdleTime: time.Minute,
		DatabaseConnectTimeout:  5 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := database.NewPostgres(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if got := db.Stats().MaxOpenConnections; got != cfg.DatabaseMaxOpenConns {
		t.Fatalf("max open connections = %d, want %d", got, cfg.DatabaseMaxOpenConns)
	}
	healthy, err := sqlc.New(db).HealthCheck(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if healthy != 1 {
		t.Fatalf("health check = %d, want 1", healthy)
	}
}

func TestNewPostgresRejectsMalformedURL(t *testing.T) {
	cfg := &config.Config{
		DatabaseURL:            "postgres://localhost:invalid/database",
		DatabaseConnectTimeout: time.Second,
	}
	db, err := database.NewPostgres(context.Background(), cfg)
	if err == nil || db != nil {
		t.Fatalf("expected error and no pool, got pool %v, error %v", db, err)
	}
}

func TestNewPostgresHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := &config.Config{
		DatabaseURL:            "postgres://localhost:5432/database?sslmode=disable",
		DatabaseConnectTimeout: time.Second,
	}
	db, err := database.NewPostgres(ctx, cfg)
	if !errors.Is(err, context.Canceled) || db != nil {
		t.Fatalf("expected canceled error and no pool, got pool %v, error %v", db, err)
	}
}
