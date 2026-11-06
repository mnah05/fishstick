package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/fishstick/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// NewPostgres returns a shared connection pool after checking connectivity.
// The caller owns the pool and must close it during application shutdown.
func NewPostgres(ctx context.Context, cfg *config.Config) (*sql.DB, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("postgres configuration: %w", err)
	}
	pgConfig, err := pgx.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres configuration: %w", err)
	}
	// Apply the timeout to future connections as well as the initial ping.
	pgConfig.ConnectTimeout = cfg.DatabaseConnectTimeout
	db := stdlib.OpenDB(*pgConfig)

	db.SetMaxOpenConns(cfg.DatabaseMaxOpenConns)
	db.SetMaxIdleConns(cfg.DatabaseMaxIdleConns)
	db.SetConnMaxLifetime(cfg.DatabaseConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.DatabaseConnMaxIdleTime)

	pingCtx, cancel := context.WithTimeout(ctx, cfg.DatabaseConnectTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return db, nil
}
