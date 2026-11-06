package database

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
)

//go:embed seed.sql
var seedFile embed.FS

// ApplySeed executes seed.sql (dev fixtures; truncates job tables first).
func ApplySeed(ctx context.Context, db *sql.DB) error {
	raw, err := seedFile.ReadFile("seed.sql")
	if err != nil {
		return fmt.Errorf("read seed.sql: %w", err)
	}
	if _, err := db.ExecContext(ctx, string(raw)); err != nil {
		return fmt.Errorf("apply seed: %w", err)
	}
	return nil
}
