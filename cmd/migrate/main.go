package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/fishstick/internal/config"
	"github.com/fishstick/internal/database"
)

func main() {
	if err := run(); err != nil {
		slog.Error("database migration failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.NewConfig()
	if err != nil {
		return err
	}
	db, err := database.NewPostgres(context.Background(), cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := database.RunMigrations(db, "up"); err != nil {
		return err
	}
	slog.Info("database migrations applied")
	return nil
}
