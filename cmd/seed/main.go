// Loads dev seed data after migrations (internal/database/seed.sql).
package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/fishstick/internal/database"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := database.ApplySeed(ctx, db); err != nil {
		log.Fatal(err)
	}
	log.Println("seed data applied")
}
