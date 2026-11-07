// Loads dev seed data after migrations (internal/database/seed.sql).
package main

import (
	"context"
	"log"
	"time"

	"github.com/fishstick/internal/config"
	"github.com/fishstick/internal/database"
)

func main() {
	cfg, err := config.NewConfig()
	if err != nil {
		log.Fatal(err)
	}
	db, err := database.NewPostgres(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := database.ApplySeed(ctx, db); err != nil {
		log.Fatal(err)
	}
	log.Println("seed data applied")
}
