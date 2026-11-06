// Applies goose migrations from internal/database/migrations.
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/fishstick/internal/database"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [up|down|status]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Requires DATABASE_URL.\n")
	}
	flag.Parse()

	command := "up"
	if flag.NArg() > 0 {
		command = flag.Arg(0)
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()

	if err := database.RunMigrations(db, command); err != nil {
		log.Fatal(err)
	}
}
