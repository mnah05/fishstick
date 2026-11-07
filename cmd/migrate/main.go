// Applies goose migrations from internal/database/migrations.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/fishstick/internal/config"
	"github.com/fishstick/internal/database"
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

	cfg, err := config.NewConfig()
	if err != nil {
		log.Fatal(err)
	}
	db, err := database.NewPostgres(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := database.RunMigrations(db, command); err != nil {
		log.Fatal(err)
	}
}
