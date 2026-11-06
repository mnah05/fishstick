// Command api wires configuration, PostgreSQL, sqlc, and Echo into one HTTP server.
//
// Flow:
//
//	config.NewConfig() → DATABASE_* env + defaults
//	database.NewPostgres() → *sql.DB pool (pgx via database/sql), ping on startup
//	sqlc.New(db) → generated Queries (HealthCheck runs SELECT 1)
//	api.NewRouter(db) → Echo; GET /v1/health/db calls queries.HealthCheck per request
//	http.Server.ListenAndServe → graceful shutdown on SIGINT/SIGTERM, then db.Close()
//
// Shutdown:
//
//	On SIGINT/SIGTERM the server stops claiming new work (new requests receive
//	503 and the listener closes), then waits for in-flight handlers—and the
//	transactions they own—to finish for up to shutdownGracePeriod. The database
//	pool is closed only after the HTTP server has drained.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fishstick/internal/api"
	"github.com/fishstick/internal/config"
	"github.com/fishstick/internal/database"
	"github.com/fishstick/internal/database/sqlc"
)

const (
	listenAddress = ":8080"

	// shutdownGracePeriod bounds how long the server waits for in-flight
	// requests and their database transactions after a termination signal.
	shutdownGracePeriod = 10 * time.Second
)

func main() {
	if err := run(); err != nil {
		slog.Error("API stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	// The signal context only wakes the supervisor below. Do not derive HTTP
	// request contexts from it: cancelling it on SIGTERM must not abort the
	// in-flight transaction we are deliberately draining.
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.NewConfig()
	if err != nil {
		return err
	}

	db, err := database.NewPostgres(signalCtx, cfg)
	if err != nil {
		return err
	}
	// Registered before the server so the pool outlives request draining.
	defer db.Close()

	queries := sqlc.New(db)

	queryCtx, cancelQuery := context.WithTimeout(signalCtx, cfg.DatabaseConnectTimeout)
	healthy, err := queries.HealthCheck(queryCtx)
	cancelQuery()
	if err != nil {
		return fmt.Errorf("startup database query: %w", err)
	}
	if healthy != 1 {
		return fmt.Errorf("startup database query returned %d, expected 1", healthy)
	}
	slog.Info("PostgreSQL connected; sqlc health query succeeded")

	router := api.NewRouter(db)
	var draining atomic.Bool
	server := &http.Server{
		Addr: listenAddress,
		// Reject work that arrives after shutdown has begun. Requests already
		// past this check are in flight and are allowed to finish below.
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if draining.Load() {
				w.Header().Set("Connection", "close")
				http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
				return
			}
			router.ServeHTTP(w, r)
		}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErrors := make(chan error, 1)
	go func() {
		slog.Info("starting HTTP server", "address", server.Addr)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP: %w", err)
		}
		return nil
	case <-signalCtx.Done():
		// Restore default signal handling so a second SIGINT/SIGTERM exits
		// immediately instead of waiting out the drain deadline.
		stop()
		draining.Store(true)
		slog.Info("shutdown requested; refusing new work and draining in-flight requests",
			"grace_period", shutdownGracePeriod)
	}

	// Shutdown closes the listener immediately, so no new work is claimed, and
	// then blocks until active handlers—and their transactions—have returned.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGracePeriod)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		// The grace period expired. Force connections closed so a stuck client
		// cannot keep the process alive; closing them cancels request contexts
		// so handlers roll back any transaction still open.
		_ = server.Close()
		return fmt.Errorf("shutdown HTTP: %w", err)
	}

	slog.Info("in-flight requests drained; closing database pool")
	return nil
}
