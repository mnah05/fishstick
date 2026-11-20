package process

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// PollerConfig describes the runtime shared by database-backed polling
// processes. Process-specific work remains in the supplied poll function.
type PollerConfig struct {
	Name          string
	Interval      time.Duration
	BatchSize     int
	ShutdownGrace time.Duration
	HealthAddress string
}

// RunPoller serves PostgreSQL readiness and runs one batch at a time until ctx
// is cancelled or the health listener fails. Readiness follows database
// availability, and a slow poll never overlaps the next batch.
func RunPoller(ctx context.Context, db *sql.DB, logger *slog.Logger, cfg PollerConfig, poll func(context.Context, int) (int, error), report func(int, error)) error {
	health := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			http.NotFound(w, r)
			return
		}
		pingCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.PingContext(pingCtx); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	server := &http.Server{Addr: cfg.HealthAddress, Handler: health, ReadHeaderTimeout: 5 * time.Second}
	serverErr := make(chan error, 1)
	go func() {
		logger.Info(cfg.Name+" health endpoint listening", "address", cfg.HealthAddress)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	logger.Info(cfg.Name+" started", "interval", cfg.Interval, "batch_size", cfg.BatchSize)
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
			if err := server.Shutdown(shutdownCtx); err != nil {
				logger.Error("shutdown health endpoint", "process", cfg.Name, "error", err)
			}
			cancel()
			logger.Info(cfg.Name + " stopped")
			return nil
		case err := <-serverErr:
			return fmt.Errorf("%s health endpoint failed: %w", cfg.Name, err)
		case <-ticker.C:
			count, err := poll(ctx, cfg.BatchSize)
			report(count, err)
		}
	}
}
