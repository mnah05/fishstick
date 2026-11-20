package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/fishstick/internal/config"
	"github.com/fishstick/internal/database"
	"github.com/fishstick/internal/database/sqlc"
	"github.com/fishstick/internal/messaging"
	"github.com/fishstick/internal/process"
	"github.com/fishstick/internal/runstatus"
	"github.com/google/uuid"
)

func recoverExpiredRun(ctx context.Context, db *sql.DB, runID uuid.UUID) (bool, error) {
	tx, q, err := database.BeginQueries(ctx, db, nil)
	if err != nil {
		return false, fmt.Errorf("begin recovery transaction: %w", err)
	}
	defer tx.Rollback()

	// Keep the same job-before-run lock order as worker claims and API updates.
	run, err := database.LockJobRunForUpdate(ctx, q, runID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("lock job and run: %w", err)
	}
	if run.Status != runstatus.Running {
		return false, nil
	}

	if run.AttemptCount >= run.MaxAttempts {
		n, err := q.FailExpiredRun(ctx, runID)
		if err != nil {
			return false, fmt.Errorf("fail expired run: %w", err)
		}
		if n == 0 {
			return false, nil
		}
		if err := q.InsertDeadLetter(ctx, runID); err != nil {
			return false, fmt.Errorf("insert dead-letter entry: %w", err)
		}
	} else {
		n, err := q.RetryExpiredRun(ctx, runID)
		if err != nil {
			return false, fmt.Errorf("schedule expired run retry: %w", err)
		}
		if n == 0 {
			return false, nil
		}
		eventID, err := uuid.NewV7()
		if err != nil {
			return false, fmt.Errorf("generate retry outbox id: %w", err)
		}
		message, err := messaging.EncodeDispatch(runID)
		if err != nil {
			return false, fmt.Errorf("encode retry dispatch: %w", err)
		}
		if err := q.InsertRetryDispatch(ctx, sqlc.InsertRetryDispatchParams{ID: eventID, RunID: runID, Message: message}); err != nil {
			return false, fmt.Errorf("enqueue retry dispatch: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit recovery: %w", err)
	}
	return true, nil
}

func processBatch(ctx context.Context, db *sql.DB, batchSize int) (int, error) {
	ids, err := sqlc.New(db).ListExpiredRunningRunIDs(ctx, int32(batchSize))
	if err != nil {
		return 0, fmt.Errorf("list expired runs: %w", err)
	}
	recovered := 0
	var failures error
	for _, id := range ids {
		ok, err := recoverExpiredRun(ctx, db, id)
		if err != nil {
			failures = errors.Join(failures, fmt.Errorf("recover run %s: %w", id, err))
			continue
		}
		if ok {
			recovered++
		}
	}
	return recovered, failures
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	dbCfg, err := config.NewConfig()
	if err != nil {
		logger.Error("load database config", "error", err)
		os.Exit(1)
	}
	cfg, err := config.NewWatcherConfig()
	if err != nil {
		logger.Error("load watcher config", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := database.NewPostgres(ctx, dbCfg)
	if err != nil {
		logger.Error("connect database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	err = process.RunPoller(ctx, db, logger, process.PollerConfig{
		Name: "watcher", Interval: cfg.Interval, BatchSize: cfg.BatchSize,
		ShutdownGrace: cfg.ShutdownGrace, HealthAddress: cfg.HealthAddress,
	}, func(ctx context.Context, batchSize int) (int, error) {
		return processBatch(ctx, db, batchSize)
	}, func(count int, err error) {
		if err != nil && ctx.Err() == nil {
			logger.Error("lease watcher poll failed", "error", err)
		}
		if count > 0 {
			logger.Info("expired runs recovered", "count", count)
		}
	})
	if err != nil {
		logger.Error("lease watcher stopped", "error", err)
	}
}
