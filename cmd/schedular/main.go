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
	"time"

	"github.com/fishstick/internal/config"
	"github.com/fishstick/internal/database"
	"github.com/fishstick/internal/database/sqlc"
	"github.com/fishstick/internal/messaging"
	"github.com/fishstick/internal/process"
	"github.com/fishstick/internal/schedule"
	"github.com/google/uuid"
)

// dueJob is the stable snapshot read while its database row is locked. The
// scheduler copies these values into the run so later job edits cannot change
// an already-created occurrence.
type dueJob struct {
	id                uuid.UUID
	eventType         string
	payload           []byte
	scheduleType      string
	cronExpression    sql.NullString
	timezone          string
	nextRunAt         time.Time
	maxAttempts       int32
	retryDelaySeconds int32
	timeoutSeconds    int32
}

func main() {
	// [1] Load database and scheduler settings, then open the PostgreSQL pool.
	// slog's JSON handler keeps process logs machine-readable for log collectors.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.NewConfig()
	if err != nil {
		logger.Error("load configuration", "error", err)
		os.Exit(1)
	}
	schedCfg, err := config.NewSchedulerConfig()
	if err != nil {
		logger.Error("load scheduler configuration", "error", err)
		os.Exit(1)
	}

	// [2] Watch for shutdown signals so polling and health serving can stop cleanly.
	// The same cancellation signal stops both polling and any in-flight database
	// work, then the HTTP health server gets a bounded graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := database.NewPostgres(ctx, cfg)
	if err != nil {
		logger.Error("connect database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	// [3] Run the shared poller, which handles intervals, health checks, and shutdown.
	err = process.RunPoller(ctx, db, logger, process.PollerConfig{
		Name: "scheduler", Interval: schedCfg.Interval, BatchSize: schedCfg.BatchSize,
		ShutdownGrace: schedCfg.ShutdownGrace, HealthAddress: schedCfg.HealthAddress,
	}, func(ctx context.Context, batchSize int) (int, error) {
		// [4] Take one UTC timestamp for this batch and process jobs due by that time.
		return processDueJobs(ctx, db, batchSize, time.Now().UTC())
	}, func(count int, err error) {
		if err != nil && ctx.Err() == nil {
			logger.Error("scheduler poll failed", "error", err)
		}
		if count > 0 {
			logger.Info("scheduled occurrences created", "count", count)
		}
	})
	if err != nil {
		logger.Error("scheduler stopped", "error", err)
	}
}

func processDueJobs(ctx context.Context, db *sql.DB, batchSize int, now time.Time) (int, error) {
	// [5] Begin one transaction for this batch. Its row locks stay held until
	// the batch commits. Each job uses a savepoint so one bad schedule rolls
	// back only that occurrence and leaves it due.
	tx, qtx, err := database.BeginQueries(ctx, db, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("begin scheduler transaction: %w", err)
	}
	defer tx.Rollback()
	// [6] Select due rows and lock them with FOR UPDATE SKIP LOCKED.
	// sqlc binds generated query methods to this transaction. The generated
	// query uses SKIP LOCKED, so competing schedulers take different rows.
	jobs, err := qtx.ListDueScheduledJobs(ctx, sqlc.ListDueScheduledJobsParams{
		Now:       sql.NullTime{Time: now, Valid: true},
		BatchSize: int32(batchSize),
	})
	if err != nil {
		return 0, fmt.Errorf("select due jobs: %w", err)
	}

	var failed error
	done := 0
	for _, job := range jobs {
		if !job.NextRunAt.Valid {
			failed = errors.Join(failed, fmt.Errorf("due job %s has no next_run_at", job.ID))
			continue
		}
		if err := qtx.BeginSchedulerOccurrence(ctx); err != nil {
			return 0, fmt.Errorf("begin job savepoint: %w", err)
		}
		// [7] Build the immutable snapshot that will be stored on this run.
		occurrence := dueJob{
			id: job.ID, eventType: job.EventType, payload: job.Payload,
			scheduleType: job.ScheduleType, cronExpression: job.CronExpression,
			timezone: job.Timezone, nextRunAt: job.NextRunAt.Time,
			maxAttempts: job.MaxAttempts, retryDelaySeconds: job.RetryDelaySeconds, timeoutSeconds: job.TimeoutSeconds,
		}
		if err := createOccurrence(ctx, qtx, occurrence, now); err != nil {
			if rollbackErr := qtx.RollbackSchedulerOccurrence(ctx); rollbackErr != nil {
				return done, fmt.Errorf("roll back job %s: %w", occurrence.id, rollbackErr)
			}
			if releaseErr := qtx.ReleaseSchedulerOccurrence(ctx); releaseErr != nil {
				return done, fmt.Errorf("release failed job savepoint: %w", releaseErr)
			}
			failed = errors.Join(failed, fmt.Errorf("process job %s: %w", occurrence.id, err))
			continue
		}
		if err := qtx.ReleaseSchedulerOccurrence(ctx); err != nil {
			return done, fmt.Errorf("release job savepoint: %w", err)
		}
		done++
	}
	// [8] Commit the occurrences that succeeded. Failed jobs stay due.
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit scheduler transaction: %w", err)
	}
	return done, failed
}

func createOccurrence(ctx context.Context, qtx *sqlc.Queries, job dueJob, now time.Time) error {
	// [9] Calculate the schedule after this occurrence: clear ONCE jobs, while
	// cron jobs move directly to their next future tick after downtime.
	var nextRun sql.NullTime
	switch job.scheduleType {
	case "ONCE":
		// The occurrence consumes this one-time schedule. Migration 00003 allows
		// NULL here to represent that it has been exhausted.
		nextRun = sql.NullTime{}
	case "CRON":
		if !job.cronExpression.Valid {
			return fmt.Errorf("cron job has no expression")
		}
		// Compute from now so ticks missed during downtime are skipped. The
		// shared calculator uses the job's IANA timezone, including DST.
		next, err := schedule.NextCron(job.cronExpression.String, job.timezone, now)
		if err != nil {
			return err
		}
		nextRun = sql.NullTime{Time: next, Valid: true}
	default:
		return fmt.Errorf("unsupported due schedule type %q", job.scheduleType)
	}

	// [10] Create (or reuse by unique occurrence key) the queued scheduled run.
	runID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("generate run ID: %w", err)
	}
	outboxID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("generate outbox ID: %w", err)
	}
	// The partial unique index is the final duplicate guard for an occurrence.
	// QUEUED means the run can be dispatched now; SCHEDULED is its source.
	if err := qtx.CreateScheduledRun(ctx, sqlc.CreateScheduledRunParams{
		ID: runID, JobID: job.id, ScheduledFor: job.nextRunAt,
		EventTypeSnapshot: job.eventType, PayloadSnapshot: job.payload,
		TimeoutSeconds: job.timeoutSeconds, RetryDelaySeconds: job.retryDelaySeconds, MaxAttempts: job.maxAttempts, CreatedAt: now,
	}); err != nil {
		return fmt.Errorf("insert scheduled run: %w", err)
	}
	// Resolve the run by its unique occurrence key. This also finds the original
	// ID when the insert hit the uniqueness guard, so the message points to the
	// persisted run rather than a newly generated, unused ID.
	// [11] Add one pending dispatch event that refers to the persisted run ID.
	actualRunID, err := qtx.GetScheduledRunID(ctx, sqlc.GetScheduledRunIDParams{
		JobID: job.id, ScheduledFor: job.nextRunAt,
	})
	if err != nil {
		return fmt.Errorf("read scheduled run: %w", err)
	}
	message, err := messaging.EncodeDispatch(actualRunID)
	if err != nil {
		return fmt.Errorf("encode dispatch message: %w", err)
	}
	// Store dispatch intent in the outbox in the same transaction as the run.
	// A separate relay can publish it later without risking a lost event.
	if err := qtx.CreateScheduledDispatchOutbox(ctx, sqlc.CreateScheduledDispatchOutboxParams{
		ID: outboxID, RunID: actualRunID, Message: message, CreatedAt: now,
	}); err != nil {
		return fmt.Errorf("insert dispatch outbox event: %w", err)
	}
	// [12] Advance the job only after its run and dispatch intent are recorded.
	if err := qtx.AdvanceJobSchedule(ctx, sqlc.AdvanceJobScheduleParams{ID: job.id, NextRunAt: nextRun}); err != nil {
		return fmt.Errorf("advance job schedule: %w", err)
	}
	return nil
}
