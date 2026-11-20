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
	"github.com/fishstick/internal/instanceid"
	"github.com/fishstick/internal/messaging"
	"github.com/fishstick/internal/runstatus"
	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
)

type Handler func(context.Context, sqlc.JobRun) ([]byte, error)

// The development handler succeeds with an empty result. Deployments should
// register event-specific handlers for their own event types.
var handlers = map[string]Handler{"*": func(context.Context, sqlc.JobRun) ([]byte, error) { return []byte(`{}`), nil }}

func developmentHandler(delay time.Duration) Handler {
	return func(ctx context.Context, _ sqlc.JobRun) ([]byte, error) {
		if delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
		return []byte(`{}`), nil
	}
}

func settleDelivery(logger *slog.Logger, delivery amqp091.Delivery, acknowledge, requeue bool) {
	var err error
	if acknowledge {
		err = delivery.Ack(false)
	} else {
		err = delivery.Nack(false, requeue)
	}
	if err != nil {
		logger.Error("settle RabbitMQ delivery", "acknowledge", acknowledge, "requeue", requeue, "error", err)
	}
}

func claimRun(ctx context.Context, db *sql.DB, runID uuid.UUID, workerID string, lease time.Duration) (sqlc.JobRun, error) {
	// [1] Lock the job before the run, matching API job-update serialization.
	// The SQL claim increments fencing_token so a reclaimed lease rejects old workers.
	tx, q, err := database.BeginQueries(ctx, db, nil)
	if err != nil {
		return sqlc.JobRun{}, err
	}
	defer tx.Rollback()
	run, err := database.LockJobRunForUpdate(ctx, q, runID)
	if err != nil {
		return sqlc.JobRun{}, err
	}
	claimed, err := q.ClaimRunToRunning(ctx, sqlc.ClaimRunToRunningParams{ID: runID, JobID: run.JobID, LeaseExpiresAt: sql.NullTime{Time: time.Now().Add(lease).UTC(), Valid: true}, WorkerID: sql.NullString{String: workerID, Valid: true}})
	if err != nil {
		return sqlc.JobRun{}, err
	}
	if err = tx.Commit(); err != nil {
		return sqlc.JobRun{}, err
	}
	return claimed, nil
}

func processRun(ctx context.Context, db *sql.DB, runID uuid.UUID, workerID string, lease, heartbeat, shutdownGrace time.Duration, handler Handler) error {
	// [2] Redelivered messages wait for the current lease, then either reclaim
	// the run with a new fencing token or recover an expired terminal path.
	var run sqlc.JobRun
	for {
		claimed, err := claimRun(ctx, db, runID, workerID, lease)
		if err == nil {
			run = claimed
			break
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("claim run: %w", err)
		}
		current, readErr := sqlc.New(db).GetJobRunByID(ctx, runID)
		if errors.Is(readErr, sql.ErrNoRows) {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("read unclaimed run: %w", readErr)
		}
		if current.Status == runstatus.CancelRequested && (!current.LeaseExpiresAt.Valid || !current.LeaseExpiresAt.Time.After(time.Now())) {
			if err := completeExpiredCancellation(ctx, db, runID); err != nil {
				return err
			}
			return nil
		}
		if current.Status == runstatus.Running && (!current.LeaseExpiresAt.Valid || !current.LeaseExpiresAt.Time.After(time.Now())) {
			if err := failExpiredRun(ctx, db, runID); err != nil {
				return err
			}
			continue
		}
		waitUntil := time.Time{}
		if (current.Status == runstatus.Running || current.Status == runstatus.CancelRequested) && current.LeaseExpiresAt.Valid {
			waitUntil = current.LeaseExpiresAt.Time
		}
		if current.Status == runstatus.RetryWait && current.RetryAt.Valid {
			waitUntil = current.RetryAt.Time
		}
		if waitUntil.IsZero() {
			return nil
		}
		wait := time.Until(waitUntil)
		if wait <= 0 {
			continue
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if handler == nil {
		return errors.New("no handler registered")
	}
	hctx, cancel := context.WithTimeout(ctx, time.Duration(run.TimeoutSeconds)*time.Second)
	defer cancel()
	type handlerResult struct {
		data []byte
		err  error
	}
	resultCh := make(chan handlerResult, 1)
	go func() { data, err := handler(hctx, run); resultCh <- handlerResult{data, err} }()
	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			cancel()
			shutdownTimer := time.NewTimer(shutdownGrace)
			defer shutdownTimer.Stop()
			select {
			case result := <-resultCh:
				return finishRun(context.Background(), db, run, workerID, result.data, result.err)
			case <-shutdownTimer.C:
				return ctx.Err()
			}
		case <-ticker.C:
			// [3] Renew only while this worker still owns the fencing token; observe cancellation on each tick.
			q := sqlc.New(db)
			n, err := q.HeartbeatRun(ctx, sqlc.HeartbeatRunParams{ID: run.ID, WorkerID: sql.NullString{String: workerID, Valid: true}, FencingToken: run.FencingToken, LeaseExpiresAt: sql.NullTime{Time: time.Now().Add(lease).UTC(), Valid: true}})
			if err != nil {
				return fmt.Errorf("heartbeat run: %w", err)
			}
			if n == 0 {
				return errors.New("heartbeat rejected by fencing guard")
			}
			current, err := q.GetJobRunByIDForUpdate(ctx, run.ID)
			if err != nil {
				return fmt.Errorf("read run after heartbeat: %w", err)
			}
			if current.Status == runstatus.CancelRequested {
				cancel()
			}
		case result := <-resultCh:
			return finishRun(ctx, db, run, workerID, result.data, result.err)
		}
	}
}

func finishRun(ctx context.Context, db *sql.DB, run sqlc.JobRun, workerID string, result []byte, handlerErr error) error {
	// [4] Conditional terminal writes prevent stale handlers from winning completion races.
	q := sqlc.New(db)
	owner := sql.NullString{String: workerID, Valid: true}
	current, err := q.GetJobRunByIDForUpdate(ctx, run.ID)
	if err != nil {
		return err
	}
	if current.Status == runstatus.CancelRequested {
		n, err := q.CompleteRunCancelled(ctx, sqlc.CompleteRunCancelledParams{ID: run.ID, WorkerID: owner, FencingToken: run.FencingToken})
		if err != nil {
			return err
		}
		if n == 0 {
			return errors.New("cancel completion rejected by fencing guard")
		}
		return nil
	}
	if handlerErr == nil {
		n, err := q.CompleteRunSucceeded(ctx, sqlc.CompleteRunSucceededParams{ID: run.ID, WorkerID: owner, FencingToken: run.FencingToken, Result: sql.Null[[]byte]{V: result, Valid: len(result) > 0}})
		if err != nil {
			return err
		}
		if n == 0 {
			n, err = q.CompleteRunCancelled(ctx, sqlc.CompleteRunCancelledParams{ID: run.ID, WorkerID: owner, FencingToken: run.FencingToken})
			if err != nil {
				return err
			}
			if n == 0 {
				return errors.New("success completion rejected by fencing guard")
			}
			return nil
		}
		return nil
	}
	tx, qtx, err := database.BeginQueries(ctx, db, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// [5] Retry scheduling or permanent failure plus its DLQ row commit together.
	message := sql.NullString{String: handlerErr.Error(), Valid: true}
	n, err := qtx.ScheduleRunRetry(ctx, sqlc.ScheduleRunRetryParams{ID: run.ID, WorkerID: owner, FencingToken: run.FencingToken, LastError: message})
	if err != nil {
		return err
	}
	if n == 0 {
		n, err = qtx.FailRunPermanent(ctx, sqlc.FailRunPermanentParams{ID: run.ID, WorkerID: owner, FencingToken: run.FencingToken, LastError: message})
		if err != nil {
			return err
		}
		if n == 0 {
			n, err = qtx.CompleteRunCancelled(ctx, sqlc.CompleteRunCancelledParams{ID: run.ID, WorkerID: owner, FencingToken: run.FencingToken})
			if err != nil {
				return err
			}
			if n == 0 {
				return errors.New("failure completion rejected by fencing guard")
			}
			return tx.Commit()
		}
		if err = qtx.InsertDeadLetter(ctx, run.ID); err != nil {
			return err
		}
	} else {
		retryEventID, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("generate retry outbox id: %w", err)
		}
		dispatchBody, err := messaging.EncodeDispatch(run.ID)
		if err != nil {
			return fmt.Errorf("encode retry dispatch: %w", err)
		}
		if err = qtx.InsertRetryDispatch(ctx, sqlc.InsertRetryDispatchParams{ID: retryEventID, RunID: run.ID, Message: dispatchBody}); err != nil {
			return fmt.Errorf("enqueue retry dispatch: %w", err)
		}
	}
	return tx.Commit()
}

func failExpiredRun(ctx context.Context, db *sql.DB, runID uuid.UUID) error {
	// [6] A crashed worker on its final try has no handler left to report failure.
	// The lease guard and DLQ insert run in one transaction when its message returns.
	tx, q, err := database.BeginQueries(ctx, db, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	n, err := q.FailExpiredRun(ctx, runID)
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	if err = q.InsertDeadLetter(ctx, runID); err != nil {
		return err
	}
	return tx.Commit()
}

func completeExpiredCancellation(ctx context.Context, db *sql.DB, runID uuid.UUID) error {
	tx, q, err := database.BeginQueries(ctx, db, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	n, err := q.CompleteExpiredRunCancelled(ctx, runID)
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	return tx.Commit()
}

func main() {
	// [7] Connect to Postgres and RabbitMQ before accepting dispatches.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	dbCfg, err := config.NewConfig()
	if err != nil {
		logger.Error("load database config", "error", err)
		os.Exit(1)
	}
	cfg, err := config.NewWorkerConfig()
	if err != nil {
		logger.Error("load worker config", "error", err)
		os.Exit(1)
	}
	workerID := instanceid.New(cfg.WorkerID)
	handlers["*"] = developmentHandler(cfg.StubDelay)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := database.NewPostgres(ctx, dbCfg)
	if err != nil {
		logger.Error("connect database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	conn, err := amqp091.Dial(cfg.RabbitURL)
	if err != nil {
		logger.Error("connect RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		logger.Error("open RabbitMQ channel", "error", err)
		os.Exit(1)
	}
	defer ch.Close()
	queue, err := messaging.DeclareDispatchQueue(ch, cfg.Exchange, cfg.Queue, cfg.RoutingKey)
	if err != nil {
		logger.Error("declare dispatch queue", "error", err)
		os.Exit(1)
	}
	if err = ch.Qos(cfg.Prefetch, 0, false); err != nil {
		logger.Error("set consumer prefetch", "error", err)
		os.Exit(1)
	}
	deliveries, err := ch.Consume(queue.Name, workerID, false, false, false, false, nil)
	if err != nil {
		logger.Error("start consumer", "error", err)
		os.Exit(1)
	}
	logger.Info("worker started", "queue", queue.Name, "worker_id", workerID)
	// [8] Ack only after the database transition succeeds; requeue transient failures.
	for {
		select {
		case <-ctx.Done():
			return
		case delivery, ok := <-deliveries:
			if !ok {
				logger.Error("RabbitMQ consumer stopped")
				return
			}
			runID, err := messaging.DecodeDispatch(delivery.Body)
			if err != nil {
				logger.Error("decode dispatch message", "error", err)
				settleDelivery(logger, delivery, false, false)
				continue
			}
			run, err := sqlc.New(db).GetJobRunByID(ctx, runID)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					settleDelivery(logger, delivery, true, false)
				} else {
					settleDelivery(logger, delivery, false, true)
				}
				continue
			}
			handler := handlers[run.EventTypeSnapshot]
			if handler == nil {
				handler = handlers["*"]
			}
			if err := processRun(ctx, db, runID, workerID, cfg.LeaseDuration, cfg.HeartbeatInterval, cfg.ShutdownGrace, handler); err != nil {
				logger.Error("run processing failed", "run_id", runID, "error", err)
				settleDelivery(logger, delivery, false, true)
			} else {
				settleDelivery(logger, delivery, true, false)
			}
		}
	}
}
