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
	"github.com/rabbitmq/amqp091-go"
)

type Publisher interface {
	Publish(context.Context, []byte) error
}
type amqpPublisher struct {
	ch                *amqp091.Channel
	confirms          <-chan amqp091.Confirmation
	exchange, routing string
}

func (p *amqpPublisher) Publish(ctx context.Context, body []byte) error {
	if err := p.ch.PublishWithContext(ctx, p.exchange, p.routing, true, false, amqp091.Publishing{ContentType: "application/json", DeliveryMode: amqp091.Persistent, Body: body, Timestamp: time.Now().UTC()}); err != nil {
		return err
	}
	select {
	case confirmation, ok := <-p.confirms:
		if !ok {
			return errors.New("RabbitMQ publisher confirmation channel closed")
		}
		if !confirmation.Ack {
			return errors.New("RabbitMQ rejected message")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func processBatch(ctx context.Context, db *sql.DB, publisher Publisher, workerID string, batchSize int, lockTTL time.Duration) (int, error) {
	// [1] Claim rows in one short SQL statement so parallel relays divide work safely.
	q := sqlc.New(db)
	rows, err := q.ClaimOutboxBatch(ctx, sqlc.ClaimOutboxBatchParams{LockedBy: sql.NullString{String: workerID, Valid: true}, LockSeconds: int32(lockTTL.Seconds()), BatchSize: int32(batchSize)})
	if err != nil {
		return 0, fmt.Errorf("claim outbox batch: %w", err)
	}
	completed := 0
	var failures error
	for _, row := range rows {
		// [2] Broker confirmation comes before marking the outbox record published.
		if err := publisher.Publish(ctx, row.Message); err != nil {
			// [3] Failures return to PENDING with bounded backoff; expired locks are reclaimed by the next claim.
			_, releaseErr := q.ReleaseOutboxClaim(ctx, sqlc.ReleaseOutboxClaimParams{ID: row.ID, LockedBy: sql.NullString{String: workerID, Valid: true}, DelaySeconds: int32(backoff(row.PublishAttempts).Seconds()), LastError: sql.NullString{String: err.Error(), Valid: true}})
			if releaseErr != nil {
				return completed, errors.Join(fmt.Errorf("publish event %s: %w", row.ID, err), fmt.Errorf("release claim: %w", releaseErr))
			}
			failures = errors.Join(failures, fmt.Errorf("publish event %s: %w", row.ID, err))
			continue
		}
		n, err := q.CompleteOutboxPublish(ctx, sqlc.CompleteOutboxPublishParams{ID: row.ID, LockedBy: sql.NullString{String: workerID, Valid: true}})
		if err != nil {
			return completed, fmt.Errorf("complete publish %s: %w", row.ID, err)
		}
		if n != 1 {
			return completed, fmt.Errorf("publish claim %s was lost", row.ID)
		}
		completed++
	}
	return completed, failures
}

func backoff(attempts int32) time.Duration {
	d := time.Second << min(attempts, 8)
	if d > 5*time.Minute {
		return 5 * time.Minute
	}
	return d
}

func main() {
	// [4] Load shared database settings and relay-specific transport settings.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	dbCfg, err := config.NewConfig()
	if err != nil {
		logger.Error("load database config", "error", err)
		os.Exit(1)
	}
	cfg, err := config.NewRelayConfig()
	if err != nil {
		logger.Error("load relay config", "error", err)
		os.Exit(1)
	}
	workerID := instanceid.New(cfg.WorkerID)
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
	if _, err = messaging.DeclareDispatchQueue(ch, cfg.Exchange, cfg.Queue, cfg.RoutingKey); err != nil {
		logger.Error("declare dispatch queue", "error", err)
		os.Exit(1)
	}
	if err = ch.Confirm(false); err != nil {
		logger.Error("enable publisher confirms", "error", err)
		os.Exit(1)
	}
	pub := &amqpPublisher{ch: ch, confirms: ch.NotifyPublish(make(chan amqp091.Confirmation, 1)), exchange: cfg.Exchange, routing: cfg.RoutingKey}
	// [5] Start a single poll loop so a slow publish never overlaps a batch in this process.
	logger.Info("relay started", "interval", cfg.Interval, "batch_size", cfg.BatchSize)
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := processBatch(ctx, db, pub, workerID, cfg.BatchSize, cfg.LockTTL)
			if err != nil {
				logger.Error("relay poll failed", "error", err)
			}
			if count > 0 {
				logger.Info("outbox events published", "count", count)
			}
		}
	}
}
