package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/fishstick/internal/config"
	"github.com/fishstick/internal/database"
	"github.com/fishstick/internal/database/sqlc"
	"github.com/fishstick/internal/testsupport"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestNewSchedulerConfig(t *testing.T) {
	for _, key := range []string{"SCHEDULER_INTERVAL", "SCHEDULER_BATCH_SIZE", "SCHEDULER_SHUTDOWN_GRACE", "SCHEDULER_HEALTH_ADDRESS"} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := config.NewSchedulerConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Interval != 5*time.Second || cfg.BatchSize != 100 || cfg.ShutdownGrace != 10*time.Second || cfg.HealthAddress != ":8081" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}

	tests := []struct {
		name, key, value string
	}{
		{"empty interval", "SCHEDULER_INTERVAL", ""},
		{"zero interval", "SCHEDULER_INTERVAL", "0s"},
		{"negative interval", "SCHEDULER_INTERVAL", "-1s"},
		{"invalid interval", "SCHEDULER_INTERVAL", "later"},
		{"empty batch", "SCHEDULER_BATCH_SIZE", ""},
		{"zero batch", "SCHEDULER_BATCH_SIZE", "0"},
		{"negative batch", "SCHEDULER_BATCH_SIZE", "-1"},
		{"invalid batch", "SCHEDULER_BATCH_SIZE", "many"},
		{"oversized batch", "SCHEDULER_BATCH_SIZE", "2147483648"},
		{"empty shutdown grace", "SCHEDULER_SHUTDOWN_GRACE", ""},
		{"zero shutdown grace", "SCHEDULER_SHUTDOWN_GRACE", "0s"},
		{"negative shutdown grace", "SCHEDULER_SHUTDOWN_GRACE", "-1s"},
		{"invalid shutdown grace", "SCHEDULER_SHUTDOWN_GRACE", "later"},
		{"empty health address", "SCHEDULER_HEALTH_ADDRESS", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(test.key, test.value)
			if _, err := config.NewSchedulerConfig(); err == nil {
				t.Fatalf("expected %s=%q to be rejected", test.key, test.value)
			}
		})
	}

	t.Setenv("SCHEDULER_INTERVAL", "250ms")
	t.Setenv("SCHEDULER_BATCH_SIZE", "17")
	t.Setenv("SCHEDULER_SHUTDOWN_GRACE", "3s")
	t.Setenv("SCHEDULER_HEALTH_ADDRESS", "127.0.0.1:0")
	cfg, err = config.NewSchedulerConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Interval != 250*time.Millisecond || cfg.BatchSize != 17 || cfg.ShutdownGrace != 3*time.Second || cfg.HealthAddress != "127.0.0.1:0" {
		t.Fatalf("environment values not applied: %+v", cfg)
	}
}

func TestProcessDueJobsOnceOccurrenceAndIgnoresFutureJobs(t *testing.T) {
	db := newSchedulerTestDB(t)
	ctx := context.Background()
	now := time.Date(2025, 1, 2, 12, 0, 0, 0, time.UTC)
	dueID := seedJob(t, db, "ONCE", now.Add(-time.Hour), "", "UTC")
	seedJob(t, db, "ONCE", now.Add(time.Hour), "", "UTC")

	count, err := processDueJobs(ctx, db, 10, now)
	if err != nil || count != 1 {
		t.Fatalf("processDueJobs() = (%d, %v), want (1, nil)", count, err)
	}
	assertScheduledOccurrence(t, db, dueID, now.Add(-time.Hour), now)
	var nextRun sql.NullTime
	if err := db.QueryRow(`SELECT next_run_at FROM jobs WHERE id = $1`, dueID).Scan(&nextRun); err != nil {
		t.Fatal(err)
	}
	if nextRun.Valid {
		t.Fatalf("exhausted ONCE job next_run_at = %v, want NULL", nextRun.Time)
	}

	count, err = processDueJobs(ctx, db, 10, now.Add(time.Minute))
	if err != nil || count != 0 {
		t.Fatalf("second poll = (%d, %v), want (0, nil)", count, err)
	}
}

func TestProcessDueJobsCronSkipsMissedTicksAndHonorsTimezone(t *testing.T) {
	t.Run("missed ticks", func(t *testing.T) {
		db := newSchedulerTestDB(t)
		now := time.Date(2025, 1, 2, 12, 13, 0, 0, time.UTC)
		id := seedJob(t, db, "CRON", time.Date(2025, 1, 2, 12, 5, 0, 0, time.UTC), "*/5 * * * *", "UTC")
		count, err := processDueJobs(context.Background(), db, 10, now)
		if err != nil || count != 1 {
			t.Fatalf("processDueJobs() = (%d, %v), want (1, nil)", count, err)
		}
		assertScheduledOccurrence(t, db, id, time.Date(2025, 1, 2, 12, 5, 0, 0, time.UTC), now)
		var got time.Time
		if err := db.QueryRow(`SELECT next_run_at FROM jobs WHERE id = $1`, id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		want := time.Date(2025, 1, 2, 12, 15, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("next_run_at = %s, want %s", got, want)
		}
	})

	t.Run("local timezone", func(t *testing.T) {
		db := newSchedulerTestDB(t)
		now := time.Date(2025, 1, 2, 15, 0, 0, 0, time.UTC)
		due := time.Date(2025, 1, 2, 14, 0, 0, 0, time.UTC) // 9 AM in New York.
		id := seedJob(t, db, "CRON", due, "0 9 * * *", "America/New_York")
		if count, err := processDueJobs(context.Background(), db, 10, now); err != nil || count != 1 {
			t.Fatalf("processDueJobs() = (%d, %v), want (1, nil)", count, err)
		}
		var got time.Time
		if err := db.QueryRow(`SELECT next_run_at FROM jobs WHERE id = $1`, id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		want := time.Date(2025, 1, 3, 14, 0, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("next_run_at = %s, want %s", got, want)
		}
	})
}

func TestProcessDueJobsRollbackKeepsOccurrenceDue(t *testing.T) {
	db := newSchedulerTestDB(t)
	now := time.Date(2025, 1, 2, 12, 0, 0, 0, time.UTC)
	id := seedJob(t, db, "ONCE", now.Add(-time.Hour), "", "UTC")
	if _, err := db.Exec(`
		CREATE FUNCTION reject_scheduler_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'forced outbox failure'; END $$;
		CREATE TRIGGER reject_scheduler_outbox BEFORE INSERT ON outbox_events
		FOR EACH ROW EXECUTE FUNCTION reject_scheduler_outbox();`); err != nil {
		t.Fatal(err)
	}
	if _, err := processDueJobs(context.Background(), db, 10, now); err == nil {
		t.Fatal("expected forced outbox insertion failure")
	}
	var runCount, outboxCount int
	var nextRun time.Time
	if err := db.QueryRow(`SELECT count(*) FROM job_runs WHERE job_id = $1`, id).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM outbox_events`).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT next_run_at FROM jobs WHERE id = $1`, id).Scan(&nextRun); err != nil {
		t.Fatal(err)
	}
	if runCount != 0 || outboxCount != 0 || !nextRun.Equal(now.Add(-time.Hour)) {
		t.Fatalf("rollback state: runs=%d outbox=%d next_run_at=%s", runCount, outboxCount, nextRun)
	}
}

func TestProcessDueJobsRejectsInvalidCronAndTimezoneAtomically(t *testing.T) {
	for _, test := range []struct {
		name, expression, timezone string
	}{
		{"invalid cron", "not a cron", "UTC"},
		{"invalid timezone", "0 * * * *", "Not/A_Timezone"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := newSchedulerTestDB(t)
			now := time.Date(2025, 1, 2, 12, 0, 0, 0, time.UTC)
			id := seedJob(t, db, "CRON", now.Add(-time.Hour), test.expression, test.timezone)
			if _, err := processDueJobs(context.Background(), db, 10, now); err == nil {
				t.Fatal("expected invalid schedule to fail")
			}
			var count int
			var next time.Time
			if err := db.QueryRow(`SELECT count(*) FROM job_runs WHERE job_id = $1`, id).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT next_run_at FROM jobs WHERE id = $1`, id).Scan(&next); err != nil {
				t.Fatal(err)
			}
			if count != 0 || !next.Equal(now.Add(-time.Hour)) {
				t.Fatalf("invalid schedule changed state: runs=%d next_run_at=%s", count, next)
			}
		})
	}
}

func TestProcessDueJobsKeepsValidJobsWhenAnotherScheduleIsInvalid(t *testing.T) {
	db := newSchedulerTestDB(t)
	now := time.Date(2025, 1, 2, 12, 0, 0, 0, time.UTC)
	badID := seedJob(t, db, "CRON", now.Add(-2*time.Hour), "not a cron", "UTC")
	goodID := seedJob(t, db, "ONCE", now.Add(-time.Hour), "", "UTC")
	count, err := processDueJobs(context.Background(), db, 10, now)
	if err == nil || count != 1 {
		t.Fatalf("processDueJobs() = (%d, %v), want (1, error)", count, err)
	}
	var badRuns int
	var badNext time.Time
	if err := db.QueryRow(`SELECT count(*) FROM job_runs WHERE job_id = $1`, badID).Scan(&badRuns); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT next_run_at FROM jobs WHERE id = $1`, badID).Scan(&badNext); err != nil {
		t.Fatal(err)
	}
	if badRuns != 0 || !badNext.Equal(now.Add(-2*time.Hour)) {
		t.Fatalf("invalid job changed: runs=%d next_run_at=%s", badRuns, badNext)
	}
	assertScheduledOccurrence(t, db, goodID, now.Add(-time.Hour), now)
}

func TestProcessDueJobsBatchLimit(t *testing.T) {
	db := newSchedulerTestDB(t)
	now := time.Date(2025, 1, 2, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		seedJob(t, db, "ONCE", now.Add(-time.Hour), "", "UTC")
	}
	count, err := processDueJobs(context.Background(), db, 2, now)
	if err != nil || count != 2 {
		t.Fatalf("processDueJobs() = (%d, %v), want (2, nil)", count, err)
	}
	var runs int
	if err := db.QueryRow(`SELECT count(*) FROM job_runs`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 2 {
		t.Fatalf("created %d runs, want 2", runs)
	}
}

func TestProcessDueJobsSkipsRowsLockedByAnotherScheduler(t *testing.T) {
	db := newSchedulerTestDB(t)
	now := time.Date(2025, 1, 2, 12, 0, 0, 0, time.UTC)
	seedJob(t, db, "ONCE", now.Add(-time.Hour), "", "UTC")
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	_, err = sqlc.New(db).WithTx(tx).ListDueScheduledJobs(context.Background(), sqlc.ListDueScheduledJobsParams{
		Now: sql.NullTime{Time: now, Valid: true}, BatchSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	count, err := processDueJobs(context.Background(), db, 10, now)
	if err != nil || count != 0 {
		t.Fatalf("poll with locked row = (%d, %v), want (0, nil)", count, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	count, err = processDueJobs(context.Background(), db, 10, now)
	if err != nil || count != 1 {
		t.Fatalf("poll after lock release = (%d, %v), want (1, nil)", count, err)
	}
}

func TestCreateOccurrenceIsIdempotentForSameOccurrence(t *testing.T) {
	db := newSchedulerTestDB(t)
	ctx := context.Background()
	now := time.Date(2025, 1, 2, 12, 0, 0, 0, time.UTC)
	id := seedJob(t, db, "ONCE", now.Add(-time.Hour), "", "UTC")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	qtx := sqlc.New(db).WithTx(tx)
	job := dueJob{
		id: id, eventType: "test.event", payload: json.RawMessage(`{"ok":true}`),
		scheduleType: "ONCE", timezone: "UTC", nextRunAt: now.Add(-time.Hour),
		maxAttempts: 3, timeoutSeconds: 120,
	}
	if err := createOccurrence(ctx, qtx, job, now); err != nil {
		t.Fatal(err)
	}
	if err := createOccurrence(ctx, qtx, job, now); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var runs, outbox int
	if err := db.QueryRow(`SELECT count(*) FROM job_runs WHERE job_id = $1`, id).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM outbox_events`).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if runs != 1 || outbox != 1 {
		t.Fatalf("duplicate occurrence produced runs=%d outbox=%d, want 1 each", runs, outbox)
	}
}

func newSchedulerTestDB(t *testing.T) *sql.DB {
	t.Helper()
	rawURL := testsupport.DatabaseURL(t)
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Fatal("TEST_DATABASE_URL must be a postgres:// or postgresql:// URL")
	}
	admin, err := sql.Open("pgx", rawURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	schema := "scheduler_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec("DROP SCHEMA " + schema + " CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})
	query := u.Query()
	query.Set("search_path", schema)
	query.Set("application_name", schema)
	u.RawQuery = query.Encode()
	db, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := database.RunMigrations(db, "up"); err != nil {
		t.Fatal(err)
	}
	return db
}

func seedJob(t *testing.T, db *sql.DB, scheduleType string, nextRun time.Time, expression, timezone string) uuid.UUID {
	t.Helper()
	id := uuid.Must(uuid.NewV7())
	var scheduledAt any
	var cronExpression any
	switch scheduleType {
	case "ONCE":
		scheduledAt = nextRun
	case "CRON":
		cronExpression = expression
	default:
		t.Fatalf("unsupported fixture schedule type %q", scheduleType)
	}
	_, err := db.Exec(`
		INSERT INTO jobs (
			id, name, event_type, payload, schedule_type, scheduled_at,
			cron_expression, timezone, next_run_at, max_attempts, retry_delay_seconds, timeout_seconds
		) VALUES ($1, $2, 'test.event', '{"ok":true}', $3, $4, $5, $6, $7, 3, 17, 120)`,
		id, "scheduler test "+id.String(), scheduleType, scheduledAt, cronExpression, timezone, nextRun)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func assertScheduledOccurrence(t *testing.T, db *sql.DB, jobID uuid.UUID, scheduledFor, createdAt time.Time) {
	t.Helper()
	var runID uuid.UUID
	var source, status string
	var gotScheduledFor, gotCreatedAt time.Time
	var retryDelaySeconds int32
	if err := db.QueryRow(`
		SELECT id, trigger_source, status, scheduled_for, created_at, retry_delay_seconds
		FROM job_runs WHERE job_id = $1`, jobID).Scan(&runID, &source, &status, &gotScheduledFor, &gotCreatedAt, &retryDelaySeconds); err != nil {
		t.Fatal(err)
	}
	if source != "SCHEDULED" || status != "QUEUED" || !gotScheduledFor.Equal(scheduledFor) {
		t.Fatalf("run source=%q status=%q scheduled_for=%s", source, status, gotScheduledFor)
	}
	if !gotCreatedAt.Equal(createdAt) {
		t.Fatalf("run created_at = %s, want %s", gotCreatedAt, createdAt)
	}
	if retryDelaySeconds != 17 {
		t.Fatalf("run retry_delay_seconds=%d, want job snapshot 17", retryDelaySeconds)
	}
	var eventRunID uuid.UUID
	var eventKind, eventStatus string
	var message []byte
	if err := db.QueryRow(`
		SELECT run_id, event_kind, status, message FROM outbox_events WHERE run_id = $1`, runID).
		Scan(&eventRunID, &eventKind, &eventStatus, &message); err != nil {
		t.Fatal(err)
	}
	if eventRunID != runID || eventKind != "JOB_DISPATCH" || eventStatus != "PENDING" {
		t.Fatalf("outbox run=%s kind=%q status=%q", eventRunID, eventKind, eventStatus)
	}
	var body struct {
		RunID string `json:"run_id"`
	}
	if err := json.Unmarshal(message, &body); err != nil {
		t.Fatal(err)
	}
	if body.RunID != runID.String() {
		t.Fatalf("outbox message run_id = %q, want %q", body.RunID, runID)
	}
}
