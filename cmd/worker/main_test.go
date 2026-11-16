package main

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/fishstick/internal/database"
	"github.com/fishstick/internal/database/sqlc"
	"github.com/fishstick/internal/testsupport"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func workerTestDB(t *testing.T) *sql.DB {
	t.Helper()
	raw := testsupport.DatabaseURL(t)
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := sql.Open("pgx", raw)
	if err != nil {
		t.Fatal(err)
	}
	schema := "worker_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP SCHEMA " + schema + " CASCADE"); _ = admin.Close() })
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	db, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err = database.RunMigrations(db, "up"); err != nil {
		t.Fatal(err)
	}
	return db
}

func seedRun(t *testing.T, db *sql.DB, maxAttempts int) (uuid.UUID, uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	jobID, runID := uuid.New(), uuid.New()
	_, err := db.ExecContext(ctx, `INSERT INTO jobs(id,name,event_type,payload,schedule_type,timezone,max_attempts,retry_delay_seconds,timeout_seconds) VALUES($1,'worker test','test.event','{}','MANUAL','UTC',$2,0,30)`, jobID, maxAttempts)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO job_runs(id,job_id,scheduled_for,trigger_source,trigger_idempotency_key,trigger_request_hash,event_type_snapshot,payload_snapshot,timeout_seconds,max_attempts,status,attempt_count) VALUES($1,$2,NOW(),'MANUAL',$3,'hash','test.event','{}',30,$4,'QUEUED',0)`, runID, jobID, runID.String(), maxAttempts)
	if err != nil {
		t.Fatal(err)
	}
	return jobID, runID
}

func TestProcessRunSuccessAndRetryExhaustion(t *testing.T) {
	t.Run("success stores result and fencing fields", func(t *testing.T) {
		db := workerTestDB(t)
		_, runID := seedRun(t, db, 2)
		err := processRun(context.Background(), db, runID, "worker-a", time.Second, time.Hour, time.Second, func(context.Context, sqlc.JobRun) ([]byte, error) { return []byte(`{"ok":true}`), nil })
		if err != nil {
			t.Fatal(err)
		}
		var status string
		var attempts int
		var fence int64
		var result []byte
		if err = db.QueryRow(`SELECT status,attempt_count,fencing_token,result FROM job_runs WHERE id=$1`, runID).Scan(&status, &attempts, &fence, &result); err != nil {
			t.Fatal(err)
		}
		if status != "SUCCEEDED" || attempts != 1 || fence != 1 || string(result) != `{"ok": true}` && string(result) != `{"ok":true}` {
			t.Fatalf("state status=%s attempts=%d fence=%d result=%s", status, attempts, fence, result)
		}
	})
	t.Run("last failure writes DLQ atomically", func(t *testing.T) {
		db := workerTestDB(t)
		_, runID := seedRun(t, db, 2)
		fail := func(context.Context, sqlc.JobRun) ([]byte, error) { return nil, errors.New("planned failure") }
		if err := processRun(context.Background(), db, runID, "worker-a", time.Second, time.Hour, time.Second, fail); err != nil {
			t.Fatal(err)
		}
		var retryEvents int
		if err := db.QueryRow(`SELECT count(*) FROM outbox_events WHERE run_id=$1 AND status='PENDING'`, runID).Scan(&retryEvents); err != nil {
			t.Fatal(err)
		}
		if retryEvents != 1 {
			t.Fatalf("pending retry dispatches=%d, want 1", retryEvents)
		}
		if _, err := db.Exec(`UPDATE job_runs SET retry_at=NOW()-interval '1 second' WHERE id=$1`, runID); err != nil {
			t.Fatal(err)
		}
		if err := processRun(context.Background(), db, runID, "worker-a", time.Second, time.Hour, time.Second, fail); err != nil {
			t.Fatal(err)
		}
		var status, resolution string
		var count int
		if err := db.QueryRow(`SELECT r.status,d.resolution_status,d.attempt_count FROM job_runs r JOIN dead_letter_jobs d ON d.run_id=r.id WHERE r.id=$1`, runID).Scan(&status, &resolution, &count); err != nil {
			t.Fatal(err)
		}
		if status != "FAILED" || resolution != "OPEN" || count != 2 {
			t.Fatalf("DLQ state status=%s resolution=%s attempts=%d", status, resolution, count)
		}
	})
}

func TestProcessRunCancellationAndExpiredLeaseFencing(t *testing.T) {
	t.Run("cooperative cancel becomes terminal", func(t *testing.T) {
		db := workerTestDB(t)
		_, runID := seedRun(t, db, 2)
		done := make(chan error, 1)
		go func() {
			done <- processRun(context.Background(), db, runID, "worker-a", time.Second, 5*time.Millisecond, time.Second, func(ctx context.Context, _ sqlc.JobRun) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() })
		}()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			var status string
			if err := db.QueryRow(`SELECT status FROM job_runs WHERE id=$1`, runID).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status == "RUNNING" {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if _, err := db.Exec(`UPDATE job_runs SET status='CANCEL_REQUESTED',cancel_requested_at=NOW() WHERE id=$1`, runID); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("worker did not finish cooperative cancellation")
		}
		var status string
		if err := db.QueryRow(`SELECT status FROM job_runs WHERE id=$1`, runID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "CANCELLED" {
			t.Fatalf("status=%s, want CANCELLED", status)
		}
	})

	t.Run("expired lease can be reclaimed and fences old handler", func(t *testing.T) {
		db := workerTestDB(t)
		_, runID := seedRun(t, db, 3)
		release := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- processRun(context.Background(), db, runID, "worker-old", 40*time.Millisecond, time.Second, time.Second, func(context.Context, sqlc.JobRun) ([]byte, error) { <-release; return []byte(`{}`), nil })
		}()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			var status string
			if err := db.QueryRow(`SELECT status FROM job_runs WHERE id=$1`, runID).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status == "RUNNING" {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		time.Sleep(60 * time.Millisecond)
		newClaim, err := claimRun(context.Background(), db, runID, "worker-new", time.Second)
		if err != nil {
			t.Fatalf("reclaim expired lease: %v", err)
		}
		close(release)
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("old worker unexpectedly completed after fencing")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("old worker did not return")
		}
		var owner string
		var fence int64
		var status string
		if err := db.QueryRow(`SELECT status,worker_id,fencing_token FROM job_runs WHERE id=$1`, runID).Scan(&status, &owner, &fence); err != nil {
			t.Fatal(err)
		}
		if status != "RUNNING" || owner != "worker-new" || fence != newClaim.FencingToken {
			t.Fatalf("reclaimed state status=%s owner=%s fence=%d new_token=%d", status, owner, fence, newClaim.FencingToken)
		}
	})
}

func TestExpiredFinalAttemptCreatesDeadLetterOnce(t *testing.T) {
	db := workerTestDB(t)
	_, runID := seedRun(t, db, 1)
	if _, err := claimRun(context.Background(), db, runID, "worker-crashed", time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE job_runs SET lease_expires_at=NOW()-interval '1 second' WHERE id=$1`, runID); err != nil {
		t.Fatal(err)
	}
	if err := failExpiredRun(context.Background(), db, runID); err != nil {
		t.Fatal(err)
	}
	if err := failExpiredRun(context.Background(), db, runID); err != nil {
		t.Fatal(err)
	}
	var status, resolution string
	var rows int
	if err := db.QueryRow(`SELECT r.status,d.resolution_status,(SELECT count(*) FROM dead_letter_jobs WHERE run_id=r.id) FROM job_runs r JOIN dead_letter_jobs d ON d.run_id=r.id WHERE r.id=$1`, runID).Scan(&status, &resolution, &rows); err != nil {
		t.Fatal(err)
	}
	if status != "FAILED" || resolution != "OPEN" || rows != 1 {
		t.Fatalf("expired final attempt status=%s resolution=%s rows=%d", status, resolution, rows)
	}
}

func TestRedeliveredMessageWaitsForLeaseThenReclaims(t *testing.T) {
	db := workerTestDB(t)
	_, runID := seedRun(t, db, 3)
	if _, err := claimRun(context.Background(), db, runID, "worker-crashed", 80*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err := processRun(context.Background(), db, runID, "worker-recovery", time.Second, time.Hour, time.Second, func(context.Context, sqlc.JobRun) ([]byte, error) { return []byte(`{}`), nil })
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Fatal("redelivered run did not wait for its existing lease")
	}
	var status string
	var owner sql.NullString
	var fence int64
	if err := db.QueryRow(`SELECT status,worker_id,fencing_token FROM job_runs WHERE id=$1`, runID).Scan(&status, &owner, &fence); err != nil {
		t.Fatal(err)
	}
	if status != "SUCCEEDED" || owner.Valid || fence != 2 {
		t.Fatalf("recovered state status=%s owner=%+v fence=%d", status, owner, fence)
	}
}

func TestExpiredCancellationRecovery(t *testing.T) {
	db := workerTestDB(t)
	_, runID := seedRun(t, db, 2)
	if _, err := claimRun(context.Background(), db, runID, "worker-crashed", time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE job_runs SET status='CANCEL_REQUESTED',cancel_requested_at=NOW(),lease_expires_at=NOW()-interval '1 second' WHERE id=$1`, runID); err != nil {
		t.Fatal(err)
	}
	if err := completeExpiredCancellation(context.Background(), db, runID); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM job_runs WHERE id=$1`, runID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "CANCELLED" {
		t.Fatalf("recovered cancellation status=%s", status)
	}
}
