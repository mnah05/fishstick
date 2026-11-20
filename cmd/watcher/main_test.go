package main

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/fishstick/internal/testsupport"
	"github.com/google/uuid"
)

func seedExpiredRun(t *testing.T, db *sql.DB, attempts, maxAttempts int) (uuid.UUID, uuid.UUID) {
	t.Helper()
	jobID, runID := uuid.New(), uuid.New()
	if _, err := db.Exec(`INSERT INTO jobs(id,name,event_type,payload,schedule_type,timezone,max_attempts,retry_delay_seconds,timeout_seconds)
VALUES($1,'watcher test','test.event','{}','MANUAL','UTC',$2,1,30)`, jobID, maxAttempts); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO job_runs(id,job_id,scheduled_for,trigger_source,trigger_idempotency_key,trigger_request_hash,
event_type_snapshot,payload_snapshot,timeout_seconds,max_attempts,status,attempt_count,worker_id,lease_expires_at)
VALUES($1,$2,NOW(),'MANUAL',$3,'hash','test.event','{}',30,$4,'RUNNING',$5,'crashed-worker',NOW()-interval '1 second')`, runID, jobID, runID.String(), maxAttempts, attempts); err != nil {
		t.Fatal(err)
	}
	return jobID, runID
}

func TestProcessBatchRetriesExpiredLeaseAndWritesDispatchAtomically(t *testing.T) {
	db := testsupport.IsolatedPostgresDB(t, "watcher_test", nil)
	_, runID := seedExpiredRun(t, db, 1, 3)
	count, err := processBatch(context.Background(), db, 10)
	if err != nil || count != 1 {
		t.Fatalf("processBatch() = (%d, %v), want (1, nil)", count, err)
	}
	var status string
	var worker sql.NullString
	var lease, retryAt sql.NullTime
	var attempts int
	if err := db.QueryRow(`SELECT status,worker_id,lease_expires_at,retry_at,attempt_count FROM job_runs WHERE id=$1`, runID).Scan(&status, &worker, &lease, &retryAt, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "RETRY_WAIT" || worker.Valid || lease.Valid || !retryAt.Valid || attempts != 1 {
		t.Fatalf("recovered run status=%s worker=%+v lease=%+v retry=%+v attempts=%d", status, worker, lease, retryAt, attempts)
	}
	var events int
	var availableAt time.Time
	if err := db.QueryRow(`SELECT count(*),min(available_at) FROM outbox_events WHERE run_id=$1 AND event_kind='JOB_DISPATCH'`, runID).Scan(&events, &availableAt); err != nil {
		t.Fatal(err)
	}
	if events != 1 || availableAt.Before(retryAt.Time.Add(-time.Millisecond)) || availableAt.After(retryAt.Time.Add(time.Millisecond)) {
		t.Fatalf("retry dispatches=%d available_at=%s retry_at=%s", events, availableAt, retryAt.Time)
	}
	count, err = processBatch(context.Background(), db, 10)
	if err != nil || count != 0 {
		t.Fatalf("second processBatch() = (%d, %v), want (0, nil)", count, err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM outbox_events WHERE run_id=$1 AND event_kind='JOB_DISPATCH'`, runID).Scan(&events); err != nil || events != 1 {
		t.Fatalf("retry dispatches after repeat=%d err=%v, want 1 and nil", events, err)
	}
}

func TestProcessBatchDeadLettersExpiredFinalAttempt(t *testing.T) {
	db := testsupport.IsolatedPostgresDB(t, "watcher_test", nil)
	_, runID := seedExpiredRun(t, db, 2, 2)
	count, err := processBatch(context.Background(), db, 10)
	if err != nil || count != 1 {
		t.Fatalf("processBatch() = (%d, %v), want (1, nil)", count, err)
	}
	var status, resolution string
	var rows int
	if err := db.QueryRow(`SELECT r.status,d.resolution_status,(SELECT count(*) FROM dead_letter_jobs WHERE run_id=r.id)
FROM job_runs r JOIN dead_letter_jobs d ON d.run_id=r.id WHERE r.id=$1`, runID).Scan(&status, &resolution, &rows); err != nil {
		t.Fatal(err)
	}
	if status != "FAILED" || resolution != "OPEN" || rows != 1 {
		t.Fatalf("dead-letter state status=%s resolution=%s rows=%d", status, resolution, rows)
	}
}

func TestRecoverExpiredRunRechecksLeaseAndSerializesWatchers(t *testing.T) {
	db := testsupport.IsolatedPostgresDB(t, "watcher_test", nil)
	_, runID := seedExpiredRun(t, db, 1, 3)
	if _, err := db.Exec(`UPDATE job_runs SET lease_expires_at=NOW()+interval '1 minute' WHERE id=$1`, runID); err != nil {
		t.Fatal(err)
	}
	recovered, err := recoverExpiredRun(context.Background(), db, runID)
	if err != nil || recovered {
		t.Fatalf("recovery after heartbeat = (%v, %v), want (false, nil)", recovered, err)
	}
	if _, err := db.Exec(`UPDATE job_runs SET lease_expires_at=NOW()-interval '1 second' WHERE id=$1`, runID); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var recoveredCount int
	var firstErr error
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			count, err := processBatch(context.Background(), db, 10)
			mu.Lock()
			defer mu.Unlock()
			recoveredCount += count
			if err != nil {
				firstErr = err
			}
		}()
	}
	wg.Wait()
	if firstErr != nil || recoveredCount != 1 {
		t.Fatalf("concurrent recovery count=%d err=%v, want one recovery and nil", recoveredCount, firstErr)
	}
	var events int
	if err := db.QueryRow(`SELECT count(*) FROM outbox_events WHERE run_id=$1 AND event_kind='JOB_DISPATCH'`, runID).Scan(&events); err != nil || events != 1 {
		t.Fatalf("retry dispatches=%d err=%v, want one and nil", events, err)
	}
}
