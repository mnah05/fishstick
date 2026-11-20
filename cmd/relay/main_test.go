package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fishstick/internal/messaging"
	"github.com/fishstick/internal/testsupport"
	"github.com/google/uuid"
)

type fakePublisher struct {
	err      error
	messages [][]byte
}

func (p *fakePublisher) Publish(_ context.Context, body []byte) error {
	if p.err != nil {
		return p.err
	}
	p.messages = append(p.messages, append([]byte(nil), body...))
	return nil
}

func TestBackoffCapsAtFiveMinutes(t *testing.T) {
	for _, tc := range []struct {
		attempts int32
		want     time.Duration
	}{
		{attempts: 0, want: time.Second},
		{attempts: 1, want: 2 * time.Second},
		{attempts: 8, want: 256 * time.Second},
		{attempts: 9, want: 5 * time.Minute},
		{attempts: 100, want: 5 * time.Minute},
	} {
		if got := backoff(tc.attempts); got != tc.want {
			t.Errorf("backoff(%d) = %s, want %s", tc.attempts, got, tc.want)
		}
	}
}

func TestProcessBatchReleasesFailureAndPublishesAfterRetry(t *testing.T) {
	db := testsupport.IsolatedPostgresDB(t, "relay_test", nil)
	ctx := context.Background()
	jobID, runID, eventID := uuid.New(), uuid.New(), uuid.New()
	if _, err := db.Exec(`INSERT INTO jobs(id,name,event_type,payload,schedule_type,timezone) VALUES($1,'relay test','relay.event','{}','MANUAL','UTC')`, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO job_runs(id,job_id,scheduled_for,trigger_source,trigger_idempotency_key,trigger_request_hash,event_type_snapshot,payload_snapshot,status) VALUES($1,$2,NOW(),'MANUAL',$3,'hash','relay.event','{}','QUEUED')`, runID, jobID, runID.String()); err != nil {
		t.Fatal(err)
	}
	body, err := messaging.EncodeDispatch(runID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO outbox_events(id,run_id,event_kind,status,message) VALUES($1,$2,'JOB_DISPATCH','PENDING',$3)`, eventID, runID, body); err != nil {
		t.Fatal(err)
	}
	broken := &fakePublisher{err: errors.New("broker unavailable")}
	count, err := processBatch(ctx, db, broken, "relay-a", 10, time.Minute)
	if err == nil || count != 0 {
		t.Fatalf("failed publish = (%d,%v), want zero published and visible error", count, err)
	}
	var status string
	var attempts int
	var lastError sql.NullString
	if err = db.QueryRow(`SELECT status,publish_attempts,last_error FROM outbox_events WHERE id=$1`, eventID).Scan(&status, &attempts, &lastError); err != nil {
		t.Fatal(err)
	}
	if status != "PENDING" || attempts != 1 || !lastError.Valid {
		t.Fatalf("released outbox status=%s attempts=%d error=%v", status, attempts, lastError)
	}
	if _, err = db.Exec(`UPDATE outbox_events SET available_at=NOW() WHERE id=$1`, eventID); err != nil {
		t.Fatal(err)
	}
	working := &fakePublisher{}
	count, err = processBatch(ctx, db, working, "relay-a", 10, time.Minute)
	if err != nil || count != 1 {
		t.Fatalf("successful publish = (%d,%v)", count, err)
	}
	var wantMessage, gotMessage bytes.Buffer
	if len(working.messages) == 1 {
		if err := json.Compact(&wantMessage, body); err != nil {
			t.Fatal(err)
		}
		if err := json.Compact(&gotMessage, working.messages[0]); err != nil {
			t.Fatal(err)
		}
	}
	if len(working.messages) != 1 || !bytes.Equal(gotMessage.Bytes(), wantMessage.Bytes()) {
		t.Fatalf("published messages=%q", working.messages)
	}
	if err = db.QueryRow(`SELECT status FROM outbox_events WHERE id=$1`, eventID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "PUBLISHED" {
		t.Fatalf("status=%s, want PUBLISHED", status)
	}
}
