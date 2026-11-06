package api_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fishstick/internal/api"
	"github.com/fishstick/internal/database/sqlc"
	"github.com/google/uuid"
)

func TestTriggerValidation(t *testing.T) {
	for _, tc := range []struct{ name, id, key, body string }{
		{"invalid ID", "bad", "key", ""},
		{"missing key", uuid.NewString(), "", ""},
		{"blank key", uuid.NewString(), "  ", ""},
		{"unsupported body", uuid.NewString(), "key", `{}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/job/trigger/"+tc.id, strings.NewReader(tc.body))
			req.Header.Set("Idempotency-Key", tc.key)
			rec := httptest.NewRecorder()
			api.NewTestRouter(nil).ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "VALIDATION_ERROR") {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

// Each test run uses an isolated schema; it never edits existing application tables.
func TestTriggerPostgresIntegration(t *testing.T) {
	db := newIntegrationDB(t)
	jobID := uuid.NewString()
	if _, err := db.Exec(`INSERT INTO jobs (id,name,event_type,payload,timeout_seconds,max_attempts) VALUES ($1,'test','test.run','{"value":1}',45,5)`, jobID); err != nil {
		t.Fatal(err)
	}
	router := api.NewTestRouter(db)
	trigger := func(id, key string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/job/trigger/"+id, nil)
		req.Header.Set("Idempotency-Key", key)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	if rec := trigger(uuid.NewString(), "missing"); rec.Code != 404 {
		t.Fatalf("missing job: %d %s", rec.Code, rec.Body.String())
	}
	// Concurrent submissions must create exactly one run and one dispatch event.
	const count = 8
	results := make(chan *httptest.ResponseRecorder, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- trigger(jobID, "same-key") }()
	}
	wg.Wait()
	close(results)
	var runID string
	for rec := range results {
		if rec.Code != 202 {
			t.Fatalf("trigger: %d %s", rec.Code, rec.Body.String())
		}
		var run api.RunResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &run); err != nil {
			t.Fatal(err)
		}
		if runID == "" {
			runID = run.ID
		}
		if run.ID != runID {
			t.Fatal("duplicate run")
		}
		if run.Status != "QUEUED" || run.AttemptCount != 0 || run.TriggerSource != "MANUAL" || run.EventType != "test.run" || run.TimeoutSeconds != 45 || run.MaxAttempts != 5 || run.JobID != jobID {
			t.Fatalf("bad snapshot: %+v", run)
		}
		for _, field := range []string{"fencing_token", "trigger_request_hash", "trigger_idempotency_key"} {
			if strings.Contains(rec.Body.String(), field) {
				t.Fatalf("leaked %s", field)
			}
		}
	}
	assertCounts := func(want int) {
		t.Helper()
		for _, table := range []string{"job_runs", "outbox_events"} {
			var n int
			if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != want {
				t.Fatalf("%s count=%d want=%d", table, n, want)
			}
		}
	}
	assertCounts(1)
	var eventRunID, status string
	if err := db.QueryRow(`SELECT message->>'run_id',status FROM outbox_events`).Scan(&eventRunID, &status); err != nil {
		t.Fatal(err)
	}
	if eventRunID != runID || status != "PENDING" {
		t.Fatalf("bad dispatch %s %s", eventRunID, status)
	}
	if _, err := db.Exec(`UPDATE job_runs SET trigger_request_hash='different' WHERE id=$1`, runID); err != nil {
		t.Fatal(err)
	}
	if rec := trigger(jobID, "same-key"); rec.Code != 409 {
		t.Fatalf("hash conflict: %d", rec.Code)
	}
	assertCounts(1)
	// A database failure on the second insert must roll back the first insert.
	if _, err := db.Exec(`CREATE FUNCTION reject_dispatch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'forced outbox failure'; END $$;
 CREATE TRIGGER reject_dispatch BEFORE INSERT ON outbox_events FOR EACH ROW EXECUTE FUNCTION reject_dispatch();`); err != nil {
		t.Fatal(err)
	}
	if rec := trigger(jobID, "rollback-key"); rec.Code != 500 {
		t.Fatalf("expected outbox failure: %d %s", rec.Code, rec.Body.String())
	}
	assertCounts(1)
	if _, err := db.Exec(`DROP TRIGGER reject_dispatch ON outbox_events`); err != nil {
		t.Fatal(err)
	}
	if rec := trigger(jobID, "rollback-key"); rec.Code != 202 {
		t.Fatalf("retry after rollback: %d %s", rec.Code, rec.Body.String())
	}
	assertCounts(2)
	// The request context must reach BeginTx and prevent writes after cancellation.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1/job/trigger/%s", jobID), nil).WithContext(ctx)
	req.Header.Set("Idempotency-Key", "cancelled")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != 500 {
		t.Fatalf("cancelled request: %d", rec.Code)
	}
	assertCounts(2)
}

func TestListAndGetJobRunsPostgresIntegration(t *testing.T) {
	db := newIntegrationDB(t)
	jobID := uuid.New()
	otherJob := uuid.New()
	if _, err := db.Exec(`INSERT INTO jobs (id,name,event_type,payload,timeout_seconds,max_attempts) VALUES ($1,'r1','e','{}',45,5)`, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO jobs (id,name,event_type,payload,timeout_seconds,max_attempts) VALUES ($1,'r2','e','{}',45,5)`, otherJob); err != nil {
		t.Fatal(err)
	}
	r1 := insertTestRun(t, db, jobID, "QUEUED")
	r2 := insertTestRun(t, db, jobID, "SUCCEEDED")
	insertTestRun(t, db, otherJob, "QUEUED")
	router := api.NewTestRouter(db)
	rec := jobRequest(router, "GET", "/v1/job/"+jobID.String()+"/runs?limit=10", "")
	if rec.Code != 200 {
		t.Fatalf("list: %d %s", rec.Code, rec.Body.String())
	}
	var list api.ListJobRunsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Data) != 2 {
		t.Fatalf("want 2 runs, got %d", len(list.Data))
	}
	// Newest UUID first (r2 > r1)
	if list.Data[0].ID != r2.String() || list.Data[1].ID != r1.String() {
		t.Fatalf("order: %+v", list.Data)
	}
	get := jobRequest(router, "GET", fmt.Sprintf("/v1/job/%s/run/%s", jobID.String(), r1.String()), "")
	if get.Code != 200 {
		t.Fatalf("get run: %d %s", get.Code, get.Body.String())
	}
	var one api.RunResponse
	if err := json.Unmarshal(get.Body.Bytes(), &one); err != nil {
		t.Fatal(err)
	}
	if one.ID != r1.String() || one.JobID != jobID.String() {
		t.Fatalf("bad run: %+v", one)
	}
	// Wrong composite: same run id under another job → 404
	wrong := jobRequest(router, "GET", fmt.Sprintf("/v1/job/%s/run/%s", otherJob.String(), r1.String()), "")
	if wrong.Code != 404 {
		t.Fatalf("wrong job_id: %d %s", wrong.Code, wrong.Body.String())
	}
	// Bad UUID
	bad := jobRequest(router, "GET", "/v1/job/"+jobID.String()+"/run/not-a-uuid", "")
	if bad.Code != 400 {
		t.Fatalf("bad run_id: %d", bad.Code)
	}
}

func TestListRunsPostgresIntegration(t *testing.T) {
	db := newIntegrationDB(t)
	jobID := uuid.New()
	if _, err := db.Exec(`INSERT INTO jobs (id,name,event_type,payload,timeout_seconds,max_attempts) VALUES ($1,'lr','original.event','{}',45,5)`, jobID); err != nil {
		t.Fatal(err)
	}
	insertTestRun(t, db, jobID, "QUEUED")
	insertTestRun(t, db, jobID, "SUCCEEDED")
	other := uuid.New()
	if _, err := db.Exec(`INSERT INTO jobs (id,name,event_type,payload,timeout_seconds,max_attempts) VALUES ($1,'lr2','other.event','{}',45,5)`, other); err != nil {
		t.Fatal(err)
	}
	insertTestRun(t, db, other, "QUEUED")
	router := api.NewTestRouter(db)

	all := jobRequest(router, "GET", "/v1/list_runs?limit=10", "")
	if all.Code != 200 {
		t.Fatalf("list_runs: %d %s", all.Code, all.Body.String())
	}
	var full api.ListJobRunsResponse
	if err := json.Unmarshal(all.Body.Bytes(), &full); err != nil {
		t.Fatal(err)
	}
	if len(full.Data) != 3 {
		t.Fatalf("want 3 runs, got %d", len(full.Data))
	}
	byJob := jobRequest(router, "GET", "/v1/list_runs?job_id="+jobID.String()+"&limit=10", "")
	if byJob.Code != 200 || !strings.Contains(byJob.Body.String(), jobID.String()) {
		t.Fatalf("filter job: %d %s", byJob.Code, byJob.Body.String())
	}
	var fj api.ListJobRunsResponse
	if err := json.Unmarshal(byJob.Body.Bytes(), &fj); err != nil {
		t.Fatal(err)
	}
	if len(fj.Data) != 2 {
		t.Fatalf("want 2 runs for job, got %d", len(fj.Data))
	}
	st := jobRequest(router, "GET", "/v1/list_runs?status=QUEUED&limit=10", "")
	if st.Code != 200 {
		t.Fatalf("status filter: %d", st.Code)
	}
	var fs api.ListJobRunsResponse
	if err := json.Unmarshal(st.Body.Bytes(), &fs); err != nil {
		t.Fatal(err)
	}
	if len(fs.Data) != 2 {
		t.Fatalf("want 2 QUEUED, got %d", len(fs.Data))
	}
	ev := jobRequest(router, "GET", "/v1/list_runs?event_type=original.event&limit=10", "")
	if ev.Code != 200 {
		t.Fatalf("event_type: %d", ev.Code)
	}
	ts := jobRequest(router, "GET", "/v1/list_runs?trigger_source=SCHEDULED&limit=10", "")
	if ts.Code != 200 {
		t.Fatalf("trigger_source: %d", ts.Code)
	}
	badStatus := jobRequest(router, "GET", "/v1/list_runs?status=NOT_A_STATUS", "")
	if badStatus.Code != 400 {
		t.Fatalf("bad status: %d", badStatus.Code)
	}
	badTS := jobRequest(router, "GET", "/v1/list_runs?trigger_source=INVALID", "")
	if badTS.Code != 400 {
		t.Fatalf("bad trigger_source: %d", badTS.Code)
	}
}

func TestCancelRunPostgresIntegration(t *testing.T) {
	db := newIntegrationDB(t)
	router := api.NewTestRouter(db)
	jobID := uuid.New()
	if _, err := db.Exec(`INSERT INTO jobs (id,name,event_type,payload,timeout_seconds,max_attempts) VALUES ($1,'c','e','{}',45,5)`, jobID); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Queued → immediate CANCELLED (200)
	qID := insertTestRun(t, db, jobID, "QUEUED")
	rec := jobRequest(router, "POST", "/v1/cancel_run/"+qID.String(), `{"reason":"x"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("cancel queued: %d %s", rec.Code, rec.Body.String())
	}
	var cancelled api.RunResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &cancelled); err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != "CANCELLED" || cancelled.CancelReason == nil || *cancelled.CancelReason != "x" {
		t.Fatalf("bad cancelled run: %+v", cancelled)
	}

	// Running → CANCEL_REQUESTED (202)
	runID := insertTestRun(t, db, jobID, "QUEUED")
	tx := holdJobLock(t, db, jobID)
	q := sqlc.New(db).WithTx(tx)
	if _, err := q.ClaimRunToRunning(ctx, sqlc.ClaimRunToRunningParams{
		ID:    runID,
		JobID: jobID,
		LeaseExpiresAt: sql.NullTime{
			Time:  time.Now().Add(time.Hour),
			Valid: true,
		},
		WorkerID: sql.NullString{String: "w", Valid: true},
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	rec2 := jobRequest(router, "POST", "/v1/cancel_run/"+runID.String(), `{}`)
	if rec2.Code != http.StatusAccepted {
		t.Fatalf("cancel running: %d %s", rec2.Code, rec2.Body.String())
	}
	var reqed api.RunResponse
	if err := json.Unmarshal(rec2.Body.Bytes(), &reqed); err != nil {
		t.Fatal(err)
	}
	if reqed.Status != "CANCEL_REQUESTED" {
		t.Fatalf("want CANCEL_REQUESTED, got %s", reqed.Status)
	}

	// Idempotent: CANCEL_REQUESTED → 200 unchanged
	rec3 := jobRequest(router, "POST", "/v1/cancel_run/"+runID.String(), "")
	if rec3.Code != http.StatusOK {
		t.Fatalf("idempotent cancel: %d %s", rec3.Code, rec3.Body.String())
	}

	// Terminal → 409
	doneID := insertTestRun(t, db, jobID, "SUCCEEDED")
	rec4 := jobRequest(router, "POST", "/v1/cancel_run/"+doneID.String(), "")
	if rec4.Code != http.StatusConflict || !strings.Contains(rec4.Body.String(), "RUN_ALREADY_TERMINAL") {
		t.Fatalf("terminal cancel: %d %s", rec4.Code, rec4.Body.String())
	}

	bad := jobRequest(router, "POST", "/v1/cancel_run/bad-uuid", "")
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("bad run_id: %d", bad.Code)
	}
}
