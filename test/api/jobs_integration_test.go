package api_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fishstick/internal/api"
	"github.com/fishstick/internal/database/sqlc"
	"github.com/google/uuid"
)

func insertTestJob(t *testing.T, db *sql.DB) uuid.UUID {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO jobs (id,name,event_type,payload,timeout_seconds,max_attempts)
 VALUES ($1,'original','original.event','{"version":"original"}',45,5)`, id); err != nil {
		t.Fatal(err)
	}
	return id
}

func setRunLease(t *testing.T, db *sql.DB, runID uuid.UUID, expiresAt time.Time) {
	t.Helper()
	if _, err := db.Exec(`UPDATE job_runs SET lease_expires_at=$2 WHERE id=$1`, runID, expiresAt); err != nil {
		t.Fatal(err)
	}
}

func insertTestRun(t *testing.T, db *sql.DB, jobID uuid.UUID, status string) uuid.UUID {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO job_runs
 (id,job_id,scheduled_for,trigger_source,event_type_snapshot,payload_snapshot,timeout_seconds,max_attempts,status)
 VALUES ($1,$2,clock_timestamp(),'SCHEDULED','original.event','{"version":"original"}',45,5,$3)`, id, jobID, status); err != nil {
		t.Fatal(err)
	}
	return id
}

func jobRequest(router http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func TestListJobsPostgresIntegration(t *testing.T) {
	db := newIntegrationDB(t)
	router := api.NewTestRouter(db)
	empty := jobRequest(router, "GET", "/v1/job/list", "")
	if empty.Code != 200 || !strings.Contains(empty.Body.String(), `"data":[]`) {
		t.Fatalf("empty list: %d %s", empty.Code, empty.Body.String())
	}
	ids := make([]uuid.UUID, 4)
	for i := range ids {
		ids[i] = insertTestJob(t, db)
	}
	insertTestRun(t, db, ids[1], "FAILED")
	latest := insertTestRun(t, db, ids[1], "SUCCEEDED")
	// Tied creation times must still choose the highest run UUID.
	if _, err := db.Exec(`UPDATE job_runs SET created_at='2026-01-01T00:00:00Z' WHERE job_id=$1`, ids[1]); err != nil {
		t.Fatal(err)
	}
	other := insertTestRun(t, db, ids[3], "QUEUED")
	wantRuns := map[string]string{ids[1].String(): latest.String(), ids[3].String(): other.String()}
	cursor := ""
	for page := 0; page < 2; page++ {
		path := "/v1/job/list?limit=2"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		rec := jobRequest(router, "GET", path, "")
		if rec.Code != 200 {
			t.Fatalf("page %d: %d %s", page, rec.Code, rec.Body.String())
		}
		var result api.ListJobsResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Data) != 2 {
			t.Fatalf("page length=%d", len(result.Data))
		}
		var wire struct {
			Data []map[string]json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &wire); err != nil {
			t.Fatal(err)
		}
		for i, item := range result.Data {
			wantID := ids[3-page*2-i].String()
			if item.ID != wantID {
				t.Fatalf("got job %s want %s", item.ID, wantID)
			}
			if wantRun, ok := wantRuns[item.ID]; ok {
				wantStatus := "SUCCEEDED"
				if item.ID == ids[3].String() {
					wantStatus = "QUEUED"
				}
				if item.LatestRun == nil || item.LatestRun.ID != wantRun || item.LatestRun.Status != wantStatus {
					t.Fatalf("bad latest run: %+v", item.LatestRun)
				}
			} else if _, exists := wire.Data[i]["latest_run"]; exists {
				t.Fatal("latest_run must be omitted for a job without runs")
			}
		}
		if page == 0 {
			if result.NextCursor == nil || *result.NextCursor != ids[2].String() {
				t.Fatal("bad next cursor")
			}
			cursor = *result.NextCursor
		} else if result.NextCursor != nil {
			t.Fatal("unexpected final cursor")
		}
	}
}

func TestUpdateJobStatesPostgresIntegration(t *testing.T) {
	db := newIntegrationDB(t)
	router := api.NewTestRouter(db)
	for _, status := range []string{"", "SCHEDULED", "QUEUED", "RETRY_WAIT", "SUCCEEDED", "FAILED", "CANCELLED", "RUNNING", "CANCEL_REQUESTED"} {
		t.Run("state_"+status, func(t *testing.T) {
			id := insertTestJob(t, db)
			if status != "" {
				runID := insertTestRun(t, db, id, status)
				// Busy check only treats RUNNING/CANCEL_REQUESTED as blocking while lease is active or unset.
				if status == "RUNNING" || status == "CANCEL_REQUESTED" {
					setRunLease(t, db, runID, time.Now().Add(time.Hour))
				}
			}
			before, err := sqlc.New(db).GetJob(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			rec := jobRequest(router, "POST", "/v1/job/update/"+id.String(), `{"name":"updated","event_type":"new.event","payload":{"version":"new"},"timeout_seconds":60,"max_attempts":7}`)
			want := 200
			if status == "RUNNING" || status == "CANCEL_REQUESTED" {
				want = 409
			}
			if rec.Code != want {
				t.Fatalf("status=%d want=%d body=%s", rec.Code, want, rec.Body.String())
			}
			after, err := sqlc.New(db).GetJob(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			if want == 409 {
				if !strings.Contains(rec.Body.String(), "JOB_BUSY") || after.Name != before.Name || !after.UpdatedAt.Equal(before.UpdatedAt) {
					t.Fatal("busy job changed")
				}
			} else {
				if after.Name != "updated" || after.EventType != "new.event" || after.TimeoutSeconds != 60 || after.MaxAttempts != 7 {
					t.Fatalf("update not persisted: %+v", after)
				}
				var response api.JobResponse
				if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if !response.UpdatedAt.Equal(after.UpdatedAt) {
					t.Fatal("response differs from committed row")
				}
			}
			if status != "" {
				var event string
				var timeout, attempts int
				var payload string
				if err := db.QueryRow(`SELECT event_type_snapshot,timeout_seconds,max_attempts,payload_snapshot->>'version' FROM job_runs WHERE job_id=$1`, id).Scan(&event, &timeout, &attempts, &payload); err != nil {
					t.Fatal(err)
				}
				if event != "original.event" || timeout != 45 || attempts != 5 || payload != "original" {
					t.Fatal("existing snapshot changed")
				}
			}
		})
	}
}

func TestUpdateRollbackPostgresIntegration(t *testing.T) {
	db := newIntegrationDB(t)
	router := api.NewTestRouter(db)
	id := insertTestJob(t, db)
	path := "/v1/job/update/" + id.String()
	if rec := jobRequest(router, "POST", path, `{"schedule_type":"ONCE"}`); rec.Code != 400 {
		t.Fatalf("invalid merge: %d", rec.Code)
	}
	// Fail at COMMIT, after UPDATE and the response-row read have succeeded.
	if _, err := db.Exec(`CREATE FUNCTION reject_job_commit() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN RAISE EXCEPTION 'forced commit failure'; END $$;
 CREATE CONSTRAINT TRIGGER reject_job_commit AFTER UPDATE ON jobs
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_job_commit();`); err != nil {
		t.Fatal(err)
	}
	rec := jobRequest(router, "POST", path, `{"name":"must roll back"}`)
	if rec.Code != 500 {
		t.Fatalf("commit failure: %d %s", rec.Code, rec.Body.String())
	}
	job, err := sqlc.New(db).GetJob(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if job.Name != "original" || job.ScheduleType != "MANUAL" {
		t.Fatal("failed update persisted")
	}
	if _, err := db.Exec(`DROP TRIGGER reject_job_commit ON jobs`); err != nil {
		t.Fatal(err)
	}
	if rec := jobRequest(router, "POST", path, `{"name":"works"}`); rec.Code != 200 {
		t.Fatalf("retry: %d %s", rec.Code, rec.Body.String())
	}
	if rec := jobRequest(router, "POST", "/v1/job/update/"+uuid.NewString(), `{"name":"missing"}`); rec.Code != 404 {
		t.Fatalf("missing job: %d", rec.Code)
	}
}

func holdJobLock(t *testing.T, db *sql.DB, id uuid.UUID) *sql.Tx {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.Rollback() })
	if _, err := sqlc.New(db).WithTx(tx).GetJobForUpdate(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	return tx
}

func receiveResponse(t *testing.T, ch <-chan *httptest.ResponseRecorder, want int) *httptest.ResponseRecorder {
	t.Helper()
	select {
	case rec := <-ch:
		if rec.Code != want {
			t.Fatalf("status=%d want=%d body=%s", rec.Code, want, rec.Body.String())
		}
		return rec
	case <-time.After(12 * time.Second):
		t.Fatal("request did not finish")
		return nil
	}
}

func TestConcurrentUpdatesPostgresIntegration(t *testing.T) {
	db := newIntegrationDB(t)
	router := api.NewTestRouter(db)
	id := insertTestJob(t, db)
	tx := holdJobLock(t, db, id)
	results := make(chan *httptest.ResponseRecorder, 2)
	for _, body := range []string{`{"name":"new name"}`, `{"timeout_seconds":75}`} {
		go func(body string) { results <- jobRequest(router, "POST", "/v1/job/update/"+id.String(), body) }(body)
	}
	waitForJobLockWaiters(t, db, 2)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	receiveResponse(t, results, 200)
	receiveResponse(t, results, 200)
	job, err := sqlc.New(db).GetJob(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if job.Name != "new name" || job.TimeoutSeconds != 75 {
		t.Fatal("concurrent partial update lost a field")
	}
}

func TestConcurrentUpdateTriggerPostgresIntegration(t *testing.T) {
	db := newIntegrationDB(t)
	router := api.NewTestRouter(db)
	id := insertTestJob(t, db)
	tx := holdJobLock(t, db, id)
	updates := make(chan *httptest.ResponseRecorder, 1)
	triggers := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		updates <- jobRequest(router, "POST", "/v1/job/update/"+id.String(), `{"event_type":"new.event","payload":{"version":"new"},"timeout_seconds":75,"max_attempts":7}`)
	}()
	go func() {
		req := httptest.NewRequest("POST", "/v1/job/trigger/"+id.String(), nil)
		req.Header.Set("Idempotency-Key", "concurrent")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		triggers <- rec
	}()
	waitForJobLockWaiters(t, db, 2)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	receiveResponse(t, updates, 200)
	rec := receiveResponse(t, triggers, 202)
	var run api.RunResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(run.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	old := run.EventType == "original.event" && payload.Version == "original" && run.TimeoutSeconds == 45 && run.MaxAttempts == 5
	new := run.EventType == "new.event" && payload.Version == "new" && run.TimeoutSeconds == 75 && run.MaxAttempts == 7
	if !old && !new {
		t.Fatalf("snapshot mixes job versions: %+v", run)
	}
	job, err := sqlc.New(db).GetJob(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if job.EventType != "new.event" || job.TimeoutSeconds != 75 {
		t.Fatal("update was not committed")
	}
}

func TestUpdateSeesWorkerClaimPostgresIntegration(t *testing.T) {
	db := newIntegrationDB(t)
	router := api.NewTestRouter(db)
	id := insertTestJob(t, db)
	runID := insertTestRun(t, db, id, "QUEUED")
	// Simulate worker claim: lock job row then transition with an active lease (required for JOB_BUSY).
	tx := holdJobLock(t, db, id)
	q := sqlc.New(db).WithTx(tx)
	if _, err := q.ClaimRunToRunning(context.Background(), sqlc.ClaimRunToRunningParams{
		ID:    runID,
		JobID: id,
		LeaseExpiresAt: sql.NullTime{
			Time:  time.Now().Add(time.Hour),
			Valid: true,
		},
		WorkerID: sql.NullString{String: "integration-worker", Valid: true},
	}); err != nil {
		t.Fatal(err)
	}
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		result <- jobRequest(router, "POST", "/v1/job/update/"+id.String(), `{"name":"must not change"}`)
	}()
	waitForJobLockWaiters(t, db, 1)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	rec := receiveResponse(t, result, 409)
	if !strings.Contains(rec.Body.String(), "JOB_BUSY") {
		t.Fatal("wrong conflict")
	}
	job, err := sqlc.New(db).GetJob(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if job.Name != "original" {
		t.Fatal("busy job changed")
	}
}

func TestListJobsJobWithNoRunsPostgresIntegration(t *testing.T) {
	db := newIntegrationDB(t)
	router := api.NewTestRouter(db)
	id := insertTestJob(t, db)
	rec := jobRequest(router, "GET", "/v1/job/list?limit=10", "")
	if rec.Code != 200 {
		t.Fatalf("list: %d %s", rec.Code, rec.Body.String())
	}
	var wire struct {
		Data []map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire.Data) != 1 {
		t.Fatalf("want 1 job, got %d", len(wire.Data))
	}
	if string(wire.Data[0]["id"]) != `"`+id.String()+`"` {
		t.Fatalf("unexpected id: %s", wire.Data[0]["id"])
	}
	if _, has := wire.Data[0]["latest_run"]; has {
		t.Fatal("latest_run must be omitted for a job without runs")
	}
}

func TestUpdateAllowedWhenRunLeaseExpiredPostgresIntegration(t *testing.T) {
	db := newIntegrationDB(t)
	router := api.NewTestRouter(db)
	id := insertTestJob(t, db)
	runID := insertTestRun(t, db, id, "RUNNING")
	setRunLease(t, db, runID, time.Now().Add(-time.Hour))
	rec := jobRequest(router, "POST", "/v1/job/update/"+id.String(), `{"name":"after expired lease"}`)
	if rec.Code != 200 {
		t.Fatalf("want 200, got %d %s", rec.Code, rec.Body.String())
	}
	job, err := sqlc.New(db).GetJob(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if job.Name != "after expired lease" {
		t.Fatalf("update not applied: %+v", job)
	}
}

func TestClaimThenUpdateConflictPostgresIntegration(t *testing.T) {
	db := newIntegrationDB(t)
	router := api.NewTestRouter(db)
	id := insertTestJob(t, db)
	runID := insertTestRun(t, db, id, "QUEUED")

	tx := holdJobLock(t, db, id)
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		result <- jobRequest(router, "POST", "/v1/job/update/"+id.String(), `{"name":"blocked"}`)
	}()
	waitForJobLockWaiters(t, db, 1)

	q := sqlc.New(db).WithTx(tx)
	if _, err := q.ClaimRunToRunning(context.Background(), sqlc.ClaimRunToRunningParams{
		ID:    runID,
		JobID: id,
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
	rec := receiveResponse(t, result, 409)
	if !strings.Contains(rec.Body.String(), "JOB_BUSY") {
		t.Fatal("expected JOB_BUSY")
	}
	job, err := sqlc.New(db).GetJob(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if job.Name != "original" {
		t.Fatal("job changed under conflict")
	}
}

func TestUpdateThenClaimPostgresIntegration(t *testing.T) {
	db := newIntegrationDB(t)
	router := api.NewTestRouter(db)
	id := insertTestJob(t, db)
	runID := insertTestRun(t, db, id, "QUEUED")

	if rec := jobRequest(router, "POST", "/v1/job/update/"+id.String(), `{"name":"v2"}`); rec.Code != 200 {
		t.Fatalf("update: %d %s", rec.Code, rec.Body.String())
	}

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.Rollback() })
	qtx := sqlc.New(db).WithTx(tx)
	if _, err := qtx.GetJobForUpdate(ctx, id); err != nil {
		t.Fatal(err)
	}
	job, err := qtx.GetJob(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if job.Name != "v2" {
		t.Fatalf("locked job not at v2: %q", job.Name)
	}
	if _, err := qtx.ClaimRunToRunning(ctx, sqlc.ClaimRunToRunningParams{
		ID:    runID,
		JobID: id,
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
	run, err := sqlc.New(db).GetLatestJobRunSummary(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if run.ID != runID || run.Status != "RUNNING" {
		t.Fatalf("bad claim result: %+v", run)
	}
}
