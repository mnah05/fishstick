package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fishstick/internal/api"
	"github.com/google/uuid"
)

// TestAllEndpointsPostgresIntegration exercises every registered /v1 route against a real schema.
func TestAllEndpointsPostgresIntegration(t *testing.T) {
	db := newIntegrationDB(t)
	router := api.NewTestRouter(db)

	// GET /v1/health/db
	hc := jobRequest(router, http.MethodGet, "/v1/health/db", "")
	if hc.Code != http.StatusOK {
		t.Fatalf("health: %d %s", hc.Code, hc.Body.String())
	}

	// POST /v1/job/create
	createRec := jobRequest(router, http.MethodPost, "/v1/job/create", `{"name":"e2e-all","event_type":"e2e.event","schedule_type":"MANUAL"}`)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create job: %d %s", createRec.Code, createRec.Body.String())
	}
	var created api.JobResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	jobID := created.ID
	if _, err := uuid.Parse(jobID); err != nil {
		t.Fatalf("job id: %s", jobID)
	}

	// GET /v1/job/get/:job_id
	getJob := jobRequest(router, http.MethodGet, "/v1/job/get/"+jobID, "")
	if getJob.Code != http.StatusOK {
		t.Fatalf("get job: %d %s", getJob.Code, getJob.Body.String())
	}

	// GET /v1/job/list
	listJobs := jobRequest(router, http.MethodGet, "/v1/job/list?limit=10", "")
	if listJobs.Code != http.StatusOK {
		t.Fatalf("list jobs: %d %s", listJobs.Code, listJobs.Body.String())
	}

	// POST /v1/job/trigger/:job_id (no JSON body)
	req := httptest.NewRequest(http.MethodPost, "/v1/job/trigger/"+jobID, nil)
	req.Header.Set("Idempotency-Key", "e2e-trigger")
	trig := httptest.NewRecorder()
	router.ServeHTTP(trig, req)
	if trig.Code != http.StatusAccepted {
		t.Fatalf("trigger: %d %s", trig.Code, trig.Body.String())
	}
	var run api.RunResponse
	if err := json.Unmarshal(trig.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	runID := run.ID
	if run.JobID != jobID {
		t.Fatalf("run job_id mismatch: %s vs %s", run.JobID, jobID)
	}

	// GET /v1/list_runs
	listRuns := jobRequest(router, http.MethodGet, "/v1/list_runs?job_id="+jobID+"&limit=10", "")
	if listRuns.Code != http.StatusOK {
		t.Fatalf("list_runs: %d %s", listRuns.Code, listRuns.Body.String())
	}

	// GET /v1/job/:job_id/runs
	scopeRuns := jobRequest(router, http.MethodGet, "/v1/job/"+jobID+"/runs?limit=10", "")
	if scopeRuns.Code != http.StatusOK {
		t.Fatalf("job runs: %d %s", scopeRuns.Code, scopeRuns.Body.String())
	}

	// GET /v1/job/:job_id/run/:run_id
	oneRun := jobRequest(router, http.MethodGet, "/v1/job/"+jobID+"/run/"+runID, "")
	if oneRun.Code != http.StatusOK {
		t.Fatalf("get run: %d %s", oneRun.Code, oneRun.Body.String())
	}

	// POST /v1/cancel_run/:run_id (queued → CANCELLED)
	cancelRec := jobRequest(router, http.MethodPost, "/v1/cancel_run/"+runID, `{"reason":"e2e"}`)
	if cancelRec.Code != http.StatusOK {
		t.Fatalf("cancel: %d %s", cancelRec.Code, cancelRec.Body.String())
	}

	// POST /v1/job/update/:job_id
	up := jobRequest(router, http.MethodPost, "/v1/job/update/"+jobID, `{"name":"e2e-renamed"}`)
	if up.Code != http.StatusOK {
		t.Fatalf("update: %d %s", up.Code, up.Body.String())
	}
}
