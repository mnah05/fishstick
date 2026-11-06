package api_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fishstick/internal/api"
	"github.com/google/uuid"
)

// Smoke tests for handlers that fail validation before calling the database (nil pool safe).
func TestRouteValidationWithoutDB(t *testing.T) {
	t.Parallel()
	router := api.NewTestRouter(nil)
	validUUID := uuid.NewString()

	t.Run("health_no_checker", func(t *testing.T) {
		t.Parallel()
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/health/db", nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("health: %d", rec.Code)
		}
	})

	t.Run("get_job_bad_uuid", func(t *testing.T) {
		t.Parallel()
		rec := jobRequest(router, http.MethodGet, "/v1/job/get/not-a-uuid", "")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("get job: %d", rec.Code)
		}
	})

	t.Run("list_jobs_bad_limit", func(t *testing.T) {
		t.Parallel()
		rec := jobRequest(router, http.MethodGet, "/v1/job/list?limit=101", "")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("list jobs: %d", rec.Code)
		}
	})

	t.Run("list_runs_bad_status", func(t *testing.T) {
		t.Parallel()
		rec := jobRequest(router, http.MethodGet, "/v1/list_runs?status=INVALID", "")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("list_runs: %d", rec.Code)
		}
	})

	t.Run("list_runs_bad_job_id", func(t *testing.T) {
		t.Parallel()
		rec := jobRequest(router, http.MethodGet, "/v1/list_runs?job_id=bad", "")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("list_runs job_id: %d", rec.Code)
		}
	})

	t.Run("job_runs_bad_job_param", func(t *testing.T) {
		t.Parallel()
		rec := jobRequest(router, http.MethodGet, "/v1/job/not-uuid/runs", "")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("job runs: %d", rec.Code)
		}
	})

	t.Run("get_run_bad_job", func(t *testing.T) {
		t.Parallel()
		rec := jobRequest(router, http.MethodGet, "/v1/job/bad/run/"+validUUID, "")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("get run job: %d", rec.Code)
		}
	})

	t.Run("cancel_bad_run_id", func(t *testing.T) {
		t.Parallel()
		rec := jobRequest(router, http.MethodPost, "/v1/cancel_run/not-uuid", "")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("cancel: %d", rec.Code)
		}
	})

	t.Run("update_bad_job_id", func(t *testing.T) {
		t.Parallel()
		rec := jobRequest(router, http.MethodPost, "/v1/job/update/not-uuid", `{"name":"x"}`)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("update: %d", rec.Code)
		}
	})

	t.Run("create_invalid_json", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodPost, "/v1/job/create", strings.NewReader(`{`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("create json: %d", rec.Code)
		}
	})

	t.Run("create_validation_before_db", func(t *testing.T) {
		t.Parallel()
		rec := jobRequest(router, http.MethodPost, "/v1/job/create", `{}`)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("create validate: %d", rec.Code)
		}
	})
}
