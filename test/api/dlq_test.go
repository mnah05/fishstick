package api_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/fishstick/internal/api"
	"github.com/google/uuid"
)

func TestDeadLetterListGetFiltersAndPagination(t *testing.T) {
	db := newIntegrationDB(t)
	jobID := insertTestJob(t, db)
	runIDs := []uuid.UUID{insertTestRun(t, db, jobID, "FAILED"), insertTestRun(t, db, jobID, "FAILED")}
	for i, runID := range runIDs {
		if _, err := db.Exec(`INSERT INTO dead_letter_jobs(run_id,job_id,failure_reason,last_error,attempt_count,failed_at) VALUES($1,$2,'MAX_ATTEMPTS_EXCEEDED','failed',3,$3)`, runID, jobID, time.Now().Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	router := api.NewTestRouter(db)
	first := jobRequest(router, http.MethodGet, "/v1/dlq?limit=1&resolution_status=OPEN&job_id="+jobID.String(), "")
	if first.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", first.Code, first.Body.String())
	}
	var page struct {
		Data       []map[string]any `json:"data"`
		NextCursor string           `json:"next_cursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Data) != 1 || page.NextCursor == "" {
		t.Fatalf("first page=%+v", page)
	}
	second := jobRequest(router, http.MethodGet, "/v1/dlq?limit=1&cursor="+page.NextCursor, "")
	if second.Code != http.StatusOK {
		t.Fatalf("second page status=%d body=%s", second.Code, second.Body.String())
	}
	if err := json.Unmarshal(second.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Data) != 1 {
		t.Fatalf("second page rows=%d", len(page.Data))
	}
	get := jobRequest(router, http.MethodGet, "/v1/dlq/"+page.Data[0]["run_id"].(string), "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status=%d body=%s", get.Code, get.Body.String())
	}
	missing := jobRequest(router, http.MethodGet, "/v1/dlq/"+uuid.NewString(), "")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing status=%d body=%s", missing.Code, missing.Body.String())
	}
	badCursor := jobRequest(router, http.MethodGet, "/v1/dlq?cursor=broken", "")
	if badCursor.Code != http.StatusBadRequest {
		t.Fatalf("bad cursor status=%d", badCursor.Code)
	}
}
