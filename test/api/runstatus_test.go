package api_test

import (
	"net/http"
	"testing"

	"github.com/fishstick/internal/api"
)

func TestListRunsRejectsRemovedScheduledRunStatus(t *testing.T) {
	router := api.NewTestRouter(nil)
	response := jobRequest(router, http.MethodGet, "/v1/list_runs?status=SCHEDULED", "")
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=SCHEDULED returned %d, want %d: %s", response.Code, http.StatusBadRequest, response.Body.String())
	}
}
