package api_test

import (
	"github.com/fishstick/internal/api"
	"net/http/httptest"
	"testing"
)

func TestParseListCursorDefaults(t *testing.T) {
	t.Parallel()
	limit, cursor, err := api.ParseListCursor(httptest.NewRequest("GET", "/v1/job/list", nil))
	if err != nil || limit != 50 || cursor.String() != "00000000-0000-0000-0000-000000000000" {
		t.Fatalf("limit=%d cursor=%v err=%v", limit, cursor, err)
	}
}

func TestParseListCursorRejectsHighLimit(t *testing.T) {
	t.Parallel()
	_, _, err := api.ParseListCursor(httptest.NewRequest("GET", "/v1/job/list?limit=101", nil))
	if err == nil {
		t.Fatal("expected error for limit > 100")
	}
}
