package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/fishstick/internal/api"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDatabaseHealth(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		result int32
		err    error
		status int
	}{
		{name: "connected", result: 1, status: http.StatusOK},
		{name: "query error", err: errors.New("secret connection details"), status: http.StatusServiceUnavailable},
		{name: "unexpected result", result: 2, status: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			called := false
			healthCheck := func(ctx context.Context) (int32, error) {
				called = true
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 3*time.Second {
					t.Fatalf("expected a deadline within three seconds, got %v, %v", deadline, ok)
				}
				return test.result, test.err
			}
			response := httptest.NewRecorder()
			router := api.NewTestRouter(nil)
			router.GET("/v1/health/db", api.DatabaseHealth(healthCheck))
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/health/db", nil))
			if !called || response.Code != test.status {
				t.Fatalf("called=%v, status=%d, want %d", called, response.Code, test.status)
			}
			if response.Header().Get("Content-Type") != "application/json" {
				t.Fatal("response must be JSON")
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("health response must not be cached")
			}
			var body map[string]any
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if test.status == http.StatusOK {
				if body["status"] != "ok" || body["database"] != "connected" {
					t.Fatalf("unexpected success body: %v", body)
				}
			} else {
				failure, ok := body["error"].(map[string]any)
				if !ok || failure["code"] != "DATABASE_UNAVAILABLE" {
					t.Fatalf("unexpected error body: %v", body)
				}
				if strings.Contains(response.Body.String(), "secret") {
					t.Fatal("response leaked internal error")
				}
			}
		})
	}
}

func TestDatabaseHealthUsesRequestContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	healthCheck := func(ctx context.Context) (int32, error) {
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("request cancellation was lost: %v", ctx.Err())
		}
		return 0, ctx.Err()
	}
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/health/db", nil).WithContext(ctx)
	router := api.NewTestRouter(nil)
	router.GET("/v1/health/db", api.DatabaseHealth(healthCheck))
	router.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", response.Code)
	}
}
