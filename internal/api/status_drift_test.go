package api

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/fishstick/internal/runstatus"
)

// TestCancelDispositionsMatchCancelRunSQL keeps the API's per-status cancel
// behavior and the conditional update in CancelRun on one definition.
func TestCancelDispositionsMatchCancelRunSQL(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(file), "..", "database", "sqlc", "queries", "job_runs.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)

	const marker = "-- name: CancelRun "
	start := strings.Index(text, marker)
	if start < 0 {
		t.Fatal("job_runs.sql does not define CancelRun")
	}
	body := text[start+len(marker):]
	if next := strings.Index(body, "-- name: "); next >= 0 {
		body = body[:next]
	}

	inList := regexp.MustCompile(`status IN \(([^)]*)\)`).FindStringSubmatch(body)
	if len(inList) != 2 {
		t.Fatal("CancelRun has no status IN (...) guard")
	}
	sqlStatuses := map[string]bool{}
	for _, match := range regexp.MustCompile(`'([A-Z_]+)'`).FindAllStringSubmatch(inList[1], -1) {
		sqlStatuses[match[1]] = true
	}

	apiStatuses := map[string]bool{}
	seen := map[string]bool{}
	for _, status := range runstatus.All() {
		disposition, ok := runStatusCancellation[status]
		if !ok {
			t.Errorf("no cancel disposition for %q", status)
			continue
		}
		if disposition == cancelUnsupported {
			t.Errorf("%q maps to cancelUnsupported despite being a persisted status", status)
		}
		seen[status] = true
		switch disposition {
		case cancelImmediately, cancelCooperatively:
			apiStatuses[status] = true
		case cancelAlreadyRequested:
			if status != runstatus.CancelRequested {
				t.Errorf("%q maps to cancelAlreadyRequested", status)
			}
		case cancelTerminal:
			if !runstatus.Terminal(status) {
				t.Errorf("non-terminal %q maps to cancelTerminal", status)
			}
		}
	}
	for status := range runStatusCancellation {
		if !seen[status] {
			t.Errorf("cancel disposition exists for unregistered status %q", status)
		}
	}

	for status := range sqlStatuses {
		if !apiStatuses[status] {
			t.Errorf("CancelRun guard includes %q but the API does not cancel it", status)
		}
	}
	for status := range apiStatuses {
		if !sqlStatuses[status] {
			t.Errorf("API cancels %q but CancelRun guard omits it", status)
		}
	}
}
