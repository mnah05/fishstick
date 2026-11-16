package runstatus_test

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/fishstick/internal/runstatus"
)

func TestSQLRunStatusesMatchRegistryAndHaveWriters(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	paths, err := filepath.Glob(filepath.Join(root, "internal/database/sqlc/queries/*.sql"))
	if err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile(filepath.Join(root, "internal/database/migrations/00004_drop_scheduled_run_status.sql"))
	if err != nil {
		t.Fatal(err)
	}
	check := regexp.MustCompile(`(?s)CHECK\s*\(status\s+IN\s*\(([^)]*)\)\)`).FindSubmatch(migration)
	if len(check) != 2 {
		t.Fatal("migration 00004 does not define the run status check")
	}
	constraintStatuses := map[string]bool{}
	for _, match := range regexp.MustCompile(`'([A-Z_]+)'`).FindAllSubmatch(check[1], -1) {
		constraintStatuses[string(match[1])] = true
	}
	for _, status := range runstatus.All() {
		if !constraintStatuses[status] {
			t.Errorf("migration status check omits %q", status)
		}
	}
	if constraintStatuses["SCHEDULED"] {
		t.Error("migration status check still permits SCHEDULED")
	}
	known := map[string]bool{}
	for _, status := range runstatus.All() {
		known[status] = true
	}
	statusLiteral := regexp.MustCompile(`(?i)status\s*(?:=|<>|IN)\s*(?:\(([^)]*)\)|'([A-Z_]+)')`)
	quoted := regexp.MustCompile(`'([A-Z_]+)'`)
	queryText := strings.Builder{}
	queries := map[string]string{}
	for _, path := range paths {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		queryText.Write(b)
		name := ""
		body := strings.Builder{}
		flush := func() {
			if name != "" {
				queries[name] = body.String()
			}
		}
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "-- name: ") {
				flush()
				name = strings.Fields(strings.TrimPrefix(line, "-- name: "))[0]
				body.Reset()
			}
			body.WriteString(line)
			body.WriteByte('\n')
		}
		flush()
		for _, match := range statusLiteral.FindAllStringSubmatch(string(b), -1) {
			part := match[1]
			if part == "" {
				part = match[2]
			}
			for _, token := range quoted.FindAllStringSubmatch(part, -1) {
				if !known[token[1]] {
					t.Errorf("%s contains unregistered run status %q", filepath.Base(path), token[1])
				}
			}
		}
	}
	writers := map[string][]string{
		runstatus.Queued:          {"ManualJobRun", "CreateScheduledRun"},
		runstatus.Running:         {"ClaimRunToRunning"},
		runstatus.RetryWait:       {"ScheduleRunRetry"},
		runstatus.CancelRequested: {"CancelRun"},
		runstatus.Succeeded:       {"CompleteRunSucceeded"},
		runstatus.Failed:          {"CompleteRunFailed", "FailRunPermanent"},
		runstatus.Cancelled:       {"CancelRun", "CompleteRunCancelled"},
	}
	for status, names := range writers {
		found := false
		for _, name := range names {
			if body, ok := queries[name]; ok && strings.Contains(body, "'"+status+"'") {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("writable status %q has no transition query", status)
		}
	}
}

func TestRunStatusPredicates(t *testing.T) {
	if len(runstatus.All()) != 7 {
		t.Fatalf("All()=%v, want seven statuses", runstatus.All())
	}
	for _, status := range runstatus.All() {
		if !runstatus.Valid(status) {
			t.Errorf("Valid(%q)=false", status)
		}
	}
	if runstatus.Valid("SCHEDULED") {
		t.Fatal("removed run status SCHEDULED is still valid")
	}
	for _, status := range []string{runstatus.Succeeded, runstatus.Failed, runstatus.Cancelled} {
		if !runstatus.Terminal(status) {
			t.Errorf("Terminal(%q)=false", status)
		}
	}
	for _, status := range []string{runstatus.Queued, runstatus.RetryWait, runstatus.Running} {
		if !runstatus.Claimable(status) {
			t.Errorf("Claimable(%q)=false", status)
		}
	}
}
