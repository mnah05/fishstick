package api_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/fishstick/internal/testsupport"
)

// Each integration test gets its own schema, including tables and test triggers.
func newIntegrationDB(t *testing.T) *sql.DB {
	return testsupport.IsolatedPostgresDB(t, "api_test", map[string]string{
		"statement_timeout": "10000",
	})
}

// Observe actual database lock waits, rather than guessing goroutine timing.
func waitForJobLockWaiters(t *testing.T, db *sql.DB, count int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var got int
		err := db.QueryRow(`SELECT count(*) FROM pg_stat_activity
   WHERE application_name = current_setting('application_name')
     AND state = 'active' AND wait_event_type = 'Lock'
     AND query LIKE '-- name: GetJobForUpdate%'`).Scan(&got)
		if err != nil {
			t.Fatal(err)
		}
		if got == count {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("did not observe %d requests waiting on the job row lock", count)
}
