// Package testsupport holds helpers shared by database-backed tests.
package testsupport

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

// DatabaseURL returns TEST_DATABASE_URL or stops the test. Local runs skip when
// the variable is unset; CI sets TEST_REQUIRE_DATABASE so a missing database is
// a hard failure instead of a silent skip.
func DatabaseURL(t testing.TB) string {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("TEST_DATABASE_URL"))
	if raw != "" {
		return raw
	}

	required := false
	if value := strings.TrimSpace(os.Getenv("TEST_REQUIRE_DATABASE")); value != "" {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			t.Fatalf("TEST_REQUIRE_DATABASE must be a boolean, got %q", value)
		}
		required = parsed
	}
	if required {
		t.Fatal("TEST_DATABASE_URL must be set when TEST_REQUIRE_DATABASE is enabled")
	}
	t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
	return ""
}
