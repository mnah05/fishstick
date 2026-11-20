// Package testsupport holds helpers shared by database-backed tests.
package testsupport

import (
	"database/sql"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/fishstick/internal/database"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
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

// IsolatedPostgresDB creates a unique schema, applies migrations, and returns
// a pool whose search_path points at that schema. Extra connection settings
// are useful for tests that inspect PostgreSQL activity or need a timeout.
func IsolatedPostgresDB(t *testing.T, prefix string, settings map[string]string) *sql.DB {
	t.Helper()
	raw := DatabaseURL(t)
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Fatal("TEST_DATABASE_URL must be a postgres:// or postgresql:// URL")
	}
	admin, err := sql.Open("pgx", raw)
	if err != nil {
		t.Fatal(err)
	}
	schemaPrefix := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' {
			return r
		}
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return '_'
	}, prefix)
	if schemaPrefix == "" || schemaPrefix[0] < 'a' || schemaPrefix[0] > 'z' {
		schemaPrefix = "test_" + schemaPrefix
	}
	schema := schemaPrefix + "_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec("CREATE SCHEMA " + schema); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec("DROP SCHEMA " + schema + " CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
		if err := admin.Close(); err != nil {
			t.Errorf("close admin database pool: %v", err)
		}
	})
	query := u.Query()
	query.Set("search_path", schema)
	query.Set("application_name", schema)
	for key, value := range settings {
		query.Set(key, value)
	}
	u.RawQuery = query.Encode()
	db, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close test database pool: %v", err)
		}
	})
	if err := database.RunMigrations(db, "up"); err != nil {
		t.Fatal(err)
	}
	return db
}
