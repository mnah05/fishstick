package api

import (
	"context"
	"database/sql"
	"net/http"

	"github.com/fishstick/internal/database/sqlc"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

// server owns the pool and the queries built from it, so handlers cannot end up
// using a transaction from one pool with queries from another.
type server struct {
	db      *sql.DB
	queries *sqlc.Queries
}

func newServer(db *sql.DB) *server {
	return &server{db: db, queries: sqlc.New(db)}
}

func (s *server) beginTx(ctx context.Context) (*sql.Tx, *sqlc.Queries, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, nil, err
	}
	return tx, s.queries.WithTx(tx), nil
}

// pathUUID parses a UUID route parameter and writes the standard validation
// response when it is malformed.
func pathUUID(c echo.Context, name string) (uuid.UUID, bool) {
	id, err := uuid.Parse(c.Param(name))
	if err != nil {
		_ = writeAPIError(c, http.StatusBadRequest, "VALIDATION_ERROR", name+" must be a UUID", nil)
		return uuid.Nil, false
	}
	return id, true
}

// paginate asks callers to fetch limit+1 rows, trims the extra row, and returns
// the cursor for the next page.
func paginate[T any](rows []T, limit int32, idOf func(T) uuid.UUID) ([]T, *string) {
	if len(rows) <= int(limit) {
		return rows, nil
	}
	rows = rows[:limit]
	return rows, nextCursorFromID(idOf(rows[len(rows)-1]))
}
