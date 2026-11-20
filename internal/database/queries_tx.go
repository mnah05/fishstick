package database

import (
	"context"
	"database/sql"

	"github.com/fishstick/internal/database/sqlc"
	"github.com/google/uuid"
)

// BeginQueries starts a transaction and returns SQLC queries bound to it.
func BeginQueries(ctx context.Context, db *sql.DB, options *sql.TxOptions) (*sql.Tx, *sqlc.Queries, error) {
	tx, err := db.BeginTx(ctx, options)
	if err != nil {
		return nil, nil, err
	}
	return tx, sqlc.New(tx), nil
}

// LockJobRunForUpdate enforces the job-before-run lock order used by worker
// claims and expired-lease recovery.
func LockJobRunForUpdate(ctx context.Context, queries *sqlc.Queries, runID uuid.UUID) (sqlc.JobRun, error) {
	if _, err := queries.GetJobForRunUpdate(ctx, runID); err != nil {
		return sqlc.JobRun{}, err
	}
	return queries.GetJobRunByIDForUpdate(ctx, runID)
}
