# Database queries

SQL sources live in `queries/`. SQLC reads `../migrations/` as its schema;
those migration files are the source of truth for database changes. Apply them
with `go run ./cmd/migrate`. SQLC generation does not change the database.

## Generate Go code

From the project root, using sqlc v1.31.1:

```sh
sqlc generate -f internal/database/sqlc/sqlc.yaml
sqlc vet -f internal/database/sqlc/sqlc.yaml
```

Alternatively, `go generate ./internal/database/sqlc` regenerates the code.
Edit SQL sources and configuration, not generated Go files.

| Query source | Queries |
| --- | --- |
| `health.sql` | Database health check (`SELECT 1`) |
| `jobs.sql` | Create, update, and load a job with a row lock |
| `jobs_list.sql` | Fetch jobs, creation idempotency, busy-run check, paginated listing with nullable latest-run fields |
| `job_runs.sql` | Latest run, status counts, get/list runs by job, filtered global run list, cancel run (immediate / request), manual run insert, trigger idempotency lookup, claim-to-running |
| `outbox.sql` | Insert a pending dispatch event |

The package uses `database/sql` and application-generated UUIDv7 IDs. Nullable
JSONB uses `sql.Null[[]byte]`; check `Valid` before decoding `V`. API handlers map
database models into JSON response types rather than exposing SQL null wrappers.

## Manual-trigger transaction

The API calls `db.BeginTx` with read-committed isolation, then
`queries.WithTx(tx)`. All these queries run on the same transaction:

1. `GetJobForUpdate` locks the job row until commit or rollback.
2. `GetManualRunByIdempotencyKey` checks for an existing run for that job/key.
3. `ManualJobRun` inserts the run with snapshots supplied from the locked job.
4. `CreateOutboxRecord` inserts the dispatch event with `{"run_id":"..."}`.

The caller commits after both inserts or rolls back on failure. A replay rolls
back the read-only transaction and returns the existing run. The job lock
serializes concurrent trigger requests; the unique manual-trigger index also
enforces uniqueness in PostgreSQL. Calling either insert on the pool directly
does not provide atomicity across the two writes.

The outbox query only persists an event. No relay, publisher, or worker exists.
See [database setup](../README.md) and [HTTP API](../../api/README.md).

## Job-update transaction

Updates also bind queries with `WithTx` and lock the job with `GetJobForUpdate`.
`JobHasBusyRun` blocks `RUNNING` and `CANCEL_REQUESTED` executions **while the row’s
`lease_expires_at` is NULL or still in the future**. An expired lease is treated as
not busy so operators can recover stuck work; workers must set `lease_expires_at`
when claiming (see `ClaimRunToRunning`). Validation,
`UpdateJob`, and the final `GetJob` run before commit; errors roll back the
transaction. This uses the same job lock as manual triggers. Future worker claims
must lock that job before transitioning a run, using job-before-run lock ordering.
