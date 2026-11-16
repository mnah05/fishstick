# Database queries

SQL sources live in `queries/`. SQLC reads `../migrations/` as its schema;
those migration files are the source of truth for database changes. Apply them
with the goose CLI:
`goose -dir internal/database/migrations postgres "$DATABASE_URL" up`.
SQLC generation does not change the database.

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
| `job_runs.sql` | Run lookup/listing, claim and lease fencing, heartbeat, success/failure/retry/cancel transitions, expired-lease recovery, manual run snapshots |
| `outbox.sql` | Pending event insertion, relay claims, publish completion/release, retry dispatch, dead-letter insertion and reads |
| `scheduler.sql` | Lock due jobs, isolate each occurrence with savepoints, insert one queued scheduled run with retry snapshots and its pending dispatch event, advance `next_run_at` |

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

The relay and worker own the outbox and run lifecycle queries. See [database
setup](../README.md), [relay](../../../cmd/relay/README.md), [worker](../../../cmd/worker/README.md), and [HTTP API](../../api/README.md).

## Job-update transaction

Updates also bind queries with `WithTx` and lock the job with `GetJobForUpdate`.
`JobHasBusyRun` blocks `RUNNING` and `CANCEL_REQUESTED` executions **while the row’s
`lease_expires_at` is NULL or still in the future**. An expired lease is treated as
not busy so operators can recover stuck work; workers set `lease_expires_at`
when claiming (see `ClaimRunToRunning`). An expired `RUNNING` lease can be
reclaimed with a new fencing token. Validation,
`UpdateJob`, and the final `GetJob` run before commit; errors roll back the
transaction. This uses the same job lock as manual triggers. Worker claims keep
job-before-run lock ordering.
