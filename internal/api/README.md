# HTTP API

`NewRouter(db *sql.DB)` creates the SQLC queries and registers Echo routes under
`/v1`. `cmd/api` serves the router on `:8080`. `NewTestRouter(db)` is the same
routes with minimal Echo middleware for tests (no request-logging middleware).
There is no authentication.
[router.go](router.go) is the authoritative route list.

See [state-machines.md](../../docs/state-machines.md) for persisted state
values, implemented transitions, and transitions that depend on components not
included in this repository.

## Routes

| Method | Path | Success response |
| --- | --- | --- |
| GET | `/v1/health/db` | `200` with database health |
| GET | `/v1/list_runs` | `200` with a filtered page of runs (all jobs) |
| POST | `/v1/job/create` | `201` with job; `200` for idempotent replay |
| GET | `/v1/job/get/:job_id` | `200` with job, latest run, and status counts |
| GET | `/v1/job/list` | `200` with a page of jobs |
| GET | `/v1/job/:job_id/runs` | `200` with a page of runs for that job |
| GET | `/v1/job/:job_id/run/:run_id` | `200` with one run (404 if wrong job or missing) |
| POST | `/v1/cancel_run/:run_id` | `200` immediate cancel or idempotent; `202` cancel requested while running |
| POST | `/v1/job/update/:job_id` | `200` with updated job |
| POST | `/v1/job/trigger/:job_id` | `202` with new or existing run |

Dead-letter queue HTTP routes are **not** implemented yet (see [future-endpoints.md](../docs/future-endpoints.md)).
Neither are the scheduler, outbox relay, RabbitMQ publishing, or workers.

## Create a job

Send a JSON object containing `name`, `event_type`, and `schedule_type`.
`payload` defaults to `{}`. Optional settings are `scheduled_at`,
`cron_expression`, `timezone`, `max_attempts`, `retry_delay_seconds`, and
`timeout_seconds`.

| Schedule | Validation and stored next run |
| --- | --- |
| `MANUAL` | No scheduled timestamp or nonblank cron expression; `next_run_at` is null |
| `ONCE` | Future `scheduled_at` required; no nonblank cron expression; next run equals that timestamp |
| `CRON` | Cron expression required; no scheduled timestamp; next run is computed in the selected timezone |

Cron accepts five-field expressions and descriptors supported by `robfig/cron`.
Timezone defaults to `UTC` and must be a valid IANA location. Retry/timeout
defaults are 3 attempts, 30 seconds of retry delay, and 120 seconds of timeout.
Attempts and timeout must be positive; retry delay may be zero.

The optional `Idempotency-Key` header is trimmed and stored on the job. Reusing
it with the same decoded request hash returns the existing job; a different hash
returns `409 IDEMPOTENCY_KEY_REUSED`. The key is global across job creation.
Validation runs before replay lookup, so a repeated ONCE request whose timestamp
is now in the past fails validation. Hashing occurs before applying storage
defaults; omitted values and explicit defaults need not produce the same hash.

Creation stores a definition only. It does not create a run or dispatch event.

## Read and list jobs

Get returns the job fields plus `latest_run` (null if absent) and `run_counts`
(a map of status to count). Jobs have no stored status field; run status is
available through the latest-run summary.

List accepts `schedule_type`, `event_type`, `limit` (default 50, maximum 100), and
`cursor` (a UUID). Results use descending job IDs; pass `next_cursor` into the
next request. The response has `data` and, when more results exist, `next_cursor`.
Each item includes the job and a brief latest-run ID/status when available.

Jobs without runs are included and omit `latest_run`. The query joins the latest
run by ID and SQLC maps the outer-joined ID and status to nullable Go fields.

## List and get runs

`GET /v1/list_runs` returns pages of **all** runs (newest run `id` first), with the
same `limit` and `cursor` pagination as `/v1/job/list`. Optional filters (all query
string):

- `job_id` — restrict to one job (UUID).
- `status` — exact match; must be one of `SCHEDULED`, `QUEUED`, `RUNNING`, `RETRY_WAIT`, `CANCEL_REQUESTED`, `SUCCEEDED`, `FAILED`, `CANCELLED`.
- `trigger_source` — `MANUAL` or `SCHEDULED`.
- `event_type` — matches `event_type_snapshot` on the run (exact string).

The response shape is the same as job-scoped run listing: `data` (array of run objects) and optional `next_cursor`.

`GET /v1/job/:job_id/runs` returns pages of runs for that job only, newest first
(order by run `id` descending). It uses the same `limit` (default 50, max 100)
and `cursor` (run id) query parameters as job list.

`GET /v1/job/:job_id/run/:run_id` returns a single run. The composite `(job_id,
run_id)` must match; otherwise the API responds `404 NOT_FOUND` (no existence leak
across jobs).

## Cancel a run

`POST /v1/cancel_run/:run_id` cancels execution by **run id** only (no `job_id` in the path).

Optional JSON body:

```json
{ "reason": "user requested stop" }
```

An empty body is allowed.

The handler uses a transaction, locks the run with `SELECT ... FOR UPDATE`, then:

| Current status | Effect | HTTP |
| --- | --- | --- |
| `QUEUED`, `SCHEDULED`, `RETRY_WAIT` | Set `CANCELLED`, `cancel_requested_at`, `cancelled_at`, optional `cancel_reason` | `200` |
| `RUNNING` | Set `CANCEL_REQUESTED`, `cancel_requested_at`, optional `cancel_reason` (worker should observe and finish) | `202` |
| `CANCEL_REQUESTED` | No row change (idempotent) | `200` |
| `SUCCEEDED`, `FAILED`, `CANCELLED` | Refuse | `409` with code `RUN_ALREADY_TERMINAL` |
| Any other status | Refuse | `409` with code `RUN_NOT_CANCELLABLE` |

The run row is locked with `FOR UPDATE` before the status switch, so the conditional update always sees the same status the switch did.

**Planned later (not implemented):** DLQ list/get — see [future-endpoints.md](../docs/future-endpoints.md).

## Update a job

Send a JSON object with any subset of the create fields. Omitted fields keep
their values. `scheduled_at: null` explicitly clears that timestamp; schedule
type changes also clear incompatible fields. Validation runs on the merged job
and recomputes `next_run_at`, so an existing ONCE timestamp in the past must be
changed before the update can succeed. Existing run snapshots are not updated.

Updates start a read-committed transaction and acquire the job row with
`GetJobForUpdate`. The busy check, merged-field validation, update, and response
row read all use transaction-bound queries. The handler commits before returning
success; any failure rolls back the transaction.

Only `RUNNING` and `CANCEL_REQUESTED` runs block updates with `409 JOB_BUSY`, and
only while the run’s `lease_expires_at` is **NULL** (legacy or unset) or **after**
`CURRENT_TIMESTAMP`. If a worker wrote a lease and it has expired, the run no
longer counts as busy and updates may proceed (so stuck claims do not block edits
forever). Queued, scheduled, retry-waiting, and terminal runs allow changes while keeping
their existing snapshots. Concurrent partial updates read the latest committed
job after acquiring its lock, preventing one update from overwriting an unrelated
field changed by another. Triggers use the same lock, so a new run snapshots one
consistent job version.

Worker-style claims should use **`ClaimRunToRunning`** in the same transaction as
`GetJobForUpdate`: set `lease_expires_at` and `worker_id`, and only transition from
`QUEUED` / `SCHEDULED` / `RETRY_WAIT`. That keeps the job row lock through the
state change so updates and claims serialize.

## Trigger a manual run

Send no body and supply a nonblank `Idempotency-Key`. The route accepts any
existing job, including ONCE and CRON definitions, and leaves automatic schedule
fields unchanged. Overlapping manual executions are allowed.

The handler in [api.runs.go](api.runs.go) performs these steps:

1. Validate the job UUID, header, and absence of a body.
2. Begin a read-committed transaction and bind queries with `WithTx`.
3. Load the job with `GetJobForUpdate`, holding its row lock.
4. Look up the key for that job. Return the existing run if its hash matches;
   return `409 IDEMPOTENCY_KEY_REUSED` otherwise. Release the lock by rolling back
   the read-only transaction before responding.
5. Allocate UUIDv7 IDs and call `ManualJobRun` to insert a `QUEUED` run with zero
   attempts and `scheduled_for = NOW()`. Copy event type, payload, timeout, and
   maximum attempts from the locked job.
6. Call `CreateOutboxRecord` separately to insert a `PENDING`, `JOB_DISPATCH`
   event with a message containing the run ID.
7. Commit, then return `202` with the run. An error before commit rolls back both
   inserts.

The lock serializes triggers for the same job. Once a waiting transaction gets
the lock, its next query sees any run committed by the previous request. A
unique index on the job/key pair provides database-level duplicate protection.
Keys are scoped to each job. Because no request body is accepted, the hash is
computed from an empty object; a changed job definition does not change the
meaning of an already-used key. Replay returns the stored run's current state.

Run JSON includes IDs, status, attempts, execution snapshots as `event_type` and
`payload`, timeout, worker information, timestamps, errors, and result. Nullable
fields become JSON null. Internal idempotency fields and the fencing token are
not exposed. `202` means the run and event are persisted, not that execution has
started. Events stay pending until a relay is implemented.

## Health and errors

Health executes `SELECT 1` with a three-second timeout and sends
`Cache-Control: no-store`. Success is `{"status":"ok","database":"connected"}`;
a query failure or unexpected result returns `503 DATABASE_UNAVAILABLE`.

Handler errors use `{"error":{"code":"...","message":"..."}}`, with optional
`details.fields` for field validation errors. Echo-generated routing or panic
errors may use Echo's own format.

| HTTP status | Code | Meaning |
| --- | --- | --- |
| 400 | `VALIDATION_ERROR` | Invalid fields, UUID, pagination, trigger header, or body |
| 404 | `NOT_FOUND` | Unknown job or run |
| 409 | `JOB_BUSY` | A RUNNING or CANCEL_REQUESTED run blocks the update |
| 409 | `RUN_ALREADY_TERMINAL` | Cancel requested for a finished run |
| 409 | `RUN_NOT_CANCELLABLE` | Run is not in a cancellable state |
| 409 | `IDEMPOTENCY_KEY_REUSED` | Stored request hash differs |
| 500 | `INTERNAL_ERROR` | Database or internal operation failed |
| 503 | `DATABASE_UNAVAILABLE` | Health check failed |

## Verification

```sh
go test ./...
TEST_DATABASE_URL='postgres://postgres:postgres@localhost:5432/fishstick?sslmode=disable' \
  go test ./test/api ./test/database -count=1
```

The API integration tests create and remove isolated schemas. They test trigger
replay and rollback, nullable latest runs with pagination, update eligibility
for every run state, unchanged snapshots, commit failure rollback, concurrent
partial updates and triggers, and the worker-claim locking protocol. See [database test setup](../database/README.md#integration-tests).
