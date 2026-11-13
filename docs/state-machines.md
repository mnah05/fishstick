# Application state machines

This document records the states and transitions currently represented by
Fishstick. The migrations are authoritative for persisted values. API behavior
is implemented in `internal/api`; scheduler, worker, outbox relay, and
dead-letter resolution processes are not part of this repository yet.

## Job definitions

Jobs do not have a lifecycle status. Their schedule is a tagged configuration
constrained by the database and API:

| `schedule_type` | Required fields | Fields that must be absent | `next_run_at` |
| --- | --- | --- | --- |
| `MANUAL` | — | `scheduled_at`, `cron_expression` | null |
| `ONCE` | `scheduled_at` | `cron_expression` | one-time timestamp |
| `CRON` | nonblank `cron_expression` | `scheduled_at` | next cron occurrence |

Migration `00002_require_scheduled_next_run.sql` makes `next_run_at` mandatory
for `ONCE` and `CRON`; `MANUAL` requires it to remain null.

Create validates this configuration. Update merges the patch, clears fields
that conflict with a changed schedule type, then validates and recalculates the
next run. A scheduler that consumes `next_run_at` and creates scheduled runs is
not implemented.

## Job and trigger idempotency

Create keys are global. Trigger keys are scoped to a job. Both pair the key
with a request hash:

| Existing key | Matching hash | Different hash |
| --- | --- | --- |
| No | Create resource | — |
| Yes | Return existing resource | `409 IDEMPOTENCY_KEY_REUSED` |

For manual triggers, creating the queued run and pending outbox event is one
database transaction. The unique indexes are the final guard against duplicate
keys when requests race.

## Run lifecycle

Persisted run statuses are constrained by the `job_runs.status` check in the
initial migration. `MANUAL` trigger creates `QUEUED`; a scheduled-run producer
may create `SCHEDULED`. The implemented claim query permits `QUEUED`,
`SCHEDULED`, or `RETRY_WAIT` to become `RUNNING`.

| From | To | Current owner / condition |
| --- | --- | --- |
| `QUEUED` | `RUNNING` | Worker claim query; worker is not included |
| `SCHEDULED` | `RUNNING` | Worker claim query; scheduler is not included |
| `RETRY_WAIT` | `RUNNING` | Worker claim query; retry producer is not included |
| `QUEUED`, `SCHEDULED`, `RETRY_WAIT` | `CANCELLED` | Implemented by cancel API |
| `RUNNING` | `CANCEL_REQUESTED` | Implemented by cancel API |
| `RUNNING` | `SUCCEEDED`, `FAILED`, `RETRY_WAIT`, `CANCELLED` | Worker completion; not included |
| `CANCEL_REQUESTED` | `SUCCEEDED`, `FAILED`, `CANCELLED` | Worker completion/recovery; not included |

`SUCCEEDED`, `FAILED`, and `CANCELLED` are terminal. Re-cancelling
`CANCEL_REQUESTED` is idempotent. Cancelling a terminal run returns conflict.
The API classification lives in `internal/api/status.go`; `CancelRun` in
`internal/database/sqlc/queries/job_runs.sql` applies the corresponding update
to pre-run and `RUNNING` states. Worker completion and recovery transitions
remain intentionally absent until those components are implemented.

## Job update versus run claim

This is a coordination gate rather than a job status. Both job update and the
documented worker claim protocol lock the same job row. After that lock, an
update returns `JOB_BUSY` when a `RUNNING` or `CANCEL_REQUESTED` run has no
lease expiry or an unexpired lease; otherwise it may save the job definition.
The claim query must keep the lock through its run status change. This serializes
job snapshots and edits. Claim execution is not currently wired to a worker.

## Outbox delivery

The schema in `00001_initial_schema.sql` allows `PENDING`, `INFLIGHT`, and
`PUBLISHED`. Current trigger code
only inserts `PENDING`. The intended relay lifecycle is:

```text
PENDING -> INFLIGHT -> PUBLISHED
              |
              +-> PENDING (failed or expired claim, when relay is implemented)
```

No relay currently claims, retries, or publishes events.

## Dead-letter resolution

The schema in `00001_initial_schema.sql` allows `OPEN`, `RETRIED`, and
`RESOLVED`. The intended resolution
transitions are `OPEN -> RETRIED` or `OPEN -> RESOLVED`. No dead-letter
producer, resolver, or API route is currently implemented.

## Request and process control

- A `scheduled_at` patch value is **omitted** (leave unchanged), **null**
  (clear), or a **timestamp** (set). This is field-presence state, not a
  persisted job lifecycle.
- A transactional handler follows **begin → commit** or **begin → rollback**.
  Deferred rollback covers early returns; trigger and cancellation also have
  explicit read-only/idempotent branches.
- The API process moves **serving → draining** on SIGINT/SIGTERM. Draining
  rejects new requests while in-flight handlers finish within the grace period.

