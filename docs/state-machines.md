# Application state machines

This document records the states and transitions currently represented by
Fishstick. The migrations are authoritative for persisted values. API behavior
is implemented in `internal/api`. The scheduler in `cmd/schedular` creates
due occurrences. Worker completion, outbox relay, and dead-letter resolution
are not part of this repository yet.

## Job definitions

Jobs do not have a lifecycle status. Their schedule is a tagged configuration
constrained by the database and API:

| `schedule_type` | Required fields | Fields that must be absent | `next_run_at` |
| --- | --- | --- | --- |
| `MANUAL` | — | `scheduled_at`, `cron_expression` | null |
| `ONCE` | `scheduled_at` | `cron_expression` | the one-time timestamp, or null after the scheduler consumes it |
| `CRON` | nonblank `cron_expression` | `scheduled_at` | next cron occurrence |

Migration `00002_require_scheduled_next_run.sql` required `next_run_at` for
`ONCE` and `CRON`. Migration `00003_allow_exhausted_once_schedule.sql` allows
null `next_run_at` on `ONCE` only. That null means the one-time occurrence has
been created and the job will not be selected again. `CRON` still requires
`next_run_at`. `MANUAL` requires it to remain null.

Create and update validate through `internal/api/schedule.go` and
`internal/schedule`. An update of an exhausted `ONCE` job still requires a
future `scheduled_at`, because the API recomputes `next_run_at` from the merged
schedule. The scheduler is the only writer that stores a null `next_run_at` on
an existing `ONCE` job.

## Scheduler occurrence

`cmd/schedular` polls `next_run_at <= now`. Each poll locks a batch with
`FOR UPDATE SKIP LOCKED`, then for each locked job, in a savepoint:

| Schedule | Run written | `next_run_at` after commit |
| --- | --- | --- |
| `ONCE` | one `QUEUED` run, `trigger_source = SCHEDULED`, `scheduled_for` equal to the due `next_run_at` | null (exhausted) |
| `CRON` | same run shape | the next tick strictly after poll time, in the job timezone |
| anything else, or a cron/timezone the shared parser rejects | no run and no outbox row | unchanged; the job stays due |

The same transaction inserts one `PENDING` `JOB_DISPATCH` outbox row whose
message is `{"run_id":"<persisted run id>"}`. A unique `(job_id, scheduled_for)`
index for `SCHEDULED` runs, plus the outbox existence check, makes a retried
occurrence idempotent. A failed savepoint does not undo other jobs in the
batch. No scheduler state is stored on the job beyond `next_run_at`.

### Scheduler configuration

`cmd/schedular` loads its settings with `config.NewSchedulerConfig()`. Koanf
applies defaults, then reads `SCHEDULER_*` environment overrides and validates
the result. Scheduler settings are separate from the database config used by
the API.

| Setting | Default | Effect |
| --- | --- | --- |
| `SCHEDULER_INTERVAL` | `5s` | Delay between polling passes |
| `SCHEDULER_BATCH_SIZE` | `100` | Maximum due job rows locked per pass |
| `SCHEDULER_SHUTDOWN_GRACE` | `10s` | Shutdown deadline for the health HTTP server |
| `SCHEDULER_HEALTH_ADDRESS` | `:8081` | Listen address for `/healthz` |

Durations use Go duration syntax. The interval, batch size, and shutdown grace
must be positive; the batch size must fit in a signed 32-bit integer. The health
address cannot be blank. `internal/config/.env.example` lists these settings,
but dotenv files are not loaded automatically.

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
initial migration. Manual trigger and the scheduler both create `QUEUED`.
`SCHEDULED` remains a schema value with no writer in this repository. The
implemented claim query permits `QUEUED`, `SCHEDULED`, or `RETRY_WAIT` to
become `RUNNING`.

| From | To | Current owner / condition |
| --- | --- | --- |
| `QUEUED` | `RUNNING` | Worker claim query; worker is not included |
| `SCHEDULED` | `RUNNING` | Worker claim query; no code inserts `SCHEDULED` |
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
`PUBLISHED`. Manual trigger and the scheduler insert `PENDING` only. The intended relay lifecycle is:

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
- The scheduler process moves **polling → stopped** on SIGINT/SIGTERM. The
  in-flight poll uses the cancelled context, then the health server shuts down
  within `SCHEDULER_SHUTDOWN_GRACE`. A failed health listener also stops the
  process. There is no separate scheduler lifecycle row.

