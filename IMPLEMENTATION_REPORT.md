# PLAN.md implementation report

## Delivered

- Added `internal/runstatus` as the Go registry for persisted run states. API
  validation and cancellation dispositions use it. Tests compare SQL literals
  and the current migration constraint against the registry.
- Added migration `00004_drop_scheduled_run_status.sql`. It maps legacy
  `job_runs.status = 'SCHEDULED'` rows to `QUEUED`, removes the status from the
  constraint, and drops its partial index. `trigger_source = 'SCHEDULED'`
  remains supported. The API filter change is recorded in `CHANGELOG.md`.
- Added migration `00005_snapshot_retry_delay.sql` after discovering the plan's
  retry-delay snapshot column was absent from the actual schema. Manual and
  scheduled runs now copy `retry_delay_seconds` from their job definition.
- Added `cmd/relay`. It claims pending or expired outbox rows, declares a durable
  RabbitMQ exchange and worker queue, publishes persistent JSON with broker
  confirms, and records success or retry with bounded backoff.
- Added `cmd/worker`. It consumes run IDs from RabbitMQ, locks job before run,
  increments attempt and fencing tokens, renews leases, runs a handler with a
  timeout, completes success/cancel/failure paths, and schedules delayed retry
  dispatches. Expired leases are reclaimed; exhausted attempts create a DLQ
  record atomically with failure.
- Added `GET /v1/dlq` and `GET /v1/dlq/:run_id`, including resolution/job/run
  filters and an opaque cursor for stable pagination.
- Added separate relay and worker configuration, optional RabbitMQ Compose
  profile, run instructions, updated state and cancellation docs, and numbered
  comments that walk through the new code paths.

## Important tests added

- SQL status registry, migration constraint, SQL transition drift, and removed
  `SCHEDULED` API filter behavior.
- Relay publish failure release, retry attempt accounting, and successful
  publication state.
- Worker success, retry dispatch, retry exhaustion/DLQ, cooperative cancellation,
  expired cancellation recovery, expired final-attempt DLQ, redelivery waiting,
  and stale-worker fencing.
- DLQ filters, pagination, item lookup, missing-item response, and invalid cursor.
- Relay/worker configuration defaults and invalid lease/batch settings.

The worker's built-in `*` handler is a development stub that returns `{}` for
every event type. Replace it with event-specific handlers before processing real
jobs. DLQ read endpoints and production are implemented; manual DLQ resolution
and retry linkage remain deferred as allowed by the plan.

## Verification

- `go test ./...` — passed.
- `sqlc generate -f internal/database/sqlc/sqlc.yaml` — passed.
- `sqlc vet -f internal/database/sqlc/sqlc.yaml` — passed.
- `docker compose -f .docker/docker-compose.yml --profile messaging config` — passed.
- `git diff --check` — passed.

`TEST_DATABASE_URL` was unset, so PostgreSQL integration tests skipped in this
environment. The tests are ready to run against an isolated test database by
setting that variable. No live RabbitMQ publish/consume run was performed.
