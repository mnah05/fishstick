# Execution worker

The worker consumes `JOB_DISPATCH` JSON containing a `run_id`; it then reads the
run and immutable event snapshot from PostgreSQL. This uses the RabbitMQ intake
path recommended in `PLAN.md`; it does not poll queued rows directly.

Claiming locks the job before the run, increments the fencing token and attempt
count, and assigns a lease. Heartbeats renew the lease and observe cancellation.
Redelivered messages wait for an active lease to expire before reclaiming the
run. The separate `cmd/watcher` process proactively recovers expired leases:
attempts remaining schedule a delayed outbox dispatch from the run snapshot;
exhausted attempts create an `OPEN` DLQ row. Both paths clear the old worker and
lease, and stale handlers cannot complete after the state transition.

Configure `DATABASE_*` and `WORKER_*` from
[`internal/config/.env.example`](../../internal/config/.env.example). The
development handler registered as `*` returns `{}` successfully for any event
type. Replace it with event-specific handlers before using the worker to execute
real jobs. Handler code must honor its context for cooperative cancellation and
shutdown. `WORKER_STUB_DELAY` slows this development handler for concurrency
checks; it defaults to `0s` and should stay disabled for normal execution.

The lifecycle functions are in [main.go](main.go). PostgreSQL integration tests
cover success, retry exhaustion, cancellation, lease recovery, and fencing when
`TEST_DATABASE_URL` is set.
