# Job run cancellation

PostgreSQL stores the authoritative state. The API records a cancellation request;
the worker observes it during lease heartbeats and stops the handler through its
`context.Context`. A cooperative handler ends in `CANCELLED`.

Run statuses are `QUEUED`, `RUNNING`, `RETRY_WAIT`, `CANCEL_REQUESTED`,
`SUCCEEDED`, `FAILED`, and `CANCELLED`. The source value
`trigger_source = 'SCHEDULED'` identifies scheduler-created runs and is not a
run status.

## Transitions

| Current state | Request / event | Result |
| --- | --- | --- |
| `QUEUED`, `RETRY_WAIT` | Cancel API request | `CANCELLED` immediately |
| `RUNNING` | Cancel API request | `CANCEL_REQUESTED`; worker stops cooperatively |
| `CANCEL_REQUESTED` | Worker handler stops | `CANCELLED` |
| `CANCEL_REQUESTED` | Lease expires after worker crash | Worker recovery finalizes `CANCELLED` |
| Terminal state | Cancel API request | Conflict; terminal state is preserved |

`CancelRun` makes the API transition conditional. Worker completion queries also
check `worker_id` and `fencing_token`; a worker whose lease was reclaimed cannot
overwrite the current owner. Heartbeats extend the lease and check for
`CANCEL_REQUESTED`. Cancellation is cooperative: Go cannot stop a handler that
ignores its context.

The worker uses the database state directly and does not publish a separate
`JOB_CANCEL` message. The outbox schema still permits that event kind for a
future integration.

## Related files

- API behavior: [`internal/api/README.md`](../internal/api/README.md)
- State owners and transition table: [`state-machines.md`](state-machines.md)
- SQL transitions: [`internal/database/sqlc/queries/job_runs.sql`](../internal/database/sqlc/queries/job_runs.sql)
