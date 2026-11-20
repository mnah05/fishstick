# Dead-letter queue API

The dead-letter introspection endpoints are implemented:

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/v1/dlq` | Paginated list; supports `resolution_status`, `job_id`, and `run_id` filters. |
| GET | `/v1/dlq/:run_id` | Read one entry by run ID. |

List responses use `data` and optional `next_cursor`, matching other list
endpoints. The cursor is opaque and should be passed back unchanged.

The worker inserts an `OPEN` entry in the same transaction that marks a handler
failure `FAILED` after its configured attempts are exhausted. The lease watcher
does the same for an expired `RUNNING` lease at the attempt limit. This MVP
exposes reads; manual resolution and retry linkage remain follow-up work.
