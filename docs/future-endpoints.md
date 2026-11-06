# Planned HTTP endpoints (not implemented)

Dead-letter queue introspection — **not wired yet**. Likely shapes for a later iteration:

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/v1/dlq` | Paginated list of rows in `dead_letter_jobs` (filter by resolution, job, run id). |
| GET | `/v1/dlq/:run_id` | Single DLQ entry by `run_id` (joined with job/run context as needed). |

Schema reference: `dead_letter_jobs` in [`internal/database/migrations/00001_initial_schema.sql`](../internal/database/migrations/00001_initial_schema.sql).
