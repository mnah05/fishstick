# Job run cancellation (design)

This document describes how cancellation is intended to work in Fishstick: what the database already supports, how the API and workers should behave, and what is still not wired up in this repository.

**PostgreSQL is the source of truth.** In-process `context.Context` cancellation is for cooperatively stopping handlers; it does not replace row state in `job_runs`.

---

## What already exists in the schema

The initial migration and sqlc snapshot define everything needed at the row level:

- **Statuses** on `job_runs.status` include: `SCHEDULED`, `QUEUED`, `RUNNING`, `RETRY_WAIT`, `CANCEL_REQUESTED`, `SUCCEEDED`, `FAILED`, `CANCELLED` (see [`internal/database/migrations/00001_initial_schema.sql`](../internal/database/migrations/00001_initial_schema.sql)).
- **Cancellation metadata** on `job_runs`:
  - `cancel_requested_at` — when a cancel request was recorded (API or system).
  - `cancelled_at` — when cancellation was completed (terminal “we are done stopping”).
  - `cancel_reason` — optional human-readable reason.
- **Leases and workers**: `worker_id`, `lease_expires_at`, `heartbeat_at` — used so only the current lease holder should drive execution and finalize state; stale workers must not win races.
- **Outbox** allows `event_kind = 'JOB_CANCEL'` on [`outbox_events`](../internal/database/migrations/00001_initial_schema.sql) for future dispatch of cancel signals; the MVP described here does not require a separate RabbitMQ cancel queue.
- **Job updates** block while a run is considered “busy”: the API’s busy check treats `RUNNING` and `CANCEL_REQUESTED` as blocking **only while** `lease_expires_at` is NULL or still in the future (see [`internal/api/README.md`](../internal/api/README.md)).

---

## Intended state machine (runs)

| State | Meaning |
| --- | --- |
| `QUEUED` / `SCHEDULED` | Waiting to be claimed / executed |
| `RUNNING` | Worker is executing the handler |
| `CANCEL_REQUESTED` | User/system asked to cancel; worker must stop cooperatively |
| `CANCELLED` | Run ended as cancelled |
| `SUCCEEDED` / `FAILED` | Other terminals |

**Target transitions (conceptual):**

- Not yet running (`QUEUED`, `SCHEDULED`, and similar pre-run states): cancel can move straight to **`CANCELLED`** (set `cancelled_at`, clear or set reason as needed).
- **`RUNNING`**: cancel should set **`CANCEL_REQUESTED`**, set `cancel_requested_at`, and let the worker observe that and/or a cancelled `context.Context`, then transition to **`CANCELLED`** when the handler has stopped and the row is updated safely.
- **`CANCEL_REQUESTED`**: idempotent “already cancelling”; repeat cancel requests should return a stable success-ish response without fighting the worker.
- **Terminal states** (`SUCCEEDED`, `FAILED`, `CANCELLED`, etc.): **no further cancellation** — respond with conflict (e.g. HTTP `409`).

If the handler **finishes successfully** while a cancel is in flight, the run may end as **`SUCCEEDED`** instead of **`CANCELLED`**. The database layer should use **conditional updates** (status-only transitions) so exactly one terminal outcome wins without silent overwrites.

---

## API shape (recommended, not all implemented here)

A dedicated endpoint is the usual surface:

```http
POST /v1/cancel_run/{run_id}
```

Optional JSON body for `cancel_reason`. Responsibilities:

1. Load the run by `run_id` (and enforce `job_id` if you include it in the path for safety).
2. In a **transaction**, apply **conditional** updates:
   - If status is pre-running → set `CANCELLED`, `cancel_requested_at` / `cancelled_at` as appropriate, **`200`**.
   - If `RUNNING` → set `CANCEL_REQUESTED`, `cancel_requested_at`, **`202`** (accepted, stop in progress).
   - If already `CANCEL_REQUESTED` → idempotent **`200`** (or **`202`** — pick one policy and document it).
   - If already terminal → **`409`** with a clear code.

Returning the **updated** `job_runs` row (or the existing API `RunResponse` shape) keeps clients aligned with [`internal/api/api.runs.go`](../internal/api/api.runs.go).

---

## Worker model (cooperative cancellation)

Workers should **not** rely on a second message queue for MVP cancellation.

1. **Claim** a run with the same rules as the rest of the system: acquire the **job row lock** before transitioning a run to `RUNNING` where applicable (see job update / trigger docs under [`internal/api/README.md`](../internal/api/README.md)).
2. **Handler signature** should take `context.Context` and honor cancellation:

   ```go
   type Handler func(ctx context.Context, payload json.RawMessage) error
   ```

3. For each active run, the worker may keep an in-memory `map[runID]context.CancelFunc` (or equivalent) keyed by run id. When the DB shows `CANCEL_REQUESTED` (or the API signals cancel), call **`cancel()`** so `ctx.Done()` closes. This is **signalling only**; the row in Postgres still authorizes whether the run was really cancelled.
4. On a **heartbeat** interval (or batched with lease renewal), **re-read** the run row. If `cancel_requested_at` is set (or status is `CANCEL_REQUESTED`), trigger the in-proc cancel and/or stop scheduling more work.
5. When the handler returns after cooperative shutdown, the worker sets status to **`CANCELLED`** and sets **`cancelled_at`**, using a **conditional update** so it does not clobber a concurrent transition to `SUCCEEDED`/`FAILED` unless that matches your product rules.

**Limitation:** Go cannot kill a goroutine. Handlers that ignore `ctx` may run until they finish; operators should treat “cancel requested” as best-effort until the row is terminal.

---

## Crash and lease expiry (watcher / recovery)

If a worker **crashes** after setting `CANCEL_REQUESTED`:

- A **recovery** or **watcher** process should find runs with **expired leases** (and optionally stuck `CANCEL_REQUESTED`).
- If cancellation was requested, **finalize** to `CANCELLED` (or define explicit retry policy — but cancelled runs should not silently enter normal retry/DLQ paths unless you document that).

This repository does not yet implement that watcher; it belongs in the same “recovery” layer that handles lease expiry for `RUNNING` / `CANCEL_REQUESTED` generally.

---

## Outbox `JOB_CANCEL` (optional later)

The schema permits `outbox_events.event_kind = 'JOB_CANCEL'`. A future design could enqueue a cancel event for downstream systems. The MVP described in this doc can rely on **DB state + worker polling** without publishing a separate cancel message first.

---

## Testing checklist (when implementing)

| Scenario | Expectation |
| --- | --- |
| Cancel queued run | Becomes `CANCELLED` quickly |
| Cancel running run | `CANCEL_REQUESTED` then worker → `CANCELLED` |
| Duplicate cancel | Idempotent safe response |
| Cancel terminal run | `409` (or equivalent) |
| Handler ignores context | Row may not become `CANCELLED` until timeout/recovery; document operator behavior |
| Race: success vs cancel | One valid terminal state via conditional updates |

---

## Related reading

- HTTP API overview: [`internal/api/README.md`](../internal/api/README.md)
- SQL / sqlc queries: [`internal/database/sqlc/README.md`](../internal/database/sqlc/README.md)
