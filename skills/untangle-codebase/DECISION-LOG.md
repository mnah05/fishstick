# Untangle codebase decision log

Scope: fishstick MVP as described in the Notion page and current repository docs. Keep behavior/API contract stable. Existing worktree edits were present before this continuation; preserve them.

## Fixes and problems

- Keep request handlers on the `server` receiver with one pool and query set; transaction-bound queries come from `beginTx`.
  - Prevents mixing a transaction from one pool with a query set from another. The current `internal/api/server.go` establishes this ownership boundary and is used by the API handlers.
- Keep schedule validation and next-run calculation in `internal/api/schedule.go`.
  - Create and update share schedule semantics; one schedule module avoids drift.
- Keep SQLC query definitions under `internal/database/sqlc/queries` as source of truth and regenerate generated Go from them.
  - Cancellation handler called missing generated method names while SQL source exposed a single `CancelRun`. Both paths now call that generated operation.
- Keep request/response conversion helpers in `internal/api/values.go` and response status decisions in `internal/api/status.go`.
  - These transformations are shared API vocabulary and should not be reimplemented per handler.
- Keep comments that document external behavior or concurrency contracts; remove restatements only.

## Inventory findings

- Hidden link: run cancellation handler and SQLC-generated API must stay aligned with `queries/job_runs.sql`. Resolved by calling `CancelRun` for both locked cancellation branches.
- Hidden link: job update and manual trigger must serialize against the same job row to preserve snapshots. Current handlers use `GetJobForUpdate` in transaction-bound queries.
- Hidden link: SQL schema, query sources, generated SQLC models, and API DTOs evolve together. Current repository docs identify query sources and generator; rerun generation after SQL edits.
- Hidden link: config defaults and empty override checks share a key set; `defaultValues` is the single key list.
- Needless links/surplus: no further safe removals established in this rapid pass. Avoid removing wrappers or generated code based only on apparent shallow structure.
- Special cases retained: empty-body manual trigger; explicit null handling for PATCH `scheduled_at`; UTC fallback for cron timezone; empty environment override rejection; cancellation state branches and deadline-bounded shutdown. Each represents an API or operational requirement, not an arbitrary exception.

## Tests

Existing tests are relevant behavior checks: API validation and routing, scheduling semantics, partial update merging, idempotency/cancellation behavior, plus opt-in PostgreSQL integration for transactions, concurrency, and rollback. Keep them as regression coverage; do not delete tests that exercise these contracts.
