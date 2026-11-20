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

- Keep cron syntax and next-tick calculation in `internal/schedule`. The API and the scheduler both call it.
  - The scheduler had copied the parser flags and the timezone next-tick formula from `internal/api/schedule.go`.
- Give each due job its own savepoint inside the scheduler batch transaction.
  - One invalid cron or timezone previously rolled back every other job locked in that poll, and a permanently due bad row blocked later jobs.
- Record the scheduler's writes in `docs/state-machines.md`: `QUEUED` plus `SCHEDULED` trigger, exhausted `ONCE` as null `next_run_at` (migration `00003`). Migration `00004` later removed the unused `SCHEDULED` run status.

## Tests

Existing tests are relevant behavior checks: API validation and routing, scheduling semantics, partial update merging, idempotency/cancellation behavior, plus opt-in PostgreSQL integration for transactions, concurrency, and rollback. Keep them as regression coverage; do not delete tests that exercise these contracts.

## Untangle pass: lease watcher and runtime integration

Inventory covered the API, scheduler, relay, worker, watcher, SQLC query sources,
configuration, process setup, and integration-test helpers. Existing worktree
changes for the watcher and its documentation are part of the baseline and must
remain intact.

### Findings and redesign decisions

- **Repeated polling runtime:** scheduler and watcher each own almost identical
  database `/healthz`, ticker, shutdown, and logging loops. Move this shared
  lifecycle into one small polling runner; leave `processDueJobs` and
  `processBatch` in their owning commands.
- **Repeated process config parsing:** scheduler/watcher duplicate Koanf load,
  blank override checks, decoding, and validation; relay/worker already use a
  prefix-based loader. Route all process config parsing through the shared
  loader while retaining process-specific defaults, types, and validation.
- **Repeated dispatch schema:** the `{"run_id": ...}` contract is built by API,
  scheduler and SQL, then decoded separately by worker. Give the message shape
  one Go type and encoder/decoder; pass encoded message data into SQLC queries
  instead of constructing the JSON object in SQL.
- **Implicit transaction and lock protocol:** handlers and worker lifecycle
  code manually bind SQLC to transactions, and worker/watcher repeat
  job-before-run locking. Put transaction binding and the job/run lock sequence
  behind the database package boundary so callers cannot accidentally reverse
  the protocol.
- **Repeated AMQP topology:** relay and worker independently declare the same
  exchange, queue, and binding. Share that protocol in the messaging package;
  keep publisher confirms, QoS, publishing, and consumption at their owners.
- **Repeated isolated-database test setup:** API, relay, scheduler, worker, and
  watcher tests repeat schema creation, search_path, migrations, and cleanup.
  Move that fixture into `internal/testsupport`, retaining API-specific
  statement timeout and application-name settings.
- **Process identity collision:** relay claims use the configured static owner
  ID, and worker IDs only append hostname. Use a shared per-process identity
  that includes a unique instance token, preserving the configured label for
  logs and operations.
- **Suppressed delivery errors:** relay records publish errors in PostgreSQL
  but suppresses them from process logs after successful release; worker
  discards AMQP ack/nack errors. Surface these errors without changing retry,
  acknowledgment, or database state transitions.
- **API/persistence coupling:** API handlers and response DTOs use generated
  SQLC rows. Keep the existing explicit response mapping because it protects
  the HTTP contract. Do not add an extra domain/persistence layer without a
  concrete caller that benefits from it. The worker handler is internal to the
  worker command; its SQLC model parameter is not currently a public extension
  contract, so leave it alone in this pass.
- **Necessary special cases:** preserve job-before-run locks, conditional
  updates, transaction-plus-outbox/DLQ atomicity, fencing, cancellation
  handling, retry/backoff timing, `OptionalTime` tri-state semantics, null
  mapping, SQLC-generated files, and the API/migration `run` test seams. These
  represent observable contracts or concurrency guards.
- **Potentially swallowed but low-value errors:** HTTP response body write
  failures and test cleanup errors remain best-effort; do not turn ordinary
  client disconnects or cleanup failures into new lifecycle behavior.

The comments/tests preference was requested before editing. Preserve all
behavioral tests and comments unless the user selects the corresponding cleanup
work; adapt tests only where a code refactor requires signature or fixture
updates.

### Implemented

- Added `internal/process.RunPoller` and moved scheduler/watcher health,
  polling, and shutdown lifecycle into it.
- Unified environment loading and polling defaults/validation while retaining
  process-specific config types and names.
- Added the shared dispatch payload and RabbitMQ topology helpers in
  `internal/messaging`.
- Added transaction-bound SQLC queries and a shared job-before-run lock helper
  in `internal/database`.
- Added unique per-process instance IDs and surfaced relay publish and worker
  delivery-settlement errors.
- Centralized isolated PostgreSQL setup in `internal/testsupport` and updated
  relevant command and API docs.
- Left optional comment/test deletion untouched; behavioral tests were kept
  and adjusted only for shared helpers and fixtures.

Verification: `go test ./...` passed and `git diff --check` passed. PostgreSQL
integration tests follow the repository's opt-in test database configuration.
