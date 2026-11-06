-- name: GetJobRun :one
SELECT *
FROM job_runs
WHERE id = $1 AND job_id = $2;

-- name: ListJobRunsPage :many
SELECT *
FROM job_runs
WHERE job_id = $1
  AND (sqlc.narg('cursor')::uuid IS NULL OR id < sqlc.narg('cursor')::uuid)
ORDER BY id DESC
LIMIT sqlc.arg('limit');

-- name: ListRunsPage :many
-- Global run listing with optional filters; newest run id first.
SELECT *
FROM job_runs
WHERE (sqlc.narg('job_id')::uuid IS NULL OR job_id = sqlc.narg('job_id'))
  AND (sqlc.narg('status')::text IS NULL OR status = sqlc.narg('status'))
  AND (sqlc.narg('trigger_source')::text IS NULL OR trigger_source = sqlc.narg('trigger_source'))
  AND (sqlc.narg('event_type')::text IS NULL OR event_type_snapshot = sqlc.narg('event_type'))
  AND (sqlc.narg('cursor')::uuid IS NULL OR id < sqlc.narg('cursor')::uuid)
ORDER BY id DESC
LIMIT sqlc.arg('limit');

-- name: GetLatestJobRunSummary :one
SELECT
    id,
    status,
    attempt_count,
    scheduled_for,
    started_at,
    finished_at,
    last_error
FROM job_runs
WHERE job_id = $1
ORDER BY created_at DESC, id DESC
LIMIT 1;

-- name: JobRunStatusCounts :many
SELECT status, COUNT(*)::bigint AS count
FROM job_runs
WHERE job_id = $1
GROUP BY status
ORDER BY status;

-- name: ManualJobRun :one
INSERT INTO job_runs (
    id, job_id, scheduled_for, trigger_source,
    trigger_idempotency_key, trigger_request_hash,
    event_type_snapshot, payload_snapshot, timeout_seconds, max_attempts,
    status, attempt_count, created_at
)
VALUES (
    $1, $2, NOW(), 'MANUAL',
    $3, $4,
    $5, $6, $7, $8,
    'QUEUED', 0, NOW()
)
RETURNING *;

-- name: GetManualRunByIdempotencyKey :one
SELECT * FROM job_runs
WHERE job_id = $1 AND trigger_idempotency_key = $2
  AND trigger_source = 'MANUAL';

-- name: ClaimRunToRunning :one
-- Worker claim: must run after locking the job row (same transaction as GetJobForUpdate).
UPDATE job_runs
SET
    status = 'RUNNING',
    started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
    lease_expires_at = $3,
    worker_id = $4,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
  AND job_id = $2
  AND status IN ('QUEUED', 'SCHEDULED', 'RETRY_WAIT')
RETURNING *;

-- name: GetJobRunByIDForUpdate :one
SELECT * FROM job_runs WHERE id = $1 FOR UPDATE;

-- name: CancelRunImmediate :one
-- Pre-running: mark fully cancelled in one step.
UPDATE job_runs
SET
    status = 'CANCELLED',
    cancel_requested_at = $2,
    cancelled_at = $2,
    cancel_reason = COALESCE(sqlc.narg('cancel_reason')::text, cancel_reason),
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
  AND status IN ('QUEUED', 'SCHEDULED', 'RETRY_WAIT')
RETURNING *;

-- name: RequestRunCancellation :one
-- Running: record request; worker completes transition to CANCELLED.
UPDATE job_runs
SET
    status = 'CANCEL_REQUESTED',
    cancel_requested_at = COALESCE(cancel_requested_at, $2),
    cancel_reason = COALESCE(sqlc.narg('cancel_reason')::text, cancel_reason),
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
  AND status = 'RUNNING'
RETURNING *;

