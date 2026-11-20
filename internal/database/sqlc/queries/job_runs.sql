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
    event_type_snapshot, payload_snapshot, timeout_seconds, retry_delay_seconds, max_attempts,
    status, attempt_count, created_at
)
VALUES (
    $1, $2, NOW(), 'MANUAL',
    $3, $4,
    $5, $6, $7, $8, $9,
    'QUEUED', 0, NOW()
)
RETURNING *;

-- name: FailExpiredRun :execrows
UPDATE job_runs SET status = 'FAILED',
    last_error = COALESCE(last_error, 'worker lease expired after max attempts'),
    finished_at = NOW(), lease_expires_at = NULL, worker_id = NULL
WHERE id = sqlc.arg('id') AND status = 'RUNNING'
  AND (lease_expires_at IS NULL OR lease_expires_at <= NOW())
  AND attempt_count >= max_attempts;

-- name: ListExpiredRunningRunIDs :many
SELECT id
FROM job_runs
WHERE status = 'RUNNING'
  AND (lease_expires_at IS NULL OR lease_expires_at <= NOW())
ORDER BY lease_expires_at NULLS FIRST, id
LIMIT sqlc.arg('batch_size');

-- name: RetryExpiredRun :execrows
UPDATE job_runs SET status = 'RETRY_WAIT',
    last_error = COALESCE(last_error, 'worker lease expired'),
    retry_at = NOW() + make_interval(secs => retry_delay_seconds),
    lease_expires_at = NULL, worker_id = NULL
WHERE id = sqlc.arg('id') AND status = 'RUNNING'
  AND (lease_expires_at IS NULL OR lease_expires_at <= NOW())
  AND attempt_count < max_attempts;

-- name: GetManualRunByIdempotencyKey :one
SELECT * FROM job_runs
WHERE job_id = $1 AND trigger_idempotency_key = $2
  AND trigger_source = 'MANUAL';

-- name: ClaimRunToRunning :one
-- Worker claim: must run after locking the job row (same transaction as GetJobForRunUpdate).
-- A missing or elapsed lease is expired; keep the inclusive boundary aligned with watcher recovery.
UPDATE job_runs
SET
    status = 'RUNNING',
    started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
    last_attempt_started_at = CURRENT_TIMESTAMP,
    heartbeat_at = CURRENT_TIMESTAMP,
    lease_expires_at = $3,
    worker_id = $4,
    fencing_token = fencing_token + 1,
    attempt_count = attempt_count + 1,
    retry_at = NULL
WHERE id = $1
  AND job_id = $2
  AND attempt_count < max_attempts
  AND ((status IN ('QUEUED', 'RETRY_WAIT') AND (status <> 'RETRY_WAIT' OR retry_at <= NOW()))
       OR (status = 'RUNNING' AND (lease_expires_at IS NULL OR lease_expires_at <= NOW())))
  AND (status <> 'RETRY_WAIT' OR retry_at <= NOW())
RETURNING *;

-- name: HeartbeatRun :execrows
UPDATE job_runs SET heartbeat_at = NOW(), lease_expires_at = sqlc.arg('lease_expires_at')
WHERE id = sqlc.arg('id') AND status IN ('RUNNING', 'CANCEL_REQUESTED')
  AND worker_id = sqlc.arg('worker_id') AND fencing_token = sqlc.arg('fencing_token');

-- name: CompleteRunSucceeded :execrows
UPDATE job_runs SET status = 'SUCCEEDED', result = sqlc.narg('result')::jsonb,
    finished_at = NOW(), lease_expires_at = NULL, worker_id = NULL
WHERE id = sqlc.arg('id') AND status = 'RUNNING'
  AND worker_id = sqlc.arg('worker_id') AND fencing_token = sqlc.arg('fencing_token');

-- name: ScheduleRunRetry :execrows
UPDATE job_runs SET status = 'RETRY_WAIT', last_error = sqlc.arg('last_error'),
    retry_at = NOW() + make_interval(secs => retry_delay_seconds),
    lease_expires_at = NULL, worker_id = NULL
WHERE id = sqlc.arg('id') AND status = 'RUNNING'
  AND worker_id = sqlc.arg('worker_id') AND fencing_token = sqlc.arg('fencing_token')
  AND attempt_count < max_attempts;

-- name: FailRunPermanent :execrows
UPDATE job_runs SET status = 'FAILED', last_error = sqlc.arg('last_error'),
    finished_at = NOW(), lease_expires_at = NULL, worker_id = NULL
WHERE id = sqlc.arg('id') AND status = 'RUNNING'
  AND worker_id = sqlc.arg('worker_id') AND fencing_token = sqlc.arg('fencing_token')
  AND attempt_count >= max_attempts;

-- name: CompleteRunCancelled :execrows
UPDATE job_runs SET status = 'CANCELLED', finished_at = NOW(), cancelled_at = NOW(),
    lease_expires_at = NULL, worker_id = NULL
WHERE id = sqlc.arg('id') AND status = 'CANCEL_REQUESTED'
  AND worker_id = sqlc.arg('worker_id') AND fencing_token = sqlc.arg('fencing_token');

-- name: CompleteExpiredRunCancelled :execrows
UPDATE job_runs SET status = 'CANCELLED', finished_at = NOW(), cancelled_at = NOW(),
    lease_expires_at = NULL, worker_id = NULL
WHERE id = sqlc.arg('id') AND status = 'CANCEL_REQUESTED'
  AND (lease_expires_at IS NULL OR lease_expires_at <= NOW());

-- name: GetJobRunByIDForUpdate :one
SELECT * FROM job_runs WHERE id = $1 FOR UPDATE;

-- name: GetJobRunByID :one
SELECT * FROM job_runs WHERE id = $1;

-- name: GetJobForRunUpdate :one
SELECT j.* FROM jobs j JOIN job_runs r ON r.job_id = j.id
WHERE r.id = $1 FOR UPDATE OF j;

-- name: CancelRun :one
-- Pre-running rows become CANCELLED; RUNNING rows become CANCEL_REQUESTED for the
-- worker to finish. The status set is the single source of truth for cancellable states.
UPDATE job_runs
SET
    status = CASE WHEN status = 'RUNNING' THEN 'CANCEL_REQUESTED' ELSE 'CANCELLED' END,
    cancel_requested_at = sqlc.arg('cancel_requested_at'),
    cancelled_at = CASE WHEN status = 'RUNNING' THEN NULL
        ELSE sqlc.arg('cancel_requested_at')::timestamptz END,
    cancel_reason = COALESCE(sqlc.narg('cancel_reason')::text, cancel_reason)
WHERE id = $1
  AND status IN ('QUEUED', 'RETRY_WAIT', 'RUNNING')
RETURNING *;

