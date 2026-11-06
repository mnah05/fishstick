-- name: GetJobByCreateIdempotencyKey :one
SELECT
    id,
    name,
    event_type,
    payload,
    schedule_type,
    scheduled_at,
    cron_expression,
    timezone,
    next_run_at,
    max_attempts,
    retry_delay_seconds,
    timeout_seconds,
    create_idempotency_key,
    create_request_hash,
    created_at,
    updated_at
FROM jobs
WHERE create_idempotency_key = $1;

-- name: JobHasBusyRun :one
-- RUNNING/CANCEL_REQUESTED block updates only while the lease is active (or unset).
-- Expired leases allow edits; workers must set lease_expires_at when claiming.
SELECT EXISTS (
    SELECT 1
    FROM job_runs
    WHERE job_id = $1
      AND status IN ('RUNNING', 'CANCEL_REQUESTED')
      AND (
        lease_expires_at IS NULL
        OR lease_expires_at > CURRENT_TIMESTAMP
      )
)::boolean AS busy;

-- name: ListJobsPage :many
SELECT
    j.id,
    j.name,
    j.event_type,
    j.payload,
    j.schedule_type,
    j.scheduled_at,
    j.cron_expression,
    j.timezone,
    j.next_run_at,
    j.max_attempts,
    j.retry_delay_seconds,
    j.timeout_seconds,
    j.create_idempotency_key,
    j.create_request_hash,
    j.created_at,
    j.updated_at,
    lr.id AS latest_run_id,
    lr.status AS latest_run_status
FROM jobs j
LEFT JOIN job_runs lr ON lr.id = (
    SELECT id
    FROM job_runs
    WHERE job_id = j.id
    ORDER BY created_at DESC, id DESC
    LIMIT 1
)
WHERE (sqlc.narg('schedule_type')::text IS NULL OR j.schedule_type = sqlc.narg('schedule_type'))
  AND (sqlc.narg('event_type')::text IS NULL OR j.event_type = sqlc.narg('event_type'))
  AND (sqlc.narg('cursor')::uuid IS NULL OR j.id < sqlc.narg('cursor'))
ORDER BY j.id DESC
LIMIT sqlc.arg('limit');

-- name: GetJob :one
SELECT
    id,
    name,
    event_type,
    payload,
    schedule_type,
    scheduled_at,
    cron_expression,
    timezone,
    next_run_at,
    max_attempts,
    retry_delay_seconds,
    timeout_seconds,
    create_idempotency_key,
    create_request_hash,
    created_at,
    updated_at
FROM jobs
WHERE id = $1;
