-- name: CreateJob :one
INSERT INTO jobs (
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
    create_request_hash
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
)
RETURNING *;

-- name: UpdateJob :exec
UPDATE jobs
SET
    name = $2,
    event_type = $3,
    payload = $4,
    schedule_type = $5,
    scheduled_at = $6,
    cron_expression = $7,
    timezone = $8,
    next_run_at = $9,
    max_attempts = $10,
    retry_delay_seconds = $11,
    timeout_seconds = $12
WHERE id = $1;

-- name: GetJobForUpdate :one
SELECT * FROM jobs WHERE id = $1 FOR UPDATE;
