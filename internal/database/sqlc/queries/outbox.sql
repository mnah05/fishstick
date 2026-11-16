-- name: CreateOutboxRecord :one
INSERT INTO outbox_events (
    id, run_id, message, event_kind, status, created_at
)
VALUES (
    $1, $2, $3, 'JOB_DISPATCH', 'PENDING', NOW()
)
RETURNING *;

-- name: ClaimOutboxBatch :many
WITH candidates AS (
    SELECT id
    FROM outbox_events
    WHERE event_kind = 'JOB_DISPATCH'
      AND available_at <= NOW()
      AND (status = 'PENDING' OR (status = 'INFLIGHT' AND locked_until < NOW()))
    ORDER BY created_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT sqlc.arg('batch_size')
)
UPDATE outbox_events AS o
SET status = 'INFLIGHT', locked_by = sqlc.arg('locked_by'),
    locked_until = NOW() + make_interval(secs => sqlc.arg('lock_seconds')::int)
FROM candidates
WHERE o.id = candidates.id
RETURNING o.*;

-- name: CompleteOutboxPublish :execrows
UPDATE outbox_events SET status = 'PUBLISHED', published_at = NOW(),
    locked_by = NULL, locked_until = NULL, last_error = NULL
WHERE id = $1 AND status = 'INFLIGHT' AND locked_by = $2;

-- name: ReleaseOutboxClaim :execrows
UPDATE outbox_events SET status = 'PENDING', publish_attempts = publish_attempts + 1,
    available_at = NOW() + make_interval(secs => sqlc.arg('delay_seconds')::int),
    locked_by = NULL, locked_until = NULL, last_error = sqlc.arg('last_error')
WHERE id = sqlc.arg('id') AND status = 'INFLIGHT' AND locked_by = sqlc.arg('locked_by');

-- name: InsertDeadLetter :exec
INSERT INTO dead_letter_jobs (run_id, job_id, failure_reason, last_error, attempt_count)
SELECT id, job_id, 'MAX_ATTEMPTS_EXCEEDED', last_error, attempt_count
FROM job_runs WHERE id = sqlc.arg('run_id')
ON CONFLICT (run_id) DO NOTHING;

-- name: InsertRetryDispatch :exec
INSERT INTO outbox_events (id, run_id, event_kind, status, message, available_at)
SELECT sqlc.arg('id'), r.id, 'JOB_DISPATCH', 'PENDING',
       jsonb_build_object('run_id', r.id), r.retry_at
FROM job_runs r
WHERE r.id = sqlc.arg('run_id') AND r.status = 'RETRY_WAIT';

-- name: ListDeadLetterPage :many
SELECT * FROM dead_letter_jobs
WHERE (sqlc.narg('resolution_status')::text IS NULL OR resolution_status = sqlc.narg('resolution_status'))
  AND (sqlc.narg('job_id')::uuid IS NULL OR job_id = sqlc.narg('job_id'))
  AND (sqlc.narg('run_id')::uuid IS NULL OR run_id = sqlc.narg('run_id'))
  AND (sqlc.narg('cursor')::timestamptz IS NULL OR (failed_at, run_id) < (sqlc.narg('cursor')::timestamptz, sqlc.narg('cursor_run_id')::uuid))
ORDER BY failed_at DESC, run_id DESC LIMIT sqlc.arg('limit');

-- name: GetDeadLetter :one
SELECT * FROM dead_letter_jobs WHERE run_id = $1;
