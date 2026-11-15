-- name: ListDueScheduledJobs :many
-- Lock only the due job definitions this poll will handle. SKIP LOCKED lets
-- multiple scheduler processes divide the work without waiting on each other.
SELECT id, event_type, payload, schedule_type, cron_expression, timezone,
       next_run_at, max_attempts, timeout_seconds
FROM jobs
WHERE next_run_at <= sqlc.arg('now')
ORDER BY next_run_at, id
LIMIT sqlc.arg('batch_size')
FOR UPDATE SKIP LOCKED;

-- name: CreateScheduledRun :exec
-- The partial unique index makes one row per scheduled occurrence authoritative.
INSERT INTO job_runs (
    id, job_id, scheduled_for, trigger_source, event_type_snapshot,
    payload_snapshot, timeout_seconds, max_attempts, status, attempt_count, created_at
)
VALUES ($1, $2, $3, 'SCHEDULED', $4, $5, $6, $7, 'QUEUED', 0, $8)
ON CONFLICT (job_id, scheduled_for) WHERE trigger_source = 'SCHEDULED' DO NOTHING;

-- name: GetScheduledRunID :one
SELECT id
FROM job_runs
WHERE job_id = $1 AND scheduled_for = $2 AND trigger_source = 'SCHEDULED';

-- name: CreateScheduledDispatchOutbox :exec
-- Run and event creation share a transaction, so the existence check mainly
-- protects retries if data was produced by an older scheduler implementation.
INSERT INTO outbox_events (id, run_id, event_kind, status, message, created_at)
SELECT $1, $2, 'JOB_DISPATCH', 'PENDING', $3, $4
WHERE NOT EXISTS (
    SELECT 1 FROM outbox_events WHERE run_id = $2 AND event_kind = 'JOB_DISPATCH'
);

-- name: AdvanceJobSchedule :exec
UPDATE jobs
SET next_run_at = $2
WHERE id = $1;

-- name: BeginSchedulerOccurrence :exec
SAVEPOINT scheduler_occurrence;

-- name: RollbackSchedulerOccurrence :exec
ROLLBACK TO SAVEPOINT scheduler_occurrence;

-- name: ReleaseSchedulerOccurrence :exec
RELEASE SAVEPOINT scheduler_occurrence;
