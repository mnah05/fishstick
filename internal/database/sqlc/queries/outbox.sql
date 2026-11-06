-- name: CreateOutboxRecord :one
INSERT INTO outbox_events (
    id, run_id, message, event_kind, status, created_at
)
VALUES (
    $1, $2, $3, 'JOB_DISPATCH', 'PENDING', NOW()
)
RETURNING *;
