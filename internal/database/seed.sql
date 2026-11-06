-- Dev seed data. Apply with: go run ./cmd/seed (after migrations).
-- Truncates job-related tables, then inserts fixed UUID fixtures for local API tests.
BEGIN;

TRUNCATE dead_letter_jobs, outbox_events, job_runs, jobs RESTART IDENTITY CASCADE;

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
    created_at,
    updated_at
) VALUES
    (
        '01933e8c-0000-7000-8000-000000000001',
        'demo-manual',
        'email.send',
        '{"template":"welcome","to":"user@example.com"}'::jsonb,
        'MANUAL',
        NULL,
        NULL,
        'UTC',
        NULL,
        3,
        30,
        120,
        TIMESTAMPTZ '2026-11-03T05:00:00Z',
        TIMESTAMPTZ '2026-11-03T05:00:00Z'
    ),
    (
        '01933e8c-0000-7000-8000-000000000002',
        'demo-once',
        'report.generate',
        '{"report":"daily"}'::jsonb,
        'ONCE',
        TIMESTAMPTZ '2026-12-01T09:00:00Z',
        NULL,
        'UTC',
        TIMESTAMPTZ '2026-12-01T09:00:00Z',
        3,
        30,
        120,
        TIMESTAMPTZ '2026-11-03T05:00:00Z',
        TIMESTAMPTZ '2026-11-03T05:00:00Z'
    ),
    (
        '01933e8c-0000-7000-8000-000000000003',
        'demo-cron',
        'email.send',
        '{"template":"daily_report","to":"team@example.com"}'::jsonb,
        'CRON',
        NULL,
        '0 9 * * *',
        'Asia/Kolkata',
        TIMESTAMPTZ '2026-11-04T03:30:00Z',
        3,
        30,
        120,
        TIMESTAMPTZ '2026-11-03T05:00:00Z',
        TIMESTAMPTZ '2026-11-03T05:00:00Z'
    );

INSERT INTO job_runs (
    id,
    job_id,
    scheduled_for,
    trigger_source,
    trigger_idempotency_key,
    trigger_request_hash,
    event_type_snapshot,
    payload_snapshot,
    timeout_seconds,
    status,
    attempt_count,
    max_attempts,
    worker_id,
    fencing_token,
    lease_expires_at,
    started_at,
    finished_at,
    last_error,
    created_at,
    updated_at
) VALUES
    (
        '01933e8c-0000-7000-8000-000000000011',
        '01933e8c-0000-7000-8000-000000000001',
        TIMESTAMPTZ '2026-11-03T03:00:00Z',
        'MANUAL',
        'seed-trigger-001',
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        'email.send',
        '{"template":"welcome","to":"user@example.com"}'::jsonb,
        120,
        'SUCCEEDED',
        1,
        3,
        'worker-seed-1',
        1,
        NULL,
        TIMESTAMPTZ '2026-11-03T03:00:02Z',
        TIMESTAMPTZ '2026-11-03T03:00:05Z',
        NULL,
        TIMESTAMPTZ '2026-11-03T03:00:00Z',
        TIMESTAMPTZ '2026-11-03T03:00:05Z'
    ),
    (
        '01933e8c-0000-7000-8000-000000000012',
        '01933e8c-0000-7000-8000-000000000001',
        TIMESTAMPTZ '2026-11-03T03:30:00Z',
        'MANUAL',
        'seed-trigger-002',
        'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        'email.send',
        '{"template":"welcome","to":"user@example.com"}'::jsonb,
        120,
        'RUNNING',
        1,
        3,
        'worker-seed-1',
        2,
        TIMESTAMPTZ '2026-11-03T03:35:00Z',
        TIMESTAMPTZ '2026-11-03T03:30:02Z',
        NULL,
        NULL,
        TIMESTAMPTZ '2026-11-03T03:30:00Z',
        TIMESTAMPTZ '2026-11-03T03:30:12Z'
    ),
    (
        '01933e8c-0000-7000-8000-000000000013',
        '01933e8c-0000-7000-8000-000000000001',
        TIMESTAMPTZ '2026-11-02T12:00:00Z',
        'MANUAL',
        'seed-trigger-003',
        'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
        'email.send',
        '{"template":"welcome","to":"user@example.com"}'::jsonb,
        120,
        'FAILED',
        3,
        3,
        NULL,
        3,
        NULL,
        TIMESTAMPTZ '2026-11-02T12:00:01Z',
        TIMESTAMPTZ '2026-11-02T12:05:00Z',
        'smtp timeout',
        TIMESTAMPTZ '2026-11-02T12:00:00Z',
        TIMESTAMPTZ '2026-11-02T12:05:00Z'
    );

INSERT INTO dead_letter_jobs (
    run_id,
    job_id,
    failure_reason,
    last_error,
    attempt_count,
    failed_at,
    resolution_status
) VALUES (
    '01933e8c-0000-7000-8000-000000000013',
    '01933e8c-0000-7000-8000-000000000001',
    'max_attempts_exhausted',
    'smtp timeout',
    3,
    TIMESTAMPTZ '2026-11-02T12:05:00Z',
    'OPEN'
);

INSERT INTO outbox_events (
    id,
    run_id,
    event_kind,
    message,
    status,
    available_at,
    created_at
) VALUES (
    '01933e8c-0000-7000-8000-000000000021',
    '01933e8c-0000-7000-8000-000000000012',
    'JOB_DISPATCH',
    '{"run_id":"01933e8c-0000-7000-8000-000000000012"}'::jsonb,
    'PUBLISHED',
    TIMESTAMPTZ '2026-11-03T03:30:00Z',
    TIMESTAMPTZ '2026-11-03T03:30:00Z'
);

COMMIT;
