-- +goose Up
CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL CHECK (btrim(name) <> ''),
    event_type TEXT NOT NULL CHECK (btrim(event_type) <> ''),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    schedule_type TEXT NOT NULL DEFAULT 'MANUAL'
        CHECK (schedule_type IN ('MANUAL', 'ONCE', 'CRON')),
    scheduled_at TIMESTAMPTZ,
    cron_expression TEXT,
    timezone TEXT NOT NULL DEFAULT 'UTC' CHECK (btrim(timezone) <> ''),
    next_run_at TIMESTAMPTZ,
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    retry_delay_seconds INTEGER NOT NULL DEFAULT 30 CHECK (retry_delay_seconds >= 0),
    timeout_seconds INTEGER NOT NULL DEFAULT 120 CHECK (timeout_seconds > 0),
    create_idempotency_key TEXT UNIQUE CHECK (btrim(create_idempotency_key) <> ''),
    create_request_hash TEXT CHECK (btrim(create_request_hash) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT jobs_schedule_check CHECK (
        (schedule_type = 'MANUAL' AND scheduled_at IS NULL
            AND cron_expression IS NULL AND next_run_at IS NULL)
        OR (schedule_type = 'ONCE' AND scheduled_at IS NOT NULL
            AND cron_expression IS NULL)
        OR (schedule_type = 'CRON' AND scheduled_at IS NULL
            AND cron_expression IS NOT NULL AND btrim(cron_expression) <> '')
    ),
    CONSTRAINT jobs_create_idempotency_check CHECK (
        (create_idempotency_key IS NULL) = (create_request_hash IS NULL)
    )
);

CREATE TABLE job_runs (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs (id),
    retry_of_run_id UUID,
    scheduled_for TIMESTAMPTZ NOT NULL,
    trigger_source TEXT NOT NULL CHECK (trigger_source IN ('MANUAL', 'SCHEDULED')),
    trigger_idempotency_key TEXT CHECK (btrim(trigger_idempotency_key) <> ''),
    trigger_request_hash TEXT CHECK (btrim(trigger_request_hash) <> ''),
    event_type_snapshot TEXT NOT NULL CHECK (btrim(event_type_snapshot) <> ''),
    payload_snapshot JSONB NOT NULL,
    timeout_seconds INTEGER NOT NULL DEFAULT 120 CHECK (timeout_seconds > 0),
    status TEXT NOT NULL DEFAULT 'QUEUED' CHECK (status IN (
        'SCHEDULED', 'QUEUED', 'RUNNING', 'RETRY_WAIT',
        'CANCEL_REQUESTED', 'SUCCEEDED', 'FAILED', 'CANCELLED'
    )),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    worker_id TEXT,
    fencing_token BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    lease_expires_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    retry_at TIMESTAMPTZ,
    deadline_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    last_attempt_started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    cancel_requested_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    cancel_reason TEXT,
    last_error TEXT,
    exit_code INTEGER,
    result JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT job_runs_id_job_id_unique UNIQUE (id, job_id),
    CONSTRAINT job_runs_retry_of_fk FOREIGN KEY (retry_of_run_id, job_id)
        REFERENCES job_runs (id, job_id),
    CONSTRAINT job_runs_retry_not_self CHECK (retry_of_run_id <> id),
    CONSTRAINT job_runs_trigger_idempotency_check CHECK (
        (trigger_source = 'MANUAL' AND trigger_idempotency_key IS NOT NULL
            AND trigger_request_hash IS NOT NULL)
        OR (trigger_source = 'SCHEDULED' AND trigger_idempotency_key IS NULL
            AND trigger_request_hash IS NULL)
    )
);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES job_runs (id),
    event_kind TEXT NOT NULL DEFAULT 'JOB_DISPATCH'
        CHECK (event_kind IN ('JOB_DISPATCH', 'JOB_CANCEL')),
    message JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'INFLIGHT', 'PUBLISHED')),
    publish_attempts INTEGER NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    locked_by TEXT,
    locked_until TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dead_letter_jobs (
    run_id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs (id),
    failure_reason TEXT NOT NULL CHECK (btrim(failure_reason) <> ''),
    last_error TEXT,
    attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
    failed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolution_status TEXT NOT NULL DEFAULT 'OPEN'
        CHECK (resolution_status IN ('OPEN', 'RETRIED', 'RESOLVED')),
    resolved_at TIMESTAMPTZ,
    retry_run_id UUID,
    raw_message JSONB,
    CONSTRAINT dead_letter_jobs_run_fk FOREIGN KEY (run_id, job_id)
        REFERENCES job_runs (id, job_id),
    CONSTRAINT dead_letter_jobs_retry_run_fk FOREIGN KEY (retry_run_id, job_id)
        REFERENCES job_runs (id, job_id),
    CONSTRAINT dead_letter_jobs_retry_not_self CHECK (retry_run_id <> run_id)
);

CREATE UNIQUE INDEX job_runs_scheduled_occurrence_unique
    ON job_runs (job_id, scheduled_for) WHERE trigger_source = 'SCHEDULED';
CREATE UNIQUE INDEX job_runs_manual_trigger_unique
    ON job_runs (job_id, trigger_idempotency_key) WHERE trigger_source = 'MANUAL';
CREATE INDEX jobs_next_run_at_idx
    ON jobs (next_run_at) WHERE next_run_at IS NOT NULL;
CREATE INDEX jobs_schedule_type_id_idx ON jobs (schedule_type, id DESC);
CREATE INDEX jobs_event_type_id_idx ON jobs (event_type, id DESC);
CREATE INDEX job_runs_active_lease_idx
    ON job_runs (lease_expires_at) WHERE status IN ('RUNNING', 'CANCEL_REQUESTED');
CREATE INDEX job_runs_job_id_id_idx ON job_runs (job_id, id DESC);
CREATE INDEX job_runs_status_id_idx ON job_runs (status, id DESC);
CREATE INDEX job_runs_retry_at_idx ON job_runs (retry_at) WHERE status = 'RETRY_WAIT';
CREATE INDEX job_runs_scheduled_for_idx
    ON job_runs (scheduled_for) WHERE status = 'SCHEDULED';
CREATE INDEX job_runs_retry_of_run_id_idx
    ON job_runs (retry_of_run_id) WHERE retry_of_run_id IS NOT NULL;
CREATE INDEX outbox_events_status_available_at_idx ON outbox_events (status, available_at);
CREATE INDEX outbox_events_locked_until_idx
    ON outbox_events (locked_until) WHERE status = 'INFLIGHT';
CREATE INDEX outbox_events_run_id_idx ON outbox_events (run_id);
CREATE INDEX dead_letter_jobs_resolution_failed_at_idx
    ON dead_letter_jobs (resolution_status, failed_at);
CREATE INDEX dead_letter_jobs_job_id_idx ON dead_letter_jobs (job_id);
CREATE INDEX dead_letter_jobs_retry_run_id_idx
    ON dead_letter_jobs (retry_run_id) WHERE retry_run_id IS NOT NULL;

-- +goose StatementBegin
CREATE FUNCTION set_updated_at() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = statement_timestamp();
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER jobs_set_updated_at
    BEFORE UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER job_runs_set_updated_at
    BEFORE UPDATE ON job_runs FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- +goose Down
DROP TRIGGER IF EXISTS job_runs_set_updated_at ON job_runs;
DROP TRIGGER IF EXISTS jobs_set_updated_at ON jobs;
-- +goose StatementBegin
DROP FUNCTION IF EXISTS set_updated_at();
-- +goose StatementEnd
DROP TABLE IF EXISTS dead_letter_jobs;
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS job_runs;
DROP TABLE IF EXISTS jobs;
