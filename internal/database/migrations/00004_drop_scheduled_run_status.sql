-- +goose Up
-- SCHEDULED remains a trigger_source; queued work uses status QUEUED.
UPDATE job_runs SET status = 'QUEUED' WHERE status = 'SCHEDULED';
ALTER TABLE job_runs DROP CONSTRAINT job_runs_status_check;
ALTER TABLE job_runs ADD CONSTRAINT job_runs_status_check CHECK (status IN (
    'QUEUED', 'RUNNING', 'RETRY_WAIT', 'CANCEL_REQUESTED',
    'SUCCEEDED', 'FAILED', 'CANCELLED'
));
DROP INDEX IF EXISTS job_runs_scheduled_for_idx;

-- +goose Down
ALTER TABLE job_runs DROP CONSTRAINT job_runs_status_check;
ALTER TABLE job_runs ADD CONSTRAINT job_runs_status_check CHECK (status IN (
    'SCHEDULED', 'QUEUED', 'RUNNING', 'RETRY_WAIT',
    'CANCEL_REQUESTED', 'SUCCEEDED', 'FAILED', 'CANCELLED'
));
CREATE INDEX job_runs_scheduled_for_idx
    ON job_runs (scheduled_for) WHERE status = 'SCHEDULED';
