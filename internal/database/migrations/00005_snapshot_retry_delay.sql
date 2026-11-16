-- +goose Up
ALTER TABLE job_runs
    ADD COLUMN retry_delay_seconds INTEGER NOT NULL DEFAULT 30
        CHECK (retry_delay_seconds >= 0);

UPDATE job_runs r
SET retry_delay_seconds = j.retry_delay_seconds
FROM jobs j
WHERE j.id = r.job_id;

-- +goose Down
ALTER TABLE job_runs DROP COLUMN retry_delay_seconds;
