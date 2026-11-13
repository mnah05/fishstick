-- +goose Up
ALTER TABLE jobs DROP CONSTRAINT jobs_schedule_check;

ALTER TABLE jobs ADD CONSTRAINT jobs_schedule_check CHECK (
    (schedule_type = 'MANUAL' AND scheduled_at IS NULL
        AND cron_expression IS NULL AND next_run_at IS NULL)
    OR (schedule_type = 'ONCE' AND scheduled_at IS NOT NULL
        AND cron_expression IS NULL AND next_run_at IS NOT NULL)
    OR (schedule_type = 'CRON' AND scheduled_at IS NULL
        AND cron_expression IS NOT NULL AND btrim(cron_expression) <> ''
        AND next_run_at IS NOT NULL)
);

-- +goose Down
ALTER TABLE jobs DROP CONSTRAINT jobs_schedule_check;

ALTER TABLE jobs ADD CONSTRAINT jobs_schedule_check CHECK (
    (schedule_type = 'MANUAL' AND scheduled_at IS NULL
        AND cron_expression IS NULL AND next_run_at IS NULL)
    OR (schedule_type = 'ONCE' AND scheduled_at IS NOT NULL
        AND cron_expression IS NULL)
    OR (schedule_type = 'CRON' AND scheduled_at IS NULL
        AND cron_expression IS NOT NULL AND btrim(cron_expression) <> '')
);
