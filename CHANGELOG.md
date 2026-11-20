# Changelog

## Unreleased

- **Breaking API filter change:** `status=SCHEDULED` is no longer accepted by
  `GET /v1/list_runs`. `SCHEDULED` remains valid as a `trigger_source`; queued
  work is represented by run status `QUEUED`. Existing rows with the removed
  status are converted to `QUEUED` by migration `00004`.
- Added the outbox relay, lease-fenced execution worker, retry dispatch, and
  dead-letter list/get endpoints.
- Added `cmd/watcher` to recover expired `RUNNING` leases. Runs with attempts
  remaining return to `RETRY_WAIT` through the outbox; exhausted runs are
  failed and recorded in the DLQ atomically.
- Added a retry-delay snapshot to runs so retries use the job settings captured
  when the run was created.
- Fixed the relay backoff to reach its documented five-minute maximum.
- **Behavior change:** `JOB_BUSY` now requires an unexpired run lease. A
  `RUNNING` or `CANCEL_REQUESTED` run with a missing lease no longer blocks job
  updates, matching the claim and watcher recovery rule. `deadline_at`,
  `exit_code`, and `retry_of_run_id` are documented as reserved run fields with
  no current writer.
