# Changelog

## Unreleased

- **Breaking API filter change:** `status=SCHEDULED` is no longer accepted by
  `GET /v1/list_runs`. `SCHEDULED` remains valid as a `trigger_source`; queued
  work is represented by run status `QUEUED`. Existing rows with the removed
  status are converted to `QUEUED` by migration `00004`.
- Added the outbox relay, lease-fenced execution worker, retry dispatch, and
  dead-letter list/get endpoints.
- Added a retry-delay snapshot to runs so retries use the job settings captured
  when the run was created.
