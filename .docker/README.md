# Docker Compose workflows

`docker-compose.yml` starts PostgreSQL. RabbitMQ is optional and joins the
`messaging` profile. `docker-compose.e2e.yml` adds a migration runner, API,
three-process scheduler pool, relay, nine-worker pool, lease watcher, and a Go test runner.

## Run scaled E2E

From the repository root:

```sh
docker compose \
  -f .docker/docker-compose.yml \
  -f .docker/docker-compose.e2e.yml \
  -p fishstick-e2e \
  --profile e2e \
  up --build --scale schedular=3 --scale worker=9 \
  --abort-on-container-exit --exit-code-from e2e e2e
```

The `e2e` runner sets `TEST_REQUIRE_DATABASE=1` and runs `go test -count=1 -v
./...`, so missing PostgreSQL coverage fails instead of skipping. It also tests
the real HTTP API → outbox → RabbitMQ → worker path for manual and scheduled
runs, twelve simultaneous scheduled jobs, and increasing manual bursts from
18 to 288 runs (or another profile through `E2E_LOAD_BATCHES`). The scheduler
batch size is one to create contention across its three replicas. The worker
stub waits two seconds to make ownership observable; each worker ID includes
its configured label, hostname, and a unique process token.

The suite checks that the scheduled job creates one run and one dispatch, that
each run succeeds once with the captured retry delay, that outbox rows reach
`PUBLISHED`, and that all nine RabbitMQ consumers are active and execute runs
concurrently. `go test` logs each burst's elapsed time, jobs per second, peak
concurrent runs, and distinct worker count. The recorded run also tested a
576-job burst; see the results report for throughput measurements and limits.

For a fixed-rate test, set `E2E_TARGET_RPS` and `E2E_RATE_DURATION`. The rate
test records offered requests/sec, trigger errors, failed or missing jobs,
completion throughput, and queue size. It stops if either job failures or
trigger errors exceed 5%. `WORKER_STUB_DELAY` sets the test handler delay; use a
short delay when measuring high end-to-end completion rates.

See [the recorded E2E results](../docs/e2e-results.md) for the measured counts
and timings from the nine-worker run.

Compose exits with the E2E test runner's status. Clean up the containers and
throwaway database volume afterward:

```sh
docker compose \
  -f .docker/docker-compose.yml \
  -f .docker/docker-compose.e2e.yml \
  -p fishstick-e2e \
  --profile e2e down -v --remove-orphans
```

The E2E stack uses local-only sample database and RabbitMQ credentials. Do not
reuse them for a deployed environment.
