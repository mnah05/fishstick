# End-to-end run results

The recorded throughput run predates the lease watcher. The current E2E Compose
stack also starts `cmd/watcher`; the recorded run did not measure expired-lease
recovery.

Run date: 2026-11-16 (UTC)

## Setup

- 3 scheduler containers, each polling every 100 ms with batch size 1.
- 9 worker containers connected to the same RabbitMQ queue.
- 1 relay, isolated PostgreSQL database, and isolated RabbitMQ broker.
- The worker's development handler waited 2 seconds so concurrent ownership
  could be measured in PostgreSQL.
- The test container ran the complete Go suite with `go test -count=1 -v ./...`
  and `TEST_REQUIRE_DATABASE=1`.

The Compose project was named `fishstick-e2e`; it used its own network and
`fishstick_pgdata` volume. The test stack was removed after the run.

## Measured results

| Scenario | Result |
| --- | ---: |
| Full Go test suite | Passed |
| E2E cases | 4 passed |
| Manual run | 1 succeeded, 1 attempt, 1 published dispatch (2.159 s) |
| Scheduled once run | 1 scheduled occurrence, 1 succeeded, 1 attempt, 1 dispatch (7.227 s) |
| Concurrent scheduler batch | 12 due jobs → 12 unique runs → 12 published dispatches → 12 attempts (9.243 s) |
| Active worker consumers | 9 of 9 |
| Parallel manual runs | 18 succeeded, 18 published, 18 attempts |
| Peak concurrent runs | 9 |
| Distinct workers at peak | 9 |
| Parallel workload interval | 4.069 s |

## Capacity ramp

The live burst test sent each workload to one manual job, measured elapsed time
from the first API trigger through the final successful run, and checked run,
outbox, attempt, and peak worker counts. All 9 consumers were active at every
level.

| Burst size | Succeeded / published / attempts | Peak workers | Elapsed | Throughput |
| ---: | ---: | ---: | ---: | ---: |
| 18 | 18 / 18 / 18 | 9 | 4.166 s | 4.321 jobs/s |
| 36 | 36 / 36 / 36 | 9 | 8.203 s | 4.389 jobs/s |
| 72 | 72 / 72 / 72 | 9 | 16.215 s | 4.440 jobs/s |
| 144 | 144 / 144 / 144 | 9 | 32.339 s | 4.453 jobs/s |
| 288 | 288 / 288 / 288 | 9 | 65.137 s | 4.421 jobs/s |
| 576 | 576 / 576 / 576 | 9 | 134.258 s | 4.290 jobs/s |
| 1,152 | 1,152 / 1,152 / 1,152 | 9 | 258.010 s | 4.465 jobs/s |
| 2,304 | 2,304 / 2,304 / 2,304 | 9 | 515.859 s | 4.466 jobs/s |

### Observed capacity

With 9 workers and the test handler's 2-second delay, observed throughput
settled around **4.3–4.45 completed jobs per second** as the burst grew from
18 to 576 jobs. The largest run drained 576 jobs without failures or retries.
The simple worker-bound estimate is 9 workers ÷ 2 seconds = **4.5 jobs/s**,
which matches the measurement closely. The 144-job burst was the highest single
measurement at 4.453 jobs/s; the 576-job confirmation measured 4.290 jobs/s.
The later 1,152-job and 2,304-job bursts also completed with 0 job failures and
0 trigger errors, at 4.465 and 4.466 jobs/sec respectively.

This is the capacity of this test profile, where the simulated handler sleeps
for two seconds. It is not a universal production limit: actual handler time,
database load, machine resources, and worker count will change throughput. The
test found no overload or throughput collapse in a 576-job burst.

The worker concurrency result confirms the nine replicas each owned a run at
the same time. The 18-run batch supplied two tasks per worker; its peak remained
9 concurrent runs, as expected from the configured pool size.

## Test note

The first full run exposed a relay test that compared JSONB output byte for byte.
PostgreSQL normalizes JSON formatting, so the assertion now compares compacted
JSON. The complete rerun passed, including that relay test and the E2E suite.

## Reproduce

From the repository root:

```sh
docker compose -p fishstick-e2e \
  -f .docker/docker-compose.yml \
  -f .docker/docker-compose.e2e.yml \
  --profile e2e \
  up --build --scale schedular=3 --scale worker=9 \
  --abort-on-container-exit --exit-code-from e2e e2e
```

To repeat only a larger capacity point after starting the app services, set
`E2E_LOAD_BATCHES` to the desired comma-separated burst sizes. For example,
`E2E_LOAD_BATCHES=576` runs a single 576-job burst in the worker test.

## Follow-up load observations — 2026-11-17

The later rate ramp used 3 schedulers, 9 workers, and a 50 ms simulated handler.
Each rate was offered for 10 seconds, except the 100/sec sustained check, which
ran for 60 seconds. The tests allowed at most 5% failed jobs and at most 5%
failed trigger requests.

| Target requests/s | Offered requests/s | Offered | Successful runs | Trigger errors | Failure rate | End-to-end jobs/s |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100, 10 s | 99.89 | 999 | 999 | 0 | 0.00% | 98.78 |
| 100, 60 s | 99.98 | 5,999 | 5,999 | 0 | 0.00% | 99.78 |
| 125 | 124.89 | 1,249 | 1,249 | 0 | 0.00% | 122.86 |
| 150 | 149.80 | 1,498 | 1,498 | 0 | 0.00% | 147.43 |
| 200 | 199.88 | 1,999 | 1,999 | 0 | 0.00% | 167.71 |
| 250 | 249.30 | 2,493 | 2,493 | 0 | 0.00% | 163.11 |
| 300 | 299.48 | 2,995 | 2,995 | 0 | 0.00% | 161.80 |
| 500 | 499.49 | 4,995 | 4,995 | 0 | 0.00% | 162.17 |
| 1,000 | 965.76 | 9,658 | 9,658 | 0 | 0.00% | 159.85 |
| 1,500 | 1,498.41 | 14,985 | 14,985 | 0 | 0.00% | 159.72 |
| 2,000 | 1,995.80 | 19,960 | 19,769 | 191 | 0.96% | 143.44 |

At 100 requests/sec for 60 seconds, 5,989 runs were complete by the post-offer
snapshot, 10 were still queued, and all 5,999 eventually succeeded. At 2,000/sec,
the API returned errors for 191 requests (0.96%); all 19,769 created runs
succeeded. The 5% ceiling was not crossed.

The 3,000/sec attempt was interrupted before it produced a result, so it is not
included. No failure rate or capacity result is claimed for that attempt.

The completion and queue snapshot is taken after the generator finishes sending
and all trigger responses settle; at high rates, it can include jobs that
completed just after the 10-second offer window. End-to-end jobs/sec is measured
through final queue drain. The 1,500/sec first attempt only generated 1,282.58
requests/sec and was discarded; the batched-generator retry reached 1,498.41.

**Current measured boundary:** the stack processed 100 jobs/sec end to end for a
60-second run with 0% failures. It accepted offered rates up to 1,995.8/sec with
a 0.96% trigger/job failure rate, below the 5% ceiling. At the higher rates,
end-to-end drain throughput was about 143–160 jobs/sec; excess work queued and
completed after the offer stopped. The exact point where the 5% ceiling is
crossed remains unmeasured.

To repeat the 100/sec end-to-end check, start the isolated services with
`WORKER_STUB_DELAY=50ms`, then run:

```sh
E2E_TARGET_RPS=100 E2E_RATE_DURATION=60s E2E_TIMEOUT=600s \
docker compose -p fishstick-e2e \
  -f .docker/docker-compose.yml \
  -f .docker/docker-compose.e2e.yml \
  --profile e2e run --build --no-deps --rm e2e \
  go test -count=1 -timeout=12m -v ./test/e2e -run '^TestSustainedTriggerRate$'
```
