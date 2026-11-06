# Run the API

## Docker database (recommended)

From the project root:

```sh
docker compose up -d
export DATABASE_URL='postgres://postgres:postgres@localhost:5432/fishstick?sslmode=disable'
go run ./cmd/migrate
go run ./cmd/seed   # optional: replaces existing job/run/outbox/DLQ data
go run ./cmd/api
```

Reset DB: `docker compose down -v`, then repeat the steps above. See [internal/database/README.md](../../internal/database/README.md).

## Example curls

Health:

```sh
curl -s http://localhost:8080/v1/health/db | jq .
```

Seeded job:

```sh
curl -s http://localhost:8080/v1/job/get/01933e8c-0000-7000-8000-000000000001 | jq .
curl -s 'http://localhost:8080/v1/job/list?limit=10' | jq .
curl -s 'http://localhost:8080/v1/job/01933e8c-0000-7000-8000-000000000001/runs?limit=5' | jq .
# After seed: use a run id from the list response:
curl -s 'http://localhost:8080/v1/job/01933e8c-0000-7000-8000-000000000001/run/01933e8c-0000-7000-8000-000000000011' | jq .
```

Create job (server assigns id):

```sh
curl -s -X POST http://localhost:8080/v1/job/create \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "local-test",
    "event_type": "ping",
    "schedule_type": "MANUAL"
  }' | jq .
```

Manual trigger (no request body):

```sh
curl -i -X POST http://localhost:8080/v1/job/trigger/01933e8c-0000-7000-8000-000000000001 \
  -H 'Idempotency-Key: local-trigger-001'
```

Returns `202` with the queued run. Repeat the same key for the same job to get
that run again; use a new key for another execution. Both the run and its pending
outbox event are committed together. There is no relay or worker yet, so this
does not publish to RabbitMQ or execute the job.

## Without Docker

Create database `fishstick`, then:

```sh
export DATABASE_URL='postgres://...'
go run ./cmd/migrate
go run ./cmd/seed   # optional dev fixtures
go run ./cmd/api
```

Environment variables: [internal/config/.env.example](../../internal/config/.env.example).

## Startup flow

`main.go` loads config → opens pool → `sqlc.New(db)` → health check → Echo router on `:8080`.

```text
GET /v1/health/db → sqlc HealthCheck → SELECT 1
```

Full route list: [internal/api/README.md](../../internal/api/README.md).

The server listens on `:8080` and has no authentication. On SIGINT/SIGTERM it
stops accepting new work (new requests get `503`) and drains in-flight requests,
including their database transactions, for up to ten seconds before closing the
pool. A second signal exits immediately.
