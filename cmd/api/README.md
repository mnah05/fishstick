# Run the API

## Docker database (recommended)

From the project root:

```sh
docker compose up -d
export DATABASE_URL='postgres://postgres:postgres@localhost:5432/fishstick?sslmode=disable'
goose -dir internal/database/migrations postgres "$DATABASE_URL" up
go run ./cmd/api
```

Reset DB: `docker compose down -v`, then repeat the steps above. See [internal/database/README.md](../../internal/database/README.md).

## Example curls

Health:

```sh
curl -s http://localhost:8080/v1/health/db | jq .
```

Jobs (replace `<job-id>` with an id returned by the create or list endpoint):

```sh
curl -s http://localhost:8080/v1/job/get/<job-id> | jq .
curl -s 'http://localhost:8080/v1/job/list?limit=10' | jq .
curl -s 'http://localhost:8080/v1/job/<job-id>/runs?limit=5' | jq .
# Use a run id from the runs response:
curl -s 'http://localhost:8080/v1/job/<job-id>/run/<run-id>' | jq .
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
goose -dir internal/database/migrations postgres "$DATABASE_URL" up
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
