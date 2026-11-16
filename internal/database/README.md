# Local PostgreSQL (Docker)

## Setup

From the project root:

```sh
docker compose -f .docker/docker-compose.yml up -d postgres
export DATABASE_URL='postgres://postgres:postgres@localhost:5432/fishstick?sslmode=disable'

# Schema (goose migrations)
goose -dir internal/database/migrations postgres "$DATABASE_URL" up
```

Migration files: `internal/database/migrations/` (tool: [goose](https://github.com/pressly/goose)). sqlc reads the same migration files, so the schema has one source of truth. Install the goose CLI with `go install github.com/pressly/goose/v3/cmd/goose@latest`.

```sh
goose -dir internal/database/migrations postgres "$DATABASE_URL" status   # view applied version
goose -dir internal/database/migrations postgres "$DATABASE_URL" down     # rollback last migration
```

## Reset everything

```sh
docker compose -f .docker/docker-compose.yml down -v
docker compose -f .docker/docker-compose.yml up -d postgres
export DATABASE_URL='postgres://postgres:postgres@localhost:5432/fishstick?sslmode=disable'
goose -dir internal/database/migrations postgres "$DATABASE_URL" up
```

sqlc and the integration tests both use the migration files; do not maintain a separate schema snapshot.

## Run the API

```sh
go run ./cmd/api
```

## Run the execution pipeline

Start both databases, migrate Postgres as above, and use one terminal for each
process:

```sh
docker compose -f .docker/docker-compose.yml --profile messaging up -d
go run ./cmd/api
go run ./cmd/schedular
go run ./cmd/relay
go run ./cmd/worker
```

Set `RELAY_RABBIT_URL` and `WORKER_RABBIT_URL` if the local broker credentials
or address differ from the example values. The worker's built-in development
handler returns an empty result for every event type; replace it before running
real jobs.

## Integration tests

```sh
export TEST_DATABASE_URL="$DATABASE_URL"
go test ./test/database ./test/api -count=1
```

Use a PostgreSQL URL pointing at a development/test database. The database
connectivity test performs only `SELECT 1`. Each API integration test
creates a unique schema, applies the embedded goose migrations to it, and drops that schema when
finished. Its database role needs permission to create schemas, tables,
functions, and triggers. The tests verify trigger idempotency and rollback, listing jobs without runs,
pagination, update busy states (including lease-aware `RUNNING` / `CANCEL_REQUESTED`)
and rollback, snapshot preservation, concurrent updates/triggers,
claim-then-update / update-then-claim interleaving, and the worker-claim locking
protocol. Without `TEST_DATABASE_URL`, integration tests skip.

Compose starts PostgreSQL 16 by default. RabbitMQ is an optional development
dependency; start it with `docker compose -f .docker/docker-compose.yml --profile messaging up -d`.
Both relay and worker fail at startup with a clear error if the broker is
unreachable. The relay declares the durable worker queue and binding so
messages remain available before workers start. Manual triggering and the scheduler write
`QUEUED` runs and `PENDING` outbox events for the relay.
