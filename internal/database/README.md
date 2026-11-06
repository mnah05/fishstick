# Local PostgreSQL (Docker)

## Setup

From the project root:

```sh
docker compose up -d
export DATABASE_URL='postgres://postgres:postgres@localhost:5432/fishstick?sslmode=disable'

# 1) Schema (goose migrations)
go run ./cmd/migrate

# 2) Dev seed data (separate step; truncates job tables first)
go run ./cmd/seed
```

Migration files: `internal/database/migrations/` (tool: [goose](https://github.com/pressly/goose)).

```sh
go run ./cmd/migrate status   # view applied version
go run ./cmd/migrate down     # rollback last migration
```

## Reset everything

```sh
docker compose down -v
docker compose up -d
export DATABASE_URL='postgres://postgres:postgres@localhost:5432/fishstick?sslmode=disable'
go run ./cmd/migrate
go run ./cmd/seed
```

## Schema vs seed

| Step | Command | What it does |
|------|---------|----------------|
| Migrations | `go run ./cmd/migrate` | Creates tables, indexes, triggers (`goose_db_version`) |
| Seed | `go run ./cmd/seed` | Truncates all four application tables and loads `internal/database/seed.sql` demo rows |

`init.sql` is kept for **sqlc** schema reference only; do not apply it manually if you use migrations.

## Seed UUIDs

| Resource | ID |
|----------|-----|
| Job `demo-manual` | `01933e8c-0000-7000-8000-000000000001` |
| Job `demo-once` | `01933e8c-0000-7000-8000-000000000002` |
| Job `demo-cron` | `01933e8c-0000-7000-8000-000000000003` |

## Run the API

```sh
go run ./cmd/api
```

## Integration tests

```sh
export TEST_DATABASE_URL="$DATABASE_URL"
go test ./test/database ./test/api -count=1
```

Use a PostgreSQL URL pointing at a development/test database. The database
connectivity test performs only `SELECT 1`. Each API integration test
creates a unique schema, loads `init.sql` into it, and drops that schema when
finished. Its database role needs permission to create schemas, tables,
functions, and triggers. The tests verify trigger idempotency and rollback, listing jobs without runs,
pagination, update busy states (including lease-aware `RUNNING` / `CANCEL_REQUESTED`)
and rollback, snapshot preservation, concurrent updates/triggers,
claim-then-update / update-then-claim interleaving, and the worker-claim locking
protocol. Without `TEST_DATABASE_URL`, integration tests skip.

Compose starts PostgreSQL 16 only. It does not start RabbitMQ, the API, an outbox
relay, a scheduler, or workers. The schema supports those future components;
currently manual triggering writes `QUEUED` runs and `PENDING` outbox events.
