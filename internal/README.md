# Internal packages

The API uses Echo, generated SQLC queries, and one shared PostgreSQL connection
pool. Start with [the local setup guide](../cmd/api/README.md).

## Package responsibilities

| Package | Responsibility |
| --- | --- |
| `config` | Load and validate `DATABASE_*` environment variables |
| `database` | Open the pool, apply embedded goose migrations, load development fixtures |
| `database/sqlc` | Generated queries and database models |
| `api` | Routes, validation, response mapping, and update/trigger transactions |

`cmd/api/main.go` loads configuration, opens the pool, checks connectivity, and
passes `db` to `api.NewRouter(db)`. The router creates its own `sqlc.Queries`.
The update and trigger handlers also receive the pool to start transactions and bind their
queries with `queries.WithTx(tx)`. Shutdown drains HTTP requests before closing
the pool.

Job creation stores definitions and scheduling metadata. Manual triggering
stores a run and dispatch event. Automatic scheduling, outbox publishing,
RabbitMQ consumers, and execution workers are not implemented.

## Development

Use the dependencies pinned in `go.mod` (Go version: `1.27.3`):

```sh
go mod download
go test ./...
```

Database integration tests skip unless `TEST_DATABASE_URL` is set. The
connectivity test only runs a health query. The API integration tests create
and drop isolated schemas to check listing, update/trigger transactions,
concurrency, and rollback. See
[database testing](database/README.md#integration-tests).

## References

- [Configuration](config/README.md)
- [Migrations and development fixtures](database/README.md)
- [SQL sources and generation](database/sqlc/README.md)
- [HTTP API behavior](api/README.md)

Handlers pass request contexts into queries; ordinary job handlers do not add a
query deadline. The health endpoint adds a three-second deadline. Connection
timeouts control connection establishment, not general query execution.
PostgreSQL TLS and session settings can be supplied in `DATABASE_URL`, such as
`sslmode=verify-full` and `statement_timeout=30000`.
