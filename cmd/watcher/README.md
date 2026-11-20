# Lease watcher

`cmd/watcher` periodically scans for expired `RUNNING` run leases. It moves
eligible runs to `RETRY_WAIT` and adds a delayed dispatch to the PostgreSQL
outbox, or marks exhausted runs `FAILED` and inserts an `OPEN` dead-letter row.
Each transition and its side effect commit in one transaction. The relay
publishes retry dispatches to RabbitMQ.

The watcher uses `DATABASE_*` plus `WATCHER_*` settings from
[`internal/config/.env.example`](../../internal/config/.env.example). It exposes
`/healthz` on `:8082` by default. It can run as multiple replicas; candidates
are rechecked while holding the job and run locks, so only one replica recovers
each run.

Start it with the other pipeline processes:

```sh
go run ./cmd/watcher
```
