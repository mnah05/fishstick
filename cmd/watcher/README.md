# Lease watcher

`cmd/watcher` periodically scans for expired `RUNNING` run leases. A missing
lease timestamp counts as expired, as does a timestamp at or before the current
time. Eligible runs move to `RETRY_WAIT` and get a delayed dispatch in the
PostgreSQL outbox, or exhausted runs become `FAILED` with an `OPEN` dead-letter
row. Each transition and its side effect commit in one transaction. The relay
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
