# Outbox relay

The relay owns the `PENDING`/`INFLIGHT`/`PUBLISHED` outbox transitions. It
claims dispatch batches with `FOR UPDATE SKIP LOCKED`, publishes persistent
JSON to RabbitMQ, waits for broker confirms, then records `PUBLISHED`. A publish
failure returns the row to `PENDING` with bounded backoff. Expired locks can be
claimed by another relay process.

Set `DATABASE_URL` and the `RELAY_*` variables in
[`internal/config/.env.example`](../../internal/config/.env.example). The relay
declares a durable exchange and worker queue before polling. It exits at startup
if PostgreSQL or RabbitMQ is unavailable. Delivery is at least once: if RabbitMQ
accepts a message but PostgreSQL cannot record the confirmation, a later publish
may duplicate it; run claims are idempotent and fenced.

The batch loop and `Publisher` interface are in [main.go](main.go). The
integration test injects a fake publisher to check release and publish state
without needing RabbitMQ.
