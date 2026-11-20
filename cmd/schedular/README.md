# Scheduler

`cmd/schedular` creates runs for scheduled jobs whose `next_run_at` is due. Each
poll opens a database transaction and locks a bounded batch of due jobs with
`FOR UPDATE SKIP LOCKED`, allowing scheduler replicas to work on separate jobs.

For each job, the scheduler saves an immutable snapshot of the event and retry
settings, creates or finds the run for that scheduled occurrence, and inserts
a dispatch event into the PostgreSQL outbox. It then advances `next_run_at`.
Those writes commit together, so a run cannot be created without its dispatch
intent or schedule advancement. A savepoint isolates each occurrence: a bad
schedule leaves that job due while other valid occurrences in the batch can
commit.

`ONCE` jobs are cleared after their occurrence is recorded. `CRON` jobs advance
to the next future tick using the job's IANA timezone; missed ticks during
downtime are skipped. The relay later publishes outbox events to RabbitMQ, and
the worker claims and executes each run. See [the state machine guide](../../docs/state-machines.md)
for run and outbox transitions.

The scheduler uses `DATABASE_*` plus `SCHEDULER_*` settings from
[`internal/config/.env.example`](../../internal/config/.env.example). Start it
with the other pipeline processes:

```sh
go run ./cmd/schedular
```
