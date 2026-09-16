# ADR 0001 — The offset ledger is the source of truth, not the broker

**Status:** accepted (M1)

## Context

The loader is a scheduled micro-batch: each run reads a range of messages from Redpanda and
writes them to Postgres. Something has to record how far consumption has got.

The obvious choice is the broker's own consumer-group offsets. That produces a well-known
failure: the database transaction commits, the process dies before the offset commit, and the
next run reloads the same messages. Rows are duplicated. The usual mitigation is a unique
constraint plus `ON CONFLICT DO NOTHING`, which turns every insert into an index probe on a
table that is only ever appended to.

The two commits cannot be made atomic: they are different systems.

## Decision

The **ledger row and the data are written in the same Postgres transaction**. A ledger row
recording `[start_offset, end_offset)` exists if and only if those rows are in `raw`.

Broker offsets are still committed, *after* the transaction, so `rpk group describe` and lag
dashboards work. They are never read back to decide what to load.

A rerun of a given `dag_run_id` re-reads **the range that run recorded** and replaces its rows
(delete by `ledger_id` within the entry's `loaded_date`, re-insert, bump `attempt`).

## Consequences

- The crash-between-commits case cannot duplicate data: if the transaction did not commit,
  nothing happened at all.
- Reruns are deterministic. Clearing a task after fixing a parser bug reprocesses exactly the
  messages that task saw, not "everything currently in the topic".
- Ranges are contiguous by construction: a claim that would leave a gap raises `LedgerGap`
  instead of quietly skipping data.
- If a range has expired from broker retention, a rerun raises `OffsetOutOfRange` rather than
  loading a shorter range. Failing loudly is the point: loading less would look like success.
- Airflow's logical date is irrelevant to what gets loaded, so `catchup=False` and
  `max_active_runs=1`. Two concurrent runs would read the same watermark and claim overlapping
  ranges.
- Dedup of *client-side* duplicates (the same `event_id` genuinely sent twice) is a separate
  problem, handled in staging. Raw keeps every copy as the record of what actually arrived.

## Alternatives rejected

- **Broker offsets + idempotent upsert.** Costs a unique index on the largest table in the
  warehouse, and hides real double-loads as no-ops rather than surfacing them.
- **Kafka transactions / exactly-once semantics.** Only applies to Kafka-to-Kafka flows; the
  sink here is Postgres, so the transactional boundary must be Postgres's.
- **A long-running consumer service.** Holds an Airflow worker slot forever if run as a task,
  or becomes a separate deployment with its own supervision, restart and back-pressure story
  for no gain at this cadence.
