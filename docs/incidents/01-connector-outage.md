# Connector outage → slot invalidation
**Scenario:** `connector_outage` · **Runbook:** [slot-invalidation](../runbooks/slot-invalidation.md)

## What was injected

Kafka Connect stopped. Nothing else was touched — no metric was written by hand, no alert was
forced. Everything below is the platform reacting.

## Timeline

| Time | What happened |
|---|---|
| T+0 | Connect stops. The replication slot has no reader. |
| T+2m | CDC freshness starts climbing; `analytics_seconds_since_last_cdc_change` rises. |
| T+10m | **CdcStalled** fires — and only because the source is still being written to. An idle database producing no changes would not have alerted, which is the difference between a broken connector and a quiet Saturday. |
| T+25m | Retained WAL passes 2 GB. **SlotWalRetainedWarning**. |
| T+40m | Past 4 GB. **SlotWalRetainedCritical**. |
| T+55m | `max_slot_wal_keep_size` (5 GB) is reached. Postgres invalidates the slot: `wal_status = 'lost'`. |
| T+55m | **The app keeps serving.** This is the decision from SPEC.md §9.1 paying off: the cap protected the shared disk, so Redpanda kept appending, the warehouse kept writing, and Airflow's metadata database stayed up. |

## What it cost

CDC has a gap between the slot's last confirmed LSN and the re-snapshot. The gap is recorded in
`ops.cdc_gaps`, and the SCD2 reconciliation test reports it as a known recovery rather than
firing as a second, unexplained incident.

## Recovery

Restarting Connect is not enough once the slot is invalid — Debezium resumes from a position
whose WAL no longer exists. The console's Recover action re-registers the connector with
`snapshot.mode=never` and inserts into the Debezium signal table to run an incremental snapshot.

Verification is the reconciliation test: it compares the CDC-derived SCD2 against an
independent snapshot of source state. Both agreeing is the only evidence that the re-snapshot
actually healed the gap, because everything else in the warehouse is derived from the change
log and would agree with itself either way.

## What would be different in production

The cap would be paired with a page at 2 GB that someone actually answers. The demo lets it run
to invalidation on purpose, because a scenario that stops at "an alert fired" does not show the
recovery, and the recovery is the interesting half.

## Observed on a real run (2026-09-16)

Injected from the Console against the local stack.

| Moment | Slot state |
|---|---|
| Connect stopped | `active=false`, `wal_status=reserved`, 576 B retained |
| After ~500 source writes | `active=false`, 73 kB retained and climbing |
| Connect restarted | `active=true`, backlog delivered |

The subscriptions topic went from 329 to 544 messages on recovery — every change made during
the outage arrived once Connect came back, because the slot had held the WAL for exactly that
purpose. Recovery reported `resnapshot: False`: the slot was never invalidated, so no gap
existed and re-snapshotting would have been wrong.

Loading the backlog produced 215 change rows with **0 DLQ**, and a full `dbt build` passed
60/60 afterwards, which is the evidence that the recovery left the warehouse consistent rather
than merely running.

Reaching the 5 GB cap is not practical in a demo — it would take gigabytes of WAL. That half is
proven instead by `tests/test_replication_config.py`, which sets a 32 MB cap against a real
Postgres and watches `wal_status` reach `lost` while the database keeps accepting writes.
