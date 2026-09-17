# Poison message and loader crash
**Scenario:** `poison_message` · **Runbook:** [loader-stalled](../runbooks/loader-stalled.md)

## What was injected

Five messages the parser cannot turn into rows: malformed JSON, a valid envelope with an
unparseable timestamp, a non-object body, an invalid UUID, and a tombstone on an event topic.

## What did NOT happen

That is the whole scenario:

- **The partition kept moving.** A loader that stops at the first message it cannot parse stops
  forever, because the message does not improve on retry.
- **The rest of the batch loaded.** Good messages in the same range landed normally.
- **Nothing was silently dropped.** Each bad message is in `ops.load_dlq` with its reason
  (`invalid_json`, `invalid_envelope: …`, `tombstone_on_event_topic`) and its exact offset, so
  it can be inspected and — after a parser fix — reprocessed.
- **No duplicates appeared**, including when the loader was killed mid-batch. Rows and the
  ledger entry commit in one transaction, so a crash before commit leaves no trace and the
  retry redoes the range cleanly (ADR 0001).

## The tombstone is the interesting one

On a CDC topic a tombstone is normal and meaningful: it follows every delete and lets log
compaction drop the key. On an event topic it is meaningless. So the loader treats it
differently per stream — skipped and counted for CDC, DLQ'd for events — rather than having one
rule that is wrong somewhere.

Counting matters as much as skipping: if tombstones were merely ignored, a connector emitting
them in error would be invisible.

## Recovery

Automatic. The DLQ *is* the recovery: the partition advanced, and the messages are kept. If the
parser was wrong rather than the messages, fixing it and clearing the Airflow task re-reads the
same recorded offset range and replaces those rows — no duplicates, no manual offset surgery.

## Observed on a real run (2026-09-16)

Five unparseable messages injected onto a live topic. The next load:

```
rows=8661 dlq=5 range=[144,8810)
```

8,661 good rows loaded, exactly five diverted, and **the partition advanced past them** —
144 to 8810 — rather than stalling. Each landed with its reason and offset:

| Offset | Reason |
|---|---|
| 8805 | `invalid_json: Expecting property name enclosed in double quotes` |
| 8806 | `missing_envelope_fields: event_time,received_at` |
| 8807 | `missing_envelope_fields: event_id,received_at` |
| 8808 | `envelope_not_object` |
| 8809 | `tombstone_on_event_topic` |

The tombstone is the one worth noting: normal and meaningful on a CDC topic, meaningless on an
event topic, and counted rather than ignored — a connector emitting them in error would
otherwise be invisible.

## Second run: on the always-on host, with Airflow orchestrating (2026-09-17)

The first run drove the loader by hand. This one ran on the 16 GB host with the scheduler
actually running the DAGs, which is the arrangement the design assumes.

Five poison messages injected; five rows in the DLQ within 90 seconds, each with a distinct
reason:

| Reason | Count |
|---|---|
| `tombstone_on_event_topic` | 1 |
| `invalid_json: Expecting property name enclosed in double quotes` | 1 |
| `envelope_not_object` | 1 |
| `missing_envelope_fields: event_id,received_at` | 1 |
| `missing_envelope_fields: event_time,received_at` | 1 |

The ledger entry that covered them is the evidence worth keeping:

    product.feature_usage  [1868, 1905)  row_count=32  dlq_count=5

Thirty-seven messages in the range, thirty-two loaded, five quarantined in the DLQ, and the
partition advanced past all of them. Nothing stalled and nothing was silently dropped: the
range accounts for every message exactly once. `raw.product_events` went from 31,350 to 31,950
across the same window, so the rest of the batch kept flowing.
