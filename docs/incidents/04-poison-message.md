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
