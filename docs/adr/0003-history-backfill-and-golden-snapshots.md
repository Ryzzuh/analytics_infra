# ADR 0003 — History arrives on its own topics, and resets catch up rather than gap

**Status:** accepted (M4)

## Context

Two problems that look unrelated and are the same problem.

**Seeding history.** The platform needs twelve months of past data. Replayed events are months
old, and the lateness policy exists precisely to catch events that are months old.

**Resetting.** The public instance must be restorable to a known-good state. But a snapshot
taken on day T0 and restored on T0+N leaves an N-shaped hole in every mart: no signups, no
usage, renewals that never happened, and freshness checks that think the world stopped.

## Decision

**History is produced to separate `*.backfill` topics** and loaded by its own DAG, landing with
`source_path = 'backfill'`, which exempts it from the lateness cutoff. Not a "backfill mode"
flag: a flag has to be turned off afterwards, and a flag left on disables the lateness policy
permanently and silently, which is indistinguishable from it working.

**The cutoff measures load lag** (`loaded_at - event_time`), not client lag
(`received_at - event_time`). Client lag says how long a device sat on an event; load lag says
whether a day that has already been built is about to change, which is the thing worth
governing. Both are kept as columns, since they answer different questions.

**Beyond tolerance, a live event is held back rather than folded in.** Quietly rewriting a
closed period is how reported numbers change under people who already acted on them. Held
events stay in raw, appear in `stg_product_events_quarantined`, and are absorbed by an explicit
`--full-refresh` — a decision, not a default. A model's first build is also non-incremental and
absorbs everything, which is the same rule: an initial load is explicit too.

**The quarantine list is an anti-join against staging**, not a restatement of the cutoff rule.
What matters is whether an event is actually being withheld, so the list empties after an
absorb rather than continuing to list events that are now present.

**Golden snapshots are cold copies of every volume, taken together** — warehouse, source
database, Redpanda (which holds the connector's offsets), and Airflow's metadata. Restoring the
databases alone would leave Debezium's stored offsets pointing at a position the restored log
does not match, so CDC would break or, worse, silently skip.

**A restore is always followed by a catch-up**: the window between `golden_at` and now is
replayed through the backfill path, at flat live density.

## Consequences

- The backfill path is exercised on every reset, not only when history is rebuilt, so it
  cannot rot quietly between seedings.
- `source_path` makes the distinction visible in the data. "Why is this event exempt?" is
  answerable with a `select`, not by reading loader configuration.
- Density is deliberately uneven: ~1/s for most of the year, ramping to 20/s over the final
  fortnight. `simulator-history --estimate-only` reports the consequence before producing
  anything — 43M events, ~21.5 GB. Uniform 20/s for a year would be ~630M events (~300 GB),
  which does not fit the VM.

## Deviation from the spec

SPEC.md §9.3 paired a **weekly** golden rebuild with restore-plus-catch-up. The arithmetic does
not support it. Catch-up runs at the live rate, so a seven-day gap is **~12.1M events, ~6 GB**,
and roughly fifteen minutes of generating and loading — during which the public instance is
mid-restore. A day-old snapshot is ~1.7M events and about two minutes.

So **golden is rebuilt daily**, and `catch_up_window` refuses a gap over **two days** rather
than grinding through it: past that, replaying at live density is slower than rebuilding
history properly, and the platform should say so.
