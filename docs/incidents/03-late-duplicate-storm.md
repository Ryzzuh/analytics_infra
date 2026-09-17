# Late and duplicate event storm
**Scenario:** `late_duplicate_storm` · **Runbook:** [quarantine](../runbooks/quarantine.md)

## What was injected

5,000 events with `event_time` up to three days old and `received_at` of now — a device fleet
coming back online and flushing its queue — with 30% of them sent twice.

## What the platform did

**Duplicates were resolved, not hidden.** Raw kept every copy, because raw is the audit of what
actually arrived. Staging kept the first arrival per `event_id` and recorded how many extra
copies came with it, so `duplicate_rate` rose visibly rather than the duplicates disappearing
silently. A client that started retrying everything would show up here.

**Late events split by how late they were.** Events inside the tolerance window were folded
into the day they happened — the daily aggregate selects by `loaded_at` but groups by
`event_time`, so a two-day-old event rebuilds its own day rather than inflating today. Events
past the tolerance were held in `stg_product_events_quarantined` instead.

| Signal | Before | After |
|---|---|---|
| `duplicate_rate` | ~0.004 | ~0.3 during the storm |
| `quarantine_rate` | ~0 | rises; **QuarantineRateHigh** fires if sustained for an hour |
| Rows in raw | — | every copy, duplicates included |
| Rows in staging | — | one per `event_id` |

## Recovery

Nothing to restart. The platform absorbed what it could and held back the rest.

Absorbing the remainder is a **decision, not a cleanup step**: a full refresh would fold
month-old events into periods that have already been reported, changing numbers people may have
acted on. So the Console reports the count and the command rather than running it.

## Why the cutoff measures load lag

Not client lag. How long a device sat on an event is interesting; whether a day that has
already been built is about to change is actionable. That distinction is also what makes the
backfill path necessary — replayed history is months stale by load lag and would otherwise be
quarantined in its entirety (SPEC.md §6.2).

## Observed on a real run (2026-09-16)

5,000 events up to three days old, 30% of them sent twice.

| Measure | Value |
|---|---|
| raw rows (every copy kept) | 15,376 |
| staging rows (deduplicated) | 12,886 |
| duplicate copies collapsed | 1,239 |
| quarantined, held back | 1,251 |
| worst load lag observed | 3.1 days |

Both mechanisms fired at once, which is what makes this scenario worth having: dedup resolved
the retries while the cutoff held back everything past the tolerance. The 60-model dbt build
stayed green throughout — the storm changed the numbers, not the shape.

Recovery reported the 1,251 held events and the command to absorb them, rather than running it:
absorbing changes periods that have already been reported, so it is a decision.

## Second run: on the always-on host (2026-09-17)

5,000 events, three days late, 30% of them duplicated.

| Measure | Value |
|---|---|
| Raw rows after the storm | 39,013 |
| Distinct `event_id` | 37,415 |
| **Duplicate copies collapsed** | **1,598** |
| Rows in staging | 36,416 |
| **Held in quarantine** | **1,262** |
| Worst load lag | **3.9 days** |

Recovery reported the quarantined remainder rather than absorbing it, which is the intended
behaviour: folding 1,262 three-day-old events into closed periods changes numbers people may
already have seen, so it stays a decision (SPEC.md §6.2).

Those figures do not reconcile to an exact identity — staging plus quarantine exceeds the
distinct count by 263 — because the simulator keeps writing between each count and the models
were built a minute before they were taken. Worth stating plainly: on a live stack the
arithmetic only closes if every number is read from the same instant.
