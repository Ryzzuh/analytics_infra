# Schema drift release
**Scenario:** `schema_drift` · **Runbook:** [schema-drift](../runbooks/schema-drift.md)

## What was injected

The simulator starts sending `plan_code` where it used to send `plan` — an app release, from
the platform's point of view. Nothing rejects it: payloads are schemaless by design, because a
new field must never cost a client a 4xx (SPEC.md §7).

## What happens without a detector

Nothing fails. Rows keep loading, every one of them individually valid. `plan` is simply null
for new events, and revenue-by-plan quietly drops toward zero as old data ages out. The first
sign is usually someone asking why a dashboard looks wrong, days later.

## Timeline

| Time | What happened |
|---|---|
| T+0 | The release ships. Events arrive with the new shape and load normally. |
| T+≤1h | The drift detector's hourly run compares the window against the recorded baseline. It finds `plan` missing and `plan_code` new. |
| T+≤1h | `plan` is marked **blocking** — a model declares it in `meta.required_fields`, read from the dbt manifest, so the registry cannot drift from the SQL. `plan_code` is recorded as a new field and blocks nothing. |
| T+≤1h | **DriftBlocking** fires. The Console shows the finding with the exact event type and path. |
| Next build | `stg_product_events` fails. **Only that model** — other event families keep flowing, which is why the topics are split per family. |

## Recovery

Forward, in a real incident: teach staging to read both shapes during the transition
(`coalesce(payload ->> 'plan_code', payload ->> 'plan')`), update `required_fields`, resolve the
finding. The console's Recover action rolls the release back instead, so the scenario can be
re-run.

## The judgement call

A detector that fires on everything gets an exception list, and then gets ignored. So: a new
field never blocks, a field seen only a handful of times is not treated as a baseline, one
stray null is not a type change, and an event family that has gone quiet is a freshness problem
rather than a hundred "field removed" findings.
