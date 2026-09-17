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

## Observed on a real run (2026-09-16)

After the release, payload keys became `['duration_ms', 'feature', 'plan_code', 'surface']`.
Nothing rejected it, nothing failed, and events kept loading — which is the point.

The detector's first pass over the release window reported only:

```
field_added    feature_invoked.plan_code    blocking=False
```

**`plan` was not yet reported as removed, and that was correct.** The loader batches by load
time, not by release, so that batch still contained pre-release events carrying `plan` — the
field had not stopped appearing yet. One window later, on purely post-release traffic:

```
BLOCKING field_removed  feature_invoked.plan   ['stg_product_events']
BLOCKING field_removed  api_called.plan        ['stg_product_events']
BLOCKING field_removed  report_exported.plan   ['stg_product_events']
```

Blocking, and citing the model that declares the field required.

**The lag is inherent**, not a defect: drift is detected against observed history, so a removal
is only visible once a full window contains none of the old shape. Expect up to one window
(an hour in production) between a release and the finding.

## Second run: on the always-on host (2026-09-17)

The release renaming `plan` to `plan_code` was injected at 00:47 UTC. Within one detector run
the new field was recorded for all four event types:

    page_view / feature_invoked / report_exported / api_called
    plan_code   field_added   blocking=false

The *blocking* finding — `field_removed` on `plan` — did not appear for the rest of the hour,
and that is correct rather than a delay to engineer away. The detector compares a one-hour
window against the recorded baseline, so until the whole window sits after the release it still
contains pre-release events carrying `plan`. A field absent for ten minutes is not a removed
field; treating it as one would fire on every optional field that happens to go quiet.

Running the same detector over a ten-minute window — the first window entirely after the
release — produced what the hourly run will produce once an hour has passed:

    page_view / report_exported / feature_invoked / api_called
    plan         field_removed   blocking=True

So the lag is a property of the window size, not of the detector. The cost is that a blocking
drift takes up to an hour to surface; the benefit is that it never fires on a quiet field. If
that trade ever needs revisiting, the knob is `window` in `drift_detection.py`, and the
consequence is more false positives, not faster true ones.
