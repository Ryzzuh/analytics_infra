# Quarantine rate high
**Symptom:** `QuarantineRateHigh` — more than 1% of recent events are being held back by the
lateness cutoff for over an hour.

**What quarantine means here:** a live event arrived more than the tolerance window after it
happened, so folding it in would rewrite a period that has already been reported (SPEC.md §6.2).
Held events are not lost; they are in `raw`, and visible in
`staging.stg_product_events_quarantined`.

```sql
SELECT would_have_landed_on, count(*), round(avg(load_lag_days), 1) AS avg_lag_days
FROM staging.stg_product_events_quarantined GROUP BY 1 ORDER BY 1 DESC LIMIT 10;
```

**Two very different causes**

1. **Clients genuinely buffering** (a device fleet offline, a mobile release with a bad queue):
   the lag will cluster a few days back. Decide whether to absorb them with a full refresh.
2. **The platform was stalled**, so ordinary live traffic now looks stale by load lag. Fix the
   stall first; the quarantine rate is a symptom, not the fault.

**Absorbing them is a decision:** `dbt build --select stg_product_events+ --full-refresh`. Say so
in the incident notes — it changes numbers people may already have seen.
