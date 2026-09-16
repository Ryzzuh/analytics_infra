# Data-quality check failing
**Symptom:** `DqCheckFailing` — a check in `ops.dq_results` has status `fail` for 15 minutes.

```sql
SELECT check_name, target, observed, threshold, details, checked_at
FROM ops.dq_results WHERE status = 'fail' ORDER BY checked_at DESC LIMIT 20;
```

**By check**

- `volume_anomaly`: yesterday's volume is outside ±50% of the 7-day median. Check whether the
  simulator was stopped or the loader stalled before assuming the data is wrong.
- `duplicate_rate`: clients retrying more than usual. Look for collector 5xx or timeouts — the
  collector acks only after the broker does, so slow broker acks produce real duplicates.
- `quarantine_rate`: see `quarantine.md`.
- `reverse_etl_error_rate`: see `reverse-etl.md`.

**Never fix a check by widening its threshold during an incident.** Record why the threshold was
wrong, change it deliberately afterwards, or the check stops meaning anything.
