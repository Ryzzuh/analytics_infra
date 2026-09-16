# Freshness SLO breach
**Symptom:** `FreshnessSloBreach` — a layer is older than its target in `ops.freshness_slo`.

**Read it bottom-up.** Freshness breaches cascade: if `raw` is stale, `staging` and `marts`
will follow, and only the deepest breach is the real one.

```sql
SELECT target, status, observed, threshold, checked_at
FROM ops.dq_results WHERE check_name = 'freshness'
ORDER BY checked_at DESC LIMIT 12;
```

- **raw stale** → an ingestion problem: see `loader-stalled.md` or `slot-invalidation.md`.
- **staging stale but raw fresh** → the dbt build is failing or not running. Check the
  `transform` DAG; Cosmos names the failing model as the failing task.
- **marts stale, staging fresh** → expected for up to 24h: marts are built daily by design.
  If the alert fires anyway, the SLO in `ops.freshness_slo` disagrees with the schedule — fix
  the number rather than muting the alert.
