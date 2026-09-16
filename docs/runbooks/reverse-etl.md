# Reverse ETL error rate
**Symptom:** `ReverseEtlErrorRate` — more than 2% of accounts failed their last sync with a
server error.

```sql
SELECT status, count(*), max(last_error) AS example
FROM ops.reverse_etl_sync_state GROUP BY status;
```

**Read the taxonomy before acting** (SPEC.md §8)

- `skipped_client_error` (4xx): expected. A deleted account returns 404 forever, and it is
  excluded from this metric on purpose.
- `failed` (5xx): the product API is unhealthy, or rate limiting harder than the client expects.

**Check the destination first:** `curl -s localhost:8003/health`. If it is unhealthy, this alert
is a symptom and the product API is the incident.

**Recovery is automatic.** Failed accounts are retried on the next run because their content
still differs from the last successful send; no queue needs draining by hand.
