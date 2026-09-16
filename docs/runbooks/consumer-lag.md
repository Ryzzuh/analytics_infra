# Consumer lag high
**Symptom:** `ConsumerLagHigh` — a consumer group is more than ~5 minutes of data behind, for
10 minutes or more.

**First question: is the loader running, or just slow?**

```bash
docker compose logs --tail 100 airflow | grep load_
psql "$WAREHOUSE_DSN" -c "SELECT topic, max(created_at), max(end_offset) FROM ops.load_ledger GROUP BY topic"
```

If the ledger is advancing, this is throughput: the loader is reading, just not fast enough.
If it is not advancing, treat it as `LoaderStalled` instead — lag is the symptom, not the fault.

**Fix**
- Throughput: raise `LOADER_MAX_RECORDS` (default 50,000) so each run claims a larger range,
  or shorten the schedule interval. Both are safe: ranges are claimed transactionally.
- A single hot partition: check skew with `rpk group describe warehouse-loader`. One account
  dominating a partition is the usual cause (SPEC.md §17).

**Do not** reset the consumer group to skip ahead. Offsets are advisory here; the ledger is the
source of truth, and skipping loses data silently rather than loudly.
