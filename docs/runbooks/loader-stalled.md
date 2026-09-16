# Loader stalled
**Symptom:** `LoaderStalled` — no committed ledger entry for a topic in 15 minutes.

**Diagnose**

```bash
# Is the DAG running at all?
curl -s localhost:8080/api/v2/dags/load_product_events/dagRuns | python3 -m json.tool | head -40
# Did a task fail and stop retrying?
docker compose logs --tail 200 airflow | grep -iE "error|traceback"
```

**Common causes, in the order they actually happen**

1. **A poison message the parser cannot handle.** Look in `ops.load_dlq` — if rows are landing
   there the loader is fine; if the task is erroring instead, the parse failure is one the
   loader treats as fatal, which is a bug worth fixing rather than skipping.
2. **`OffsetOutOfRange`**: the loader fell so far behind that retention deleted the data it
   needed. This is data loss and the task fails loudly on purpose. Decide explicitly whether to
   rebuild from raw or accept the gap, and record it.
3. **The warehouse is refusing writes** — check disk first (`df -h`), then whether a replication
   slot is holding WAL (see `slot-invalidation.md`).

**Recovery:** clear the failed task. Reruns re-read the exact recorded offset range and replace
their rows, so a retry cannot duplicate (ADR 0001).
