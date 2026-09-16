# DAG failure
**Symptom:** `DagFailure` — a DAG run failed within the last 10 minutes.

**Triage by DAG**

- `load_product_events` / `load_cdc` / `load_billing_webhooks`: see `loader-stalled.md`. Clearing
  the task is safe — reruns replace their recorded offset range rather than appending.
- `transform`: Cosmos renders one task per model, so the failing task names the failing model.
  A failed *test* task is data, not code: read the failure before rerunning it.
- `pull_billing_batch`: the cursor is only advanced with the rows, so a failed run retries the
  same window. Safe to clear.
- `reverse_etl_sync`: check whether the destination is failing broadly before retrying — the
  task fails on error *rate*, so it firing means something systemic.
- `snapshot_oltp`: the reconciliation test goes quiet without a snapshot rather than failing, so
  this is not urgent, but it does mean CDC completeness is unverified until it runs.
