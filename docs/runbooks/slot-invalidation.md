# Replication slot growing or invalidated
**Symptom:** `SlotWalRetainedWarning` (2 GB), `SlotWalRetainedCritical` (4 GB), `CdcStalled`, or
`CdcGapOpen`.

**Why this matters more than it looks:** every service shares one disk. WAL held by an unread
slot fills it, and then Redpanda cannot append, the warehouse cannot write, and Airflow's
metadata database fails. The app is not the only casualty (SPEC.md §9.1).

**Diagnose**

```sql
SELECT slot_name, active, wal_status,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained
FROM pg_replication_slots;
```

- `active = false` → Debezium is not reading. Check Kafka Connect: `curl localhost:8083/connectors/app-cdc/status`.
- `wal_status = 'lost'` → the cap already fired; the slot is invalid and CDC has a gap.

**Fix**

1. Connector down: restart it. The slot resumes from its confirmed LSN and lag drains.
2. Slot invalidated: re-create the slot, re-register the connector with `snapshot.mode=never`,
   then trigger an incremental snapshot through the signal table:

```sql
INSERT INTO debezium_signal (id, type, data)
VALUES (gen_random_uuid()::text, 'execute-snapshot',
        '{"data-collections": ["public.subscriptions", "public.accounts"]}');
```

3. Record the gap in `ops.cdc_gaps` so the SCD2 reconciliation test reports recovery rather than
   firing as a separate incident, and close it when the snapshot completes.

**Prevention:** `max_slot_wal_keep_size` is set to 5 GB deliberately. Do not remove it to "fix"
this alert — that trades a broken connector for a dead box.
