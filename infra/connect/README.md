# Debezium connector

The `connect-init` service registers this on every `make up`, so there is normally nothing to
do by hand. To re-apply it yourself:

```bash
curl --fail -sS -X PUT -H 'Content-Type: application/json' \
  --data @infra/connect/subscriptions-source.json \
  http://localhost:8083/connectors/app-cdc/config
```

This file is the bare **config object**, not a `{"name": ..., "config": {...}}` wrapper, because
`PUT /connectors/{name}/config` takes the config alone and is idempotent — it creates or updates
in place. Posting the wrapper to this endpoint fails with a deserialisation 500, and without
`--fail` curl reports that as success.

The destination topics are created by `redpanda-init`, not by the broker: auto-create is off, and
a connector whose topics do not exist reports `RUNNING` while every produce fails with
`UNKNOWN_TOPIC_OR_PARTITION` and the replication slot retains WAL indefinitely.

Settings that are load-bearing rather than default, and why:

| Setting | Why |
|---|---|
| `publication.autocreate.mode: disabled` | Debezium would otherwise create a `FOR ALL TABLES` publication, capturing `account_insights` and closing the reverse-ETL feedback loop (SPEC.md §8). The publication is created explicitly in `db/app/ddl/002_replication.sql`. |
| `heartbeat.interval.ms` + `heartbeat.action.query` | Without a heartbeat write, an idle source never advances the slot's confirmed LSN, so WAL accumulates even though nothing is happening (SPEC.md §4.2). |
| `signal.data.collection` | Automated recovery after a slot invalidation triggers an incremental snapshot by inserting into this table (SPEC.md §9.1). |
| `tombstones.on.delete: true` | Keeps compaction able to drop deleted keys. The loader skips tombstones rather than storing or DLQ-ing them. |
| `schemas.enable: false` | Smaller messages, and the loader accepts both shapes anyway. |
| `snapshot.mode: initial` | First run snapshots current state; after a slot invalidation the connector is re-registered with `never` and re-synced by incremental snapshot instead. |
| `errors.tolerance: none` | A change the connector cannot emit must stop the connector, not be dropped silently: the whole design assumes the change log is complete. |

`time.precision.mode: connect` keeps timestamps as millisecond epochs rather than Debezium's
microsecond variant, which is what `loader/cdc.py` expects when reading `effective_at`.
