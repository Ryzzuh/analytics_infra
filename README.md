# analytics_infra

An end-to-end analytics platform, built to show what happens **when things go wrong**: late
data, duplicates, schema drift, connector outages, crashes mid-commit, erasure requests.

Full design: [SPEC.md](SPEC.md). Decisions: [docs/adr](docs/adr).

> **Status: M2 (CDC).** M1's event path plus: the source OLTP schema with its publication and
> replica identity, Debezium change events into `raw.cdc_changes`, SCD2 dimensions derived from
> the change log, and a reconciliation test against an independent snapshot.
> Next: M3 (full dbt layering, marts, `mart_account_health`).

## The idea in one paragraph

A synthetic B2B SaaS product emits product events, mutates an OLTP database, and bills through
a Stripe-like mock. Those feed a platform of the shape most companies actually run: a broker,
CDC, an orchestrator, a warehouse, transformation, and activation back to the product. The
demo value is not the happy path — it is the offset ledger, the WAL cap, the drift policy and
the chaos scenarios that prove the failure paths behave.

## Run it

```bash
make install     # uv workspace
make test        # 56 tests: embedded Postgres (incl. logical replication) + real dbt runs
make up          # core stack (~4 GB): Redpanda, warehouse, Airflow, collector, simulator
```

Then: Airflow at http://localhost:8080, collector at http://localhost:8000,
warehouse on `localhost:5433` (`warehouse`/`warehouse`).

`make up-full` adds the Redpanda console. `make nuke` removes volumes.

## What M1 proves

The loader's five invariants, each with a test in
[tests/test_loader_invariants.py](tests/test_loader_invariants.py):

| Invariant | Test |
|---|---|
| Rows and ledger entry commit together | `test_crash_before_commit_leaves_no_trace` |
| The ledger, not the broker, decides where the next run starts | `test_crash_after_commit_before_offset_commit_does_not_duplicate` |
| A rerun replays its recorded range and replaces its rows | `test_rerun_replaces_rather_than_duplicating` |
| An expired range fails loudly rather than loading less | `test_rerun_outside_retention_fails_loudly` |
| Erased accounts cannot re-enter on any path | `test_erased_accounts_cannot_re_enter_on_any_path` |

And M2's, in [tests/test_scd2.py](tests/test_scd2.py) and
[tests/test_replication_config.py](tests/test_replication_config.py):

| Claim | Test |
|---|---|
| Three transitions in one day are three SCD2 versions | `test_three_transitions_in_one_day_are_three_versions` |
| A delete closes an interval without opening a version | `test_a_delete_closes_the_interval_without_opening_a_version` |
| Updates that change nothing tracked create no version | `test_updates_that_change_nothing_tracked_do_not_create_versions` |
| Reverse ETL's target table is not captured by CDC | `test_reverse_etl_target_is_not_captured` |
| A WAL cap invalidates the slot instead of filling the disk | `test_the_cap_invalidates_the_slot_instead_of_filling_the_disk` |
| Reconciliation catches changes CDC never received | `test_reconciliation_test_fails_when_cdc_missed_a_change` |

Debezium needs a JVM and is not run by the test suite. Everything it *depends on* is: the tests
use a real logical-decoding Postgres, so publication membership, replica identity and slot
invalidation under `max_slot_wal_keep_size` are verified rather than asserted in prose.

Alongside those: the collector's contract (envelope-only validation, account-keyed
partitioning, no ack without a broker ack) in
[tests/test_collector.py](tests/test_collector.py), and the staging model run for real against
an embedded Postgres in [tests/test_dbt_staging.py](tests/test_dbt_staging.py). CI adds a
DagBag import check, since Airflow itself is not a workspace dependency.

Try the crash yourself against a running stack:

```bash
uv run python -m loader.cli load --topic product.feature_usage --crash-before-commit
```

It dies with the transaction open. Nothing lands in `raw`, no ledger row appears, and the
broker's committed offset does not move. Run it again without the flag and the same messages
load exactly once.

## Layout

| Path | What |
|---|---|
| `platform/loader/` | Ledger, deterministic range reads, replace-on-rerun, DLQ, erasure filter |
| `services/collector/` | FastAPI event collector; validates the envelope only |
| `services/simulator/` | Synthetic SaaS traffic, including duplicates and late events |
| `airflow/dags/` | Micro-batch load DAGs |
| `dbt/` | `staging` → `core` (SCD2) → (`marts` from M3) |
| `db/app/ddl/` | Source OLTP schema, publication and replica identity |
| `infra/connect/` | Debezium connector config, with the reasoning per setting |
| `db/warehouse/ddl/` | `ops` (ledger, DLQ, erasure) and `raw` schemas |
| `infra/compose/` | Local and VM stack |

## Design notes worth reading

- **[ADR 0001](docs/adr/0001-offset-ledger.md)** — why the ledger is transactional and the
  broker's offsets are only a metric.
- **[ADR 0002](docs/adr/0002-scd2-from-the-change-log.md)** — why history comes from the change
  log rather than dbt snapshots, and why it is dated by business time.
- **[SPEC.md §5.2](SPEC.md)** — why `raw` is partitioned by *load* date, and why that does not
  help erasure.
- **[SPEC.md §6.2](SPEC.md)** — why backfill uses separate topics rather than a "backfill mode"
  flag: a year of replayed history is indistinguishable from late data by timestamp alone.
