# analytics_infra

An end-to-end analytics platform, built to show what happens **when things go wrong**: late
data, duplicates, schema drift, connector outages, crashes mid-commit, erasure requests.

Full design: [SPEC.md](SPEC.md). Decisions: [docs/adr](docs/adr).

> **Status: M1 (events thin slice).** Simulator → collector → Redpanda → micro-batch loader
> with a transactional offset ledger → partitioned `raw` → one dbt staging model.
> Next: M2 (CDC via Debezium, SCD2 from the change log).

## The idea in one paragraph

A synthetic B2B SaaS product emits product events, mutates an OLTP database, and bills through
a Stripe-like mock. Those feed a platform of the shape most companies actually run: a broker,
CDC, an orchestrator, a warehouse, transformation, and activation back to the product. The
demo value is not the happy path — it is the offset ledger, the WAL cap, the drift policy and
the chaos scenarios that prove the failure paths behave.

## Run it

```bash
make install     # uv workspace
make test        # 25 tests, embedded Postgres + real dbt run, no Docker needed
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
| `dbt/` | `staging` → (`core`, `marts` from M3) |
| `db/warehouse/ddl/` | `ops` (ledger, DLQ, erasure) and `raw` schemas |
| `infra/compose/` | Local and VM stack |

## Design notes worth reading

- **[ADR 0001](docs/adr/0001-offset-ledger.md)** — why the ledger is transactional and the
  broker's offsets are only a metric.
- **[SPEC.md §5.2](SPEC.md)** — why `raw` is partitioned by *load* date, and why that does not
  help erasure.
- **[SPEC.md §6.2](SPEC.md)** — why backfill uses separate topics rather than a "backfill mode"
  flag: a year of replayed history is indistinguishable from late data by timestamp alone.
