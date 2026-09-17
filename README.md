# analytics_infra

An end-to-end analytics platform, built to show what happens **when things go wrong**: late
data, duplicates, schema drift, connector outages, crashes mid-commit, erasure requests.

Full design: [SPEC.md](SPEC.md). Decisions: [docs/adr](docs/adr).

> **Status: complete and running locally.** All eight milestones built, and the stack has now
> been run for real: simulator → collector → Redpanda → loader → warehouse, Debezium CDC into
> SCD2, and a full dbt build producing churn scores. The remaining step is the public instance,
> which costs money and is therefore a human decision — see
> [docs/deployment.md](docs/deployment.md).
>
> Running it found four defects the 199 tests could not: a PyPI package shadowing our own
> `loader` inside the image, Kafka Connect not resolving `${env:...}` without a config provider,
> Redpanda auto-creating one-partition topics that silently discarded the configured layout, and
> a loader CLI that could not drive the CDC path at all.

## The idea in one paragraph

A synthetic B2B SaaS product emits product events, mutates an OLTP database, and bills through
a Stripe-like mock. Those feed a platform of the shape most companies actually run: a broker,
CDC, an orchestrator, a warehouse, transformation, and activation back to the product. The
demo value is not the happy path — it is the offset ledger, the WAL cap, the drift policy and
the chaos scenarios that prove the failure paths behave.

## Run it

```bash
make install         # uv workspace
make test            # 165 fast tests, ~2 min (embedded Postgres, incl. logical replication)
make test-all        # + 27 that invoke dbt for real, ~4 min
make test-alerts     # alert rule unit tests (promtool) + Alertmanager config check
make infra-check     # terraform validate/fmt, caddy validate, actionlint, secrets check
make cost            # what the current infrastructure configuration costs per month
make console-build   # build the Console (static bundle, served by Caddy)
make history-estimate  # what a 12-month seed would cost, without producing anything
make up              # core stack (~4 GB): Redpanda, warehouse, Airflow, collector, simulator
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
| A delete closes an interval at the deletion, not at the row's last change | `test_a_delete_closes_the_interval_at_the_deletion_not_the_last_change` |
| Updates that change nothing tracked create no version | `test_updates_that_change_nothing_tracked_do_not_create_versions` |
| A no-op update between versions leaves no gap in the timeline | `test_a_noop_update_between_versions_leaves_no_gap` |
| `ended_by_delete` is never NULL, so the obvious filter is not silently empty | `test_ended_by_delete_is_never_null` |
| Changes made after a snapshot are not reconciliation failures | `test_reconciliation_ignores_changes_made_after_the_snapshot` |
| Reverse ETL's target table is not captured by CDC | `test_reverse_etl_target_is_not_captured` |
| A WAL cap invalidates the slot instead of filling the disk | `test_the_cap_invalidates_the_slot_instead_of_filling_the_disk` |
| Reconciliation catches changes CDC never received | `test_reconciliation_test_fails_when_cdc_missed_a_change` |

Debezium needs a JVM and is not run by the test suite. Everything it *depends on* is: the tests
use a real logical-decoding Postgres, so publication membership, replica identity and slot
invalidation under `max_slot_wal_keep_size` are verified rather than asserted in prose.

### What CI does not cover

CI runs the test suite and the linters against an embedded Postgres. It starts no containers,
so everything below was verified by hand against the running stack and is evidenced in commit
messages and `docs/incidents/`, not by a green tick. Listed because the distinction is the
point: a passing pipeline is not the same claim as a working system, and several of the worst
defects found in this project lived precisely in the gap.

| Not covered by CI | How it was verified instead |
|---|---|
| The compose stack starting at all | Run on a 16 GB host; every service healthy |
| Debezium registration and CDC topic creation | Connector `RUNNING`, topics carrying messages, slot active |
| Airflow scheduling, DAG parsing, the Cosmos task graph | 10 DAGs parsed with no import errors, `transform` green 32/32 |
| The four chaos scenarios, inject and recover | `docs/incidents/01`–`04`, with the observed numbers |
| Deterministic replay across a whole topic | 384 tasks replayed: row count held, 0 duplicate offsets |
| Golden snapshot and restore | Run end to end: `docs/runbooks/golden-restore.md` |
| The live Hetzner instance | Not yet created; `terraform apply` is a billable human decision |

Two of those gaps have since been narrowed, because the failure was reducible to a test:
`tests/test_connector_topics.py` asserts the connector's table list against the topics
`redpanda-init` creates (a mismatch let the connector report `RUNNING` while every produce
failed), and `tests/test_fixture_teardown.py` asserts the suite stops the Postgres servers it
starts. Where a container failure *can* be pulled back into the suite, it should be.

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
| `services/billing_mock/` | A payment provider that drops, duplicates and reorders webhooks |
| `services/webhook_receiver/` | Takes webhooks, puts them on the broker, acks only what is durable |
| `services/app_api/` | The product: receives churn scores, rate-limits, 404s for erased accounts |
| `airflow/dags/` | Micro-batch load DAGs |
| `dbt/` | `staging` → `core` (SCD2, facts, date spine) → `marts` |
| `db/app/ddl/` | Source OLTP schema, publication and replica identity |
| `infra/connect/` | Debezium connector config, with the reasoning per setting |
| `platform/reverse_etl/` | Activation: diffing, idempotency keys, and the failure taxonomy |
| `platform/dq/` | Volume anomalies, duplicate/quarantine rates, freshness against SLOs |
| `platform/drift/` | Schema drift: what the payloads look like now vs before, and what breaks |
| `services/control_plane/` | The Console's API: status, pipeline runs, chaos, reset, locking |
| `console/` | The Console itself (SvelteKit, static) |
| `docs/incidents/` | One write-up per chaos scenario: timeline, cost, recovery |
| `infra/monitoring/` | Prometheus rules **and their unit tests**, Alertmanager, dashboards |
| `infra/terraform/` | The live instance: server, volume, firewall, cloud-init, cost output |
| `infra/caddy/` | TLS and the public/passcode split |
| `docs/runbooks/` | One per alert: symptom, the queries to run, the fix, and what not to do |
| `platform/opsctl/` | Golden snapshots: what to capture, how stale is too stale, restore order |
| `infra/golden/` | The docker half of snapshot and restore |
| `db/warehouse/ddl/` | `ops` (ledger, DLQ, erasure) and `raw` schemas |
| `infra/compose/` | Local and VM stack |

## Design notes worth reading

- **[ADR 0001](docs/adr/0001-offset-ledger.md)** — why the ledger is transactional and the
  broker's offsets are only a metric.
- **[ADR 0002](docs/adr/0002-scd2-from-the-change-log.md)** — why history comes from the change
  log rather than dbt snapshots, and why it is dated by business time.
- **[ADR 0003](docs/adr/0003-history-backfill-and-golden-snapshots.md)** — why replayed history
  gets its own topics, why the cutoff measures load lag, and why a reset catches up instead of
  leaving a hole.
- **[docs/incidents/](docs/incidents/)** — the four chaos scenarios as incident reviews. The
  best single read if you want to know whether this platform does anything real.
- **[docs/deployment.md](docs/deployment.md)** — what it costs, the teardown discipline that
  keeps it from becoming a surprise invoice, and what has and has not been verified.
- **[Alert rules](infra/monitoring/rules/platform.yml)** and
  **[their tests](infra/monitoring/rules_test.yml)** — the near-miss cases are the interesting
  half: a micro-batch lag sawtooth, an idle source, a 1% reverse-ETL error rate. None of them
  page, on purpose.
- **[`mart_account_health`](dbt/models/marts/mart_account_health.sql)** — the churn score, with
  every component carrying its own reason, because a score nobody can explain cannot be acted
  on by the product that receives it.
- **[SPEC.md §5.2](SPEC.md)** — why `raw` is partitioned by *load* date, and why that does not
  help erasure.
- **[SPEC.md §6.2](SPEC.md)** — why backfill uses separate topics rather than a "backfill mode"
  flag: a year of replayed history is indistinguishable from late data by timestamp alone.
