# Analytics Platform — Specification

**Project:** `analytics_infra` — an end-to-end analytics infrastructure demo (everything up to, but
excluding, the reporting/BI layer).

**Status:** Spec, pre-implementation. Written 2026-09-16 from a design interview.

**Repo:** new **public** GitHub repo, working copy at `~/Projects/claude/analytics_infra`.

---

## 1. Purpose and audience

A CV/portfolio piece demonstrating **data platform / data engineering** capability: ingestion,
orchestration, storage design, reliability, observability, IaC and CI/CD. It is deliberately *not*
another modelling showcase — `fifawc` already covers analytics engineering, and this project must
read as a different skill set.

The thing being demonstrated is **how the platform behaves when things go wrong**: late data,
duplicates, schema drift, connector outages, crashes mid-commit, erasure requests. Those are the
parts of the job that are hard to fake, so they are the centre of the design.

### What a reviewer can do

1. **Clone and run it** — `make up` on a laptop (≤ 4 GB profile), `make up-full` for everything.
2. **Browse a live instance** — public read-only Grafana, Airflow and Platform Console.
3. **Read the walkthrough** — README, architecture diagram, ADRs, chaos/incident write-ups, and a
   3–5 minute demo video.
4. **Break it on purpose** — chaos scenarios behind a demo passcode printed in the README, with the
   symptom, the alert and the recovery all visible in the Console.

### Non-goals

- No BI/reporting layer. Grafana is present for **operational** metrics only.
- No distributed compute (no Spark/Flink). Single-node Postgres is the warehouse.
- No ML. The churn score is transparent rule-based SQL.
- No multi-tenant auth, no real PII (data is synthetic).

---

## 2. Architecture

```
                    ┌──────────────────── Platform Console (SvelteKit + FastAPI) ───────────────┐
                    │  status · freshness · lag · incident feed · [Run pipeline] · [Chaos ▾]    │
                    └───────┬─────────────────────────────────────────────────────────┬────────┘
                            │ (holds Airflow token, locking, passcode)                 │
                            ▼                                                          ▼
  ┌───────────────┐   ┌───────────┐   ┌──────────────┐   ┌───────────────┐   ┌────────────────┐
  │  Simulator    │──▶│ Collector │──▶│              │   │   Airflow 3   │   │  Prometheus/   │
  │ (SaaS users)  │   │ (FastAPI) │   │              │   │ LocalExecutor │   │  Grafana/      │
  └───────┬───────┘   └───────────┘   │              │   │               │   │  Alertmanager  │
          │ writes                     │  Redpanda   │──▶│ micro-batch   │   └────────────────┘
          ▼                            │  (Kafka API)│   │ loaders (5m)  │            ▲
  ┌───────────────┐   ┌───────────┐    │             │   │ dbt via Cosmos│            │ metrics
  │ App OLTP      │──▶│ Debezium  │───▶│              │   │ reverse ETL   │            │
  │ Postgres      │   │ (Connect) │    └──────────────┘   └───────┬───────┘            │
  └───────▲───────┘   └───────────┘                               │ COPY               │
          │                                                        ▼                    │
          │  ┌──────────────┐  webhooks  ┌───────────┐   ┌────────────────────────────┴───┐
          │  │ Billing mock │───────────▶│ Webhook   │──▶│  Warehouse Postgres            │
          │  │ (Stripe-like)│◀───────────│ receiver  │   │  raw → staging → core → marts  │
          │  └──────────────┘  list API  └───────────┘   │  + ops schema (ledger, DQ)     │
          │                     (daily)                   └────────────┬───────────────────┘
          │                                                            │ reverse ETL (daily)
          └──────────────── app REST API  ◀────────────────────────────┘
                            (writes account_insights, NOT captured by CDC)

  OpenLineage events from Airflow + dbt ──▶ Marquez
```

### Runtime

| Environment | Where | Profile |
|---|---|---|
| **prod** | Hetzner **CX42** (8 vCPU / 16 GB / 160 GB) + 100 GB attached volume, ~€17 + ~€5 per month, always on | full stack |
| **local-core** | laptop, `make up` | ≤ 4 GB: Redpanda, Connect, OLTP PG, warehouse PG, Airflow, simulator at 1/10 rate, 3-month seed |
| **local-full** | laptop ≥ 16 GB, `make up-full` | adds Prometheus, Grafana, Marquez, Console |
| **CI** | GitHub Actions | ephemeral Postgres + Redpanda via service containers / testcontainers |

Everything is `docker compose` on a single host. Terraform provisions the VM, volume, firewall, DNS
and object storage; it does not orchestrate containers.

**RAM budget (prod, approximate):** Redpanda 1.0, Connect 1.0, OLTP PG 1.0, warehouse PG 3.0,
Airflow + metadata PG 2.5, Prometheus + Grafana 1.5, Marquez + its PG 1.0, custom services 1.0, OS +
Docker 1.0 = **~13 GB**, leaving ~3 GB headroom. If headroom proves insufficient, Marquez is the
first thing to drop (fallback: store OpenLineage events in `ops` and render them in the Console).

---

## 3. Source system: synthetic SaaS product

A B2B SaaS subscription product (seats, plans, usage), chosen because it naturally produces all four
ingestion patterns.

**Scale (prod steady state):** ~2,000 accounts, ~20 product events/s ≈ 1.73M events/day ≈ 0.86 GB/day
in Postgres (assuming ~500 bytes/event with row and index overhead).

### 3.1 OLTP schema (`app` Postgres)

| Table | Notes |
|---|---|
| `accounts` | company, created_at, **hard-deleted** on GDPR erasure |
| `users` | belongs to account, `last_login_at` mutates constantly |
| `subscriptions` | plan, seats, status (`trial`→`active`→`past_due`→`cancelled`), price |
| `plans` | slowly changing prices — the SCD2 case |
| `invoices` | mirrors the billing provider, reconciled daily |
| `support_tickets` | feeds the churn score |
| `account_insights` | **written by reverse ETL, excluded from the Debezium publication** |

All captured tables are `REPLICA IDENTITY FULL` so that `before` images exist for updates and
deletes. Without it Postgres only puts the primary key in the `before` image and SCD2 loses the
prior values. The cost is more WAL per update, which is accepted and documented.

### 3.2 Simulator

A Python process driving realistic behaviour: signups, trials converting or lapsing, seat
expansion/contraction, feature usage by cohort, payment failures, support tickets, churn, and
occasional account deletions. It emits:

- **product events** → HTTP to the collector,
- **OLTP writes** → direct SQL to the app DB (which Debezium then captures),
- **billing activity** → to the billing mock, which emits webhooks.

Every row and event carries `effective_at` (simulated business time) in addition to wall-clock
timestamps. In steady state `effective_at == now()`; during historical replay it does not, and
that distinction is what makes SCD2 correct (see §6.1).

### 3.3 Billing mock

A Stripe-like service exposing:
- **webhooks** (`invoice.paid`, `invoice.payment_failed`, `subscription.updated`) — deliberately
  unreliable: duplicates, out-of-order delivery, and no delivery at all while the receiver is down;
- **`GET /v1/events?created[gte]=…&starting_after=…`** — cursor pagination, rate limited
  (HTTP 429 + `Retry-After`), eventually consistent by a few seconds.

---

## 4. Ingestion

### 4.1 Product events (stream)

**Collector** (FastAPI): validates envelope shape only (not payload schema — see §7), stamps
`received_at`, produces to Redpanda, and acks **only after the produce is acknowledged** by the
broker. A 5xx tells the client to retry, which is the source of the duplicates handled in §4.4.

**Topics:** one per event family, 6 partitions each, **key = `account_id`** (per-account ordering;
hot-account skew is accepted and monitored):

- `product.session` — `session_start`, `session_end`, `page_view`
- `product.feature_usage` — `feature_invoked`, `report_exported`, `api_called`
- `product.lifecycle` — `signup`, `invite_sent`, `seat_added`, `seat_removed`
- `product.billing_ui` — `plan_viewed`, `checkout_started`, `subscription_changed`

Per-family topics mean per-family retention and schema rules, and a poison message in one family
cannot stall the others.

Backfill counterparts: `product.<family>.backfill` (see §6.2).

**Retention:** 3 days live topics; backfill topics 12 hours, deleted after a successful load.

### 4.2 CDC (Debezium → Redpanda)

- Connector: `io.debezium.connector.postgresql`, `plugin.name=pgoutput`.
- Publication lists captured tables **explicitly** — `account_insights` is not in it, which is what
  prevents the reverse-ETL feedback loop.
- `heartbeat.interval.ms=10000` plus a heartbeat table. Without heartbeats a slot on a low-traffic
  database can hold WAL open indefinitely because the confirmed LSN never advances.
- Incremental snapshots enabled via a signal table (`debezium_signal`), used by automated recovery.
- Deletes produce an `op=d` event **and** a tombstone (null value). The loader must tolerate null
  values; that is an explicit test case.

**Topics:** `cdc.app.public.<table>`, key = primary key.

### 4.3 Batch: billing reconciliation

Two paths, merged downstream:

1. **Fast path** — webhook receiver → `billing.webhooks` topic → loader → `raw.billing_events`.
2. **Slow path** — a daily Airflow DAG pulls `GET /v1/events` with a **48-hour overlap window** from
   the last successful cursor, handling 429s with backoff, into `raw.billing_events_batch`.

`staging.billing_events` unions both, deduplicating on the provider event id. A reconciliation test
counts events present in the batch pull but never received by webhook, and exposes
`billing_webhook_miss_rate` as a metric. This mirrors how real Stripe pipelines are built.

### 4.4 Loader design (the core of the project)

Airflow micro-batch DAGs, not long-running consumers: a consumer that never returns would occupy a
worker slot forever and lose in-flight batches on restart.

- `load_product_events` — every **5 minutes**
- `load_cdc` — every **2 minutes**
- `load_billing_webhooks` — every **5 minutes**

Each run, per `(topic, partition)`:

1. Read the last committed range from `ops.load_ledger`.
2. Claim `[from_offset, to_offset)` where `to_offset` = current high-water mark, capped at
   `max_records_per_run` (back-pressure guard).
3. Consume, parse, extract typed columns (`event_id`, `account_id`, `event_type`, `event_time`),
   keep the full payload as JSONB.
4. **In one Postgres transaction:** `COPY` rows into the raw partition **and** insert the ledger row.
   Commit.
5. *Then* commit offsets to Redpanda — for consumer-lag metrics only, never as the source of truth.

**Why a ledger rather than Kafka offsets:** if the process dies between the database commit and the
offset commit, Kafka-based tracking reprocesses the batch and duplicates rows. Because the ledger
entry and the data share a transaction, the load either fully happened or did not happen at all.
Redpanda's offsets are then just a monitoring convenience.

**Rerun semantics.** Clearing an Airflow task re-reads **the same offset range** recorded in the
ledger (deterministic), not "whatever is in Kafka now". The rerun performs a **replace** in one
transaction: `DELETE FROM raw.x WHERE _ledger_id = :id`, re-insert, update the ledger row
(`attempt = attempt + 1`). If the range has fallen out of Redpanda retention the task **fails
loudly** — it does not silently load less. Rebuilding from raw is a different operation
(`dbt build --full-refresh`) and never touches Kafka.

**Dedup.** Clients mint an `event_id` (UUID) per event. Raw keeps **every** copy received — it is
the audit of what actually arrived. `staging.*` keeps the first occurrence per `event_id`
(dbt incremental, `unique_key=event_id`, `delete+insert`), and emits `duplicate_rate` to the ops
schema. Duplicates are therefore observable rather than silently suppressed.

---

## 5. Storage: warehouse Postgres

### 5.1 Layers

| Schema | Owner | Contents |
|---|---|---|
| `raw` | loader (not dbt) | untouched payloads + CDC envelopes, partitioned, append-only |
| `staging` | dbt | typed, deduped, 1:1 with sources |
| `core` | dbt | conformed dimensions (incl. SCD2), fact tables |
| `marts` | dbt | purpose-built: `mart_account_health`, `mart_revenue_mrr`, `mart_activation_funnel` |
| `ops` | platform | ledger, DQ results, drift, erasure, sync state, chaos windows |

Reverse ETL reads **marts only**. Deliberately not medallion naming — `fifawc` uses
bronze/silver/gold, and these should not look like the same project.

### 5.2 Raw events table

```sql
CREATE TABLE raw.product_events (
  loaded_date   date        NOT NULL,          -- partition key
  topic         text        NOT NULL,
  partition_id  smallint    NOT NULL,
  kafka_offset  bigint      NOT NULL,
  ledger_id     bigint      NOT NULL REFERENCES ops.load_ledger(id),
  event_id      uuid        NOT NULL,
  account_id    bigint,                        -- extracted for erasure + joins
  user_id       bigint,
  event_type    text        NOT NULL,
  event_time    timestamptz NOT NULL,          -- client-asserted
  received_at   timestamptz NOT NULL,          -- collector
  kafka_ts      timestamptz NOT NULL,
  loaded_at     timestamptz NOT NULL DEFAULT now(),
  source_path   text        NOT NULL,          -- 'live' | 'backfill'
  payload       jsonb       NOT NULL,
  PRIMARY KEY (loaded_date, topic, partition_id, kafka_offset)
) PARTITION BY RANGE (loaded_date);

CREATE INDEX ON raw.product_events USING brin (event_time);
CREATE INDEX ON raw.product_events (account_id);
```

**Why partition by load date, not event date:**

- rerun-replace deletes touch exactly one partition;
- retention drops a whole partition instead of deleting rows.

The BRIN index on `event_time` keeps lookback scans cheap **because event_time correlates with load
order** in the live path. Two honest caveats:

1. **Erasure is not helped by this partitioning.** Deleting one account's rows spans every partition
   they were active in, whatever the partition key. That is why `account_id` is a real column with a
   b-tree index — digging it out of JSONB during a purge would be far slower.
2. **Backfill breaks BRIN correlation**, because 12 months of `event_time` land in one load-date
   partition. The backfill loader therefore **sorts by `event_time` before `COPY`**, restoring
   correlation within the partition.

CDC raw tables (`raw.cdc_<table>`) follow the same shape with `op`, `before`, `after`, `source_lsn`,
`source_ts`, `effective_at`.

### 5.3 Retention

- Redpanda: 3 days (live), 12 h (backfill).
- `raw.*`: 90 days by partition drop.
- `core`/`marts`: kept.
- Because prod resets weekly (§9.3), the 90-day drop would rarely fire in production, so **retention
  is proven by an integration test with a fake clock in CI**, not by waiting.

---

## 6. Transformation

**dbt-core** against Postgres, orchestrated by **astronomer-cosmos** so each model and test is its
own Airflow task (granular retries, real lineage in the Airflow UI). Cadence: **daily** full dbt
build, triggered by Airflow Assets on raw updates plus the Console button. The loaders run
continuously regardless of dbt, so a dbt failure never stops ingestion.

### 6.1 SCD2 from the CDC change log

`core.dim_account`, `core.dim_subscription` and `core.dim_plan` are built from the **CDC event
stream**, not from dbt snapshots. dbt snapshots compare current state at each run and would collapse
a `trial → active → cancelled` sequence that happened between two runs into a single row. The change
log has every transition.

Validity intervals key off **`effective_at`** (business time), ordered by `source_lsn` as the
tiebreak. During historical replay Debezium's own timestamp is wall-clock "now", so it cannot be
used for history.

A daily **reconciliation test** compares a current-state snapshot of the OLTP tables against "latest
SCD2 row per key". A mismatch means CDC dropped something — the signal that a slot invalidation or
re-snapshot lost changes.

### 6.2 Late data

**Policy: lookback window + cutoff, declared per model** (`meta.lateness_tolerance_days`).

- Incremental models reprocess the last **N days of `event_time`** each run (default 3; revenue
  marts 7; sessions 1).
- Events arriving beyond their model's tolerance are routed to `raw.events_quarantine` with a
  `late_arrival_seconds` metric and an alert when the quarantine rate crosses a threshold.
- A manual/scheduled `--full-refresh` absorbs quarantined events.

**Backfill is separated from lateness by construction.** Historical replay produces to
`*.backfill` topics, loaded by a separate DAG with the cutoff disabled and `source_path='backfill'`.
The live path keeps strict late rules. Without this separation, seeding 12 months of history would
quarantine the entire history — the two are indistinguishable by timestamp alone.

### 6.3 Churn score (reverse-ETL payload)

`marts.mart_account_health` computes a transparent points score from seat-utilisation decline,
login recency, failed payments, support-ticket volume and feature-breadth trend. It outputs
`score`, `band`, `score_version` and a `reasons` array, so the contract can later be served by a
model without changing the sync.

---

## 7. Schema governance

**Product events are schemaless JSON at ingest** (raw accepts anything; nothing is rejected at the
door), with **drift detection downstream**.

`ops.event_shape` records, per `(event_type, json_path)`: inferred type, first_seen, last_seen,
occurrence count. An hourly job compares the last window against the recorded shape and classifies:

| Change | Response |
|---|---|
| New field appears | Log + notify. Nothing blocks. |
| Field disappears **and no model depends on it** | Log + notify. |
| Field disappears **and a model depends on it** | **Fail that event type's staging model and its downstream.** Other event types keep flowing. |
| Type changes (e.g. string → array) | Same as above: fail the affected subtree. |

Model dependencies come from a **field registry** declared in dbt `schema.yml`
(`meta.required_fields: [account_id, plan_id, …]`) and read from `manifest.json`, so the registry
cannot drift from the SQL that uses it.

The worked example: an app release renames `plan_id` → `plan_code`. Without this, revenue-by-plan
quietly drops to zero for new rows. With it, `staging.billing_ui_events` fails within the hour and
the alert names the exact field and event type.

CDC is structurally typed by Debezium already, so it needs no separate drift detection — a DDL
change appears as a schema change in the envelope and fails the affected staging model the same way.

---

## 8. Reverse ETL (activation)

Daily DAG: `marts.mart_account_health` → **the app's REST API** → `app.account_insights`.

- **Diff-based**: only accounts whose payload hash differs from the last successful sync are sent.
- `ops.reverse_etl_sync_state` tracks per account: last payload hash, last success, attempts, last
  error, status.
- Batches of 50, client-side rate limiting to the API's published limit, `Idempotency-Key` =
  `hash(account_id, score_version, payload_hash)`.
- **5xx** → retry with exponential backoff; **429** → honour `Retry-After`; **4xx** → record and
  skip (a deleted account is an expected 404, not an incident).
- The run succeeds if the error rate is below a threshold (default 2%), and fails loudly above it.
- `account_insights` is **excluded from the Debezium publication**, so these writes never re-enter
  the warehouse. The warehouse already knows what it wrote, via the sync-state table.

---

## 9. Reliability, chaos and recovery

### 9.1 Replication-slot safety — the disk-fill scenario

Everything shares one VM disk, so an unread slot does not just threaten the app: when the disk
fills, Redpanda cannot append, the warehouse cannot write, and Airflow's metadata DB fails.

**Decision: protect the disk, and automate the recovery.**

- `max_slot_wal_keep_size = 5GB` on the app Postgres.
- Warn at 2 GB retained, critical at 4 GB (Telegram).
- Past the cap, Postgres invalidates the slot. A monitor detects `pg_replication_slots.active =
  false` / invalidated, then: re-creates the slot, re-registers the connector with
  `snapshot.mode=never`, and triggers a **Debezium incremental snapshot** through the signal table to
  re-sync the gap.
- The gap is recorded in `ops.cdc_gaps`, and the SCD2 reconciliation test (§6.1) is the independent
  check that the re-sync actually healed it.

### 9.2 Chaos scenarios (v1)

Each has: an inject action in the Console, an expected symptom, an alert, a recovery (automatic or
runbook), and a written incident review.

| Scenario | Inject | Symptom | Recovery |
|---|---|---|---|
| **Connector outage → slot invalidation** | Stop Kafka Connect for N minutes | CDC lag; retained WAL climbs; cap hit; slot invalidated | Auto re-snapshot (§9.1); reconciliation test proves completeness |
| **Schema drift release** | Simulator switches to `plan_code` | Drift detector fails `staging.billing_ui_events` + downstream; other families unaffected | Ship the staging model fix; rerun |
| **Late + duplicate event storm** | Flush 3-day-old events with resends | `late_arrival_seconds` spike, `duplicate_rate` spike, quarantine grows | Lookback absorbs within tolerance; `--full-refresh` for the rest |
| **Loader crash mid-commit / poison message** | Kill the loader between DB commit and offset commit; inject a tombstone and malformed JSON | Task fails and retries; no duplicate rows (ledger); poison message to DLQ | Automatic on retry; DLQ inspected in the Console |

Nothing about "traffic spike" is in v1, but the loader's `max_records_per_run` cap and consumer-lag
alert exist to make that a cheap addition later.

### 9.3 Reset to known-good

Restoring a database while Debezium's stored offsets point at a future LSN leaves CDC broken or
silently skipping. So reset is **nuke and re-seed from a cold, whole-stack snapshot**:

1. `docker compose stop` (everything, together).
2. Restore **all volumes from one golden archive**: app PG, warehouse PG, Redpanda (including the
   Connect offsets/config/status topics), Airflow metadata.
3. Start, verify health, re-register connectors with `snapshot.mode=never`.

Because the volumes were captured cold and together, the slot position, Debezium's offsets and the
warehouse ledger are guaranteed to agree.

**Snapshot mechanism:** `zstd` tarballs of the volume directories, stored on the attached volume and
in Hetzner Object Storage (~6–10 GB compressed; restore takes a few minutes). Boring and portable —
the same code path works on a laptop.

**Golden data must not go stale.** A golden snapshot from day T0, restored at T0+60, would leave a
60-day hole in every mart. So:

- The golden archive is **rebuilt weekly** by a scheduled job.
- On reset, after restoring, the simulator **replays the gap** (≤ 7 days) through the backfill path:
  OLTP changes with real `effective_at`, product events to `*.backfill` topics.
- Consequence: the backfill path is exercised weekly rather than rotting.

**Cadence:** automatic weekly reset (with the golden rebuild), plus on demand behind the passcode.

### 9.4 Erasure (GDPR)

A deleted account's data exists in the OLTP DB, raw JSONB, Redpanda (up to 3 days), the marts, and
backups. Critically, **rerun-replace would re-load erased events from Redpanda** unless erasure is
enforced on every load path.

- `ops.erasure_requests` holds **account ids only, no PII**, with `requested_at`.
- **Every** load — live, rerun and backfill — anti-joins against it, so erased data cannot re-enter.
- A scheduled purge deletes matching rows from `raw`, `staging`, `core` and `marts`, and records
  counts per table for the audit trail.
- Redpanda's 3-day retention is the documented grace window; volume archives expire after 30 days.
- The whole position is written up in `docs/compliance.md` as the deliverable — including its
  limits, because claiming "fully erased everywhere instantly" would be false.

---

## 10. Observability

**Stack:** Prometheus + Alertmanager + Grafana, with exporters for Redpanda, both Postgres
instances, Kafka Connect (JMX) and the host. Airflow exposes StatsD → Prometheus. Custom services
expose `/metrics`.

### 10.1 SLOs (tiered by layer)

| Layer | SLO (p95) |
|---|---|
| `raw` product events behind `event_time` | ≤ 10 min |
| `raw` CDC behind commit | ≤ 5 min |
| `staging` | ≤ 1 h |
| `core` / `marts` | ≤ 24 h (daily build) |
| Reverse ETL delivered | ≤ 24 h |

### 10.2 Alerts

| Alert | Condition |
|---|---|
| Consumer lag | > 5 min of data, sustained 10 min |
| Slot WAL retained | > 2 GB (warn), > 4 GB (critical) |
| CDC stalled | no CDC events for 10 min while OLTP writes continue (heartbeat-based) |
| Drift blocking | any staging model failed by the drift policy |
| Freshness SLO breach | per-layer, from the table above |
| DAG failure | any non-retried failure |
| Quarantine rate | late events > 1% of volume over 1 h |
| DQ anomaly | row count vs 7-day median outside tolerance |
| Reverse-ETL error rate | > 2% in a run |

**Routing — chaos-aware.** Injecting a scenario opens a window in `ops.chaos_windows`, and the
control plane exports `chaos_window_active{scenario="…"}`. Pager rules (Telegram) carry
`and on() chaos_window_active == 0`; the inverse route sends chaos-time alerts to the **Console
incident feed** plus a daily digest. So a stranger triggering chaos at 3am does not wake you, **and
an alert firing with no chaos window open is a genuine incident**.

### 10.3 Data quality

dbt's own tests (`not_null`, `unique`, `relationships`, `accepted_values`) plus dbt source freshness,
plus a small custom framework for what dbt does not cover: volume anomaly vs 7-day median,
duplicate rate, late rate, CDC-vs-snapshot reconciliation. Results land in `ops.dq_results`, which
the Console renders. Deliberately no Elementary or Great Expectations — both would duplicate the
Console and add a dependency for little gain at this size.

### 10.4 Lineage

OpenLineage events from **both** Airflow and dbt → **Marquez**, so lineage spans
collector → topic → raw → staging → core → marts → reverse ETL, rather than stopping at dbt's
boundary.

---

## 11. Platform Console

A small SvelteKit UI + FastAPI control plane. It exists because the product app must not know the
orchestrator exists, and because the Airflow API token has to live somewhere that applies locking
and rate limits.

**Public (read-only):** pipeline status, per-layer freshness vs SLO, consumer lag, last N runs,
incident feed, DQ results, drift report, chaos history.

**Passcode-protected:** `Run pipeline`, chaos injections, `Reset to known-good`.

`Run pipeline` is **rejected while a run is active** (button disabled, showing "running since
14:02"), enforced server-side by `max_active_runs=1` on the DAG as well as by a lock in the control
plane. A global cooldown applies to chaos actions.

The SaaS app gets a minimal UI — enough to show churn-risk flags arriving from reverse ETL, since
its users are simulated.

---

## 12. Repo layout

```
analytics_infra/
├── services/
│   ├── simulator/          # SaaS behaviour + historical replay
│   ├── collector/          # FastAPI event collector
│   ├── billing_mock/       # Stripe-like API + webhooks
│   ├── webhook_receiver/
│   ├── app_api/            # minimal SaaS app + admin UI
│   └── control_plane/      # Console API: Airflow token, locking, chaos
├── platform/
│   ├── loader/             # ledger, offset claim, COPY, replace — shared lib
│   ├── drift/              # shape recording + classification
│   ├── erasure/
│   ├── reverse_etl/
│   └── dq/
├── airflow/dags/
├── dbt/                    # models/{staging,core,marts}, tests, macros
├── console/                # SvelteKit
├── infra/
│   ├── terraform/          # Hetzner server, volume, firewall, DNS, object storage; S3 backend
│   ├── compose/            # docker-compose.yml + profiles (core, full)
│   ├── connect/            # Debezium connector configs
│   └── monitoring/         # Prometheus rules, Alertmanager routes, Grafana dashboards
├── docs/
│   ├── architecture.md + diagram
│   ├── adr/                # one per load-bearing decision
│   ├── runbooks/
│   ├── incidents/          # chaos write-ups
│   └── compliance.md
├── tests/                  # pytest + testcontainers
└── Makefile
```

**Language:** Python throughout (uv workspace) — the loader library is imported by Airflow tasks but
is testable without Airflow, which is what makes the ledger invariants unit-testable. SvelteKit only
for the Console.

---

## 13. CI/CD

**On pull request:**
1. `ruff` + `sqlfluff` + Airflow DagBag import test (no import errors, no cycles).
2. **Loader/ledger integration tests** (testcontainers: Redpanda + Postgres) — crash between DB
   commit and offset commit; rerun-replace idempotence; tombstone handling; erasure anti-join;
   retention partition drop with a fake clock. This is the most valuable test suite in the repo.
3. `dbt build` against an ephemeral Postgres with seed fixtures (slim CI via the prod `manifest.json`
   artefact published by the deploy job).
4. `terraform fmt/validate/plan`, with the plan posted as a PR comment (read-only cloud token).

**On merge to main:** build per-service images tagged with the git SHA → GHCR → deploy by SSH
(`docker compose pull && up -d`) with health-checked rollout; rollback = redeploy the previous SHA.

**Secrets:** `sops` + `age`. Encrypted files are committed; CI decrypts with an age key held in a
GitHub secret, and you decrypt locally. Reviewers get `.env.example` with dev-only defaults.

**Ports:** Caddy terminates TLS and reverse-proxies Grafana (anonymous viewer), Airflow (read-only
role for anonymous), Console (public read, passcode for actions). SSH restricted by firewall.

---

## 14. Build order

| Milestone | Contents | Done when |
|---|---|---|
| **M1 — events thin slice** | simulator → collector → Redpanda → micro-batch loader with ledger → `raw.product_events` → one staging model; local compose | crash-between-commits and rerun-replace tests pass |
| **M2 — CDC** | OLTP schema, Debezium, `raw.cdc_*`, `core.dim_*` SCD2 from the change log, reconciliation test | `trial→active→cancelled` in one day appears as three SCD2 rows |
| **M3 — warehouse + dbt** | full layering, Cosmos DAG, marts incl. `mart_account_health`, dbt tests | daily build green end to end |
| **M4 — history + backfill** | historical replay through OLTP, backfill topics, golden archive, restore + catch-up | 12-month history present; SCD2 has pre-go-live transitions |
| **M5 — billing + reverse ETL** | billing mock, webhooks + daily reconciliation, diff-based sync back to the app | webhook-miss metric non-zero and explained; insights visible in the app UI |
| **M6 — observability** | Prometheus/Grafana/Alertmanager, Telegram, SLO checks, DQ framework, Marquez | all §10.2 alerts fire in test |
| **M7 — infra + CI/CD** | Terraform, deploy pipeline, sops, Caddy, live instance up | `make destroy` / `make apply` round-trips cleanly |
| **M8 — Console + chaos** | Console UI, 4 chaos scenarios, reset, runbooks, incident write-ups, demo video | a stranger with the passcode can break and heal it |

M1 first because the offset ledger and rerun-replace are the hardest invariants in the project;
everything else assumes they hold.

---

## 15. Deliverables

- `README.md` — architecture diagram, the "why" per decision, how to run both profiles, the demo
  passcode, live URLs.
- **ADRs** for the load-bearing calls: offset ledger vs Kafka offsets; rerun = same offset range;
  partition by load date; schemaless events + downstream drift; slot cap vs CDC completeness;
  backfill topics vs a global flag; CDC-derived SCD2 vs dbt snapshots; separate uncaptured table for
  reverse ETL.
- **Runbooks** — slot invalidation, drift block, quarantine growth, reverse-ETL error spike, reset.
- **Incident write-ups** — one per chaos scenario: symptom → alert → diagnosis → recovery →
  prevention.
- **Demo video** (3–5 min) plus GIFs, so the project survives the live instance being down.

---

## 16. Corrections made during design

Recorded because the reasoning changed:

1. **"Redpanda avoids ZooKeeper" was wrong as a justification.** Kafka 4.0 (March 2025) removed
   ZooKeeper entirely. The load-bearing reason for Redpanda is memory: ~1 GB versus 2–3 GB for
   Kafka + Connect, which is what makes the ≤ 4 GB laptop profile possible.
2. **"Load-date partitioning makes erasure purges cheap" was wrong.** Erasure spans every partition
   regardless of the key. Load-date partitioning helps rerun-replace and retention only; erasure is
   served by an indexed `account_id` column.
3. **"Both databases in one Postgres server locally" was wrong.** WAL is per *server*, so the
   Debezium slot would also retain the warehouse's `COPY` traffic, and the slot-fill scenario would
   behave differently locally than in prod. Two separate instances in both profiles; the RAM cost is
   ~150 MB.
4. **The history-replay estimate was wrong by two orders of magnitude.** "About an hour" was a CDC
   estimate (tens of thousands of rows). At 5 events/s the product-event history is 158M rows
   (~80 GB, ~90 min of `COPY`). Resolved by thinning the history: ~1/s average for 11.5 months,
   ramping to 20/s over the final 14 days (~42M events, ~21 GB, ~20–25 min).
5. **Nightly reset would have meant retention never ran.** Moved to weekly resets, with retention
   proven by a fake-clock integration test.

## 17. Open questions

- Object storage choice for Terraform state and archives (Hetzner Object Storage is the default; it
  is S3-compatible and in the same account).
- Whether the Telegram alert bot should be a **separate** bot from your personal one, so a public
  demo cannot generate noise in a chat you use for other things. Leaning: separate bot.
- Hot-partition skew from large simulated accounts — measure in M1, and only add a composite key
  (`account_id:bucket`) if it actually shows up.
- Grafana dashboards to ship in v1 (suggest three: platform health, pipeline freshness, data
  quality).
