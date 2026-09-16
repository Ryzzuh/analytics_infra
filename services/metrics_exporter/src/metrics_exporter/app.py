"""Exports the platform's own health to Prometheus (SPEC.md §10).

Redpanda, Postgres and Airflow each have their own exporters for their own internals. This one
publishes what only the warehouse knows: how far behind each layer is, what the data-quality
checks found, how the reverse-ETL sync is doing, and whether a chaos window is open.

Everything is computed at scrape time from the `ops` schema rather than kept in memory, so the
exporter is stateless: restarting it loses nothing, and two of them would agree.
"""

from __future__ import annotations

import logging
import os

import psycopg
from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Gauge, generate_latest

log = logging.getLogger(__name__)

WAREHOUSE_DSN = os.environ.get(
    "WAREHOUSE_DSN", "postgresql://warehouse:warehouse@postgres-warehouse:5432/warehouse"
)

app = FastAPI(title="Platform Metrics Exporter")

# Status is numeric so alert rules can compare it: 0 pass, 1 warn, 2 fail. A string label would
# force every rule to match on text, and `== 2` is clearer than `{status="fail"}` when the
# question is "is anything actually broken".
DQ_STATUS = {"pass": 0, "warn": 1, "fail": 2}

QUERIES: dict[str, str] = {
    # Seconds since the last committed ledger entry per topic. Distinct from consumer lag:
    # this catches a loader that is not running at all, which produces no lag growth if the
    # producer has also stopped.
    "seconds_since_last_load": """
        SELECT topic, extract(epoch from (now() - max(created_at)))
        FROM ops.load_ledger GROUP BY topic
    """,
    "freshness": """
        SELECT r.check_name, r.target, r.observed
        FROM ops.dq_results r
        JOIN (
            SELECT target, max(checked_at) AS latest FROM ops.dq_results
            WHERE check_name = 'freshness' GROUP BY target
        ) latest ON latest.target = r.target AND latest.latest = r.checked_at
        WHERE r.check_name = 'freshness'
    """,
}


def _collect(conn: psycopg.Connection, registry: CollectorRegistry) -> None:
    freshness = Gauge(
        "analytics_freshness_seconds", "Age of each layer", ["layer"], registry=registry
    )
    freshness_slo = Gauge(
        "analytics_freshness_slo_seconds",
        "Freshness target per layer",
        ["layer"],
        registry=registry,
    )
    dq_status = Gauge(
        "analytics_dq_check_status",
        "Latest data-quality check status (0 pass, 1 warn, 2 fail)",
        ["check", "target"],
        registry=registry,
    )
    since_load = Gauge(
        "analytics_seconds_since_last_load",
        "Seconds since the last committed load, per topic",
        ["topic"],
        registry=registry,
    )
    since_cdc = Gauge(
        "analytics_seconds_since_last_cdc_change",
        "Seconds since the last CDC change was loaded",
        registry=registry,
    )
    oltp_writes = Gauge(
        "analytics_oltp_writes_per_minute",
        "Recent source-database change rate, so a quiet source is not mistaken for a broken one",
        registry=registry,
    )
    quarantine_rate = Gauge(
        "analytics_quarantine_rate", "Share of recent events held by the cutoff", registry=registry
    )
    reverse_etl_error_rate = Gauge(
        "analytics_reverse_etl_error_rate",
        "Share of accounts whose last sync failed with a server error",
        registry=registry,
    )
    billing_miss_rate = Gauge(
        "analytics_billing_webhook_miss_rate",
        "Share of reconciled billing events the webhook path missed",
        registry=registry,
    )
    drift_blocking = Gauge(
        "analytics_drift_blocking_models",
        "Models currently blocked by schema drift",
        registry=registry,
    )
    cdc_gap_open = Gauge("analytics_cdc_gap_open", "Unresolved CDC gaps", registry=registry)
    chaos_active = Gauge(
        "analytics_chaos_window_active",
        "Open chaos windows; paging is inhibited while above zero",
        registry=registry,
    )

    for layer, target_seconds in conn.execute(
        "SELECT layer, target_seconds FROM ops.freshness_slo"
    ).fetchall():
        freshness_slo.labels(layer=layer).set(target_seconds)

    for _check, layer, observed in conn.execute(QUERIES["freshness"]).fetchall():
        if observed is not None:
            freshness.labels(layer=layer).set(float(observed))

    for check, target, status in conn.execute(
        """
        SELECT r.check_name, r.target, r.status
        FROM ops.dq_results r
        JOIN (
            SELECT check_name, target, max(checked_at) AS latest
            FROM ops.dq_results GROUP BY check_name, target
        ) latest
          ON latest.check_name = r.check_name
         AND latest.target = r.target
         AND latest.latest = r.checked_at
        """
    ).fetchall():
        dq_status.labels(check=check, target=target).set(DQ_STATUS.get(status, 2))

    for topic, seconds in conn.execute(QUERIES["seconds_since_last_load"]).fetchall():
        since_load.labels(topic=topic).set(float(seconds))

    since_cdc.set(
        float(
            conn.execute(
                "SELECT coalesce(extract(epoch from (now() - max(loaded_at))), 1e9) "
                "FROM raw.cdc_changes"
            ).fetchone()[0]
        )
    )
    oltp_writes.set(
        float(
            conn.execute(
                "SELECT count(*) FROM raw.cdc_changes WHERE source_ts > now() - interval '5 min'"
            ).fetchone()[0]
        )
        / 5
    )

    for gauge, query in (
        (
            quarantine_rate,
            "SELECT coalesce(max(observed), 0) FROM ops.dq_results "
            "WHERE check_name = 'quarantine_rate' AND checked_at > now() - interval '1 day'",
        ),
        (
            reverse_etl_error_rate,
            "SELECT coalesce(max(observed), 0) FROM ops.dq_results "
            "WHERE check_name = 'reverse_etl_error_rate' AND checked_at > now() - interval '1 day'",
        ),
        (
            cdc_gap_open,
            "SELECT count(*) FROM ops.cdc_gaps WHERE resolved_at IS NULL",
        ),
        (
            chaos_active,
            "SELECT count(*) FROM ops.chaos_windows WHERE closed_at IS NULL",
        ),
        (
            drift_blocking,
            "SELECT count(*) FROM ops.drift_findings WHERE resolved_at IS NULL AND blocking",
        ),
    ):
        gauge.set(float(conn.execute(query).fetchone()[0]))

    # The reconciliation model is dbt-built and may not exist yet on a fresh deployment; a
    # missing model is not a reason for the whole scrape to fail.
    try:
        row = conn.execute(
            "SELECT coalesce(avg(webhook_miss_rate), 0) FROM staging.stg_billing_reconciliation "
            "WHERE billing_day > current_date - 7"
        ).fetchone()
        billing_miss_rate.set(float(row[0]))
    except psycopg.Error:
        billing_miss_rate.set(0.0)


@app.get("/metrics")
def metrics() -> Response:
    """A fresh registry per scrape, because every value here is a point-in-time query."""
    registry = CollectorRegistry()
    with psycopg.connect(WAREHOUSE_DSN, autocommit=True) as conn:
        _collect(conn, registry)
    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
