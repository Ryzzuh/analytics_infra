"""CDC micro-batch load, and the daily reconciliation snapshot (SPEC.md §4.2, §6.1).

Runs every 2 minutes rather than the 5 of product events: the CDC freshness SLO is tighter
(≤ 5 min p95), and the volume is far smaller, so the runs are cheap.
"""

from __future__ import annotations

import os
from datetime import timedelta

import psycopg
from airflow.sdk import dag, task
from loader.kafka_source import KafkaMessageSource
from loader.run import run_load
from loader.snapshot import prune_snapshots, take_snapshot
from loader.targets import cdc_target

CDC_TOPICS = [
    "cdc.app.public.accounts",
    "cdc.app.public.users",
    "cdc.app.public.plans",
    "cdc.app.public.subscriptions",
    "cdc.app.public.invoices",
    "cdc.app.public.support_tickets",
]

WAREHOUSE_DSN = os.environ["WAREHOUSE_DSN"]
APP_DSN = os.environ.get("APP_DSN", "")
BOOTSTRAP = os.environ.get("REDPANDA_BOOTSTRAP", "redpanda:9092")


@dag(
    dag_id="load_cdc",
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["ingestion", "cdc"],
)
def load_cdc():
    @task
    def load_topic(topic: str, **context) -> dict:
        source = KafkaMessageSource(BOOTSTRAP, group_id="warehouse-loader-cdc")
        try:
            with psycopg.connect(WAREHOUSE_DSN, autocommit=False) as conn:
                result = run_load(
                    conn,
                    source,
                    topic=topic,
                    dag_run_id=context["run_id"],
                    target=cdc_target(),
                )
        finally:
            source.close()
        return {
            "topic": topic,
            "rows_loaded": result.rows_loaded,
            "dlq_rows": result.dlq_rows,
            # Tombstones: expected after every delete, and not an error.
            "records_skipped": result.records_skipped,
        }

    load_topic.expand(topic=CDC_TOPICS)


@dag(
    dag_id="snapshot_oltp",
    schedule="0 15 * * *",  # 01:00 AEST, a quiet hour for the source database
    catchup=False,
    max_active_runs=1,
    tags=["reconciliation"],
)
def snapshot_oltp():
    @task
    def snapshot() -> dict[str, int]:
        """Read current source state directly, so the CDC-derived history has something
        independent to be checked against (SPEC.md §6.1)."""
        with (
            psycopg.connect(APP_DSN, autocommit=False) as app,
            psycopg.connect(WAREHOUSE_DSN, autocommit=False) as warehouse,
        ):
            counts = take_snapshot(app, warehouse)
            prune_snapshots(warehouse, keep=7)
        return counts

    snapshot()


load_cdc()
snapshot_oltp()
