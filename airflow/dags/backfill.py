"""Load the backfill topics (SPEC.md §6.2).

A separate DAG from the live loader, not a parameter on it, because the two differ in the one
way that matters: rows land with `source_path = 'backfill'`, which exempts them from the
lateness cutoff. Every replayed event is months stale by load lag, and on the live path that
is precisely what the cutoff exists to catch.

Triggered, not scheduled: history is replayed when someone seeds or resets the platform, and a
schedule would be a standing invitation to reload it by accident.
"""

from __future__ import annotations

import os
from datetime import timedelta

import psycopg
from airflow.sdk import dag, task
from loader.kafka_source import KafkaMessageSource
from loader.run import run_load
from loader.targets import PRODUCT_EVENTS

BACKFILL_TOPICS = [
    "product.session.backfill",
    "product.feature_usage.backfill",
    "product.lifecycle.backfill",
    "product.billing_ui.backfill",
]

WAREHOUSE_DSN = os.environ["WAREHOUSE_DSN"]
BOOTSTRAP = os.environ.get("REDPANDA_BOOTSTRAP", "redpanda:9092")
# Bigger than the live loader's: backfill is throughput-bound and nothing is waiting on
# freshness, but still bounded so one run cannot hold a transaction open for an hour.
MAX_RECORDS = int(os.environ.get("BACKFILL_MAX_RECORDS", 200_000))


@dag(
    dag_id="load_backfill",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=1)},
    tags=["ingestion", "backfill"],
)
def load_backfill():
    @task
    def drain_topic(topic: str, **context) -> dict:
        """Read the topic to its end, in bounded batches.

        Each batch is its own ledger entry, so a failure part-way through a 40M-event history
        costs one batch rather than the whole replay.
        """
        source = KafkaMessageSource(BOOTSTRAP, group_id="warehouse-loader-backfill")
        loaded = 0
        batches = 0
        try:
            with psycopg.connect(WAREHOUSE_DSN, autocommit=False) as conn:
                while True:
                    result = run_load(
                        conn,
                        source,
                        topic=topic,
                        dag_run_id=f"{context['run_id']}-batch{batches}",
                        target=PRODUCT_EVENTS,
                        max_records=MAX_RECORDS,
                        source_path="backfill",
                    )
                    if all(p.skipped for p in result.partitions):
                        break
                    loaded += result.rows_loaded
                    batches += 1
        finally:
            source.close()
        return {"topic": topic, "rows_loaded": loaded, "batches": batches}

    drain_topic.expand(topic=BACKFILL_TOPICS)


load_backfill()
