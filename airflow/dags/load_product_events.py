"""Micro-batch load of product-event topics into raw (SPEC.md §4.4).

Why a scheduled micro-batch rather than a long-running consumer task: a task that never
returns holds a worker slot forever, and a restart loses whatever it was holding in memory.
This DAG claims a bounded offset range, loads it transactionally, and exits.

`max_active_runs=1` matters for correctness as well as load: two concurrent runs would both
read the same watermark and claim overlapping ranges.
"""

from __future__ import annotations

import os
from datetime import timedelta

import psycopg
from airflow.sdk import dag, task
from loader.kafka_source import KafkaMessageSource
from loader.run import run_load

TOPICS = [
    "product.session",
    "product.feature_usage",
    "product.lifecycle",
    "product.billing_ui",
]

WAREHOUSE_DSN = os.environ["WAREHOUSE_DSN"]
BOOTSTRAP = os.environ.get("REDPANDA_BOOTSTRAP", "redpanda:9092")
MAX_RECORDS_PER_RUN = int(os.environ.get("LOADER_MAX_RECORDS", 50_000))


@dag(
    dag_id="load_product_events",
    schedule="*/5 * * * *",
    catchup=False,  # ranges come from the ledger, not from logical dates (SPEC.md §4.4)
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["ingestion", "streaming"],
)
def load_product_events():
    @task
    def load_topic(topic: str, **context) -> dict:
        run_id = context["run_id"]
        source = KafkaMessageSource(BOOTSTRAP)
        try:
            with psycopg.connect(WAREHOUSE_DSN, autocommit=False) as conn:
                result = run_load(
                    conn,
                    source,
                    topic=topic,
                    dag_run_id=run_id,
                    max_records=MAX_RECORDS_PER_RUN,
                )
        finally:
            source.close()

        # Clearing this task re-reads the same offset range and replaces its rows, so a
        # retry after a parser fix is safe and does not duplicate.
        return {
            "topic": topic,
            "rows_loaded": result.rows_loaded,
            "dlq_rows": result.dlq_rows,
            "erased_skipped": result.erased_skipped,
            "partitions": [
                {
                    "partition": p.partition_id,
                    "start": p.start_offset,
                    "end": p.end_offset,
                    "rows": p.rows_loaded,
                    "replaced": p.replaced,
                }
                for p in result.partitions
                if not p.skipped
            ],
        }

    load_topic.expand(topic=TOPICS)


load_product_events()
