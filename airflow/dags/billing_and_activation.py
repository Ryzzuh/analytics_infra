"""Billing ingestion and activation (SPEC.md §4.3, §8).

Three DAGs on deliberately different cadences:

* `load_billing_webhooks` every 5 minutes — the fast path, same micro-batch loader as every
  other stream.
* `pull_billing_batch` daily — the slow, complete path, with an overlap window. This is what
  catches whatever the webhooks lost.
* `reverse_etl_sync` daily, after the transform DAG — pushes churn scores back to the product.
"""

from __future__ import annotations

import os
from datetime import timedelta

import httpx
import psycopg
from airflow.sdk import dag, task
from loader.billing import pull_billing_events
from loader.kafka_source import KafkaMessageSource
from loader.run import run_load
from loader.targets import billing_target
from reverse_etl import sync_account_health

WAREHOUSE_DSN = os.environ["WAREHOUSE_DSN"]
BOOTSTRAP = os.environ.get("REDPANDA_BOOTSTRAP", "redpanda:9092")
BILLING_API = os.environ.get("BILLING_API_URL", "http://billing-mock:8001")
APP_API = os.environ.get("APP_API_URL", "http://app-api:8003")
BILLING_TOPIC = "billing.webhooks"


@dag(
    dag_id="load_billing_webhooks",
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["ingestion", "billing"],
)
def load_billing_webhooks():
    @task
    def load(**context) -> dict:
        source = KafkaMessageSource(BOOTSTRAP, group_id="warehouse-loader-billing")
        try:
            with psycopg.connect(WAREHOUSE_DSN, autocommit=False) as conn:
                result = run_load(
                    conn,
                    source,
                    topic=BILLING_TOPIC,
                    dag_run_id=context["run_id"],
                    target=billing_target(),
                )
        finally:
            source.close()
        return {"rows_loaded": result.rows_loaded, "dlq_rows": result.dlq_rows}

    load()


@dag(
    dag_id="pull_billing_batch",
    # 01:30 AEST: after the OLTP snapshot (01:00) and BEFORE the dbt build (02:00). Ordering
    # matters — reconciliation compares webhooks against the pull, so a build that ran first
    # would report yesterday's miss rate as today's.
    schedule="30 15 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["ingestion", "billing", "reconciliation"],
)
def pull_billing_batch():
    @task
    def pull() -> dict:
        """Pull the provider's own event list, overlapping the previous cursor.

        The overlap is not defensive padding: the provider is eventually consistent, so an
        event created just before the cursor can become visible just after it, and without
        the overlap neither path would ever see it.
        """
        with (
            httpx.Client(base_url=BILLING_API, timeout=30.0) as client,
            psycopg.connect(WAREHOUSE_DSN, autocommit=False) as conn,
        ):
            result = pull_billing_events(conn, client)
        return {
            "fetched": result.fetched,
            "inserted": result.inserted,
            "already_held": result.duplicates_skipped,
            "pages": result.pages,
            "rate_limited": result.rate_limited,
        }

    pull()


@dag(
    dag_id="reverse_etl_sync",
    schedule="0 18 * * *",  # 04:00 AEST, after the daily dbt build
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=15)},
    tags=["activation"],
)
def reverse_etl_sync():
    @task
    def sync() -> dict:
        """Send changed churn scores back to the product.

        Fails the task above the error rate, not on the first bad account: one 5xx out of two
        thousand is not a failed sync, and treating it as one trains people to ignore the alert.
        """
        with (
            httpx.Client(base_url=APP_API, timeout=15.0) as client,
            psycopg.connect(WAREHOUSE_DSN, autocommit=True) as conn,
        ):
            result = sync_account_health(conn, client, pause_between_chunks=1.0)
        return {
            "considered": result.considered,
            "unchanged": result.unchanged,
            "sent": result.sent,
            "succeeded": result.succeeded,
            "client_errors": result.client_errors,
            "server_errors": result.server_errors,
        }

    sync()


load_billing_webhooks()
pull_billing_batch()
reverse_etl_sync()
