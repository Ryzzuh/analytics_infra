"""Data-quality and freshness checks (SPEC.md §10.3).

Hourly, not daily: freshness is the point. A daily check cannot tell you that ingestion stopped
three hours ago, which is exactly the thing worth knowing.

The task deliberately does NOT fail when checks fail. A failing check is data about the
platform, and it is already exported as a metric and alerted on; failing the task as well would
turn every data anomaly into a DAG failure alert too, which is how two alerts become noise.
"""

from __future__ import annotations

import os
from datetime import timedelta

import psycopg
from airflow.sdk import dag, task
from dq import run_checks

WAREHOUSE_DSN = os.environ["WAREHOUSE_DSN"]


@dag(
    dag_id="data_quality",
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["observability"],
)
def data_quality():
    @task
    def check() -> dict:
        with psycopg.connect(WAREHOUSE_DSN, autocommit=True) as conn:
            results = run_checks(conn)
        return {
            "checks": len(results),
            "failing": [f"{r.check_name}:{r.target}" for r in results if r.status == "fail"],
            "warning": [f"{r.check_name}:{r.target}" for r in results if r.status == "warn"],
        }

    check()


data_quality()
