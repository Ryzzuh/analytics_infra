"""Schema-drift detection (SPEC.md §7).

Hourly. Not per-load, because a single batch is not evidence — a field absent for one micro-batch
is normal traffic, and a detector that fires on it earns an exception list within a week.

Runs against the dbt manifest baked into the image, which is where `meta.required_fields` lives:
a field's importance is declared next to the SQL that reads it, so the two cannot disagree.
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import psycopg
from airflow.sdk import dag, task
from drift import detect_drift

WAREHOUSE_DSN = os.environ["WAREHOUSE_DSN"]
MANIFEST = Path(os.environ.get("DBT_MANIFEST_PATH", "/opt/dbt/target/manifest.json"))


@dag(
    dag_id="drift_detection",
    schedule="15 * * * *",  # offset from the DQ checks so they do not contend for the warehouse
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["observability"],
)
def drift_detection():
    @task
    def detect() -> dict:
        with psycopg.connect(WAREHOUSE_DSN, autocommit=True) as conn:
            findings = detect_drift(
                conn,
                manifest_path=MANIFEST if MANIFEST.exists() else None,
                window=timedelta(hours=1),
            )
        return {
            "findings": len(findings),
            # Blocking findings are the ones that fail a model and page someone.
            "blocking": [f"{f.event_type}.{f.json_path}" for f in findings if f.blocking],
            "informational": [f"{f.event_type}.{f.json_path}" for f in findings if not f.blocking],
        }

    detect()


drift_detection()
