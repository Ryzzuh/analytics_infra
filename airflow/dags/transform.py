"""dbt build, one Airflow task per model and test (SPEC.md §6).

Cosmos renders the dbt graph into Airflow tasks, which buys three things a single
`dbt build` task cannot: a failed test names the model it belongs to, a retry reruns only
that model, and Airflow's own graph shows where in the DAG the warehouse actually broke.

Rendered from `manifest.json`, not from `dbt ls`. The DAG file is parsed by the scheduler
every 30 seconds by default, and shelling out to dbt on each parse is the classic way to make
an Airflow scheduler miserable. The manifest is produced once at image build time.

Schedule: daily. The loaders run every 2-5 minutes regardless, so ingestion never waits on
this, and marts are allowed to be up to a day behind (SPEC.md §10.1). The Console's
"Run pipeline" button triggers this DAG out of band, which is the other half of that
trade-off: hourly-fresh numbers on demand, without a build every hour.
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

from cosmos import (
    DbtDag,
    ExecutionConfig,
    LoadMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)

DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/dbt"))
DBT_EXECUTABLE = os.environ.get("DBT_EXECUTABLE_PATH", "/home/airflow/.local/bin/dbt")
MANIFEST_PATH = Path(os.environ.get("DBT_MANIFEST_PATH", DBT_PROJECT_DIR / "target/manifest.json"))

transform = DbtDag(
    dag_id="transform",
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_DIR,
        manifest_path=MANIFEST_PATH,
    ),
    profile_config=ProfileConfig(
        profile_name="analytics_infra",
        target_name="local",
        profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
    ),
    execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
    render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST),
    # 02:00 AEST: after the OLTP snapshot (01:00) and the billing pull (01:30).
    schedule="0 16 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["transform", "dbt"],
)
