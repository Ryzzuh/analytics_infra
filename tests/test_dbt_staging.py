"""dbt staging model, run for real against the embedded Postgres.

This is the local stand-in for CI's `dbt build` on an ephemeral database (SPEC.md §13): it
proves the model compiles and that dedup, lateness flags and payload extraction behave on
data the loader actually produced, rather than on hand-written fixtures.
"""

from __future__ import annotations

import os
import re
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from conftest import TOPIC
from loader.run import load_partition
from loader.testing import event_bytes

REPO = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.dbt


def _socket_port(socket_dir: Path) -> int:
    """libpq derives the socket filename from the port, so read the port back off the socket."""
    ports = [
        int(m.group(1))
        for p in socket_dir.glob(".s.PGSQL.*")
        if (m := re.fullmatch(r"\.s\.PGSQL\.(\d+)", p.name))  # skip the .lock file
    ]
    if not ports:
        raise RuntimeError(f"no postgres socket in {socket_dir}")
    return ports[0]


@pytest.fixture
def dbt_env(pg_uri: str, tmp_path: Path) -> dict[str, str]:
    socket_dir = Path(pg_uri.split("host=")[1])
    return {
        **os.environ,
        "WAREHOUSE_HOST": str(socket_dir),
        "WAREHOUSE_PORT": str(_socket_port(socket_dir)),
        "WAREHOUSE_USER": "postgres",
        "WAREHOUSE_PASSWORD": "",
        "WAREHOUSE_DB": "postgres",
        "DBT_LOG_PATH": str(tmp_path / "logs"),
        "DBT_TARGET_PATH": str(tmp_path / "target"),
    }


def dbt(*args: str, env: dict[str, str]) -> subprocess.CompletedProcess:
    result = subprocess.run(
        ["dbt", *args, "--project-dir", str(REPO / "dbt"), "--profiles-dir", str(REPO / "dbt")],
        capture_output=True,
        text=True,
        env=env,
        cwd=REPO,
    )
    if result.returncode != 0:
        pytest.fail(
            f"dbt {' '.join(args)} failed:\n{result.stdout[-4000:]}\n{result.stderr[-2000:]}"
        )
    return result


def test_staging_dedups_and_flags_late_events(conn, source, dbt_env):
    now = datetime.now(UTC)
    duplicated_id = uuid4()

    # Two clean events, one sent three times (client retries), one very late arrival.
    clean_payload = {"feature": "export", "surface": "web", "plan": "team", "duration_ms": 120}
    source.produce(TOPIC, 0, event_bytes(account_id=1, payload=clean_payload))
    for _ in range(3):
        source.produce(TOPIC, 0, event_bytes(event_id=duplicated_id, account_id=2))
    source.produce(
        TOPIC,
        0,
        event_bytes(account_id=3, event_time=now - timedelta(days=9), received_at=now),
    )

    load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1")
    conn.commit()

    dbt("build", "--select", "stg_product_events", env=dbt_env)

    rows = conn.execute(
        "SELECT event_id, duplicate_copies, is_late_beyond_tolerance, feature "
        "FROM staging.stg_product_events ORDER BY account_id"
    ).fetchall()

    assert len(rows) == 3  # 5 raw rows, 3 distinct events
    by_id = {r[0]: r for r in rows}
    assert by_id[duplicated_id][1] == 2  # two extra copies recorded, one row kept
    assert [r[2] for r in rows] == [False, False, True]  # the 9-day-old event is flagged
    assert rows[0][3] == "export"  # payload extraction


def test_staging_is_incremental_and_idempotent(conn, source, dbt_env):
    for _ in range(4):
        source.produce(TOPIC, 0, event_bytes(account_id=1))
    load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1")
    conn.commit()

    dbt("build", "--select", "stg_product_events", env=dbt_env)
    dbt("build", "--select", "stg_product_events", env=dbt_env)  # second run must be a no-op

    count = conn.execute("SELECT count(*) FROM staging.stg_product_events").fetchone()[0]
    assert count == 4
