"""The Platform Console's API (SPEC.md §11).

This service exists for three reasons, and only the first is obvious:

1. **Somewhere has to hold the Airflow token.** Not the product app — a SaaS product that knows
   its analytics orchestrator exists is backwards, and a compromised product would then own the
   platform too.
2. **Somewhere has to say no.** Locking, cooldowns and "a run is already in progress" are
   decisions, and they belong on the server, because a disabled button is a suggestion.
3. **Reads and writes have different audiences.** Status is public so a reviewer can watch the
   platform work; actions are behind the demo passcode, enforced by Caddy in front of this
   service (SPEC.md §11).

Incidents are read from Prometheus rather than Alertmanager on purpose: Alertmanager inhibits
paging during a chaos window, and the status page should still show what is firing.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import psycopg
from fastapi import Body, FastAPI, HTTPException
from opsctl.chaos import SCENARIOS, close_window, expire_stale_windows, open_window, open_windows
from psycopg.rows import dict_row

log = logging.getLogger(__name__)

WAREHOUSE_DSN = os.environ.get(
    "WAREHOUSE_DSN", "postgresql://warehouse:warehouse@postgres-warehouse:5432/warehouse"
)
AIRFLOW_URL = os.environ.get("AIRFLOW_URL", "http://airflow:8080")
AIRFLOW_TOKEN = os.environ.get("AIRFLOW_API_TOKEN", "")
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090")
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT", "analytics-infra")

# One chaos injection at a time, globally. Two scenarios at once produce symptoms nobody can
# attribute, which is worse than no demo.
CHAOS_COOLDOWN = timedelta(minutes=2)

app = FastAPI(title="Platform Console API")

_last_chaos_at: datetime | None = None


def warehouse() -> psycopg.Connection:
    return psycopg.connect(WAREHOUSE_DSN, autocommit=True, row_factory=dict_row)


class ComposeExecutor:
    """Runs docker compose on the host. Injected into scenarios so they stay testable."""

    def __init__(self, project: str = COMPOSE_PROJECT):
        self.project = project

    def run(self, *args: str) -> str:
        import subprocess

        result = subprocess.run(
            ["docker", "-p", self.project, *args] if args[0] != "compose" else ["docker", *args],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr[-500:])
        return result.stdout


executor: Any = ComposeExecutor()


# ----------------------------------------------------------------------- status (public)


@app.get("/api/status/summary")
def summary() -> dict[str, Any]:
    """Everything the status page needs in one request, so the page is one round trip."""
    with warehouse() as conn:
        expire_stale_windows(conn)

        freshness = conn.execute(
            """
            SELECT r.target AS layer, r.observed AS age_seconds, s.target_seconds, r.status,
                   r.checked_at
            FROM ops.dq_results r
            JOIN ops.freshness_slo s ON s.layer = r.target
            JOIN (
                SELECT target, max(checked_at) AS latest FROM ops.dq_results
                WHERE check_name = 'freshness' GROUP BY target
            ) latest ON latest.target = r.target AND latest.latest = r.checked_at
            WHERE r.check_name = 'freshness'
            ORDER BY r.target
            """
        ).fetchall()

        loads = conn.execute(
            """
            SELECT topic,
                   max(created_at) AS last_load_at,
                   sum(row_count) FILTER (WHERE created_at > now() - interval '1 hour')
                       AS rows_last_hour,
                   sum(dlq_count) FILTER (WHERE created_at > now() - interval '1 day')
                       AS dlq_last_day
            FROM ops.load_ledger GROUP BY topic ORDER BY topic
            """
        ).fetchall()

        checks = conn.execute(
            """
            SELECT r.check_name, r.target, r.status, r.observed, r.threshold, r.checked_at
            FROM ops.dq_results r
            JOIN (
                SELECT check_name, target, max(checked_at) AS latest
                FROM ops.dq_results WHERE check_name <> 'freshness' GROUP BY check_name, target
            ) latest ON latest.check_name = r.check_name AND latest.target = r.target
                    AND latest.latest = r.checked_at
            ORDER BY r.status DESC, r.check_name
            """
        ).fetchall()

        drift = conn.execute(
            "SELECT event_type, json_path, change, blocking, detected_at FROM ops.drift_findings "
            "WHERE resolved_at IS NULL ORDER BY blocking DESC, detected_at DESC LIMIT 20"
        ).fetchall()

        gaps = conn.execute(
            "SELECT reason, detected_at, resnapshot_requested_at FROM ops.cdc_gaps "
            "WHERE resolved_at IS NULL"
        ).fetchall()

        windows = open_windows(conn)

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "freshness": freshness,
        "loads": loads,
        "checks": checks,
        "drift": drift,
        "cdc_gaps": gaps,
        "chaos_windows": windows,
        # Named explicitly so the page can say "paging suppressed" rather than leaving a
        # reviewer to wonder why an alert did not reach anyone.
        "paging_suppressed": bool(windows),
    }


@app.get("/api/status/incidents")
def incidents() -> dict[str, Any]:
    """Firing alerts, read from Prometheus rather than Alertmanager.

    Alertmanager inhibits paging while a chaos window is open. The status page should still
    show what is firing — that is the difference between "not paging anybody" and "nothing is
    wrong" (SPEC.md §10.2).
    """
    try:
        with httpx.Client(base_url=PROMETHEUS_URL, timeout=5.0) as client:
            response = client.get("/api/v1/alerts")
            response.raise_for_status()
            alerts = response.json()["data"]["alerts"]
    except (httpx.HTTPError, KeyError) as exc:
        # A monitoring outage must not take the status page with it.
        log.warning("could not read alerts from Prometheus: %s", exc)
        return {"available": False, "alerts": [], "error": str(exc)[:200]}

    return {
        "available": True,
        "alerts": [
            {
                "name": a["labels"].get("alertname"),
                "severity": a["labels"].get("severity"),
                "summary": a["annotations"].get("summary"),
                "runbook": a["annotations"].get("runbook"),
                "state": a["state"],
                "since": a.get("activeAt"),
            }
            for a in alerts
            if a["labels"].get("alertname") != "ChaosWindowOpen"
        ],
    }


@app.get("/api/status/scenarios")
def scenarios() -> dict[str, Any]:
    """What each scenario breaks, what it should look like, and how it recovers."""
    return {
        "scenarios": [
            {
                "key": s.key,
                "title": s.title,
                "what_breaks": s.what_breaks,
                "expected_symptoms": s.expected_symptoms,
                "recovery": s.recovery,
                "runbook": s.runbook,
                "max_duration_minutes": int(s.max_duration.total_seconds() // 60),
            }
            for s in SCENARIOS.values()
        ]
    }


# ----------------------------------------------------------------------- actions (passcode)


def _airflow(method: str, path: str, **kwargs) -> httpx.Response:
    if not AIRFLOW_TOKEN:
        raise HTTPException(status_code=503, detail="control plane has no Airflow token")
    with httpx.Client(base_url=AIRFLOW_URL, timeout=20.0) as client:
        return client.request(
            method, path, headers={"Authorization": f"Bearer {AIRFLOW_TOKEN}"}, **kwargs
        )


@app.post("/api/actions/run-pipeline")
def run_pipeline() -> dict[str, Any]:
    """Trigger the transform DAG, unless it is already running.

    Rejected rather than queued while a run is active (SPEC.md §11). Airflow's
    `max_active_runs=1` enforces the same thing server-side, so this is the polite version of a
    rule that holds either way.
    """
    running = _airflow("GET", "/api/v2/dags/transform/dagRuns", params={"state": "running"})
    if running.status_code == 200 and running.json().get("total_entries", 0) > 0:
        active = running.json()["dag_runs"][0]
        raise HTTPException(
            status_code=409,
            detail={
                "error": "a run is already in progress",
                "run_id": active.get("dag_run_id"),
                "started_at": active.get("start_date"),
            },
        )

    triggered = _airflow(
        "POST",
        "/api/v2/dags/transform/dagRuns",
        json={"logical_date": None, "conf": {"triggered_by": "console"}},
    )
    if triggered.status_code >= 400:
        raise HTTPException(status_code=502, detail=triggered.text[:300])
    return {"triggered": triggered.json().get("dag_run_id")}


@app.post("/api/actions/chaos/{key}")
def inject_chaos(key: str) -> dict[str, Any]:
    """Run a chaos scenario, opening a window so paging stays suppressed while it runs."""
    global _last_chaos_at

    scenario = SCENARIOS.get(key)
    if scenario is None:
        raise HTTPException(status_code=404, detail=f"unknown scenario: {key}")

    now = datetime.now(UTC)
    if _last_chaos_at and now - _last_chaos_at < CHAOS_COOLDOWN:
        wait = CHAOS_COOLDOWN - (now - _last_chaos_at)
        raise HTTPException(
            status_code=429,
            detail=f"cooling down; try again in {int(wait.total_seconds())}s",
            headers={"Retry-After": str(int(wait.total_seconds()))},
        )

    with warehouse() as conn:
        if open_windows(conn):
            # Two scenarios at once produce symptoms nobody can attribute to either.
            raise HTTPException(status_code=409, detail="another scenario is already running")

        window_id = open_window(conn, scenario.key)
        try:
            result = scenario.inject(conn, executor)
        except Exception as exc:  # noqa: BLE001 - a failed injection must not leave a window open
            close_window(conn, window_id, notes=f"injection failed: {exc}")
            raise HTTPException(status_code=502, detail=f"injection failed: {exc}") from exc

    _last_chaos_at = now
    return {
        "scenario": scenario.key,
        "window_id": window_id,
        "expected_symptoms": scenario.expected_symptoms,
        "runbook": scenario.runbook,
        "result": result,
    }


@app.post("/api/actions/chaos/{key}/recover")
def recover_chaos(key: str) -> dict[str, Any]:
    scenario = SCENARIOS.get(key)
    if scenario is None:
        raise HTTPException(status_code=404, detail=f"unknown scenario: {key}")

    with warehouse() as conn:
        result = scenario.recover(conn, executor)
        for window in open_windows(conn):
            if window["scenario"] == key:
                close_window(conn, window["id"], notes="recovered from the console")

    return {"scenario": scenario.key, "recovered": True, "result": result}


@app.post("/api/actions/reset")
def reset(confirm: str = Body(embed=True, default="")) -> dict[str, Any]:
    """Restore the golden snapshot and catch up the gap (SPEC.md §9.3).

    Deliberately awkward: it takes minutes, it discards whatever the last visitor did, and it
    asks for a typed confirmation rather than being one click away from the chaos buttons.
    """
    if confirm != "reset":
        raise HTTPException(status_code=400, detail='send {"confirm": "reset"} to proceed')

    try:
        executor.run("compose", "exec", "-T", "control-plane", "/app/golden.sh", "restore")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"reset failed: {exc}") from exc

    return {"reset": "started", "note": "restore plus catch-up takes a few minutes"}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
