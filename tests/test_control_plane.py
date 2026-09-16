"""The Console's API: what it shows, what it refuses, and what it never lets happen twice."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
from fastapi.testclient import TestClient
from opsctl.chaos import SCENARIOS, close_window, expire_stale_windows, open_window, open_windows

NOW = datetime(2026, 9, 16, 7, 0, tzinfo=UTC)


def mock_client(handler):
    """A drop-in httpx.Client whose transport is faked.

    The real class is captured first: `console.httpx` IS the httpx module, so patching its
    Client attribute patches it everywhere, and a builder that called httpx.Client would
    recurse into itself.
    """
    real_client = httpx.Client

    def build(*_args, **kwargs):
        kwargs.pop("transport", None)
        return real_client(transport=httpx.MockTransport(handler), **kwargs)

    return build


class FakeExecutor:
    """Stands in for the Docker API. Records what a scenario asked the stack to do."""

    def __init__(self, fail: bool = False):
        self.calls: list[tuple[str, ...]] = []
        self.fail = fail

    def _record(self, *args: str) -> str:
        if self.fail:
            raise RuntimeError("docker unavailable")
        self.calls.append(args)
        return ""

    def start(self, service: str) -> str:
        return self._record("start", service)

    def stop(self, service: str) -> str:
        return self._record("stop", service)

    def exec(self, service: str, *command: str) -> str:
        return self._record("exec", service, *command)


@pytest.fixture
def console(conn, pg_uri, monkeypatch):
    from control_plane import app as module

    monkeypatch.setattr(module, "WAREHOUSE_DSN", pg_uri)
    monkeypatch.setattr(module, "AIRFLOW_TOKEN", "test-token")
    monkeypatch.setattr(module, "_last_chaos_at", None)
    module.executor = FakeExecutor()
    return module


@pytest.fixture
def client(console):
    return TestClient(console.app)


# ----------------------------------------------------------------- status is public and honest


def test_the_summary_is_one_request(conn, client):
    """A status page that needs six round trips is a status page that renders in pieces."""
    conn.execute(
        "INSERT INTO ops.dq_results (check_name, target, status, observed) "
        "VALUES ('freshness', 'staging', 'pass', 120)"
    )
    conn.execute(
        "INSERT INTO ops.load_ledger (dag_run_id, topic, partition_id, start_offset, end_offset, "
        "loaded_date, row_count) VALUES ('r1', 'product.session', 0, 0, 10, current_date, 10)"
    )

    body = client.get("/api/status/summary").json()

    assert body["freshness"][0]["layer"] == "staging"
    assert body["freshness"][0]["target_seconds"] == 3600  # the SLO, from the warehouse
    assert body["loads"][0]["topic"] == "product.session"
    assert body["paging_suppressed"] is False


def test_the_page_says_when_paging_is_suppressed(conn, client):
    """Otherwise a reviewer sees an alert firing, no page sent, and no explanation."""
    open_window(conn, "connector_outage")

    body = client.get("/api/status/summary").json()

    assert body["paging_suppressed"] is True
    assert body["chaos_windows"][0]["scenario"] == "connector_outage"


def test_incidents_come_from_prometheus_not_alertmanager(client, console, monkeypatch):
    """Alertmanager inhibits paging during chaos. The status page must still show what fires —
    the difference between "not paging anybody" and "nothing is wrong"."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/alerts"
        return httpx.Response(
            200,
            json={
                "data": {
                    "alerts": [
                        {
                            "labels": {"alertname": "CdcStalled", "severity": "critical"},
                            "annotations": {
                                "summary": "CDC produced nothing for 10m",
                                "runbook": "docs/runbooks/slot-invalidation.md",
                            },
                            "state": "firing",
                            "activeAt": "2026-09-16T06:50:00Z",
                        },
                        {
                            "labels": {"alertname": "ChaosWindowOpen", "severity": "info"},
                            "annotations": {"summary": "chaos in progress"},
                            "state": "firing",
                        },
                    ]
                }
            },
        )

    monkeypatch.setattr(console.httpx, "Client", mock_client(handler))

    body = client.get("/api/status/incidents").json()

    assert body["available"] is True
    # The chaos marker itself is not an incident; everything else is shown.
    assert [a["name"] for a in body["alerts"]] == ["CdcStalled"]
    assert body["alerts"][0]["runbook"] == "docs/runbooks/slot-invalidation.md"


def test_a_monitoring_outage_does_not_take_the_status_page_with_it(client, console, monkeypatch):
    def broken(**kwargs):
        raise httpx.ConnectError("prometheus unreachable")

    monkeypatch.setattr(console.httpx, "Client", broken)

    response = client.get("/api/status/incidents")

    assert response.status_code == 200
    assert response.json()["available"] is False


def test_every_scenario_is_described_before_it_is_run(client):
    """A button that breaks something without saying what to expect is a trap, not a demo."""
    body = client.get("/api/status/scenarios").json()

    assert {s["key"] for s in body["scenarios"]} == set(SCENARIOS)
    for scenario in body["scenarios"]:
        assert scenario["expected_symptoms"]
        assert scenario["recovery"]
        assert scenario["runbook"].startswith("docs/runbooks/")


# ----------------------------------------------------------------- actions refuse the wrong thing


def test_running_the_pipeline_twice_is_rejected_not_queued(client, console, monkeypatch):
    """SPEC.md §11: rejected while a run is active. Airflow's max_active_runs=1 enforces the
    same rule server-side, so this is the polite version of something true either way."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "total_entries": 1,
                "dag_runs": [{"dag_run_id": "manual__2026", "start_date": "2026-09-16T06:02:00Z"}],
            },
        )

    monkeypatch.setattr(console.httpx, "Client", mock_client(handler))

    response = client.post("/api/actions/run-pipeline")

    assert response.status_code == 409
    assert response.json()["detail"]["run_id"] == "manual__2026"


def test_chaos_injection_opens_a_window_and_acts_on_the_real_stack(conn, client, console):
    response = client.post("/api/actions/chaos/connector_outage")

    assert response.status_code == 200
    assert console.executor.calls == [("stop", "connect")]
    assert [w["scenario"] for w in open_windows(conn)] == ["connector_outage"]
    assert response.json()["expected_symptoms"]


def test_two_scenarios_at_once_are_refused(conn, client):
    """Overlapping symptoms cannot be attributed to either scenario, which makes both useless."""
    open_window(conn, "schema_drift")

    response = client.post("/api/actions/chaos/connector_outage")

    assert response.status_code == 409
    assert "already running" in response.json()["detail"]


def test_a_cooldown_stops_a_visitor_hammering_the_button(conn, client, console):
    client.post("/api/actions/chaos/poison_message")
    client.post("/api/actions/chaos/poison_message/recover")

    again = client.post("/api/actions/chaos/poison_message")

    assert again.status_code == 429
    assert "Retry-After" in again.headers


def test_a_failed_injection_does_not_leave_paging_suppressed(conn, client, console):
    """The dangerous failure: a window opens, the injection dies, and alerts stay muted."""
    console.executor = FakeExecutor(fail=True)

    response = client.post("/api/actions/chaos/connector_outage")

    assert response.status_code == 502
    assert open_windows(conn) == []


def test_recovery_closes_the_window(conn, client, console):
    client.post("/api/actions/chaos/schema_drift")
    assert open_windows(conn)

    response = client.post("/api/actions/chaos/schema_drift/recover")

    assert response.status_code == 200
    assert open_windows(conn) == []
    assert ("exec", "simulator", "sh", "-c", "rm -f /tmp/rename_plan_field") in (
        console.executor.calls
    )


def test_an_unknown_scenario_is_a_404_not_a_silent_no_op(client):
    assert client.post("/api/actions/chaos/delete_everything").status_code == 404


def test_reset_requires_a_typed_confirmation(client):
    """It takes minutes and discards whatever the last visitor did, so it should not be one
    click away from the chaos buttons."""
    assert client.post("/api/actions/reset", json={"confirm": ""}).status_code == 400


def test_reset_says_it_runs_on_the_host_rather_than_pretending(client):
    """The golden script needs the host's docker and compose file, which this container does
    not have. A button that reports success while doing nothing is worse than no button."""
    response = client.post("/api/actions/reset", json={"confirm": "reset"})

    assert response.status_code == 501
    assert "golden.sh" in response.json()["detail"]


# ----------------------------------------------------------------- windows expire


def test_an_abandoned_window_expires_so_paging_resumes(conn):
    """Otherwise the safety mechanism becomes the outage: one forgotten scenario suppresses
    every page indefinitely."""
    window_id = open_window(conn, "poison_message")
    conn.execute(
        "UPDATE ops.chaos_windows SET opened_at = %s WHERE id = %s",
        (NOW - timedelta(hours=2), window_id),
    )

    closed = expire_stale_windows(conn, now=NOW)

    assert closed == 1
    assert open_windows(conn) == []


def test_a_window_inside_its_scenarios_limit_stays_open(conn):
    window_id = open_window(conn, "connector_outage")  # 45-minute limit
    conn.execute(
        "UPDATE ops.chaos_windows SET opened_at = %s WHERE id = %s",
        (NOW - timedelta(minutes=20), window_id),
    )

    assert expire_stale_windows(conn, now=NOW) == 0
    assert len(open_windows(conn)) == 1


def test_closing_a_window_twice_is_harmless(conn):
    window_id = open_window(conn, "poison_message")

    close_window(conn, window_id, notes="first")
    close_window(conn, window_id, notes="second")

    notes = conn.execute(
        "SELECT notes FROM ops.chaos_windows WHERE id = %s", (window_id,)
    ).fetchone()[0]
    assert notes == "first"  # the first close wins; the second does not reopen or overwrite


# ----------------------------------------------------------------- scenarios are documented


def test_every_scenario_has_an_incident_write_up():
    """A scenario without a write-up is a button whose point nobody can reconstruct later."""
    from pathlib import Path

    incidents = Path(__file__).resolve().parents[1] / "docs" / "incidents"
    written = " ".join(p.read_text() for p in incidents.glob("*.md"))

    for key in SCENARIOS:
        assert f"`{key}`" in written, f"no incident write-up mentions the {key} scenario"


def test_every_scenario_points_at_a_runbook_that_exists():
    from pathlib import Path

    repo = Path(__file__).resolve().parents[1]

    for scenario in SCENARIOS.values():
        assert (repo / scenario.runbook).exists(), f"{scenario.key}: {scenario.runbook} missing"


# ----------------------------------------------------------------- row factories

@pytest.mark.parametrize("key", sorted(SCENARIOS))
def test_recovery_works_on_a_dict_row_connection(pg_uri, conn, key):
    """The Console connects with dict rows; the tests used tuples, so positional row access in
    a recovery function passed every test and raised KeyError: 0 in production.

    Parametrised over every scenario, because finding this twice in the same file was enough.
    """
    import psycopg
    from psycopg.rows import dict_row

    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS staging.stg_product_events_quarantined (event_id uuid)"
    )

    with psycopg.connect(pg_uri, autocommit=True, row_factory=dict_row) as dict_conn:
        result = SCENARIOS[key].recover(dict_conn, FakeExecutor())

    assert isinstance(result, dict)


@pytest.mark.parametrize("key", sorted(SCENARIOS))
def test_injection_works_on_a_dict_row_connection(pg_uri, conn, key):
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(pg_uri, autocommit=True, row_factory=dict_row) as dict_conn:
        result = SCENARIOS[key].inject(dict_conn, FakeExecutor())

    assert isinstance(result, dict)
