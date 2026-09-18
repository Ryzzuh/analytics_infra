"""The container health exporter, against a fake Docker API.

The interesting part is not reading Docker — it is deciding what counts as a problem. An exited
init job is success; an exited collector is an outage; and the restart policy cannot tell them
apart, because most services here declare none.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from container_exporter.app import (
    HEALTH_CODES,
    ONE_SHOT_LABEL,
    ContainerCollector,
    parse_docker_time,
    resource_usage,
)
from prometheus_client import CollectorRegistry, generate_latest

REPO = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 9, 17, 4, 0, tzinfo=UTC)


def container(
    service: str,
    *,
    status: str = "running",
    health: str | None = None,
    restarts: int = 0,
    oom: bool = False,
    exit_code: int = 0,
    one_shot: bool = False,
    started: str = "2026-09-17T03:00:00.123456789Z",
    env: list[str] | None = None,
) -> dict:
    labels = {
        "com.docker.compose.project": "analytics-infra",
        "com.docker.compose.service": service,
    }
    if one_shot:
        labels[ONE_SHOT_LABEL] = "true"
    state = {
        "Status": status,
        "Running": status == "running",
        "OOMKilled": oom,
        "ExitCode": exit_code,
        "StartedAt": started,
    }
    if health:
        state["Health"] = {"Status": health}
    return {
        "Name": f"/analytics-infra-{service}-1",
        "State": state,
        "RestartCount": restarts,
        "Config": {"Labels": labels, "Env": env or []},
        # Deliberately "no" for every service, as in the real compose file: the restart policy
        # must not be what decides whether an exit is expected.
        "HostConfig": {"RestartPolicy": {"Name": "no"}},
    }


class FakeContainer:
    def __init__(self, attrs: dict, stats: dict | None = None) -> None:
        self.attrs = attrs
        self.name = attrs["Name"].lstrip("/")
        self._stats = stats

    def stats(self, stream: bool = False) -> dict:
        assert stream is False
        return self._stats or {}


class FakeDocker:
    def __init__(self, items: list[FakeContainer]) -> None:
        self.items = items
        self.list_kwargs: dict = {}

        outer = self

        class Containers:
            def list(self, **kwargs):
                outer.list_kwargs = kwargs
                return list(outer.items)

        self.containers = Containers()

    def info(self) -> dict:
        return {"MemTotal": 8_268_267_520, "NCPU": 4}


def collect(collector: ContainerCollector) -> dict[tuple[str, frozenset], float]:
    samples = {}
    for family in collector.collect():
        for sample in family.samples:
            samples[(sample.name, frozenset(sample.labels.items()))] = sample.value
    return samples


def problems(samples: dict) -> set[tuple[str, str]]:
    out = set()
    for (name, labels), value in samples.items():
        if name == "container_problem" and value == 1:
            d = dict(labels)
            out.add((d["service"], d["reason"]))
    return out


def collector_for(*attrs: dict) -> ContainerCollector:
    client = FakeDocker([FakeContainer(a) for a in attrs])
    return ContainerCollector(lambda: client, now=lambda: NOW)


# --------------------------------------------------------------------------- what is a problem


def test_a_healthy_running_service_is_not_a_problem():
    samples = collect(collector_for(container("collector", health="healthy")))
    assert problems(samples) == set()


def test_unhealthy_is_a_problem_and_codes_as_the_worst_state():
    """The dead-scheduler case: running, so `Up`, but failing its healthcheck."""
    samples = collect(collector_for(container("airflow-scheduler", health="unhealthy")))
    assert ("airflow-scheduler", "unhealthy") in problems(samples)
    assert HEALTH_CODES["unhealthy"] == max(HEALTH_CODES.values()), (
        "the dashboard summarises with max by (service); unhealthy must sort highest"
    )


def test_an_init_job_that_finished_cleanly_is_not_a_problem():
    samples = collect(
        collector_for(container("warehouse-init", status="exited", exit_code=0, one_shot=True))
    )
    assert problems(samples) == set()
    completed = samples[
        (
            "container_completed",
            frozenset(
                {
                    "service": "warehouse-init",
                    "container": "analytics-infra-warehouse-init-1",
                }.items()
            ),
        )
    ]
    assert completed == 1


def test_an_init_job_that_failed_is_a_problem():
    """warehouse-init exited 2 on the first real run, and the warehouse DDL was never applied."""
    samples = collect(
        collector_for(container("warehouse-init", status="exited", exit_code=2, one_shot=True))
    )
    assert ("warehouse-init", "exited") in problems(samples)


def test_a_long_running_service_that_exited_cleanly_is_still_a_problem():
    """The case the one-shot label exists for.

    A collector that exits 0 has stopped serving, however politely. Deciding by restart policy
    would miss it: this service declares restart "no", exactly like the init jobs do.
    """
    samples = collect(collector_for(container("collector", status="exited", exit_code=0)))
    assert ("collector", "exited") in problems(samples)


def test_restart_loops_and_oom_kills_are_problems():
    samples = collect(
        collector_for(
            container("simulator", status="restarting", restarts=7),
            container("connect", oom=True, health="healthy"),
        )
    )
    assert ("simulator", "restart_loop") in problems(samples)
    assert ("connect", "oom_killed") in problems(samples)


def test_a_container_that_never_started_is_a_problem():
    """What a failed dependency looks like: created, waiting on something that never came up."""
    samples = collect(
        collector_for(container("control-plane", status="created", started="0001-01-01T00:00:00Z"))
    )
    assert ("control-plane", "never_started") in problems(samples)


# --------------------------------------------------------------------------- series lifecycle


def test_a_removed_container_disappears_rather_than_freezing():
    """Module-level Gauges would keep reporting a deleted container at its last value."""
    client = FakeDocker(
        [FakeContainer(container("simulator")), FakeContainer(container("collector"))]
    )
    collector = ContainerCollector(lambda: client, now=lambda: NOW)
    assert any(dict(labels).get("service") == "simulator" for _, labels in collect(collector))

    client.items = [FakeContainer(container("collector"))]
    after = collect(collector)
    assert not any(dict(labels).get("service") == "simulator" for _, labels in after)


def test_only_this_projects_containers_are_listed():
    """The desktop also ran a native daemon with fourteen-month-old containers from elsewhere."""
    client = FakeDocker([])
    collect(ContainerCollector(lambda: client, now=lambda: NOW))
    assert client.list_kwargs["filters"] == {"label": "com.docker.compose.project=analytics-infra"}
    assert client.list_kwargs["all"] is True, "exited containers are the ones worth seeing"
    assert client.list_kwargs["ignore_removed"] is True, "a recreate must not fail the scrape"


# --------------------------------------------------------------------------- Docker going away


def test_docker_being_unreachable_is_reported_not_raised():
    """When Docker Desktop quit, the stack vanished. The exporter must say so, not crash."""

    def no_docker():
        raise ConnectionError("socket gone")

    samples = collect(ContainerCollector(no_docker, now=lambda: NOW))
    assert samples == {("container_exporter_docker_up", frozenset()): 0}


def test_the_exporter_reconnects_when_docker_comes_back():
    attempts = {"n": 0}
    healthy = FakeDocker([FakeContainer(container("collector"))])

    def flaky():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise ConnectionError("Docker Desktop is restarting")
        return healthy

    collector = ContainerCollector(flaky, now=lambda: NOW)
    assert collect(collector)[("container_exporter_docker_up", frozenset())] == 0
    assert collect(collector)[("container_exporter_docker_up", frozenset())] == 1


# --------------------------------------------------------------------------- secrets


def test_nothing_from_the_environment_is_ever_exported():
    """`docker inspect` returns each container's environment, which here holds real secrets."""
    secrets = [
        "POSTGRES_PASSWORD=hunter2-not-for-prometheus",
        "AIRFLOW__API_AUTH__JWT_SECRET=jwt-secret-not-for-prometheus",
        "AIRFLOW_API_TOKEN=token-not-for-prometheus",
    ]
    collector = collector_for(container("airflow-apiserver", env=secrets))
    registry = CollectorRegistry()
    registry.register(collector)

    exposition = generate_latest(registry).decode()
    for secret in secrets:
        assert secret.split("=", 1)[1] not in exposition


# --------------------------------------------------------------------------- the arithmetic


def test_docker_timestamps_with_nanoseconds_parse():
    parsed = parse_docker_time("2026-09-17T03:00:00.123456789Z")
    assert parsed == datetime(2026, 9, 17, 3, 0, 0, 123456, tzinfo=UTC)
    assert parse_docker_time("0001-01-01T00:00:00Z") is None, "the zero time means never started"
    assert parse_docker_time(None) is None


def test_uptime_is_measured_from_the_start():
    samples = collect(collector_for(container("collector", started="2026-09-17T03:00:00Z")))
    uptime = samples[
        (
            "container_uptime_seconds",
            frozenset({"service": "collector", "container": "analytics-infra-collector-1"}.items()),
        )
    ]
    assert uptime == pytest.approx(3600)


def test_memory_excludes_reclaimable_page_cache():
    """Counting cache makes every container look near its limit; `docker stats` excludes it."""
    used, limit, cores = resource_usage(
        {
            "memory_stats": {"usage": 500, "limit": 1000, "stats": {"inactive_file": 200}},
            "cpu_stats": {
                "cpu_usage": {"total_usage": 300},
                "system_cpu_usage": 2000,
                "online_cpus": 4,
            },
            "precpu_stats": {"cpu_usage": {"total_usage": 100}, "system_cpu_usage": 1000},
        }
    )
    assert used == 300
    assert limit == 1000
    assert cores == pytest.approx(0.8)  # 200/1000 of the machine, times 4 cores
    assert resource_usage({"memory_stats": {}}) is None, "a stopped container has no usage"


def test_usage_is_only_reported_for_containers_that_still_exist():
    stats = {
        "memory_stats": {"usage": 100, "limit": 1000, "stats": {}},
        "cpu_stats": {"cpu_usage": {"total_usage": 0}, "system_cpu_usage": 0},
        "precpu_stats": {},
    }
    client = FakeDocker([FakeContainer(container("simulator"), stats)])
    collector = ContainerCollector(lambda: client, now=lambda: NOW)
    collector.refresh_usage()
    assert any(name == "container_memory_bytes" for name, _ in collect(collector))

    client.items = []  # removed between the usage sample and the scrape
    assert not any(name == "container_memory_bytes" for name, _ in collect(collector))


# --------------------------------------------------------------------------- the wiring


def exported_names() -> set[str]:
    return {family.name for family in collector_for(container("collector")).collect()}


def test_every_metric_the_dashboard_queries_is_exported():
    """A dashboard panel querying a misspelt metric shows 'No data', which reads as 'fine'."""
    dashboard = json.loads(
        (REPO / "infra/monitoring/grafana/dashboards/container-health.json").read_text()
    )
    queried = set()
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            queried.update(re.findall(r"\b((?:container|docker)_[a-z_]+)\b", target["expr"]))

    assert queried, "the dashboard should query the exporter"
    missing = queried - exported_names()
    assert not missing, f"the dashboard queries metrics nothing exports: {sorted(missing)}"


def test_prometheus_scrapes_the_exporter():
    config = yaml.safe_load((REPO / "infra/monitoring/prometheus.yml").read_text())
    jobs = {job["job_name"]: job for job in config["scrape_configs"]}
    targets = jobs["containers"]["static_configs"][0]["targets"]
    assert targets == ["container-exporter:8007"]


def test_the_exporter_is_not_published_to_the_host():
    """It can read every container's environment. Nothing outside the compose network needs it."""
    compose = yaml.safe_load((REPO / "infra/compose/docker-compose.yml").read_text())
    service = compose["services"]["container-exporter"]
    assert "ports" not in service
    assert "--port" in service["command"] and "8007" in service["command"]
    assert any(v.startswith("/var/run/docker.sock:") for v in service["volumes"])


def test_exactly_the_init_services_are_marked_one_shot():
    """Mark too few and a finished init job pages someone; mark too many and a stopped
    collector reads as a job well done."""
    compose = yaml.safe_load((REPO / "infra/compose/docker-compose.yml").read_text())
    marked = {
        name
        for name, service in compose["services"].items()
        if (service.get("labels") or {}).get(ONE_SHOT_LABEL) == "true"
    }
    inits = {name for name in compose["services"] if name.endswith("-init")}
    assert marked == inits
