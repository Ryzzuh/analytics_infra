"""Container health for Prometheus, read from the Docker API.

Why not cAdvisor, the conventional choice: cAdvisor reports what containers *consume* — CPU,
memory, I/O — and nothing about whether Docker considers them *healthy*, how many times they
have been restarted, or whether the kernel killed them for memory. Every container incident on
this stack turned on those:

* the Airflow scheduler died inside a container that went on reporting `Up`;
* services exited 255 in a loop after Docker Desktop restarted underneath them;
* the control plane failed to start on a stale bind mount;
* and the whole question of whether the stack fits in 5 GB was a memory question.

So this reads `docker inspect` for every container in the compose project, and resource usage
from `docker stats` in the background (a stats call takes about a second per container, far too
slow to do inside a scrape).

Three rules the code keeps:

* **Series come and go with containers.** A custom collector builds every family fresh on each
  scrape, so a removed container's series disappear rather than freezing at their last value —
  which a set of module-level Gauges would do, reporting a deleted container as healthy forever.
* **Nothing from `Config.Env` is ever exported.** `docker inspect` returns each container's
  environment, which here includes database passwords, the Airflow JWT secret and API token.
  Labels are built from three named fields and nothing else.
* **A Docker outage is a value, not an exception.** When Docker Desktop quit this week the stack
  vanished; the exporter reports `container_exporter_docker_up 0` and reconnects on the next
  scrape instead of taking the metrics endpoint down with it.
"""

from __future__ import annotations

import logging
import os
import re
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import docker
from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
from prometheus_client.core import GaugeMetricFamily

log = logging.getLogger("container_exporter")

PROJECT = os.environ.get("COMPOSE_PROJECT", "analytics-infra")
STATS_INTERVAL_SECONDS = float(os.environ.get("STATS_INTERVAL_SECONDS", 15))

# One number per health state, so a Grafana state timeline can colour it. Ordered so that a
# higher code is worse, which makes `max by (service)` the honest summary across containers.
HEALTH_CODES = {"none": 0, "healthy": 1, "starting": 2, "unhealthy": 3}

# Marks a service that is meant to run once and exit (schema migrations, topic creation,
# connector registration). Inferring this from the restart policy does not work: most services
# here declare no restart policy at all, so "restart: no" describes the collector and the
# warehouse as much as it describes warehouse-init.
ONE_SHOT_LABEL = "analytics.one-shot"


def parse_docker_time(value: str | None) -> datetime | None:
    """Docker timestamps carry nanoseconds, which `datetime` cannot hold.

    `2026-09-17T03:20:00.123456789Z` has nine fractional digits; fromisoformat accepts at most
    six. A container that was created but never started reports the zero time, which is not a
    start time at all.
    """
    if not value or value.startswith("0001-01-01"):
        return None
    match = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?(Z|[+-]\d\d:\d\d)", value)
    if match is None:
        return None
    base, fraction, zone = match.groups()
    fraction = (fraction or ".0")[:7]  # the dot plus at most six digits
    zone = "+00:00" if zone == "Z" else zone
    return datetime.fromisoformat(f"{base}{fraction}{zone}")


@dataclass(frozen=True)
class ContainerView:
    name: str
    service: str
    status: str  # running, exited, restarting, created, paused, dead
    health: str  # healthy, unhealthy, starting, or none when there is no healthcheck
    restart_count: int
    oom_killed: bool
    exit_code: int
    one_shot: bool
    started_at: datetime | None

    @classmethod
    def from_attrs(cls, attrs: dict[str, Any]) -> ContainerView:
        state = attrs.get("State") or {}
        labels = (attrs.get("Config") or {}).get("Labels") or {}
        return cls(
            name=(attrs.get("Name") or "").lstrip("/"),
            service=labels.get("com.docker.compose.service", "unknown"),
            status=state.get("Status", "unknown"),
            health=(state.get("Health") or {}).get("Status", "none"),
            restart_count=int(attrs.get("RestartCount") or 0),
            oom_killed=bool(state.get("OOMKilled")),
            exit_code=int(state.get("ExitCode") or 0),
            one_shot=labels.get(ONE_SHOT_LABEL) == "true",
            started_at=parse_docker_time(state.get("StartedAt")),
        )

    @property
    def completed(self) -> bool:
        """A one-shot that ran and succeeded — the one kind of exited container that is fine."""
        return self.one_shot and self.status == "exited" and self.exit_code == 0

    def problems(self) -> list[str]:
        """What is wrong with this container, if anything. Empty means healthy.

        Decided here rather than in PromQL so the rules are tested in one place, and so the
        dashboard and any future alert agree on what "a problem" means.
        """
        found: list[str] = []
        if self.status == "restarting":
            found.append("restart_loop")
        if self.health == "unhealthy":
            found.append("unhealthy")
        if self.oom_killed:
            found.append("oom_killed")
        if self.status in ("exited", "dead") and not self.completed:
            found.append("exited")
        if self.status == "created":
            found.append("never_started")
        return found


def resource_usage(stats: dict[str, Any]) -> tuple[float, float, float] | None:
    """(memory used, memory limit, CPU cores) from one `docker stats` sample.

    Memory excludes reclaimable page cache, the way `docker stats` itself reports it: raw usage
    counts files the container merely read, which the kernel will drop under pressure and which
    would make every container look close to its limit.
    """
    memory = stats.get("memory_stats") or {}
    usage = memory.get("usage")
    if usage is None:  # not running, or stats not ready yet
        return None
    inner = memory.get("stats") or {}
    cache = inner.get("inactive_file", inner.get("total_inactive_file", 0))  # cgroup v2, v1
    used = max(float(usage) - float(cache), 0.0)
    limit = float(memory.get("limit") or 0)

    cpu = stats.get("cpu_stats") or {}
    previous = stats.get("precpu_stats") or {}
    cpu_delta = (cpu.get("cpu_usage") or {}).get("total_usage", 0) - (
        previous.get("cpu_usage") or {}
    ).get("total_usage", 0)
    system_delta = cpu.get("system_cpu_usage", 0) - previous.get("system_cpu_usage", 0)
    online = (
        cpu.get("online_cpus") or len((cpu.get("cpu_usage") or {}).get("percpu_usage") or []) or 1
    )
    cores = (cpu_delta / system_delta) * online if system_delta > 0 and cpu_delta > 0 else 0.0
    return used, limit, float(cores)


class ContainerCollector:
    """A Prometheus collector that rebuilds every series from Docker on each scrape."""

    def __init__(
        self,
        client_factory: Callable[[], Any],
        *,
        project: str = PROJECT,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._client_factory = client_factory
        self._client: Any = None
        self.project = project
        self._now = now
        self._usage: dict[str, tuple[float, float, float]] = {}
        self._lock = threading.Lock()

    def _docker(self) -> Any:
        if self._client is None:
            self._client = self._client_factory()
        return self._client

    def _containers(self) -> list[Any]:
        # sparse=False (the default) inspects each container, which is where State.Health,
        # RestartCount and OOMKilled live — the list endpoint alone does not carry them.
        # ignore_removed: during a compose recreate a container can vanish between the list
        # and its inspect, and that must not fail the whole scrape.
        return self._docker().containers.list(
            all=True,
            filters={"label": f"com.docker.compose.project={self.project}"},
            ignore_removed=True,
        )

    def refresh_usage(self) -> None:
        """Sample resource usage for every running container, in parallel."""
        try:
            running = [c for c in self._containers() if c.attrs.get("State", {}).get("Running")]
        except Exception:  # noqa: BLE001 - Docker unavailable; the scrape reports it
            return

        def sample(container: Any) -> tuple[str, tuple[float, float, float] | None]:
            try:
                return container.name, resource_usage(container.stats(stream=False))
            except Exception:  # noqa: BLE001 - container went away mid-sample
                return container.name, None

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = dict(pool.map(sample, running))
        with self._lock:
            self._usage = {name: usage for name, usage in results.items() if usage is not None}

    def usage_loop(self, stop: threading.Event, interval: float) -> None:
        while not stop.is_set():
            self.refresh_usage()
            stop.wait(interval)

    def describe(self) -> list[Any]:
        # Declared empty so registering does not trigger a live Docker call.
        return []

    def collect(self) -> Iterator[GaugeMetricFamily]:
        docker_up = GaugeMetricFamily(
            "container_exporter_docker_up", "1 if the Docker API answered this scrape"
        )
        try:
            containers = self._containers()
            info = self._docker().info()
        except Exception:  # noqa: BLE001
            log.warning("Docker API unreachable", exc_info=True)
            # A Docker Desktop restart replaces the socket; reconnect next time rather than
            # holding a client bound to the old one.
            self._client = None
            docker_up.add_metric([], 0)
            yield docker_up
            return
        docker_up.add_metric([], 1)
        yield docker_up

        views = [ContainerView.from_attrs(c.attrs) for c in containers]
        labels = ["service", "container"]
        families = {
            "running": GaugeMetricFamily(
                "container_running", "1 if the container is running", labels=labels
            ),
            "health": GaugeMetricFamily(
                "container_health_status",
                "Docker healthcheck: 0 none, 1 healthy, 2 starting, 3 unhealthy",
                labels=labels,
            ),
            "restarts": GaugeMetricFamily(
                "container_restart_count",
                "Restarts by Docker's restart policy; reset when the container is recreated",
                labels=labels,
            ),
            "oom": GaugeMetricFamily(
                "container_oom_killed", "1 if the kernel killed it for memory", labels=labels
            ),
            "exit": GaugeMetricFamily(
                "container_exit_code", "Exit code of the last run", labels=labels
            ),
            "completed": GaugeMetricFamily(
                "container_completed",
                "1 for a one-shot service that ran and exited 0",
                labels=labels,
            ),
            "uptime": GaugeMetricFamily(
                "container_uptime_seconds", "Seconds since the container started", labels=labels
            ),
            "problem": GaugeMetricFamily(
                "container_problem",
                "1 per thing wrong with a container; absent when it is fine",
                labels=[*labels, "reason"],
            ),
            "memory": GaugeMetricFamily(
                "container_memory_bytes", "Memory in use, excluding page cache", labels=labels
            ),
            "memory_limit": GaugeMetricFamily(
                "container_memory_limit_bytes", "Memory the container may use", labels=labels
            ),
            "cpu": GaugeMetricFamily("container_cpu_cores", "CPU in use, in cores", labels=labels),
        }

        now = self._now()
        with self._lock:
            usage = dict(self._usage)

        for view in views:
            key = [view.service, view.name]
            families["running"].add_metric(key, 1 if view.status == "running" else 0)
            families["health"].add_metric(key, HEALTH_CODES.get(view.health, 0))
            families["restarts"].add_metric(key, view.restart_count)
            families["oom"].add_metric(key, 1 if view.oom_killed else 0)
            families["exit"].add_metric(key, view.exit_code)
            if view.one_shot:
                families["completed"].add_metric(key, 1 if view.completed else 0)
            if view.status == "running" and view.started_at is not None:
                families["uptime"].add_metric(key, (now - view.started_at).total_seconds())
            for reason in view.problems():
                families["problem"].add_metric([*key, reason], 1)
            # Only for containers present in *this* listing, so a removed container's last
            # sample is never reported against a name that no longer exists.
            if view.status == "running" and view.name in usage:
                used, limit, cores = usage[view.name]
                families["memory"].add_metric(key, used)
                families["memory_limit"].add_metric(key, limit)
                families["cpu"].add_metric(key, cores)

        yield from families.values()

        host_memory = GaugeMetricFamily(
            "docker_host_memory_bytes", "Memory available to the Docker VM"
        )
        host_memory.add_metric([], float(info.get("MemTotal") or 0))
        yield host_memory
        host_cpus = GaugeMetricFamily("docker_host_cpus", "CPUs available to the Docker VM")
        host_cpus.add_metric([], float(info.get("NCPU") or 0))
        yield host_cpus


collector = ContainerCollector(docker.from_env)
registry = CollectorRegistry()
registry.register(collector)


@asynccontextmanager
async def lifespan(_: FastAPI):
    stop = threading.Event()
    sampler = threading.Thread(
        target=collector.usage_loop, args=(stop, STATS_INTERVAL_SECONDS), daemon=True
    )
    sampler.start()
    yield
    stop.set()


app = FastAPI(title="container-exporter", lifespan=lifespan)


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
