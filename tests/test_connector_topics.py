"""The connector's destination topics must actually exist.

This pair — a connector that names topics, and an init step that creates them — broke apart
silently. Broker auto-create was disabled so that a typo could not quietly produce a
one-partition topic; nothing then created Debezium's destination topics; and the resulting
system looks *healthy*. The connector reports RUNNING, `docker ps` is green, and every produce
fails with UNKNOWN_TOPIC_OR_PARTITION while the replication slot retains WAL without limit.

Nothing downstream can catch this: with no messages on the topics, the loader has nothing to
load and reports no error. So the coupling is asserted here, against the two files that have to
agree, rather than hoped for at runtime.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]
CONNECTOR = REPO / "infra" / "connect" / "subscriptions-source.json"
COMPOSE = REPO / "infra" / "compose" / "docker-compose.yml"


def connector_config() -> dict:
    return json.loads(CONNECTOR.read_text())


def compose_config() -> dict:
    return yaml.safe_load(COMPOSE.read_text())


def redpanda_init_command() -> str:
    return compose_config()["services"]["redpanda-init"]["command"]


def created_topics() -> set[str]:
    """Topic names redpanda-init creates, with the shell loops expanded.

    The command is a shell snippet with `for t in ...` loops, so the names are assembled rather
    than listed. Expanding them here is the point: it is the assembled name that has to match
    what the connector produces to.
    """
    command = redpanda_init_command()
    topics: set[str] = set()

    # Each `for t in a b c; do ... done` block, paired with the `rpk topic create <pattern>`
    # calls inside it. `$$t` is compose's escaping for a literal `$t`.
    for loop_vars, body in re.findall(r"for t in ([^;]+); do(.*?)done", command, re.S):
        values = loop_vars.split()
        for pattern in re.findall(r"rpk topic create (\S+)", body):
            for value in values:
                topics.add(pattern.replace("$$t", value))

    # Plus any creations outside a loop.
    without_loops = re.sub(r"for t in [^;]+; do.*?done", "", command, flags=re.S)
    for pattern in re.findall(r"rpk topic create (\S+)", without_loops):
        topics.add(pattern)

    return topics


def test_every_captured_table_has_a_destination_topic():
    config = connector_config()
    prefix = config["topic.prefix"]
    tables = [t.strip() for t in config["table.include.list"].split(",")]

    expected = {f"{prefix}.{table}" for table in tables}
    missing = expected - created_topics()

    assert not missing, (
        f"the connector produces to {sorted(missing)}, which redpanda-init does not create. "
        "Auto-create is disabled, so the connector will report RUNNING while every produce "
        "fails with UNKNOWN_TOPIC_OR_PARTITION and the slot retains WAL."
    )


def test_the_heartbeat_topic_exists():
    """Debezium writes heartbeats to a topic of its own.

    It is the mechanism that keeps the slot's confirmed LSN moving while the captured tables are
    idle, so losing it reintroduces exactly the unbounded WAL growth it exists to prevent.
    """
    config = connector_config()
    assert "heartbeat.interval.ms" in config, "heartbeats are load-bearing here (SPEC.md §4.2)"
    assert f"__debezium-heartbeat.{config['topic.prefix']}" in created_topics()


def test_connector_file_is_a_bare_config_object():
    """PUT /connectors/{name}/config takes the config alone.

    Posting a {"name": ..., "config": {...}} wrapper to that endpoint fails with a
    deserialisation 500 — and curl without --fail reports that as success, which is how the
    documented registration command came to be wrong without anyone noticing.
    """
    config = connector_config()
    assert "config" not in config, "this must be the bare config object, not the wrapper"
    assert config["connector.class"].endswith("PostgresConnector")


def test_registration_is_wired_into_compose_not_just_documented():
    assert "connect-init" in compose_config()["services"], (
        "registering the connector by hand means `make up` yields a stack with no CDC at all"
    )
    command = compose_config()["services"]["connect-init"]["command"]
    assert "--fail" in command, "curl exits 0 on an HTTP 500; a rejected config must fail loudly"
    assert "/connectors/app-cdc/config" in command, "use the idempotent config endpoint"


def test_no_extension_key_is_defined_as_a_service():
    """`x-` keys are ignored at the top level of the file, but not inside `services:`.

    An anchor block written one level too deep becomes a service named `x-airflow-common`:
    compose validates it, `config -q` says nothing, and `up` tries to build and start it.
    """
    services = compose_config()["services"]
    stray = [name for name in services if name.startswith("x-")]
    assert not stray, f"{stray} are anchors indented into services:, not services"


AIRFLOW_COMPONENTS = [
    "airflow-scheduler",
    "airflow-apiserver",
    "airflow-triggerer",
    "airflow-dag-processor",
]


def test_each_airflow_component_can_be_seen_to_die():
    """The reason for splitting `standalone` apart.

    One process tree meant a dead scheduler left a container reporting `Up` and Docker with
    nothing to restart. Each component now needs a healthcheck (so death is visible) and a
    restart policy (so it is acted on).
    """
    services = compose_config()["services"]
    for name in AIRFLOW_COMPONENTS:
        assert name in services, f"{name} is missing"
        assert "healthcheck" in services[name], f"{name} has no healthcheck; death is invisible"
        assert services[name].get("restart") == "unless-stopped", f"{name} will not come back"


def test_the_shared_secrets_are_pinned():
    """Every Airflow process must agree on these, and each generates its own if unset.

    A per-container JWT secret breaks the scheduler's workers against the api-server's Task
    Execution API; a per-container Fernet key makes stored connections undecryptable. Under
    standalone both were generated once per process tree, so neither had to be configured —
    which is exactly why splitting the components apart is where this bites.
    """
    env = compose_config()["x-airflow-env"]
    assert "AIRFLOW__API_AUTH__JWT_SECRET" in env

    # HS512 signing: RFC 7518 §3.2 wants a key of at least the hash length. Airflow only warns
    # and carries on, so a short secret is easy to ship without noticing.
    secret = env["AIRFLOW__API_AUTH__JWT_SECRET"].split(":-", 1)[1].rstrip("}")
    assert len(secret) >= 64, f"JWT secret is {len(secret)} bytes; HS512 wants >= 64"
    assert "AIRFLOW__CORE__FERNET_KEY" in env
    assert "airflow:8080" in env["AIRFLOW__CORE__EXECUTION_API_SERVER_URL"], (
        "workers must reach the api-server by service name, not localhost"
    )


def test_dbt_is_pointed_at_the_warehouse_service():
    """dbt/profiles.yml defaults to localhost:5433 — the *host* port mapping.

    That default is right for running dbt from a developer machine and wrong inside every
    container. The loaders in the same DAGs use WAREHOUSE_DSN and are unaffected, so the failure
    looks like "dbt is broken" rather than "the environment is incomplete".
    """
    env = compose_config()["x-airflow-env"]
    assert env.get("WAREHOUSE_HOST") == "postgres-warehouse", (
        "without this dbt dials localhost:5433 from inside the container"
    )
    assert str(env.get("WAREHOUSE_PORT")) == "5432", (
        "5433 is the host mapping, not the service port"
    )


def test_the_makefile_passes_the_root_env_file_to_compose():
    """`.env.example` sits at the repo root; compose looks in the compose file's directory.

    Without `--env-file`, every knob in that file is silently ignored — compose falls back to the
    defaults in docker-compose.yml and reports nothing. The symptom is a setting that appears to
    have no effect, which is far harder to chase than an error.
    """
    makefile = (REPO / "Makefile").read_text()
    compose_line = next(line for line in makefile.splitlines() if line.startswith("COMPOSE :="))
    assert "--env-file" in compose_line, (
        "compose will not read the repo-root .env, so .env.example does nothing"
    )
    assert "--project-directory" not in compose_line, (
        "--project-directory re-bases the ../../ build contexts two levels too high"
    )


def test_no_repo_file_is_bind_mounted_on_its_own():
    """Mount directories, never single files.

    Binding a file pins its inode. Git replaces files rather than editing them in place, so any
    pull that touched a mounted file left the running container bound to something that no
    longer existed, and the next `compose start` failed with a runc mount error. It took down
    the control plane, then Prometheus mid-deploy — which aborted `make up-full` with nineteen
    containers stopped or never started. A directory's inode survives its contents changing.
    """
    root = REPO / "infra" / "compose"
    offenders = []
    for name, service in compose_config()["services"].items():
        for volume in service.get("volumes") or []:
            source = volume.split(":")[0]
            if source.startswith(".") and (root / source).resolve().is_file():
                offenders.append(f"{name}: {source}")
    assert not offenders, f"single-file bind mounts break on the next git pull: {offenders}"
