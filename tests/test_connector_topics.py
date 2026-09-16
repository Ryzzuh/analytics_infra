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


def redpanda_init_command() -> str:
    compose = yaml.safe_load(COMPOSE.read_text())
    return compose["services"]["redpanda-init"]["command"]


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
    compose = yaml.safe_load(COMPOSE.read_text())
    assert "connect-init" in compose["services"], (
        "registering the connector by hand means `make up` yields a stack with no CDC at all"
    )
    command = compose["services"]["connect-init"]["command"]
    assert "--fail" in command, "curl exits 0 on an HTTP 500; a rejected config must fail loudly"
    assert "/connectors/app-cdc/config" in command, "use the idempotent config endpoint"
