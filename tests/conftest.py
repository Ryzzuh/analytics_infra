"""Test fixtures.

Postgres comes from `pgserver`, which bundles real PostgreSQL binaries and runs them as a
local process. That keeps the ledger tests honest (real transactions, real partitioning, real
COPY) while needing no Docker, so they run on a laptop as well as in CI.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pgserver
import psycopg
import pytest
from loader.schema import apply_ddl
from loader.testing import FakeMessageSource

TOPIC = "product.feature_usage"


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, owned by somebody else
    return True


def server_dir(name: str) -> Path:
    """A stable data directory for an embedded Postgres, with dead handles pruned.

    Both halves of this exist because the suite was leaving postmasters behind — 32 of them on
    one machine, which is every System V shared memory id macOS allows (`kern.sysv.shmmni`).
    Once they are gone, initdb fails with "No space left on device" and every test that needs a
    database errors out, with nothing to suggest the cause is previous *test runs*.

    A fixed directory, not mkdtemp. Teardown is registered with atexit, which a SIGKILL never
    runs — a CI timeout, a stopped test run, an interrupted session. With a fresh directory per
    run, each one of those stranded a brand new server, so the count only ever grew. One
    directory per role means at most one server per role no matter how many runs die, and it
    skips initdb on every run after the first.

    Pruning, because pgserver records the pids using a server as a plain JSON list in the data
    directory and never checks whether they are still alive. A killed run leaves its pid there
    forever, and `_cleanup` stops the server only when the list holds nothing but its own pid —
    so from then on every *well-behaved* run decides someone else is still using the server and
    declines to stop it. One killed run disables teardown permanently.
    """
    pgdata = Path(tempfile.gettempdir()) / f"analytics-infra-{name}"
    pgdata.mkdir(parents=True, exist_ok=True)

    handles = pgdata / ".handle_pids.json"
    try:
        pids = json.loads(handles.read_text())
    except (OSError, json.JSONDecodeError):
        return pgdata

    alive = [pid for pid in pids if _pid_is_alive(pid)]
    if alive != pids:
        handles.write_text(json.dumps(alive))
    return pgdata


@pytest.fixture(scope="session")
def pg_uri() -> str:
    server = pgserver.get_server(server_dir("pg"))
    try:
        yield server.get_uri()
    finally:
        server.cleanup()


@pytest.fixture
def conn(pg_uri: str):
    """A connection to a database that is empty apart from the platform's own DDL.

    Autocommit, so a test's SELECT does not leave a transaction open holding ACCESS SHARE on
    a table dbt then tries to replace — that blocks dbt forever rather than failing. The
    loader opens its own explicit transactions (`with conn.transaction()`), so the invariants
    under test are unaffected.
    """
    with psycopg.connect(pg_uri, autocommit=False) as admin:
        admin.autocommit = True
        # dbt-created schemas are dropped too, or one test's models leak into the next.
        for schema in ("raw", "ops", "staging", "core", "marts"):
            admin.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    with psycopg.connect(pg_uri, autocommit=True) as connection:
        apply_ddl(connection)
        yield connection


@pytest.fixture
def source() -> FakeMessageSource:
    return FakeMessageSource()


def raw_count(conn, **filters) -> int:
    where = " AND ".join(f"{k} = %s" for k in filters) or "true"
    return conn.execute(
        f"SELECT count(*) FROM raw.product_events WHERE {where}", tuple(filters.values())
    ).fetchone()[0]


def ledger_rows(conn) -> list[tuple]:
    return conn.execute(
        "SELECT id, topic, partition_id, start_offset, end_offset, row_count, dlq_count, "
        "erased_count, skipped_count, attempt FROM ops.load_ledger ORDER BY id"
    ).fetchall()


@pytest.fixture(scope="session")
def app_pg_uri() -> str:
    """The source OLTP server, configured for logical decoding.

    `wal_level=logical` and a deliberately tiny `max_slot_wal_keep_size` let the slot-retention
    behaviour behind SPEC.md §9.1 be tested for real rather than asserted in prose.
    """
    server = pgserver.get_server(server_dir("app"))
    with psycopg.connect(server.get_uri(), autocommit=True) as conn:
        conn.execute("ALTER SYSTEM SET wal_level = 'logical'")
        conn.execute("ALTER SYSTEM SET max_slot_wal_keep_size = '32MB'")
        conn.execute("ALTER SYSTEM SET max_wal_size = '48MB'")
        conn.execute("ALTER SYSTEM SET min_wal_size = '32MB'")
    pgserver.pg_ctl(["restart", "-w"], pgdata=server.pgdata)
    try:
        yield server.get_uri()
    finally:
        server.cleanup()
        # cleanup() is not enough here, and this fixture is the only one that needs the extra
        # step. pgserver decides whether to stop the server by asking whether the postmaster it
        # started is still running — but the restart above replaced that process without
        # telling pgserver, so it checks a pid that died during the restart, concludes the
        # server is already down, and leaves the *new* postmaster running. Every run leaked one
        # server this way even when teardown completed normally.
        try:
            pgserver.pg_ctl(["stop", "-w", "-m", "fast"], pgdata=server.pgdata)
        except Exception:  # noqa: BLE001 - already stopped, or never started
            pass


@pytest.fixture
def app_conn(app_pg_uri: str):
    """A freshly built copy of the app's OLTP schema, publication included."""
    with psycopg.connect(app_pg_uri, autocommit=True) as admin:
        for slot in admin.execute("SELECT slot_name FROM pg_replication_slots").fetchall():
            admin.execute("SELECT pg_drop_replication_slot(%s)", (slot[0],))
        admin.execute("DROP SCHEMA public CASCADE")
        admin.execute("CREATE SCHEMA public")
    with psycopg.connect(app_pg_uri, autocommit=True) as connection:
        apply_ddl(connection, Path(__file__).resolve().parents[1] / "db" / "app" / "ddl")
        yield connection
