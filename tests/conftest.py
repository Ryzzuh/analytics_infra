"""Test fixtures.

Postgres comes from `pgserver`, which bundles real PostgreSQL binaries and runs them as a
local process. That keeps the ledger tests honest (real transactions, real partitioning, real
COPY) while needing no Docker, so they run on a laptop as well as in CI.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pgserver
import psycopg
import pytest
from loader.schema import apply_ddl
from loader.testing import FakeMessageSource

TOPIC = "product.feature_usage"


@pytest.fixture(scope="session")
def pg_uri() -> str:
    data_dir = Path(tempfile.mkdtemp(prefix="analytics-infra-pg-"))
    server = pgserver.get_server(data_dir)
    try:
        yield server.get_uri()
    finally:
        server.cleanup()


@pytest.fixture
def conn(pg_uri: str):
    """A connection to a database that is empty apart from the platform's own DDL."""
    with psycopg.connect(pg_uri, autocommit=False) as admin:
        admin.autocommit = True
        # dbt-created schemas are dropped too, or one test's models leak into the next.
        for schema in ("raw", "ops", "staging", "core", "marts"):
            admin.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    with psycopg.connect(pg_uri, autocommit=False) as connection:
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
    data_dir = Path(tempfile.mkdtemp(prefix="analytics-infra-app-"))
    server = pgserver.get_server(data_dir)
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


@pytest.fixture
def app_conn(app_pg_uri: str):
    """A freshly built copy of the app's OLTP schema, publication included."""
    with psycopg.connect(app_pg_uri, autocommit=True) as admin:
        for slot in admin.execute("SELECT slot_name FROM pg_replication_slots").fetchall():
            admin.execute("SELECT pg_drop_replication_slot(%s)", (slot[0],))
        admin.execute("DROP SCHEMA public CASCADE")
        admin.execute("CREATE SCHEMA public")
    with psycopg.connect(app_pg_uri, autocommit=False) as connection:
        apply_ddl(connection, Path(__file__).resolve().parents[1] / "db" / "app" / "ddl")
        yield connection
