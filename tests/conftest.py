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
        admin.execute("DROP SCHEMA IF EXISTS raw CASCADE")
        admin.execute("DROP SCHEMA IF EXISTS ops CASCADE")
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
        "erased_count, attempt FROM ops.load_ledger ORDER BY id"
    ).fetchall()
