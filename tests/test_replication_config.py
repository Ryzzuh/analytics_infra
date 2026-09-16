"""Source-database CDC configuration, checked against a real logical-decoding Postgres.

Debezium itself needs a JVM and is not exercised here. What is exercised is everything
Debezium *depends on*, which is where the interesting failure modes live: what the publication
captures, whether before-images will be complete, and what happens to a slot nobody is reading.
"""

from __future__ import annotations

import psycopg
import pytest

DATA_TABLES = {
    "accounts",
    "users",
    "plans",
    "subscriptions",
    "invoices",
    "support_tickets",
}
# The heartbeat table is captured so that Debezium's periodic write generates WAL on a
# published table and the slot's confirmed LSN advances on an idle database. It carries no
# business data and its before-image is never read, so it is exempt from REPLICA IDENTITY
# FULL and pays no extra WAL per beat.
CAPTURED_TABLES = DATA_TABLES | {"debezium_heartbeat"}


def published_tables(conn) -> set[str]:
    return {
        r[0]
        for r in conn.execute(
            "SELECT tablename FROM pg_publication_tables WHERE pubname = 'analytics_cdc'"
        ).fetchall()
    }


def test_publication_captures_exactly_the_intended_tables(app_conn):
    assert published_tables(app_conn) == CAPTURED_TABLES


def test_reverse_etl_target_is_not_captured(app_conn):
    """The feedback loop this prevents: scores written back by reverse ETL would otherwise be
    captured, land in the warehouse, and feed the model that produced them (SPEC.md §8)."""
    assert "account_insights" not in published_tables(app_conn)


def test_a_new_table_is_not_captured_by_accident(app_conn):
    """`FOR ALL TABLES` would enrol every future table without anyone deciding to."""
    app_conn.execute("CREATE TABLE experiment_flags (id bigint PRIMARY KEY, name text)")
    app_conn.commit()

    assert "experiment_flags" not in published_tables(app_conn)


def test_every_data_table_in_the_publication_has_replica_identity_full(app_conn):
    """Without FULL, an UPDATE carries no old values and a DELETE carries only the key, so
    SCD2 cannot close an interval with what was actually live (SPEC.md §4.2)."""
    identities = dict(
        app_conn.execute(
            "SELECT relname, relreplident FROM pg_class WHERE relname = ANY(%s) AND relkind = 'r'",
            (list(DATA_TABLES),),
        ).fetchall()
    )

    not_full = {t: i for t, i in identities.items() if i != "f"}
    assert not_full == {}, f"captured tables without REPLICA IDENTITY FULL: {not_full}"


def test_delete_leaves_a_full_before_image_in_the_wal(app_conn, app_pg_uri):
    """Proves the consequence rather than the setting: with REPLICA IDENTITY FULL the WAL
    record for a delete is wide enough to carry every column, not just the key."""
    app_conn.execute(
        "INSERT INTO accounts (id, company_name, country) VALUES (1, 'Acme Pty Ltd', 'AU')"
    )
    app_conn.commit()

    before = app_conn.execute("SELECT pg_current_wal_lsn()").fetchone()[0]
    app_conn.execute("DELETE FROM accounts WHERE id = 1")
    app_conn.commit()
    after = app_conn.execute("SELECT pg_current_wal_lsn()").fetchone()[0]

    delete_wal_bytes = app_conn.execute(
        "SELECT pg_wal_lsn_diff(%s, %s)", (after, before)
    ).fetchone()[0]

    # A key-only delete record is tens of bytes; carrying 'Acme Pty Ltd' and the rest needs
    # substantially more. The threshold is deliberately loose: the point is that the row
    # travelled, not the exact encoding.
    assert delete_wal_bytes > 100


@pytest.fixture
def slot(app_conn, app_pg_uri):
    app_conn.execute("SELECT pg_create_logical_replication_slot('analytics_cdc_test', 'pgoutput')")
    app_conn.commit()
    yield "analytics_cdc_test"


def slot_state(conn, name: str) -> tuple[str, bool]:
    row = conn.execute(
        "SELECT wal_status, active FROM pg_replication_slots WHERE slot_name = %s", (name,)
    ).fetchone()
    return (row[0], row[1]) if row else ("gone", False)


def test_an_unread_slot_starts_by_reserving_wal(app_conn, slot):
    assert slot_state(app_conn, slot)[0] == "reserved"


def test_the_cap_invalidates_the_slot_instead_of_filling_the_disk(app_conn, slot, app_pg_uri):
    """The §9.1 decision, proven: with `max_slot_wal_keep_size` set, a slot nobody reads gets
    invalidated and WAL is recycled. Without the cap this is the scenario where the shared
    disk fills and takes down the broker, the warehouse and Airflow along with the app.

    Recovery from here is re-creating the slot and running an incremental snapshot, which is
    why `ops.cdc_gaps` and the Debezium signal table exist.
    """
    with psycopg.connect(app_pg_uri, autocommit=True) as churn:
        churn.execute("CREATE TABLE IF NOT EXISTS wal_churn (id bigserial, blob text)")
        for _ in range(12):
            # ~64 MB of WAL against a 32 MB cap, with checkpoints so the cap can act.
            churn.execute(
                "INSERT INTO wal_churn (blob) "
                "SELECT repeat('x', 4000) FROM generate_series(1, 1200)"
            )
            churn.execute("SELECT pg_switch_wal()")
            churn.execute("CHECKPOINT")

    status, _active = slot_state(app_conn, slot)
    assert status == "lost", f"slot was {status}: the cap did not invalidate it"

    # And the point of all that: the disk was protected, so writes still succeed.
    app_conn.execute("INSERT INTO accounts (company_name, country) VALUES ('Still Trading', 'AU')")
    app_conn.commit()
    assert app_conn.execute("SELECT count(*) FROM accounts").fetchone()[0] >= 1
