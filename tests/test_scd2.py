"""SCD2 built from the change log — M2's completion criterion (SPEC.md §14).

Runs dbt for real against the embedded Postgres, on data pushed through the actual loader.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from conftest import TOPIC  # noqa: F401  (imported for fixture discovery symmetry)
from loader import load_partition
from loader.targets import cdc_target
from loader.testing import change_bytes, tombstone_key
from test_dbt_staging import dbt, dbt_env  # noqa: F401  (fixture reuse)

pytestmark = pytest.mark.dbt  # these invoke dbt for real

CDC_TOPIC = "cdc.app.public.subscriptions"
DAY = datetime(2026, 3, 4, tzinfo=UTC)


def sub(**overrides) -> dict:
    row = {
        "id": 1,
        "account_id": 500,
        "plan_code": "team",
        "status": "trial",
        "seats": 5,
        "mrr_cents": 0,
        "started_at": DAY.isoformat(),
        "ended_at": None,
    }
    row.update(overrides)
    return row


def load_cdc(conn, source, run_id="cdc-run-1"):
    return load_partition(
        conn,
        source,
        topic=CDC_TOPIC,
        partition_id=0,
        dag_run_id=run_id,
        target=cdc_target(),
    )


def versions(conn) -> list[tuple]:
    return conn.execute(
        "SELECT status, valid_from, valid_to, is_current, ended_by_delete "
        "FROM core.dim_subscription WHERE subscription_id = 1 ORDER BY valid_from"
    ).fetchall()


@pytest.fixture
def built(conn, source, dbt_env):  # noqa: F811
    """Loads whatever the test produced, then builds staging + core."""

    def _build():
        load_cdc(conn, source)
        conn.commit()
        # Exactly the models these tests assert on. `stg_subscription_changes+` would now
        # drag in marts whose other parents this test never loads.
        dbt("build", "--select", "stg_subscription_changes", "dim_subscription", env=dbt_env)

    return _build


def test_three_transitions_in_one_day_are_three_versions(conn, source, built):
    """The case dbt snapshots cannot express: a daily snapshot would see only 'cancelled'."""
    trial = sub(status="trial")
    active = sub(status="active", mrr_cents=25000)
    cancelled = sub(status="cancelled", ended_at=(DAY + timedelta(hours=9)).isoformat())

    source.produce(CDC_TOPIC, 0, change_bytes(op="c", after=trial, lsn=10, effective_at=DAY))
    source.produce(
        CDC_TOPIC,
        0,
        change_bytes(
            op="u", before=trial, after=active, lsn=20, effective_at=DAY + timedelta(hours=4)
        ),
    )
    source.produce(
        CDC_TOPIC,
        0,
        change_bytes(
            op="u",
            before=active,
            after=cancelled,
            lsn=30,
            effective_at=DAY + timedelta(hours=9),
        ),
    )

    built()

    rows = versions(conn)
    assert [r[0] for r in rows] == ["trial", "active", "cancelled"]
    # Intervals are contiguous and dated by business time, not by when they were loaded.
    assert rows[0][1] == DAY and rows[0][2] == DAY + timedelta(hours=4)
    assert rows[1][2] == DAY + timedelta(hours=9)
    assert [r[3] for r in rows] == [False, False, True]


def test_a_delete_closes_the_interval_without_opening_a_version(conn, source, built):
    active = sub(status="active", mrr_cents=25000)
    source.produce(CDC_TOPIC, 0, change_bytes(op="c", after=active, lsn=10, effective_at=DAY))
    source.produce(
        CDC_TOPIC,
        0,
        change_bytes(op="d", before=active, lsn=20, effective_at=DAY + timedelta(days=30)),
    )
    source.produce(CDC_TOPIC, 0, None, key=tombstone_key(1))

    built()

    rows = versions(conn)
    assert len(rows) == 1
    assert rows[0][2] == DAY + timedelta(days=30)  # closed at the deletion
    assert rows[0][4] is True  # ended_by_delete


def test_updates_that_change_nothing_tracked_do_not_create_versions(conn, source, built):
    """CDC captures every column update. A touched timestamp is not a new version of the
    subscription, and treating it as one would inflate the dimension."""
    first = sub(status="active")
    touched = {**first, "updated_at": (DAY + timedelta(hours=1)).isoformat()}

    source.produce(CDC_TOPIC, 0, change_bytes(op="c", after=first, lsn=10, effective_at=DAY))
    source.produce(
        CDC_TOPIC,
        0,
        change_bytes(
            op="u", before=first, after=touched, lsn=20, effective_at=DAY + timedelta(hours=1)
        ),
    )

    built()

    assert len(versions(conn)) == 1

    # The count was the easy half. A no-op update must also leave the surviving version
    # *open*: if the window functions are computed before the no-op is filtered out, this
    # version points at the discarded row and is closed at its timestamp, so the subscription
    # ends up with no current version at all.
    _status, _valid_from, valid_to, is_current, _ended = versions(conn)[0]
    assert is_current, "a no-op update must not close the only version"
    assert valid_to.year == 9999, f"interval closed early at {valid_to}"


def test_a_noop_update_between_versions_leaves_no_gap(conn, source, built):
    """Consecutive versions must abut exactly.

    A gap is invisible to an overlap check and fatal to a point-in-time join: any as-of query
    landing inside it finds no row and silently drops the subscription.
    """
    trial = sub(status="trial")
    touched = {**trial, "updated_at": (DAY + timedelta(hours=1)).isoformat()}
    active = {**trial, "status": "active", "mrr_cents": 9900}

    source.produce(CDC_TOPIC, 0, change_bytes(op="c", after=trial, lsn=10, effective_at=DAY))
    # A write that touches nothing modelled, between two real versions.
    source.produce(
        CDC_TOPIC,
        0,
        change_bytes(
            op="u", before=trial, after=touched, lsn=20, effective_at=DAY + timedelta(hours=1)
        ),
    )
    source.produce(
        CDC_TOPIC,
        0,
        change_bytes(
            op="u", before=touched, after=active, lsn=30, effective_at=DAY + timedelta(hours=2)
        ),
    )

    built()

    rows = versions(conn)
    assert len(rows) == 2, f"expected trial and active, got {[r[0] for r in rows]}"
    assert rows[0][2] == rows[1][1], (
        f"gap: first version ends {rows[0][2]}, second begins {rows[1][1]}"
    )
    assert rows[1][3], "the latest version must be current"


def test_a_redelivered_change_does_not_duplicate_a_version(conn, source, built):
    """At-least-once delivery means the same LSN can arrive twice."""
    created = sub(status="trial")
    envelope = change_bytes(op="c", after=created, lsn=10, effective_at=DAY)
    source.produce(CDC_TOPIC, 0, envelope)
    source.produce(CDC_TOPIC, 0, envelope)  # redelivery, same LSN

    built()

    assert len(versions(conn)) == 1
    assert conn.execute("SELECT count(*) FROM raw.cdc_changes").fetchone()[0] == 2  # audit kept


def test_snapshot_rows_are_marked_as_having_no_prior_history(conn, source, built):
    """At go-live the initial snapshot says 'cancelled' for a subscription that churned last
    March, with no earlier transitions. Anything measuring time-in-status must exclude it."""
    source.produce(
        CDC_TOPIC,
        0,
        change_bytes(op="r", after=sub(status="cancelled"), lsn=1, snapshot=True, effective_at=DAY),
    )

    built()

    assert (
        conn.execute(
            "SELECT is_initial_snapshot FROM core.dim_subscription WHERE subscription_id = 1"
        ).fetchone()[0]
        is True
    )


def test_reconciliation_test_fails_when_cdc_missed_a_change(conn, source, dbt_env):  # noqa: F811
    """The check that catches a slot invalidation: the warehouse says 'trial', the source says
    'active', and nothing in the CDC-derived data alone could reveal that."""
    trial = sub(status="trial")
    source.produce(CDC_TOPIC, 0, change_bytes(op="c", after=trial, lsn=10, effective_at=DAY))
    load_cdc(conn, source)

    # The source has moved on; the change that took it there never arrived.
    conn.execute(
        "INSERT INTO raw.oltp_snapshots (snapshot_at, source_table, pk, row_data) "
        "VALUES (%s, 'subscriptions', '1', %s)",
        (DAY + timedelta(days=1), json.dumps(sub(status="active", mrr_cents=25000))),
    )
    conn.commit()

    # `run`, not `build`: build would execute the reconciliation test as part of the same
    # invocation, and this test needs to assert on it separately.
    dbt("run", "--select", "stg_subscription_changes", "dim_subscription", env=dbt_env)
    failures = dbt(
        "test",
        "--select",
        "assert_scd2_matches_oltp_snapshot",
        env=dbt_env,
        expect_failure=True,
    )

    assert "assert_scd2_matches_oltp_snapshot" in failures.stdout
    assert "Got 1 result" in failures.stdout

    # With a gap already recorded, the same mismatch is the platform's known recovery state
    # rather than a new alarm.
    conn.execute(
        "INSERT INTO ops.cdc_gaps (reason, slot_name) VALUES ('slot invalidated', 'analytics_cdc')"
    )
    conn.commit()
    dbt("test", "--select", "assert_scd2_matches_oltp_snapshot", env=dbt_env)
