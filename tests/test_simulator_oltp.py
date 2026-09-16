"""The simulator's OLTP writes: the source of every CDC event."""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

from simulator.oltp import OltpWriter, replay_history

MARCH = datetime(2026, 3, 4, tzinfo=UTC)


def writer(app_conn, seed: int = 7) -> OltpWriter:
    w = OltpWriter(conn=app_conn, rng=random.Random(seed))
    w.ensure_plans()
    return w


def status_of(app_conn, subscription_id: int) -> str:
    return app_conn.execute(
        "SELECT status FROM subscriptions WHERE id = %s", (subscription_id,)
    ).fetchone()[0]


def test_lifecycle_writes_carry_business_time(app_conn):
    """Business time, not commit time: a replayed March change must date to March."""
    w = writer(app_conn)
    account_id = w.create_account(effective_at=MARCH, company="Acme")
    subscription_id = w.start_trial(account_id, effective_at=MARCH)

    effective_at = app_conn.execute(
        "SELECT effective_at FROM subscriptions WHERE id = %s", (subscription_id,)
    ).fetchone()[0]
    assert effective_at == MARCH


def test_a_no_op_advance_touches_the_row_without_changing_status(app_conn):
    """These updates are captured by CDC and must not become SCD2 versions."""
    w = writer(app_conn)
    account_id = w.create_account(effective_at=MARCH, company="Acme")
    subscription_id = w.start_trial(account_id, effective_at=MARCH)

    touched = 0
    for hour in range(40):
        result = w.advance(subscription_id, effective_at=MARCH + timedelta(hours=hour))
        if result is None:
            touched += 1
        else:
            break

    assert touched >= 1
    assert app_conn.execute(
        "SELECT s.updated_at > s.started_at FROM subscriptions s WHERE s.id = %s",
        (subscription_id,),
    ).fetchone()[0]


def test_deleting_an_account_cascades(app_conn):
    """A GDPR erasure is a hard delete. Debezium emits a delete plus a tombstone per row."""
    w = writer(app_conn)
    account_id = w.create_account(effective_at=MARCH, company="Acme")
    w.start_trial(account_id, effective_at=MARCH)

    w.delete_account(account_id)

    assert app_conn.execute("SELECT count(*) FROM subscriptions").fetchone()[0] == 0
    assert app_conn.execute("SELECT count(*) FROM accounts").fetchone()[0] == 0


def test_replay_produces_dated_history_and_several_transitions(app_conn):
    w = writer(app_conn, seed=11)

    stats = replay_history(
        w, accounts=20, start=MARCH - timedelta(days=90), end=MARCH, step=timedelta(days=3)
    )

    assert stats["accounts"] == 20
    assert stats["transitions"] > 5

    spread = app_conn.execute(
        "SELECT max(effective_at) - min(effective_at) FROM accounts"
    ).fetchone()[0]
    assert spread > timedelta(days=30)  # history is spread across the replayed window
