"""The reconciliation snapshot: read from the source, written to the warehouse."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from loader.snapshot import prune_snapshots, take_snapshot

NOW = datetime(2026, 3, 4, 2, 0, tzinfo=UTC)


def seed_account_with_subscription(app_conn, status: str = "active") -> None:
    app_conn.execute(
        "INSERT INTO accounts (id, company_name, country) VALUES (1, 'Acme', 'AU') "
        "ON CONFLICT (id) DO NOTHING"
    )
    app_conn.execute(
        "INSERT INTO plans (code, name, monthly_price_cents, seat_price_cents) "
        "VALUES ('team', 'Team', 9900, 1500) ON CONFLICT (code) DO NOTHING"
    )
    app_conn.execute(
        "INSERT INTO subscriptions "
        "(id, account_id, plan_code, status, seats, mrr_cents, started_at) "
        "VALUES (1, 1, 'team', %s, 5, 25000, %s) "
        "ON CONFLICT (id) DO UPDATE SET status = excluded.status",
        (status, NOW),
    )
    app_conn.commit()


def test_snapshot_copies_current_source_state(app_conn, conn):
    seed_account_with_subscription(app_conn, status="past_due")

    counts = take_snapshot(app_conn, conn, now=NOW)

    assert counts == {"subscriptions": 1, "accounts": 1}
    row = conn.execute(
        "SELECT row_data FROM raw.oltp_snapshots WHERE source_table = 'subscriptions'"
    ).fetchone()[0]
    assert row["status"] == "past_due"
    assert row["seats"] == 5


def test_snapshot_reflects_later_changes(app_conn, conn):
    """Each run is a fresh read, with no dependence on offsets, LSNs or prior snapshots —
    which is exactly why it can detect that CDC lost something."""
    seed_account_with_subscription(app_conn, status="active")
    take_snapshot(app_conn, conn, now=NOW)

    app_conn.execute("UPDATE subscriptions SET status = 'cancelled' WHERE id = 1")
    app_conn.commit()
    take_snapshot(app_conn, conn, now=NOW + timedelta(days=1))

    statuses = [
        r[0]["status"]
        for r in conn.execute(
            "SELECT row_data FROM raw.oltp_snapshots WHERE source_table = 'subscriptions' "
            "ORDER BY snapshot_at"
        ).fetchall()
    ]
    assert statuses == ["active", "cancelled"]


def test_pruning_keeps_only_recent_snapshots(app_conn, conn):
    seed_account_with_subscription(app_conn)
    for day in range(10):
        take_snapshot(app_conn, conn, now=NOW + timedelta(days=day))

    prune_snapshots(conn, keep=3)

    remaining = conn.execute(
        "SELECT count(DISTINCT snapshot_at) FROM raw.oltp_snapshots"
    ).fetchone()[0]
    assert remaining == 3
