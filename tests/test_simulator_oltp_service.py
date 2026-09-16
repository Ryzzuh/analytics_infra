"""The OLTP service loop: the caller that was missing.

`OltpWriter` was covered by tests from the start, and every one of them passed while nothing in
the running system ever constructed one. These tests exercise the *service* — the thing compose
actually starts — because that is the gap the unit tests could not see.
"""

from __future__ import annotations

from simulator.oltp_main import run

# Small but not trivial: enough accounts that sampling picks different subscriptions across
# ticks, few enough that seeding stays fast against embedded Postgres.
ACCOUNTS = 6


def counts(conn) -> tuple[int, int]:
    accounts = conn.execute("SELECT count(*) FROM accounts").fetchone()[0]
    subscriptions = conn.execute("SELECT count(*) FROM subscriptions").fetchone()[0]
    return accounts, subscriptions


def test_seeds_a_population_so_cdc_has_a_source(app_pg_uri, app_conn):
    """The failure this whole service exists to fix: an empty source database."""
    assert counts(app_conn) == (0, 0)

    stats = run(
        dsn=app_pg_uri,
        accounts=ACCOUNTS,
        interval=0.0,
        transitions_per_tick=1,
        seat_change_rate=0.0,
        erasure_rate=0.0,
        heartbeat_seconds=0.0,
        history_days=0,
        seed=11,
        duration=0.0,  # seed only, no steady-state ticks
    )

    assert stats["signups"] == ACCOUNTS
    assert counts(app_conn) == (ACCOUNTS, ACCOUNTS)


def test_restart_does_not_reseed(app_pg_uri, app_conn):
    """A restarted container must not double the population.

    Compose restarts this service on failure, and a seeding step that ran unconditionally would
    grow the source by `SIM_ACCOUNTS` every crash.
    """
    common = dict(
        dsn=app_pg_uri,
        accounts=ACCOUNTS,
        interval=0.0,
        transitions_per_tick=1,
        seat_change_rate=0.0,
        erasure_rate=0.0,
        heartbeat_seconds=0.0,
        history_days=0,
        duration=0.0,
    )
    run(seed=11, **common)
    second = run(seed=12, **common)

    assert second["signups"] == 0
    assert counts(app_conn)[0] == ACCOUNTS


def test_steady_state_writes_and_heartbeats(app_pg_uri, app_conn):
    """The loop must actually tick: write to subscriptions and move the heartbeat.

    The heartbeat is the part that is easy to leave out and impossible to notice — its whole
    job is to keep the replication slot's confirmed LSN moving while the captured tables are
    idle, so a missing heartbeat only shows up as WAL growth days later (SPEC.md §4.2).
    """
    before = app_conn.execute("SELECT beat_at FROM debezium_heartbeat WHERE id = 1").fetchone()[0]

    stats = run(
        dsn=app_pg_uri,
        accounts=ACCOUNTS,
        interval=0.0,
        transitions_per_tick=2,
        seat_change_rate=1.0,  # deterministic: every sampled subscription gets a seat change
        erasure_rate=0.0,
        heartbeat_seconds=0.0,  # beat on every tick
        history_days=0,
        seed=13,
        duration=0.3,
    )

    assert stats["beats"] >= 1, "the loop never completed a tick"
    assert stats["seat_changes"] >= 1

    after = app_conn.execute("SELECT beat_at FROM debezium_heartbeat WHERE id = 1").fetchone()[0]
    assert after > before

    # Seat changes move mrr_cents without touching status, which is the case that distinguishes
    # "the row was written" from "a new SCD2 version is warranted".
    touched = app_conn.execute(
        "SELECT count(*) FROM subscriptions WHERE updated_at IS NOT NULL"
    ).fetchone()[0]
    assert touched >= 1


def test_erasure_hard_deletes_an_account(app_pg_uri, app_conn):
    """GDPR erasure is a hard delete, which is what produces a tombstone on the wire."""
    run(
        dsn=app_pg_uri,
        accounts=ACCOUNTS,
        interval=0.0,
        transitions_per_tick=1,
        seat_change_rate=0.0,
        erasure_rate=0.0,
        heartbeat_seconds=0.0,
        history_days=0,
        seed=11,
        duration=0.0,
    )
    seeded = counts(app_conn)[0]

    stats = run(
        dsn=app_pg_uri,
        accounts=0,  # do not backfill the deleted account, so the drop is observable
        interval=0.0,
        transitions_per_tick=1,
        seat_change_rate=0.0,
        erasure_rate=1.0,  # erase on every tick
        heartbeat_seconds=1e6,
        history_days=0,
        seed=17,
        duration=0.05,
    )

    assert stats["erasures"] >= 1
    assert counts(app_conn)[0] < seeded
