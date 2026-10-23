"""The product-event generator must emit for accounts that actually exist.

`build_accounts` invented ids 1..count with no reference to the OLTP source, and the two
simulators therefore never agreed on who the customers were. The damage only became visible
once the account population moved: events sat against ids created long after those events, so
`events_prior` counted a history that was not the account's, while every account past the
hard-coded range received nothing at all. The churn model read that silence as dormancy and
scored brand-new customers as critical risk -- 184 accounts at an identical 70.

Nothing downstream could catch it. Every event was well-formed, the collector accepted them,
the loader loaded them and the mart faithfully reported what it was given.
"""

from __future__ import annotations

from simulator.main import build_accounts, load_accounts


def test_accounts_come_from_the_source_database(app_conn):
    """The ids emitted against must be ids that exist."""
    app_conn.execute(
        "INSERT INTO accounts (id, company_name, country) VALUES "
        "(7001, 'Real Co', 'AU'), (7002, 'Other Co', 'AU')"
    )
    app_conn.commit()

    loaded = load_accounts(app_conn, seed=1)
    ids = {a.account_id for a in loaded}

    assert ids == {7001, 7002}, "the generator must mirror the source, not invent a range"
    assert 1 not in ids, "id 1 is what the hard-coded range produced and no account holds it"


def test_plan_and_seats_track_the_subscription(app_conn):
    """Seats drive the user list, so inventing them desynchronises active-user counts too."""
    app_conn.execute(
        "INSERT INTO plans (code, name, monthly_price_cents, seat_price_cents) "
        "VALUES ('business', 'Business', 50000, 2000) ON CONFLICT (code) DO NOTHING"
    )
    app_conn.execute(
        "INSERT INTO accounts (id, company_name, country) VALUES (7003, 'Seated', 'AU')"
    )
    app_conn.execute(
        "INSERT INTO subscriptions "
        "(account_id, plan_code, status, seats, mrr_cents, started_at, effective_at) "
        "VALUES (7003, 'business', 'active', 25, 50000, now(), now())"
    )
    app_conn.commit()

    account = load_accounts(app_conn, seed=1)[0]
    assert account.plan == "business"
    assert account.seats == 25
    assert len(account.users) == 25


def test_an_empty_source_yields_no_accounts(app_conn):
    """Better to emit nothing than to emit against ids that do not exist.

    The old fallback was to invent a population, which is how this went unnoticed: the stream
    looked healthy whether or not it corresponded to anything.
    """
    assert load_accounts(app_conn, seed=1) == []


def test_intensity_is_stable_across_reloads(app_conn):
    """Partition skew has to persist to be worth measuring.

    The population is re-read as accounts sign up and are erased. Redrawing intensity each time
    would average the skew away over a long run -- the opposite of what SPEC.md 17 wants to
    demonstrate.
    """
    app_conn.execute(
        "INSERT INTO accounts (id, company_name, country) VALUES (7004, 'Busy', 'AU')"
    )
    app_conn.commit()

    first = load_accounts(app_conn, seed=5)[0].intensity
    second = load_accounts(app_conn, seed=5)[0].intensity
    assert first == second

    assert load_accounts(app_conn, seed=6)[0].intensity != first, "seed must still vary it"


def test_synthetic_fallback_is_still_available_for_tests():
    """Kept deliberately, and only for running without a source database."""
    accounts = build_accounts(3, seed=1)
    assert [a.account_id for a in accounts] == [1, 2, 3]
