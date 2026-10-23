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

import json

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


def test_replayed_history_belongs_to_accounts_that_exist(app_conn):
    """The backfill path has the same obligation as the live one.

    `catch_up_history` and `generate_event_history` defaulted to inventing ids 1..2000, so a
    replay run in September wrote history for accounts that were not created until October.
    A brand-new account then arrived in the warehouse holding usage it never had and no recent
    activity to match -- and the churn model read the gap between the two as a collapse in
    usage, scoring it as critical risk on its first day.

    The live generator reading real ids is not enough on its own: both producers write to the
    same table, and one inventing a population poisons it for the other.
    """
    from simulator.backfill import catch_up_history, generate_event_history

    assert "account_pool" in generate_event_history.__code__.co_varnames
    assert "account_pool" in catch_up_history.__code__.co_varnames, (
        "catch_up_history must accept a real population, not only a count"
    )

    app_conn.execute(
        "INSERT INTO accounts (id, company_name, country) VALUES (8001, 'Exists', 'AU')"
    )
    app_conn.commit()
    pool = load_accounts(app_conn, seed=1)
    assert [a.account_id for a in pool] == [8001]
    assert all(a.account_id != 1267 for a in pool), (
        "1267 is the id the 2000-wide default invented; no such account exists"
    )


def test_the_storm_targets_real_accounts(app_conn):
    """The third producer with the same defect.

    storm.py hard-coded randint(1, 500), so a chaos run manufactured events for whatever ids
    that range happened to hit. All three producers -- live, backfill and storm -- write to the
    same table, so one inventing a population undoes the other two. A storm is also run while
    the pipeline is deliberately under strain, which is the worst moment to be adding rows the
    warehouse cannot reconcile against the source.
    """
    from simulator.storm import late_duplicate_storm

    app_conn.execute(
        "INSERT INTO accounts (id, company_name, country) VALUES (9001, 'Stormed', 'AU')"
    )
    app_conn.commit()
    pool = load_accounts(app_conn, seed=99)

    sent: list[dict] = []

    class Sink:
        def send(self, topic, payload, key=None):
            sent.append(json.loads(payload))

        def flush(self):
            pass

    late_duplicate_storm(
        Sink(), events=25, days_late=2, duplicate_rate=0.0, accounts=pool
    )

    ids = {e["account_id"] for e in sent}
    assert ids == {9001}, f"storm hit accounts that do not exist: {sorted(ids - {9001})}"
    assert all(e["user_id"] in set(pool[0].users) for e in sent), (
        "user ids must belong to the account, not be drawn from an arbitrary range"
    )
