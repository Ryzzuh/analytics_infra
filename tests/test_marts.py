"""The full warehouse build: raw -> staging -> core -> marts (M3's criterion, SPEC.md §14).

Data goes in through the real loader, and dbt runs for real against the embedded Postgres, so
these tests exercise the same path production does rather than fixtures dropped into tables.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from loader import load_partition
from loader.targets import cdc_target
from loader.testing import change_bytes, event_bytes
from test_dbt_staging import dbt, dbt_env  # noqa: F401  (fixture reuse)

pytestmark = pytest.mark.dbt  # these invoke dbt for real

EVENTS_TOPIC = "product.feature_usage"
CDC_TOPIC = "cdc.app.public.subscriptions"
INVOICE_TOPIC = "cdc.app.public.invoices"
DAY = datetime(2026, 3, 1, tzinfo=UTC)


def subscription(account_id: int, **overrides) -> dict:
    row = {
        "id": account_id,
        "account_id": account_id,
        "plan_code": "team",
        "status": "active",
        "seats": 10,
        "mrr_cents": 24900,
        "started_at": DAY.isoformat(),
        "ended_at": None,
    }
    row.update(overrides)
    return row


def account(account_id: int, **overrides) -> dict:
    row = {
        "id": account_id,
        "company_name": f"Account {account_id}",
        "country": "AU",
        "created_at": DAY.isoformat(),
    }
    row.update(overrides)
    return row


@pytest.fixture
def warehouse(conn, source, dbt_env):  # noqa: F811
    """Loads every produced topic, then builds the whole project."""

    def _build(load_at: datetime | None = None):
        for topic, target in (
            (EVENTS_TOPIC, None),
            (CDC_TOPIC, cdc_target()),
            (INVOICE_TOPIC, cdc_target()),
            ("cdc.app.public.accounts", cdc_target()),
            ("cdc.app.public.support_tickets", cdc_target()),
            ("cdc.app.public.users", cdc_target()),
        ):
            if not source.partitions(topic):
                continue
            kwargs = {"target": target} if target else {}
            load_partition(
                conn,
                source,
                topic=topic,
                partition_id=0,
                dag_run_id=f"run-{topic}",
                now=load_at,
                **kwargs,
            )
        conn.commit()
        dbt("build", env=dbt_env)

    return _build


def emit_activity(source, account_id: int, *, day: datetime, count: int, users: int = 3) -> None:
    for i in range(count):
        source.produce(
            EVENTS_TOPIC,
            0,
            event_bytes(
                account_id=account_id,
                user_id=account_id * 100 + (i % users),
                event_type="feature_invoked",
                event_time=day + timedelta(minutes=i),
                received_at=day + timedelta(minutes=i, seconds=1),
                payload={"feature": f"f{i % 4}", "surface": "web", "duration_ms": 120},
            ),
        )


def seed_account(source, account_id: int, *, lsn: int, **sub_overrides) -> None:
    source.produce(
        "cdc.app.public.accounts",
        0,
        change_bytes(
            op="c", after=account(account_id), lsn=lsn, source_table="accounts", effective_at=DAY
        ),
    )
    source.produce(
        CDC_TOPIC,
        0,
        change_bytes(
            op="c",
            after=subscription(account_id, **sub_overrides),
            lsn=lsn + 1,
            effective_at=DAY,
        ),
    )


def health(conn, account_id: int) -> dict:
    row = conn.execute(
        "SELECT churn_score, churn_band, reasons, seat_utilisation, days_since_active, "
        "events_recent, events_prior FROM marts.mart_account_health WHERE account_id = %s",
        (account_id,),
    ).fetchone()
    assert row is not None, f"account {account_id} missing from mart_account_health"
    keys = (
        "score",
        "band",
        "reasons",
        "seat_utilisation",
        "days_since_active",
        "events_recent",
        "events_prior",
    )
    return dict(zip(keys, row, strict=True))


def test_whole_project_builds_and_tests_pass(conn, source, warehouse):
    """M3's completion criterion: a green build end to end, not just models that compile."""
    seed_account(source, 1, lsn=10)
    emit_activity(source, 1, day=DAY, count=20)

    warehouse()

    assert conn.execute("SELECT count(*) FROM marts.mart_account_health").fetchone()[0] == 1
    assert conn.execute("SELECT count(*) FROM marts.mart_revenue_mrr").fetchone()[0] >= 1


def test_a_healthy_account_scores_low_and_a_collapsing_one_scores_high(conn, source, warehouse):
    """The score has to separate these two, or it is decoration."""
    # Steady account: consistent usage across both windows, most seats in use.
    seed_account(source, 1, lsn=10, seats=3)
    for week in range(8):
        emit_activity(source, 1, day=DAY + timedelta(weeks=week), count=30, users=3)

    # Collapsing account: busy, then silent for the recent window.
    seed_account(source, 2, lsn=100, seats=20)
    for week in range(4):
        emit_activity(source, 2, day=DAY + timedelta(weeks=week), count=30, users=2)

    warehouse()

    healthy, collapsing = health(conn, 1), health(conn, 2)
    assert healthy["score"] < collapsing["score"]
    assert healthy["band"] in {"low", "medium"}
    assert collapsing["band"] in {"high", "critical"}

    reasons = {r for r in collapsing["reasons"]}
    assert "dormant" in reasons or "usage_drop" in reasons
    assert "seats_unused" in reasons  # 2 users against 20 seats


def test_every_score_component_is_explained(conn, source, warehouse):
    """A score with no reasons cannot be acted on by the product that receives it."""
    seed_account(source, 1, lsn=10, status="past_due", seats=50)
    emit_activity(source, 1, day=DAY, count=5, users=1)
    source.produce(
        INVOICE_TOPIC,
        0,
        change_bytes(
            op="u",
            before={"id": 1, "account_id": 1, "status": "open"},
            after={
                "id": 1,
                "account_id": 1,
                "provider_invoice_id": "in_1",
                "amount_cents": 24900,
                "status": "payment_failed",
                "issued_at": DAY.isoformat(),
                "paid_at": None,
            },
            lsn=50,
            source_table="invoices",
            effective_at=DAY,
        ),
    )

    warehouse()

    scored = health(conn, 1)
    reasons = set(scored["reasons"])
    assert {"past_due", "payment_failures", "seats_unused"} <= reasons
    assert scored["score"] > 0


def test_an_erased_account_is_not_scored(conn, source, warehouse):
    """Nothing to score, and nowhere to send it: reverse ETL would 404 on every attempt."""
    seed_account(source, 1, lsn=10)
    emit_activity(source, 1, day=DAY, count=5)
    source.produce(
        "cdc.app.public.accounts",
        0,
        change_bytes(
            op="d",
            before=account(1),
            lsn=99,
            source_table="accounts",
            effective_at=DAY + timedelta(days=1),
        ),
    )

    warehouse()

    assert conn.execute("SELECT count(*) FROM marts.mart_account_health").fetchone()[0] == 0


def test_mrr_survives_a_later_cancellation(conn, source, warehouse):
    """Built from daily history, so cancelling in April does not erase March's revenue."""
    seed_account(source, 1, lsn=10, mrr_cents=24900)
    source.produce(
        CDC_TOPIC,
        0,
        change_bytes(
            op="u",
            before=subscription(1),
            after=subscription(1, status="cancelled", mrr_cents=0),
            lsn=20,
            effective_at=DAY + timedelta(days=30),
        ),
    )
    emit_activity(source, 1, day=DAY, count=3)

    warehouse()

    march_mrr = conn.execute(
        "SELECT mrr_cents FROM marts.mart_revenue_mrr WHERE date_day = %s AND plan_code = 'team'",
        (DAY.date(),),
    ).fetchone()[0]
    after_cancel = conn.execute(
        "SELECT coalesce(sum(mrr_cents), 0) FROM marts.mart_revenue_mrr WHERE date_day = %s",
        ((DAY + timedelta(days=31)).date(),),
    ).fetchone()[0]

    assert march_mrr == 24900
    assert after_cancel == 0


def test_a_late_event_updates_the_day_it_happened(conn, source, warehouse, dbt_env):  # noqa: F811
    """The daily aggregate keys off event_time, not load time, so a two-day-old event rebuilds
    its own day rather than inflating today.

    Load times are simulated alongside the data: an event dated March but loaded today is six
    months stale by load lag, and the cutoff would hold it back — correctly, but that is the
    quarantine behaviour, not this one (SPEC.md §6.2).
    """
    seed_account(source, 1, lsn=10)
    emit_activity(source, 1, day=DAY, count=4)
    warehouse(load_at=DAY + timedelta(hours=1))

    before = conn.execute(
        "SELECT events FROM core.fct_account_activity_daily "
        "WHERE account_id = 1 AND activity_date = %s",
        (DAY.date(),),
    ).fetchone()[0]

    # Arrives two days later, but belongs to DAY.
    source.produce(
        EVENTS_TOPIC,
        0,
        event_bytes(
            account_id=1,
            event_time=DAY + timedelta(hours=3),
            received_at=DAY + timedelta(days=2),
            payload={"feature": "export", "surface": "mobile", "duration_ms": 90},
        ),
    )
    load_partition(
        conn,
        source,
        topic=EVENTS_TOPIC,
        partition_id=0,
        dag_run_id="late-run",
        now=DAY + timedelta(days=2),  # arrived two days late: inside the tolerance window
    )
    dbt("build", "--select", "stg_product_events+", env=dbt_env)

    after = conn.execute(
        "SELECT events FROM core.fct_account_activity_daily "
        "WHERE account_id = 1 AND activity_date = %s",
        (DAY.date(),),
    ).fetchone()[0]

    assert after == before + 1
