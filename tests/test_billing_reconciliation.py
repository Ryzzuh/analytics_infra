"""Merging the two billing paths, and measuring what the fast one missed (SPEC.md §4.3).

M5's completion criterion: the webhook-miss metric is non-zero and explained.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from loader import load_partition
from loader.targets import billing_target
from loader.testing import FakeMessageSource
from test_dbt_staging import dbt, dbt_env  # noqa: F401  (fixture reuse)

pytestmark = pytest.mark.dbt  # these invoke dbt for real

TOPIC = "billing.webhooks"
DAY = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)


def provider_event(n: int, *, account_id: int = 1, minutes: int = 0) -> dict:
    return {
        "id": f"evt_{n:04d}",
        "type": "invoice.paid",
        "account_id": account_id,
        "created_at": (DAY + timedelta(minutes=minutes)).isoformat(),
        "data": {"amount_cents": 24900},
    }


def deliver_webhook(source: FakeMessageSource, event: dict, *, times: int = 1) -> None:
    body = {**event, "received_at": (DAY + timedelta(seconds=2)).isoformat()}
    for _ in range(times):
        source.produce(TOPIC, 0, json.dumps(body).encode(), key=str(event["account_id"]).encode())


def pull_into_batch(conn, events: list[dict]) -> None:
    """Stand-in for the daily pull, which has its own tests in test_billing_pull.py."""
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO raw.billing_events_batch "
            "(provider_event_id, event_type, account_id, provider_created_at, pulled_at, payload)"
            " VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            [
                (
                    e["id"],
                    e["type"],
                    e["account_id"],
                    datetime.fromisoformat(e["created_at"]),
                    DAY + timedelta(days=1),
                    json.dumps(e),
                )
                for e in events
            ],
        )


def build(conn, source, dbt_env):  # noqa: F811
    if source.partitions(TOPIC):
        load_partition(
            conn,
            source,
            topic=TOPIC,
            partition_id=0,
            dag_run_id="billing-1",
            target=billing_target(),
        )
    dbt("build", "--select", "stg_billing_events+", env=dbt_env)


def reconciliation(conn) -> list[tuple]:
    return conn.execute(
        "SELECT billing_day, events, webhook_missed, duplicate_deliveries, webhook_miss_rate "
        "FROM staging.stg_billing_reconciliation ORDER BY billing_day"
    ).fetchall()


def test_events_seen_by_both_paths_are_one_row(conn, source, dbt_env):  # noqa: F811
    events = [provider_event(n, minutes=n) for n in range(3)]
    for event in events:
        deliver_webhook(source, event)
    pull_into_batch(conn, events)

    build(conn, source, dbt_env)

    rows = conn.execute(
        "SELECT arrived_by, count(*) FROM staging.stg_billing_events GROUP BY 1"
    ).fetchall()
    assert rows == [("both", 3)]


def test_a_dropped_webhook_shows_up_as_batch_only(conn, source, dbt_env):  # noqa: F811
    """The gap the daily pull exists to close, made visible rather than inferred."""
    events = [provider_event(n, minutes=n) for n in range(4)]
    for event in events[:3]:
        deliver_webhook(source, event)
    pull_into_batch(conn, events)  # the provider has all four

    build(conn, source, dbt_env)

    missed = conn.execute(
        "SELECT provider_event_id FROM staging.stg_billing_events WHERE webhook_missed"
    ).fetchall()
    assert missed == [("evt_0003",)]

    day, events_count, missed_count, _dupes, miss_rate = reconciliation(conn)[0]
    assert (day, events_count, missed_count) == (DAY.date(), 4, 1)
    assert float(miss_rate) == 0.25


def test_duplicate_deliveries_collapse_but_are_counted(conn, source, dbt_env):  # noqa: F811
    """At-least-once delivery is normal. Silently collapsing duplicates without counting them
    would hide a provider that had started retrying everything."""
    event = provider_event(1)
    deliver_webhook(source, event, times=3)
    pull_into_batch(conn, [event])

    build(conn, source, dbt_env)

    deliveries = conn.execute(
        "SELECT webhook_deliveries FROM staging.stg_billing_events"
    ).fetchone()[0]
    assert deliveries == 3
    assert conn.execute("SELECT count(*) FROM staging.stg_billing_events").fetchone()[0] == 1
    assert reconciliation(conn)[0][3] == 1  # counted as a day with duplicate deliveries


def test_events_not_yet_pulled_are_excluded_from_the_miss_rate(conn, source, dbt_env):  # noqa: F811
    """Today's events are newer than the last daily pull. Counting them would report a ~100%
    miss rate every morning, which is how a metric becomes background noise."""
    fresh = provider_event(9, minutes=5)
    deliver_webhook(source, fresh)  # webhook arrived; the pull has not run since

    build(conn, source, dbt_env)

    assert (
        conn.execute("SELECT not_yet_reconciled FROM staging.stg_billing_events").fetchone()[0]
        is True
    )
    assert reconciliation(conn) == []


def test_a_mostly_broken_fast_path_fails_the_test(conn, source, dbt_env):  # noqa: F811
    """Missing some webhooks is designed for; missing most of them is a broken provider."""
    events = [provider_event(n, minutes=n) for n in range(40)]
    for event in events[:5]:
        deliver_webhook(source, event)
    pull_into_batch(conn, events)

    load_partition(
        conn, source, topic=TOPIC, partition_id=0, dag_run_id="billing-1", target=billing_target()
    )
    dbt("run", "--select", "stg_billing_events+", env=dbt_env)
    failure = dbt(
        "test", "--select", "assert_billing_webhook_miss_rate", env=dbt_env, expect_failure=True
    )

    assert "assert_billing_webhook_miss_rate" in failure.stdout
    assert float(reconciliation(conn)[0][4]) > 0.25
