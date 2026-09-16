"""Data-quality checks: the failures where every row is valid and the shape is wrong."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from dq import CheckResult, run_checks
from dq.checks import duplicate_rate, freshness, quarantine_rate, reverse_etl_health, volume_anomaly

NOW = datetime(2026, 9, 16, 6, 0, tzinfo=UTC)


@pytest.fixture
def warehouse(conn):
    """Just enough of the modelled warehouse for the checks to read. dbt builds these for real
    elsewhere; here the point is the check logic."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS core")
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute("CREATE SCHEMA IF NOT EXISTS marts")
    conn.execute(
        """
        CREATE TABLE core.fct_account_activity_daily (
            account_id bigint, activity_date date, events bigint,
            duplicate_copies bigint DEFAULT 0
        )
        """
    )
    conn.execute(
        "CREATE TABLE staging.stg_product_events (event_id uuid DEFAULT gen_random_uuid(), "
        "loaded_at timestamptz)"
    )
    conn.execute(
        "CREATE TABLE staging.stg_product_events_quarantined (event_id uuid "
        "DEFAULT gen_random_uuid(), loaded_at timestamptz)"
    )
    conn.execute("CREATE TABLE marts.mart_account_health (account_id bigint, as_of_date date)")
    return conn


def daily_volume(conn, *, days: int, events: int, duplicates: int = 0, end: datetime = NOW) -> None:
    for offset in range(1, days + 1):
        conn.execute(
            "INSERT INTO core.fct_account_activity_daily VALUES (1, %s, %s, %s)",
            ((end - timedelta(days=offset)).date(), events, duplicates),
        )


def test_a_collapse_in_volume_is_caught(warehouse):
    daily_volume(warehouse, days=7, events=1000, end=NOW - timedelta(days=1))
    warehouse.execute(
        "INSERT INTO core.fct_account_activity_daily VALUES (1, %s, 120, 0)",
        ((NOW - timedelta(days=1)).date(),),
    )

    result = volume_anomaly(warehouse, now=NOW)

    assert result.status == "fail"
    assert result.observed == pytest.approx(0.12)


def test_normal_variation_is_not_an_anomaly(warehouse):
    daily_volume(warehouse, days=7, events=1000, end=NOW - timedelta(days=1))
    warehouse.execute(
        "INSERT INTO core.fct_account_activity_daily VALUES (1, %s, 1150, 0)",
        ((NOW - timedelta(days=1)).date(),),
    )

    assert volume_anomaly(warehouse, now=NOW).status == "pass"


def test_one_outage_day_does_not_hide_the_next_anomaly(warehouse):
    """Why the baseline is a median: a mean would be dragged down by the outage and then
    accept the following collapse as normal."""
    daily_volume(warehouse, days=6, events=1000, end=NOW - timedelta(days=2))
    warehouse.execute(
        "INSERT INTO core.fct_account_activity_daily VALUES (1, %s, 0, 0)",
        ((NOW - timedelta(days=2)).date(),),
    )
    warehouse.execute(
        "INSERT INTO core.fct_account_activity_daily VALUES (1, %s, 300, 0)",
        ((NOW - timedelta(days=1)).date(),),
    )

    assert volume_anomaly(warehouse, now=NOW).status == "fail"


def test_a_fresh_deployment_warns_rather_than_failing_or_passing(warehouse):
    """ "No history yet" is neither healthy nor broken, and claiming either is worse."""
    daily_volume(warehouse, days=1, events=500, end=NOW)

    result = volume_anomaly(warehouse, now=NOW)

    assert result.status == "warn"
    assert result.details["reason"] == "insufficient history"


def test_duplicate_rate_is_measured_against_volume(warehouse):
    warehouse.execute(
        "INSERT INTO core.fct_account_activity_daily VALUES (1, %s, 1000, 80)", (NOW.date(),)
    )

    result = duplicate_rate(warehouse, now=NOW)

    assert result.status == "fail"
    assert result.observed == pytest.approx(0.08)


def test_a_few_duplicates_are_expected_and_pass(warehouse):
    warehouse.execute(
        "INSERT INTO core.fct_account_activity_daily VALUES (1, %s, 1000, 5)", (NOW.date(),)
    )

    assert duplicate_rate(warehouse, now=NOW).status == "pass"


def test_quarantine_rate_rises_when_clients_buffer(warehouse):
    for _ in range(95):
        warehouse.execute("INSERT INTO staging.stg_product_events (loaded_at) VALUES (%s)", (NOW,))
    for _ in range(5):
        warehouse.execute(
            "INSERT INTO staging.stg_product_events_quarantined (loaded_at) VALUES (%s)", (NOW,)
        )

    result = quarantine_rate(warehouse, now=NOW)

    assert result.status == "fail"
    assert result.observed == pytest.approx(0.05)


def test_erased_accounts_do_not_make_reverse_etl_look_broken(warehouse):
    """A 404 forever is the system working. Counting it would make the metric permanently red,
    which is the same as having no metric."""
    for account_id in range(1, 11):
        warehouse.execute(
            "INSERT INTO ops.reverse_etl_sync_state (account_id, payload_hash, score_version, "
            "status) VALUES (%s, 'h', 'rules-v1', 'skipped_client_error')",
            (account_id,),
        )
    warehouse.execute(
        "INSERT INTO ops.reverse_etl_sync_state (account_id, payload_hash, score_version, status)"
        " VALUES (99, 'h', 'rules-v1', 'synced')"
    )

    result = reverse_etl_health(warehouse, now=NOW)

    assert result.status == "pass"
    assert result.observed == 0.0


def test_a_failing_destination_is_caught(warehouse):
    for account_id in range(1, 11):
        status = "failed" if account_id <= 3 else "synced"
        warehouse.execute(
            "INSERT INTO ops.reverse_etl_sync_state (account_id, payload_hash, score_version, "
            "status) VALUES (%s, 'h', 'rules-v1', %s)",
            (account_id, status),
        )

    assert reverse_etl_health(warehouse, now=NOW).status == "fail"


def test_a_stalled_layer_breaches_its_slo(warehouse):
    warehouse.execute(
        "INSERT INTO staging.stg_product_events (loaded_at) VALUES (%s)",
        (NOW - timedelta(hours=3),),
    )

    results = {r.target: r for r in freshness(warehouse, now=NOW)}

    assert results["staging"].status == "fail"
    assert results["staging"].observed == pytest.approx(10800)


def test_a_layer_that_has_never_produced_anything_fails(warehouse):
    """On a fresh deployment, "no data yet" and "the loader is broken" look identical from
    outside, and only one of them is acceptable."""
    results = {r.target: r for r in freshness(warehouse, now=NOW)}

    assert results["raw_cdc"].status == "fail"
    assert results["raw_cdc"].details["reason"] == "no data in this layer"


def test_every_run_records_its_results_including_passes(warehouse):
    """A check that only records failures cannot answer "when did this start"."""
    daily_volume(warehouse, days=7, events=1000, end=NOW - timedelta(days=1))
    warehouse.execute(
        "INSERT INTO core.fct_account_activity_daily VALUES (1, %s, 1000, 2)",
        ((NOW - timedelta(days=1)).date(),),
    )

    results = run_checks(warehouse, now=NOW)

    recorded = warehouse.execute(
        "SELECT check_name, status FROM ops.dq_results ORDER BY check_name"
    ).fetchall()
    assert len(recorded) == len(results)
    assert any(status == "pass" for _name, status in recorded)
    assert {name for name, _status in recorded} >= {
        "volume_anomaly",
        "duplicate_rate",
        "quarantine_rate",
        "reverse_etl_error_rate",
        "freshness",
    }


def test_check_results_expose_a_simple_ok_flag():
    assert CheckResult("x", "y", "pass").ok is True
    assert CheckResult("x", "y", "warn").ok is False
