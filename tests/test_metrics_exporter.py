"""The platform's own metrics, and the contract between them and the alert rules."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parents[1]
RULES = REPO / "infra" / "monitoring" / "rules" / "platform.yml"
NOW = datetime(2026, 9, 16, 6, 0, tzinfo=UTC)


@pytest.fixture
def exporter(conn, pg_uri, monkeypatch):
    from metrics_exporter import app as module

    monkeypatch.setattr(module, "WAREHOUSE_DSN", pg_uri)
    return TestClient(module.app)


def scrape(client) -> dict[str, float]:
    """Flatten the exposition format to {name{labels}: value}, ignoring HELP/TYPE lines."""
    response = client.get("/metrics")
    assert response.status_code == 200
    samples = {}
    for line in response.text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        name, value = line.rsplit(" ", 1)
        samples[name] = float(value)
    return samples


def test_freshness_and_its_slo_are_both_published(conn, exporter):
    """The alert compares one to the other, so publishing only the observation would leave the
    threshold duplicated in the rule, where it could drift."""
    conn.execute(
        "INSERT INTO ops.dq_results (check_name, target, status, observed, threshold) "
        "VALUES ('freshness', 'staging', 'fail', 7200, 3600)"
    )

    samples = scrape(exporter)

    assert samples['analytics_freshness_seconds{layer="staging"}'] == 7200.0
    assert samples['analytics_freshness_slo_seconds{layer="staging"}'] == 3600.0


def test_only_the_latest_result_per_check_is_published(conn, exporter):
    """A gauge showing a stale failure after the next run passed would be worse than no gauge."""
    for status, observed, minutes_ago in (("fail", 0.2, 60), ("pass", 0.001, 1)):
        conn.execute(
            "INSERT INTO ops.dq_results (checked_at, check_name, target, status, observed) "
            "VALUES (%s, 'duplicate_rate', 'core.fct_account_activity_daily', %s, %s)",
            (NOW - timedelta(minutes=minutes_ago), status, observed),
        )

    samples = scrape(exporter)

    key = (
        'analytics_dq_check_status{check="duplicate_rate",target="core.fct_account_activity_daily"}'
    )
    assert samples[key] == 0.0  # pass


def test_dq_status_is_numeric_so_rules_can_compare_it(conn, exporter):
    for status, target in (("fail", "a"), ("warn", "b"), ("pass", "c")):
        conn.execute(
            "INSERT INTO ops.dq_results (check_name, target, status) VALUES ('x', %s, %s)",
            (target, status),
        )

    samples = scrape(exporter)

    assert samples['analytics_dq_check_status{check="x",target="a"}'] == 2.0
    assert samples['analytics_dq_check_status{check="x",target="b"}'] == 1.0
    assert samples['analytics_dq_check_status{check="x",target="c"}'] == 0.0


def test_an_open_chaos_window_is_visible(conn, exporter):
    """Alertmanager inhibits paging on this, so it has to be exported reliably."""
    assert scrape(exporter)["analytics_chaos_window_active"] == 0.0

    conn.execute("INSERT INTO ops.chaos_windows (scenario) VALUES ('connector_outage')")
    assert scrape(exporter)["analytics_chaos_window_active"] == 1.0

    conn.execute("UPDATE ops.chaos_windows SET closed_at = now()")
    assert scrape(exporter)["analytics_chaos_window_active"] == 0.0


def test_unresolved_cdc_gaps_and_blocking_drift_are_counted(conn, exporter):
    conn.execute("INSERT INTO ops.cdc_gaps (reason) VALUES ('slot invalidated')")
    conn.execute(
        "INSERT INTO ops.drift_findings (event_type, json_path, change, blocking) "
        "VALUES ('subscription_changed', 'plan_id', 'field_removed', true)"
    )
    conn.execute(
        "INSERT INTO ops.drift_findings (event_type, json_path, change, blocking) "
        "VALUES ('page_view', 'new_thing', 'field_added', false)"
    )

    samples = scrape(exporter)

    assert samples["analytics_cdc_gap_open"] == 1.0
    assert samples["analytics_drift_blocking_models"] == 1.0  # the non-blocking one is not counted


def test_seconds_since_last_load_is_published_per_topic(conn, exporter):
    conn.execute(
        "INSERT INTO ops.load_ledger (dag_run_id, topic, partition_id, start_offset, "
        "end_offset, loaded_date, created_at) VALUES ('r1', 'product.session', 0, 0, 10, "
        "current_date, now() - interval '20 minutes')"
    )

    samples = scrape(exporter)

    assert samples['analytics_seconds_since_last_load{topic="product.session"}'] == pytest.approx(
        1200, abs=30
    )


def test_a_quiet_source_reports_zero_writes_not_a_stalled_connector(conn, exporter):
    """The CDC alert requires both signals: without this one, an idle database would page."""
    samples = scrape(exporter)

    assert samples["analytics_oltp_writes_per_minute"] == 0.0
    assert samples["analytics_seconds_since_last_cdc_change"] == 1e9  # nothing ever loaded


def test_every_analytics_metric_an_alert_uses_is_actually_exported(conn, exporter):
    """The failure this prevents: a rule that can never fire because nothing publishes its
    metric. Reviewing either file alone would not catch it."""
    # Labelled gauges only appear once a label combination exists, so give each family one
    # row. The contract under test is "the exporter can publish this", not "it always does".
    conn.execute(
        "INSERT INTO ops.dq_results (check_name, target, status, observed) "
        "VALUES ('freshness', 'staging', 'pass', 30)"
    )
    conn.execute(
        "INSERT INTO ops.load_ledger (dag_run_id, topic, partition_id, start_offset, "
        "end_offset, loaded_date) VALUES ('r1', 'product.session', 0, 0, 1, current_date)"
    )

    rules = yaml.safe_load(RULES.read_text())
    referenced = set()
    for group in rules["groups"]:
        for rule in group["rules"]:
            referenced.update(re.findall(r"\banalytics_[a-z_]+\b", rule["expr"]))

    exported = {line.split("{")[0].split(" ")[0] for line in scrape(exporter)}

    assert referenced, "no analytics_* metrics referenced by any rule"
    assert referenced <= exported, f"alerts reference unpublished metrics: {referenced - exported}"
