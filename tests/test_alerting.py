"""Alert delivery and routing: the message, and who does not get woken up."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml
from alert_bridge.app import app as bridge_app
from alert_bridge.app import format_alert
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parents[1]
MONITORING = REPO / "infra" / "monitoring"


def tool(name: str) -> str:
    """Skip cleanly when a validator is absent.

    `subprocess.run` raises FileNotFoundError before returning, so checking its stderr for
    "executable not found" never ran — the test failed instead of skipping.
    """
    path = shutil.which(name)
    if path is None:
        pytest.skip(f"{name} not installed")
    return path


def alert(name: str = "LoaderStalled", **overrides) -> dict:
    payload = {
        "status": "firing",
        "labels": {"alertname": name, "severity": "critical", "topic": "product.session"},
        "annotations": {
            "summary": "No successful load for product.session in 25m",
            "runbook": "docs/runbooks/loader-stalled.md",
        },
    }
    payload.update(overrides)
    return payload


# ----------------------------------------------------------------- message formatting


def test_a_message_says_what_broke_and_where_to_look():
    """An alert that arrives as a wall of labels gets swiped away."""
    text = format_alert(alert())

    assert "LoaderStalled" in text
    assert "No successful load for product.session in 25m" in text
    assert "topic=product.session" in text
    assert "docs/runbooks/loader-stalled.md" in text
    assert "severity=" not in text  # carried by the icon, not repeated as noise


def test_severity_is_visible_at_a_glance():
    assert format_alert(alert(labels={"alertname": "X", "severity": "critical"})).startswith("🔴")
    assert format_alert(alert(labels={"alertname": "X", "severity": "warning"})).startswith("🟠")


def test_a_resolved_alert_looks_different_from_a_firing_one():
    text = format_alert(alert(status="resolved"))

    assert text.startswith("✅")
    assert "(resolved)" in text


# ----------------------------------------------------------------- delivery


@pytest.fixture
def bridge(monkeypatch):
    from alert_bridge import app as module

    monkeypatch.setattr(module, "BOT_TOKEN", "")  # unconfigured, as a local run would be
    monkeypatch.setattr(module, "CHAT_ID", "")
    return TestClient(bridge_app)


def test_an_unconfigured_bridge_still_acknowledges_the_webhook(bridge):
    """Failing here would make Alertmanager retry a notification nobody can receive."""
    response = bridge.post("/alerts", json={"alerts": [alert(), alert("DagFailure")]})

    assert response.status_code == 200
    assert response.json() == {"received": 2, "sent": 0}


def test_a_telegram_outage_does_not_become_an_alert_storm(bridge, monkeypatch):
    """A 5xx back to Alertmanager retries the whole group, turning one incident into a flood."""
    import httpx
    from alert_bridge import app as module

    async def explode(_text: str) -> bool:
        raise httpx.ConnectError("telegram unreachable")

    monkeypatch.setattr(module, "send_to_telegram", explode)

    response = bridge.post("/alerts", json={"alerts": [alert()]})

    assert response.status_code == 200
    assert response.json()["sent"] == 0


# ----------------------------------------------------------------- routing configuration


def test_alertmanager_config_is_valid():
    result = subprocess.run(
        [tool("amtool"), "check-config", str(MONITORING / "alertmanager.yml")],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr[-1000:]
    assert "SUCCESS" in result.stdout


def test_chaos_inhibits_paging_but_nothing_else():
    """The routing decision that makes a public demo survivable: a stranger pressing a chaos
    button must not page, while a real incident during the same window still must be visible
    (the Console reads Prometheus directly, not Alertmanager)."""
    config = yaml.safe_load((MONITORING / "alertmanager.yml").read_text())

    chaos_rule = next(
        rule
        for rule in config["inhibit_rules"]
        if "ChaosWindowOpen" in str(rule["source_matchers"])
    )

    assert 'severity =~ "warning|critical"' in str(chaos_rule["target_matchers"])
    assert chaos_rule["equal"] == []  # global: chaos suppresses paging everywhere, not per-label


def test_informational_alerts_never_page():
    config = yaml.safe_load((MONITORING / "alertmanager.yml").read_text())

    info_route = next(
        route
        for route in config["route"]["routes"]
        if 'severity = "info"' in str(route["matchers"])
    )

    assert info_route["receiver"] == "null"


def test_repeat_interval_is_long_enough_not_to_be_muted():
    config = yaml.safe_load((MONITORING / "alertmanager.yml").read_text())

    assert config["route"]["repeat_interval"] == "4h"


# ----------------------------------------------------------------- rule unit tests


def test_every_alert_rule_passes_its_unit_tests():
    """Runs promtool's own rule tests: each alert fires on its condition, and stays quiet on
    the near misses (a micro-batch lag sawtooth, an idle source, a 1% error rate)."""
    result = subprocess.run(
        [tool("promtool"), "test", "rules", "rules_test.yml"],
        cwd=MONITORING,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout[-3000:]
    assert "SUCCESS" in result.stdout


def test_every_runbook_an_alert_links_to_exists():
    """A dangling runbook link is worse than none: it costs the reader the time to discover
    there is nothing there."""
    rules = yaml.safe_load((MONITORING / "rules" / "platform.yml").read_text())

    missing = [
        runbook
        for group in rules["groups"]
        for rule in group["rules"]
        if (runbook := rule.get("annotations", {}).get("runbook")) and not (REPO / runbook).exists()
    ]

    assert missing == []


def test_every_rule_carries_a_runbook():
    """An alert with no runbook is a 3am research project."""
    rules = yaml.safe_load((MONITORING / "rules" / "platform.yml").read_text())

    missing = [
        rule["alert"]
        for group in rules["groups"]
        for rule in group["rules"]
        if rule["labels"].get("severity") != "info" and "runbook" not in rule.get("annotations", {})
    ]

    assert missing == []
