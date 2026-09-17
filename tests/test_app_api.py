"""The product API as a reverse-ETL destination (SPEC.md §8).

Run against the real app database, because the behaviours that matter — rejecting accounts
that no longer exist, upserting rather than duplicating — are database behaviours.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def product(app_pg_uri, monkeypatch):
    from app_api import app as module

    monkeypatch.setattr(module, "APP_DSN", app_pg_uri)
    module._idempotency.clear()
    module._recent_requests.clear()
    module.RATE_LIMIT_PER_MINUTE = 600
    return module


@pytest.fixture
def client(product, app_conn):
    app_conn.execute(
        "INSERT INTO accounts (id, company_name, country) VALUES (1, 'Acme', 'AU') "
        "ON CONFLICT (id) DO NOTHING"
    )
    return TestClient(product.app)


def insight(account_id: int = 1, score: int = 55, band: str = "high") -> dict:
    return {
        "account_id": account_id,
        "churn_score": score,
        "churn_band": band,
        "score_version": "rules-v1",
        "reasons": ["usage_drop"],
        "as_of_date": "2026-09-15",
    }


def stored(app_conn, account_id: int = 1) -> tuple | None:
    return app_conn.execute(
        "SELECT churn_score, churn_band, reasons FROM account_insights WHERE account_id = %s",
        (account_id,),
    ).fetchone()


def test_an_insight_is_stored(client, app_conn):
    response = client.post("/internal/insights", json=insight())

    assert response.status_code == 200
    assert stored(app_conn)[:2] == (55, "high")


def test_a_second_score_replaces_the_first(client, app_conn):
    client.post("/internal/insights", json=insight(score=55))
    client.post("/internal/insights", json=insight(score=80, band="critical"))

    assert stored(app_conn)[:2] == (80, "critical")
    assert (
        app_conn.execute("SELECT count(*) FROM account_insights").fetchone()[0] == 1
    )  # upsert, not append


def test_an_unknown_account_is_rejected(client):
    """Expected after an erasure: the sync records it and stops trying."""
    response = client.post("/internal/insights", json=insight(account_id=999))

    assert response.status_code == 404


def test_a_replayed_idempotency_key_is_not_applied_twice(client, app_conn):
    """A retry after an ambiguous timeout must not count as a second delivery."""
    headers = {"Idempotency-Key": "1:rules-v1:abc123"}
    client.post("/internal/insights", json=insight(score=55), headers=headers)
    # Same key, different body: the key wins, because the key IS the content's identity.
    second = client.post("/internal/insights", json=insight(score=99), headers=headers)

    assert second.status_code == 200
    assert stored(app_conn)[0] == 55


def test_rate_limiting_tells_the_client_when_to_come_back(client, product):
    product.RATE_LIMIT_PER_MINUTE = 2

    client.post("/internal/insights", json=insight())
    client.post("/internal/insights", json=insight(score=56))
    limited = client.post("/internal/insights", json=insight(score=57))

    assert limited.status_code == 429
    assert limited.headers["Retry-After"] == "5"


def test_a_score_outside_the_valid_range_is_rejected(client):
    response = client.post("/internal/insights", json=insight(score=140))

    assert response.status_code == 422


def test_the_product_page_shows_bands_written_back(client, app_conn):
    client.post("/internal/insights", json=insight(score=72, band="critical"))

    page = client.get("/")

    assert page.status_code == 200
    assert "critical" in page.text
    assert "Acme" in page.text


def test_every_product_button_emits_an_event_type_the_collector_knows():
    """The page's buttons and the collector's routing table have to agree.

    A button emitting an unknown type gets a 422 naming a field the visitor cannot act on, and
    the event is simply lost — there is no topic for it. The two lists live in different
    services, so nothing but this test keeps them in step.
    """
    from app_api.app import PRODUCT_ACTIONS
    from collector.app import EVENT_FAMILIES

    for label, event_type, family in PRODUCT_ACTIONS:
        assert event_type in EVENT_FAMILIES, f"{label!r} emits unknown event_type {event_type!r}"
        assert EVENT_FAMILIES[event_type] == family, (
            f"{label!r} claims family {family!r}, collector routes it to "
            f"{EVENT_FAMILIES[event_type]!r}"
        )


def test_the_page_posts_to_a_same_origin_path():
    """A relative URL is what keeps the collector free of CORS.

    An absolute URL to the collector's own port would be cross-origin, and making it work would
    mean putting CORS middleware on an unauthenticated write path into the warehouse.
    """
    source = Path(__file__).resolve().parents[1] / "services/app_api/src/app_api/app.py"
    body = source.read_text()
    assert "fetch('/v1/events'" in body, "the page must post to a relative, same-origin path"
    assert "http://collector" not in body, "an absolute collector URL would need CORS"
