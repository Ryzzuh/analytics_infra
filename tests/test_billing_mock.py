"""The billing mock's contract: pagination, eventual consistency, and rate limits."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def provider():
    """Fresh module state per test: the mock keeps its events in memory, like the real one
    keeps them in someone else's database.

    State is reset rather than the module reloaded — reloading re-registers the Prometheus
    metrics and the default registry rejects the duplicates.
    """
    from billing_mock import app as module

    module.EVENTS.clear()
    module.VISIBILITY_LAG = timedelta(0)
    module.RATE_LIMIT_EVERY = 0
    module._requests_served = 0
    module.BEHAVIOUR.drop_rate = 0
    module.BEHAVIOUR.duplicate_rate = 0
    module.BEHAVIOUR.reorder_rate = 0
    module.BEHAVIOUR.receiver_down = True  # no webhook delivery attempts in these tests
    return module


@pytest.fixture
def client(provider):
    return TestClient(provider.app)


def create(client, n: int = 1) -> list[dict]:
    return [
        client.post(
            "/internal/events",
            json={"type": "invoice.paid", "account_id": 1, "data": {"amount_cents": 100 * i}},
        ).json()
        for i in range(n)
    ]


def test_events_are_listed_in_created_order(client):
    created = create(client, 3)

    listed = client.get("/v1/events", params={"created_gte": "2020-01-01T00:00:00+00:00"}).json()

    assert [e["id"] for e in listed["data"]] == [e["id"] for e in created]
    assert listed["has_more"] is False


def test_pagination_walks_the_cursor(client):
    create(client, 5)

    first = client.get(
        "/v1/events", params={"created_gte": "2020-01-01T00:00:00+00:00", "limit": 2}
    ).json()
    second = client.get(
        "/v1/events",
        params={
            "created_gte": "2020-01-01T00:00:00+00:00",
            "limit": 2,
            "starting_after": first["data"][-1]["id"],
        },
    ).json()

    assert first["has_more"] is True
    assert len(second["data"]) == 2
    assert {e["id"] for e in first["data"]}.isdisjoint({e["id"] for e in second["data"]})


def test_an_unknown_cursor_is_an_error_not_an_empty_page(client):
    """Silently returning nothing would look like "caught up" and skip everything after it."""
    create(client, 2)

    response = client.get(
        "/v1/events",
        params={"created_gte": "2020-01-01T00:00:00+00:00", "starting_after": "evt_nope"},
    )

    assert response.status_code == 400


def test_recent_events_are_not_visible_immediately(provider):
    """Eventual consistency, which is exactly why the daily pull overlaps its window."""
    provider.VISIBILITY_LAG = timedelta(seconds=30)
    client = TestClient(provider.app)
    created = create(client, 1)

    listed = client.get("/v1/events", params={"created_gte": "2020-01-01T00:00:00+00:00"}).json()

    assert listed["data"] == []
    assert created[0]["id"]  # it exists; it just cannot be seen yet


def test_rate_limiting_reports_retry_after(provider):
    provider.RATE_LIMIT_EVERY = 2
    client = TestClient(provider.app)
    create(client, 1)

    params = {"created_gte": "2020-01-01T00:00:00+00:00"}
    client.get("/v1/events", params=params)
    limited = client.get("/v1/events", params=params)

    assert limited.status_code == 429
    assert limited.headers["Retry-After"] == "1"


def test_behaviour_is_controllable_for_chaos(client, provider):
    response = client.put(
        "/internal/behaviour",
        json={"drop_rate": 1.0, "duplicate_rate": 0, "reorder_rate": 0, "receiver_down": False},
    )

    assert response.status_code == 200
    assert provider.BEHAVIOUR.drop_rate == 1.0


def test_created_events_carry_provider_ids(client):
    created = create(client, 1)[0]

    assert created["id"].startswith("evt_")
    assert datetime.fromisoformat(created["created_at"]) <= datetime.now(UTC)
