"""Collector contract tests.

The broker is faked, because what matters here is the service's promises: route by event
family, accept unknown payload shapes, and never ack an event the broker has not accepted.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from collector import app as collector_app
from fastapi.testclient import TestClient


class FakeProducer:
    """Records produced messages; can be told to fail delivery."""

    def __init__(self, fail: bool = False):
        self.messages: list[tuple[str, bytes, bytes]] = []
        self.fail = fail
        self._callbacks: list = []

    def produce(self, topic, key, value, on_delivery):
        self.messages.append((topic, key, value))
        self._callbacks.append(on_delivery)

    def flush(self, timeout=None):
        for cb in self._callbacks:
            cb("broker down" if self.fail else None, None)
        self._callbacks.clear()
        return 0


@pytest.fixture
def producer(monkeypatch):
    fake = FakeProducer()
    monkeypatch.setattr(collector_app, "producer", lambda: fake)
    return fake


@pytest.fixture
def client(producer):
    return TestClient(collector_app.app)


def an_event(**overrides) -> dict:
    event = {
        "event_id": str(uuid4()),
        "account_id": 7,
        "user_id": 7001,
        "event_type": "feature_invoked",
        "event_time": datetime.now(UTC).isoformat(),
        "payload": {"feature": "export"},
    }
    event.update(overrides)
    return event


def test_events_route_to_their_family_topic(client, producer):
    response = client.post(
        "/v1/events",
        json={
            "events": [
                an_event(event_type="page_view"),
                an_event(event_type="checkout_started"),
            ]
        },
    )

    assert response.status_code == 202
    assert [m[0] for m in producer.messages] == ["product.session", "product.billing_ui"]


def test_partition_key_is_the_account(client, producer):
    client.post("/v1/events", json={"events": [an_event(account_id=42)]})

    assert producer.messages[0][1] == b"42"


def test_collector_stamps_its_own_received_at(client, producer):
    """Arrival time comes from the server: client clocks are skewed, and lateness is measured
    as received_at - event_time, so a client-supplied arrival time would be meaningless."""
    sent_at = datetime.now(UTC)
    client.post("/v1/events", json={"events": [an_event(received_at="1999-01-01T00:00:00+00:00")]})

    body = json.loads(producer.messages[0][2])
    received_at = datetime.fromisoformat(body["received_at"])
    assert received_at >= sent_at  # the client's claim was discarded, not trusted


def test_unknown_payload_fields_are_accepted(client, producer):
    """Payloads are schemaless by design: drift is detected downstream, not rejected here."""
    response = client.post(
        "/v1/events",
        json={"events": [an_event(payload={"brand_new_field": 1, "nested": {"a": [1, 2]}})]},
    )

    assert response.status_code == 202
    assert json.loads(producer.messages[0][2])["payload"]["brand_new_field"] == 1


def test_unknown_event_type_is_rejected(client, producer):
    """An unroutable event has no topic, so it cannot be accepted."""
    response = client.post("/v1/events", json={"events": [an_event(event_type="teleported")]})

    assert response.status_code == 422


def test_a_broker_nack_is_not_acked_to_the_client(client, producer):
    """503 makes the client retry. A duplicate is recoverable downstream; a lost event is not."""
    producer.fail = True

    response = client.post("/v1/events", json={"events": [an_event()]})

    assert response.status_code == 503


def test_malformed_envelope_is_rejected(client):
    response = client.post("/v1/events", json={"events": [{"event_type": "page_view"}]})

    assert response.status_code == 422
