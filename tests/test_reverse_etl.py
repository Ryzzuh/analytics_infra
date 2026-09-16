"""Reverse ETL: diffing, the failure taxonomy, and what counts as a failed run (SPEC.md §8)."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime

import httpx
import pytest
from reverse_etl import idempotency_key, payload_for, sync_account_health
from reverse_etl.sync import SyncFailed, payload_hash

NOW = datetime(2026, 9, 16, 3, 0, tzinfo=UTC)
AS_OF = date(2026, 9, 15)


@pytest.fixture
def health_mart(conn):
    """The mart reverse ETL reads. Created directly: dbt builds it, and this is about the sync."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS marts")
    conn.execute(
        """
        CREATE TABLE marts.mart_account_health (
            account_id bigint PRIMARY KEY,
            churn_score integer,
            churn_band text,
            score_version text,
            reasons jsonb,
            as_of_date date
        )
        """
    )

    def add(account_id: int, *, score: int = 40, band: str = "high", reasons=("dormant",)):
        conn.execute(
            "INSERT INTO marts.mart_account_health VALUES (%s, %s, %s, 'rules-v1', %s, %s) "
            "ON CONFLICT (account_id) DO UPDATE SET churn_score = excluded.churn_score, "
            "churn_band = excluded.churn_band, reasons = excluded.reasons",
            (account_id, score, band, json.dumps(list(reasons)), AS_OF),
        )

    return add


class FakeProduct:
    """The app's insights endpoint, with the failure modes a real destination has."""

    def __init__(self):
        self.received: list[tuple[str, dict]] = []
        self.status_for: dict[int, int] = {}
        self.server_errors_remaining = 0
        self.rate_limit_next = 0

    def client(self) -> httpx.Client:
        return httpx.Client(base_url="http://app.test", transport=httpx.MockTransport(self._handle))

    def _handle(self, request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        key = request.headers["Idempotency-Key"]

        if self.rate_limit_next > 0:
            self.rate_limit_next -= 1
            return httpx.Response(429, headers={"Retry-After": "0"}, json={"error": "slow down"})
        if self.server_errors_remaining > 0:
            self.server_errors_remaining -= 1
            return httpx.Response(502, text="bad gateway")

        status = self.status_for.get(payload["account_id"], 200)
        if status >= 400:
            return httpx.Response(status, text="no such account")

        self.received.append((key, payload))
        return httpx.Response(200, json={"ok": True})


def sync_state(conn) -> dict[int, tuple]:
    return {
        row[0]: row[1:]
        for row in conn.execute(
            "SELECT account_id, status, attempts, last_status_code, payload_hash "
            "FROM ops.reverse_etl_sync_state"
        ).fetchall()
    }


def test_only_changed_accounts_are_sent(conn, health_mart):
    """Re-sending everything every run burns the destination's rate limit to say nothing."""
    health_mart(1, score=40)
    health_mart(2, score=10, band="low")
    product = FakeProduct()

    first = sync_account_health(conn, product.client(), now=NOW, sleep=lambda _s: None)
    second = sync_account_health(conn, product.client(), now=NOW, sleep=lambda _s: None)

    assert (first.sent, first.succeeded) == (2, 2)
    assert (second.sent, second.unchanged) == (0, 2)
    assert len(product.received) == 2


def test_a_changed_score_is_sent_again(conn, health_mart):
    health_mart(1, score=40)
    product = FakeProduct()
    sync_account_health(conn, product.client(), now=NOW, sleep=lambda _s: None)

    health_mart(1, score=75, band="critical")
    result = sync_account_health(conn, product.client(), now=NOW, sleep=lambda _s: None)

    assert result.sent == 1
    assert product.received[-1][1]["churn_score"] == 75


def test_idempotency_key_tracks_content_not_time(conn, health_mart):
    """A retry after an ambiguous timeout must not apply the same update twice, while a
    genuinely new score must not be mistaken for a retry."""
    row = {
        "account_id": 7,
        "churn_score": 40,
        "churn_band": "high",
        "score_version": "rules-v1",
        "reasons": ["dormant"],
        "as_of_date": AS_OF,
    }
    same = payload_hash(payload_for(row))
    changed = payload_hash(payload_for({**row, "churn_score": 41}))

    assert idempotency_key(7, "rules-v1", same) == idempotency_key(7, "rules-v1", same)
    assert idempotency_key(7, "rules-v1", same) != idempotency_key(7, "rules-v1", changed)


def test_a_deleted_account_is_recorded_and_not_retried(conn, health_mart):
    """404 after an erasure is the expected outcome, not an incident. Retrying it forever is
    how a sync queue wedges behind data that will never be accepted."""
    health_mart(1)
    health_mart(2)
    product = FakeProduct()
    product.status_for[2] = 404

    first = sync_account_health(conn, product.client(), now=NOW, sleep=lambda _s: None)
    second = sync_account_health(conn, product.client(), now=NOW, sleep=lambda _s: None)

    assert (first.succeeded, first.client_errors) == (1, 1)
    assert sync_state(conn)[2][0] == "skipped_client_error"
    assert second.sent == 0  # not retried while its content is unchanged
    assert first.error_rate == 0.0  # and it does not count against the run


def test_transient_server_errors_are_retried_then_succeed(conn, health_mart):
    health_mart(1)
    product = FakeProduct()
    product.server_errors_remaining = 2

    result = sync_account_health(conn, product.client(), now=NOW, sleep=lambda _s: None)

    assert (result.succeeded, result.server_errors) == (1, 0)
    assert sync_state(conn)[1][0] == "synced"


def test_rate_limits_are_obeyed(conn, health_mart):
    health_mart(1)
    product = FakeProduct()
    product.rate_limit_next = 2
    waits: list[float] = []

    result = sync_account_health(conn, product.client(), now=NOW, sleep=waits.append)

    assert result.rate_limited == 2
    assert waits == [0.0, 0.0]  # Retry-After honoured, not a delay of our own invention
    assert result.succeeded == 1


def test_one_bad_account_does_not_fail_the_run(conn, health_mart):
    """Treating a single failure as a failed sync is how people learn to ignore the alert."""
    for account_id in range(1, 101):
        health_mart(account_id, score=account_id % 90)
    product = FakeProduct()
    product.status_for[50] = 500
    product.server_errors_remaining = 0

    result = sync_account_health(
        conn, product.client(), now=NOW, max_error_rate=0.02, sleep=lambda _s: None
    )

    assert result.server_errors == 1
    assert result.error_rate == pytest.approx(0.01)
    assert sync_state(conn)[50][0] == "failed"


def test_a_broadly_failing_destination_fails_the_run(conn, health_mart):
    for account_id in range(1, 21):
        health_mart(account_id, score=account_id)
    product = FakeProduct()
    for account_id in range(1, 21):
        product.status_for[account_id] = 500

    with pytest.raises(SyncFailed, match="100.0% > 2.0%"):
        sync_account_health(conn, product.client(), now=NOW, sleep=lambda _s: None)

    # State is still recorded for every attempt, so the next run knows what happened.
    assert all(state[0] == "failed" for state in sync_state(conn).values())


def test_a_failed_account_is_retried_next_run(conn, health_mart):
    """Unlike a 4xx, a server error is worth trying again — the destination may have recovered."""
    health_mart(1)
    product = FakeProduct()
    product.status_for[1] = 503

    with pytest.raises(SyncFailed):
        sync_account_health(conn, product.client(), now=NOW, sleep=lambda _s: None)

    product.status_for.clear()
    result = sync_account_health(conn, product.client(), now=NOW, sleep=lambda _s: None)

    assert result.sent == 1
    assert sync_state(conn)[1][0] == "synced"
    assert sync_state(conn)[1][1] == 2  # attempts accumulated across runs


def test_the_payload_is_the_documented_contract(conn, health_mart):
    health_mart(1, score=62, band="critical", reasons=("usage_drop", "past_due"))
    product = FakeProduct()

    sync_account_health(conn, product.client(), now=NOW, sleep=lambda _s: None)

    _key, payload = product.received[0]
    assert payload == {
        "account_id": 1,
        "churn_score": 62,
        "churn_band": "critical",
        "score_version": "rules-v1",
        "reasons": ["usage_drop", "past_due"],
        "as_of_date": "2026-09-15",
    }
