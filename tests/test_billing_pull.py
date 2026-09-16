"""The daily billing pull: cursors, overlap, rate limits (SPEC.md §4.3)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
from loader.billing import pull_billing_events

NOW = datetime(2026, 9, 16, 2, 0, tzinfo=UTC)


class FakeProvider:
    """A Stripe-shaped list API: cursor pagination, rate limits, and flaky moments."""

    def __init__(self, events: list[dict], *, page_size: int = 2):
        self.events = sorted(events, key=lambda e: (e["created_at"], e["id"]))
        self.page_size = page_size
        self.requests: list[dict] = []
        self.rate_limit_next = 0
        self.server_error_next = 0

    def client(self) -> httpx.Client:
        return httpx.Client(
            base_url="http://billing.test", transport=httpx.MockTransport(self._handle)
        )

    def _handle(self, request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        self.requests.append(params)

        if self.rate_limit_next > 0:
            self.rate_limit_next -= 1
            return httpx.Response(429, headers={"Retry-After": "0"}, json={"error": "slow down"})
        if self.server_error_next > 0:
            self.server_error_next -= 1
            return httpx.Response(503, json={"error": "unavailable"})

        created_gte = datetime.fromisoformat(params["created_gte"])
        visible = [e for e in self.events if datetime.fromisoformat(e["created_at"]) >= created_gte]

        after = params.get("starting_after")
        if after:
            index = next(i for i, e in enumerate(visible) if e["id"] == after)
            visible = visible[index + 1 :]

        page = visible[: self.page_size]
        return httpx.Response(200, json={"data": page, "has_more": len(visible) > len(page)})


def event(n: int, *, minutes_ago: int, account_id: int = 1, type_: str = "invoice.paid") -> dict:
    return {
        "id": f"evt_{n:04d}",
        "type": type_,
        "account_id": account_id,
        "created_at": (NOW - timedelta(minutes=minutes_ago)).isoformat(),
        "data": {"amount_cents": 24900},
    }


def state(conn) -> tuple:
    return conn.execute(
        "SELECT last_created_at, last_event_id, pages_last_pull FROM ops.billing_pull_state"
    ).fetchone()


def batch_ids(conn) -> list[str]:
    return [
        r[0]
        for r in conn.execute(
            "SELECT provider_event_id FROM raw.billing_events_batch ORDER BY provider_event_id"
        ).fetchall()
    ]


def test_pages_through_the_whole_window(conn):
    provider = FakeProvider([event(n, minutes_ago=60 - n) for n in range(5)], page_size=2)

    result = pull_billing_events(conn, provider.client(), now=NOW, sleep=lambda _s: None)

    assert (result.fetched, result.inserted) == (5, 5)
    assert result.pages == 3  # 2 + 2 + 1
    assert len(batch_ids(conn)) == 5


def test_cursor_advances_and_the_next_pull_overlaps_it(conn):
    """The provider is eventually consistent: an event created just before the cursor can
    become visible just after it, and without overlap neither path would ever see it."""
    provider = FakeProvider([event(0, minutes_ago=30)], page_size=10)
    pull_billing_events(conn, provider.client(), now=NOW, sleep=lambda _s: None)

    last_created_at, last_event_id, _pages = state(conn)
    assert last_event_id == "evt_0000"

    provider.requests.clear()
    pull_billing_events(
        conn,
        provider.client(),
        now=NOW + timedelta(days=1),
        overlap=timedelta(hours=48),
        sleep=lambda _s: None,
    )

    requested_from = datetime.fromisoformat(provider.requests[0]["created_gte"])
    assert requested_from == last_created_at - timedelta(hours=48)


def test_refetched_events_do_not_duplicate(conn):
    """The overlap re-fetches on purpose, so the insert must be a no-op the second time."""
    provider = FakeProvider([event(n, minutes_ago=10 + n) for n in range(3)], page_size=10)

    first = pull_billing_events(conn, provider.client(), now=NOW, sleep=lambda _s: None)
    second = pull_billing_events(
        conn, provider.client(), now=NOW + timedelta(hours=1), sleep=lambda _s: None
    )

    assert first.inserted == 3
    assert second.fetched == 3 and second.inserted == 0
    assert second.duplicates_skipped == 3
    assert len(batch_ids(conn)) == 3


def test_an_event_created_before_the_cursor_is_still_caught(conn):
    """The gap the overlap exists to close."""
    provider = FakeProvider([event(1, minutes_ago=10)], page_size=10)
    pull_billing_events(conn, provider.client(), now=NOW, sleep=lambda _s: None)

    # Appears afterwards, but dated a minute EARLIER than the event that moved the cursor.
    provider.events.append(event(2, minutes_ago=11))
    provider.events.sort(key=lambda e: (e["created_at"], e["id"]))
    pull_billing_events(
        conn, provider.client(), now=NOW + timedelta(minutes=5), sleep=lambda _s: None
    )

    assert batch_ids(conn) == ["evt_0001", "evt_0002"]


def test_rate_limits_are_waited_out_not_hammered(conn):
    provider = FakeProvider([event(0, minutes_ago=5)], page_size=10)
    provider.rate_limit_next = 2
    waits: list[float] = []

    result = pull_billing_events(conn, provider.client(), now=NOW, sleep=waits.append)

    assert result.rate_limited == 2
    assert waits == [0.0, 0.0]  # honoured Retry-After rather than inventing a delay
    assert batch_ids(conn) == ["evt_0000"]


def test_transient_server_errors_are_retried(conn):
    provider = FakeProvider([event(0, minutes_ago=5)], page_size=10)
    provider.server_error_next = 2

    result = pull_billing_events(conn, provider.client(), now=NOW, sleep=lambda _s: None)

    assert result.retried_server_errors == 2
    assert batch_ids(conn) == ["evt_0000"]


def test_a_persistently_failing_provider_leaves_the_cursor_untouched(conn):
    """Next run retries the same window: better than advancing past data never fetched."""
    provider = FakeProvider([event(0, minutes_ago=5)], page_size=10)
    provider.server_error_next = 99
    before = state(conn)

    with pytest.raises(RuntimeError, match="cursor has not moved"):
        pull_billing_events(conn, provider.client(), now=NOW, sleep=lambda _s: None)

    assert state(conn) == before
    assert batch_ids(conn) == []


def test_first_ever_pull_does_not_page_through_all_of_history(conn):
    provider = FakeProvider([event(0, minutes_ago=5)], page_size=10)

    pull_billing_events(
        conn, provider.client(), now=NOW, overlap=timedelta(hours=48), sleep=lambda _s: None
    )

    requested_from = datetime.fromisoformat(provider.requests[0]["created_gte"])
    assert requested_from == NOW - timedelta(hours=48)
