"""The billing provider's list API: the slow half of billing ingestion (SPEC.md §4.3).

Webhooks are the fast path and they are not reliable: they arrive twice, out of order, and not
at all when the receiver was down. That is not a flaw to design around, it is how every payment
provider behaves, so the platform runs both paths and merges them. This module is the second
path — a daily pull of the provider's own event list, which is the only thing that can tell you
what the webhooks missed.

Three things make it work:

* **An overlap window.** The pull starts earlier than where it finished last time, because the
  provider is eventually consistent: an event created a second before the previous cursor can
  become visible a second after it. Without overlap those events are never seen by either path.
* **Idempotent inserts.** The overlap re-fetches events on purpose, so inserting has to be a
  no-op the second time. Otherwise the mechanism protecting against gaps would create
  duplicates instead.
* **Cursor state written in the same transaction as the rows.** Same discipline as the offset
  ledger: a cursor that advanced without its data would skip events permanently.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from psycopg import Connection
from psycopg.types.json import Jsonb

log = logging.getLogger(__name__)

DEFAULT_OVERLAP = timedelta(hours=48)
DEFAULT_PAGE_SIZE = 100
MAX_ATTEMPTS_PER_PAGE = 5


@dataclass
class PullResult:
    fetched: int = 0
    inserted: int = 0
    pages: int = 0
    rate_limited: int = 0
    retried_server_errors: int = 0
    cursor_from: datetime | None = None
    cursor_to: datetime | None = None

    @property
    def duplicates_skipped(self) -> int:
        """Events the overlap re-fetched that were already held. Expected, not a problem."""
        return self.fetched - self.inserted


def _retry_after_seconds(response: httpx.Response, default: float = 1.0) -> float:
    raw = response.headers.get("Retry-After")
    if raw is None:
        return default
    try:
        return max(0.0, float(raw))
    except ValueError:
        return default


def _fetch_page(
    client: httpx.Client,
    *,
    created_gte: datetime,
    starting_after: str | None,
    limit: int,
    sleep: Callable[[float], None],
    result: PullResult,
) -> dict[str, Any]:
    """One page, with the provider's rate limit and transient failures handled.

    A 429 is the provider telling us how long to wait; ignoring `Retry-After` and retrying
    immediately is how a client gets itself throttled harder.
    """
    params: dict[str, Any] = {"created_gte": created_gte.isoformat(), "limit": limit}
    if starting_after:
        params["starting_after"] = starting_after

    for attempt in range(1, MAX_ATTEMPTS_PER_PAGE + 1):
        response = client.get("/v1/events", params=params)

        if response.status_code == 429:
            result.rate_limited += 1
            wait = _retry_after_seconds(response)
            log.info("rate limited by billing provider; waiting %.1fs", wait)
            sleep(wait)
            continue

        if response.status_code >= 500:
            result.retried_server_errors += 1
            sleep(min(2**attempt * 0.1, 5.0))
            continue

        response.raise_for_status()
        return response.json()

    raise RuntimeError(
        f"billing list API did not return a page after {MAX_ATTEMPTS_PER_PAGE} attempts; "
        "the cursor has not moved, so the next run will retry the same window"
    )


def pull_billing_events(
    conn: Connection,
    client: httpx.Client,
    *,
    now: datetime | None = None,
    overlap: timedelta = DEFAULT_OVERLAP,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = 1000,
    sleep: Callable[[float], None] = time.sleep,
) -> PullResult:
    """Pull the provider's event list into raw, from the last cursor minus the overlap."""
    pulled_at = now or datetime.now(UTC)
    result = PullResult()

    state = conn.execute(
        "SELECT last_created_at, last_event_id FROM ops.billing_pull_state WHERE id = 1"
    ).fetchone()
    last_created_at, last_event_id = state if state else (None, None)

    # First ever pull has no cursor: take the overlap window before now rather than the whole
    # of history, which the provider would page through for a very long time.
    created_gte = (last_created_at - overlap) if last_created_at else (pulled_at - overlap)
    result.cursor_from = created_gte

    rows: list[tuple] = []
    starting_after: str | None = None
    newest_created_at = last_created_at
    newest_event_id = last_event_id

    while result.pages < max_pages:
        page = _fetch_page(
            client,
            created_gte=created_gte,
            starting_after=starting_after,
            limit=page_size,
            sleep=sleep,
            result=result,
        )
        events = page.get("data", [])
        result.pages += 1
        result.fetched += len(events)

        for event in events:
            created_at = datetime.fromisoformat(event["created_at"])
            rows.append(
                (
                    event["id"],
                    event["type"],
                    event.get("account_id"),
                    created_at,
                    pulled_at,
                    Jsonb(event),
                )
            )
            if newest_created_at is None or created_at > newest_created_at:
                newest_created_at, newest_event_id = created_at, event["id"]

        if not page.get("has_more") or not events:
            break
        starting_after = events[-1]["id"]

    # Rows and cursor commit together, or neither does.
    with conn.transaction():
        if rows:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO raw.billing_events_batch "
                    "(provider_event_id, event_type, account_id, provider_created_at, "
                    " pulled_at, payload) VALUES (%s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (provider_event_id) DO NOTHING",
                    rows,
                )
                result.inserted = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        conn.execute(
            "UPDATE ops.billing_pull_state SET last_created_at = %s, last_event_id = %s, "
            "last_pull_at = %s, pages_last_pull = %s WHERE id = 1",
            (newest_created_at, newest_event_id, pulled_at, result.pages),
        )

    result.cursor_to = newest_created_at
    log.info(
        "billing pull: %d fetched, %d new, %d already held, %d pages, %d rate-limit waits",
        result.fetched,
        result.inserted,
        result.duplicates_skipped,
        result.pages,
        result.rate_limited,
    )
    return result
