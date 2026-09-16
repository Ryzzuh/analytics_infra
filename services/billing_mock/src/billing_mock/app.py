"""A Stripe-shaped billing provider (SPEC.md §4.3).

Its job is to be *imperfect in the ways real providers are*, because a mock that behaves
perfectly proves nothing about the pipeline that consumes it:

* webhooks are delivered at-least-once, sometimes twice, sometimes out of order, and not at all
  while the receiver is down;
* the list API pages by cursor, rate-limits, and is eventually consistent — an event created a
  moment ago may not appear for a few seconds, which is exactly why the batch pull overlaps.

Unreliability is seeded and configurable, so a demo can be reproduced rather than waited for.
"""

from __future__ import annotations

import asyncio
import os
import random
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException, Query, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from pydantic import BaseModel, Field

WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "http://webhook-receiver:8002/webhooks/billing")
VISIBILITY_LAG = timedelta(seconds=float(os.environ.get("BILLING_VISIBILITY_LAG_S", 5)))
RATE_LIMIT_EVERY = int(os.environ.get("BILLING_RATE_LIMIT_EVERY", 0))  # 0 = never

webhooks_sent = Counter("billing_webhooks_sent_total", "Webhook deliveries attempted", ["outcome"])
events_created = Counter("billing_events_created_total", "Provider events created", ["type"])

app = FastAPI(title="Billing Mock")

EVENTS: list[dict[str, Any]] = []
_rng = random.Random(int(os.environ.get("BILLING_SEED", 7)))
_requests_served = 0


class Behaviour(BaseModel):
    """How badly the provider misbehaves. Defaults are deliberately not zero."""

    drop_rate: float = Field(default=0.04, ge=0, le=1)  # webhook never arrives
    duplicate_rate: float = Field(default=0.03, ge=0, le=1)  # webhook arrives twice
    reorder_rate: float = Field(default=0.05, ge=0, le=1)  # delivery delayed behind later ones
    receiver_down: bool = False


BEHAVIOUR = Behaviour()


class CreateEvent(BaseModel):
    type: str
    account_id: int
    data: dict[str, Any] = Field(default_factory=dict)


async def _deliver(event: dict[str, Any]) -> None:
    """Attempt webhook delivery, with the provider's usual bad habits."""
    if BEHAVIOUR.receiver_down:
        webhooks_sent.labels("receiver_down").inc()
        return
    if _rng.random() < BEHAVIOUR.drop_rate:
        # The event still exists in the list API: this is precisely the gap the daily
        # reconciliation pull is there to close.
        webhooks_sent.labels("dropped").inc()
        return
    if _rng.random() < BEHAVIOUR.reorder_rate:
        await asyncio.sleep(_rng.uniform(0.5, 3.0))

    attempts = 2 if _rng.random() < BEHAVIOUR.duplicate_rate else 1
    async with httpx.AsyncClient(timeout=5.0) as client:
        for _ in range(attempts):
            try:
                await client.post(WEBHOOK_URL, json=event)
                webhooks_sent.labels("delivered").inc()
            except httpx.HTTPError:
                webhooks_sent.labels("failed").inc()


@app.post("/internal/events", status_code=201)
async def create_event(body: CreateEvent) -> dict[str, Any]:
    """Called by the simulator when something billable happens."""
    event = {
        "id": f"evt_{uuid4().hex[:24]}",
        "type": body.type,
        "account_id": body.account_id,
        "created_at": datetime.now(UTC).isoformat(),
        "data": body.data,
    }
    EVENTS.append(event)
    events_created.labels(body.type).inc()
    asyncio.create_task(_deliver(event))  # noqa: RUF006 - fire and forget, like a real provider
    return event


@app.get("/v1/events")
def list_events(
    created_gte: datetime = Query(...),
    starting_after: str | None = None,
    limit: int = Query(default=100, le=100),
) -> dict[str, Any]:
    """Cursor-paginated event list — the reconciliation source of truth.

    Eventually consistent on purpose: events created within the visibility lag are withheld,
    so a pull that used "everything since my last cursor" would miss them permanently.
    """
    global _requests_served
    _requests_served += 1
    if RATE_LIMIT_EVERY and _requests_served % RATE_LIMIT_EVERY == 0:
        raise HTTPException(status_code=429, detail="rate limited", headers={"Retry-After": "1"})

    visible_until = datetime.now(UTC) - VISIBILITY_LAG
    rows = [
        e
        for e in sorted(EVENTS, key=lambda e: (e["created_at"], e["id"]))
        if datetime.fromisoformat(e["created_at"]) >= created_gte
        and datetime.fromisoformat(e["created_at"]) <= visible_until
    ]

    if starting_after:
        index = next((i for i, e in enumerate(rows) if e["id"] == starting_after), None)
        if index is None:
            raise HTTPException(status_code=400, detail="unknown cursor")
        rows = rows[index + 1 :]

    page = rows[:limit]
    return {"data": page, "has_more": len(rows) > len(page)}


@app.put("/internal/behaviour")
def set_behaviour(behaviour: Behaviour) -> Behaviour:
    """Chaos control: the 'provider stops delivering webhooks' scenario lives here."""
    global BEHAVIOUR
    BEHAVIOUR = behaviour
    return BEHAVIOUR


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
