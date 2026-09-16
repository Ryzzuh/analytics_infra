"""Event collector.

Two rules define this service:

1. It validates the ENVELOPE only. Payloads are schemaless by design, and drift is detected
   downstream (SPEC.md §7), so a new payload field must never cost the client a 4xx.
2. It acks only after the broker has acked. Returning 202 before that would invent a
   durability guarantee the platform cannot keep, and the client would never retry.

Client retries after a timeout are therefore expected, and are the source of the duplicates
that staging deduplicates on `event_id`.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field

BOOTSTRAP = os.environ.get("REDPANDA_BOOTSTRAP", "localhost:19092")
TOPIC_PREFIX = os.environ.get("TOPIC_PREFIX", "product")

# One topic per event family: per-family retention and schema rules, and a poison message in
# one family cannot stall the others (SPEC.md §4.1).
EVENT_FAMILIES: dict[str, str] = {
    "session_start": "session",
    "session_end": "session",
    "page_view": "session",
    "feature_invoked": "feature_usage",
    "report_exported": "feature_usage",
    "api_called": "feature_usage",
    "signup": "lifecycle",
    "invite_sent": "lifecycle",
    "seat_added": "lifecycle",
    "seat_removed": "lifecycle",
    "plan_viewed": "billing_ui",
    "checkout_started": "billing_ui",
    "subscription_changed": "billing_ui",
}

events_received = Counter("collector_events_received_total", "Events accepted", ["event_type"])
events_rejected = Counter("collector_events_rejected_total", "Events rejected", ["reason"])
produce_latency = Histogram("collector_produce_seconds", "Time to broker ack for a batch")

app = FastAPI(title="Event Collector")
_producer: Producer | None = None


def producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer(
            {
                "bootstrap.servers": BOOTSTRAP,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 20,
                "compression.type": "zstd",
            }
        )
    return _producer


class Event(BaseModel):
    event_id: UUID
    account_id: int
    user_id: int | None = None
    event_type: str
    event_time: datetime
    payload: dict[str, Any] = Field(default_factory=dict)  # deliberately unvalidated


class Batch(BaseModel):
    events: list[Event] = Field(min_length=1, max_length=500)


def topic_for(event_type: str) -> str:
    family = EVENT_FAMILIES.get(event_type)
    if family is None:
        raise HTTPException(status_code=422, detail=f"unknown event_type: {event_type}")
    return f"{TOPIC_PREFIX}.{family}"


@app.post("/v1/events", status_code=202)
def ingest(batch: Batch) -> dict[str, int]:
    received_at = datetime.now(UTC)
    prod = producer()
    failures: list[str] = []

    def on_delivery(err, _msg):
        if err is not None:
            failures.append(str(err))

    for event in batch.events:
        try:
            topic = topic_for(event.event_type)
        except HTTPException:
            events_rejected.labels("unknown_event_type").inc()
            raise
        body = event.model_dump(mode="json")
        body["received_at"] = received_at.isoformat()
        prod.produce(
            topic=topic,
            # Keyed by account so a given account's events stay ordered within a partition.
            key=str(event.account_id).encode(),
            value=json.dumps(body).encode(),
            on_delivery=on_delivery,
        )
        events_received.labels(event.event_type).inc()

    with produce_latency.time():
        prod.flush(timeout=10)

    if failures:
        # The client must retry: a duplicate downstream is recoverable, a lost event is not.
        events_rejected.labels("broker_nack").inc(len(failures))
        raise HTTPException(status_code=503, detail=f"{len(failures)} events not acked by broker")

    return {"accepted": len(batch.events)}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
