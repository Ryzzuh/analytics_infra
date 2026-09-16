"""Receives billing webhooks and puts them on the broker (SPEC.md §4.3).

Deliberately thin. It does not deduplicate, reorder or validate business meaning, because the
provider is at-least-once and unordered by nature and pretending otherwise at the edge just
moves the problem somewhere less visible. Raw keeps what arrived; staging reconciles it against
the daily pull.

What it does do is refuse to acknowledge anything the broker has not accepted: a 2xx to the
provider means "this is durable", and providers stop retrying once they get one.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

BOOTSTRAP = os.environ.get("REDPANDA_BOOTSTRAP", "redpanda:9092")
TOPIC = os.environ.get("BILLING_TOPIC", "billing.webhooks")

received = Counter("webhook_received_total", "Webhooks received", ["event_type"])
rejected = Counter("webhook_rejected_total", "Webhooks rejected", ["reason"])

app = FastAPI(title="Billing Webhook Receiver")
_producer: Producer | None = None


def producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer(
            {
                "bootstrap.servers": BOOTSTRAP,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 10,
            }
        )
    return _producer


@app.post("/webhooks/billing", status_code=202)
async def receive(request: Request) -> dict[str, Any]:
    try:
        event = json.loads(await request.body())
    except json.JSONDecodeError:
        rejected.labels("invalid_json").inc()
        raise HTTPException(status_code=400, detail="invalid json") from None

    provider_event_id = event.get("id")
    if not provider_event_id:
        # Unmergeable with the batch pull, so there is no point storing it.
        rejected.labels("missing_id").inc()
        raise HTTPException(status_code=400, detail="missing event id")

    event["received_at"] = datetime.now(UTC).isoformat()
    failures: list[str] = []

    def on_delivery(err, _msg):
        if err is not None:
            failures.append(str(err))

    prod = producer()
    prod.produce(
        topic=TOPIC,
        key=str(event.get("account_id", provider_event_id)).encode(),
        value=json.dumps(event).encode(),
        on_delivery=on_delivery,
    )
    prod.flush(timeout=10)

    if failures:
        # 5xx keeps the provider retrying, which is the behaviour we want: a duplicate is
        # recoverable downstream, a silently dropped payment event is not.
        rejected.labels("broker_nack").inc()
        raise HTTPException(status_code=503, detail="not accepted by broker")

    received.labels(event.get("type", "unknown")).inc()
    return {"received": provider_event_id}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
