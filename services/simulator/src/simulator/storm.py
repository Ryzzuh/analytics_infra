"""Chaos injections that produce messages (SPEC.md §9.2).

Called by the Console through the control plane. These write to the LIVE topics deliberately:
the point is that the live path handles them, and routing them to the backfill topics would
exempt them from the very cutoff being demonstrated.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from loader.kafka_sink import KafkaMessageSink

log = logging.getLogger("simulator.storm")

TOPIC = os.environ.get("STORM_TOPIC", "product.feature_usage")

# Messages that cannot be parsed, each exercising a different branch of the loader's parser.
POISON = [
    b"{not json at all",
    json.dumps({"event_id": "not-a-uuid", "event_type": "feature_invoked"}).encode(),
    json.dumps({"event_type": "feature_invoked", "event_time": "yesterday"}).encode(),
    json.dumps(["an", "array", "not", "an", "object"]).encode(),
    None,  # a tombstone on an event topic: meaningful for CDC, meaningless here
]


def late_duplicate_storm(
    sink, *, events: int, days_late: int, duplicate_rate: float, seed: int = 99
) -> dict[str, int]:
    """Devices coming back online: old events, many of them sent twice."""
    rng = random.Random(seed)
    now = datetime.now(UTC)
    produced = duplicates = 0

    for _ in range(events):
        event_time = now - timedelta(
            days=rng.uniform(0.5, days_late), seconds=rng.uniform(0, 86400)
        )
        body = {
            "event_id": str(uuid4()),
            "account_id": rng.randint(1, 500),
            "user_id": rng.randint(1000, 9999),
            "event_type": "feature_invoked",
            "event_time": event_time.isoformat(),
            # Received now, having happened days ago: that gap is what the cutoff measures.
            "received_at": now.isoformat(),
            "payload": {"feature": "export", "surface": "mobile", "duration_ms": 120},
        }
        payload = json.dumps(body).encode()
        sink.send(TOPIC, payload, key=str(body["account_id"]).encode())
        produced += 1

        if rng.random() < duplicate_rate:
            # The same event_id again: a client retrying after a timeout the server handled.
            sink.send(TOPIC, payload, key=str(body["account_id"]).encode())
            duplicates += 1

    sink.flush()
    return {"produced": produced, "duplicates": duplicates}


def poison_messages(sink, *, count: int) -> dict[str, int]:
    for index in range(count):
        sink.send(TOPIC, POISON[index % len(POISON)], key=b"1")
    sink.flush()
    return {"poison": count}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="simulator.storm", description=__doc__)
    parser.add_argument(
        "--bootstrap", default=os.environ.get("REDPANDA_BOOTSTRAP", "redpanda:9092")
    )
    parser.add_argument("--events", type=int, default=5000)
    parser.add_argument("--days-late", type=int, default=3)
    parser.add_argument("--duplicate-rate", type=float, default=0.3)
    parser.add_argument("--poison", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    sink = KafkaMessageSink(args.bootstrap)
    result = (
        poison_messages(sink, count=args.events)
        if args.poison
        else late_duplicate_storm(
            sink,
            events=args.events,
            days_late=args.days_late,
            duplicate_rate=args.duplicate_rate,
        )
    )
    log.info("storm complete: %s", result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
