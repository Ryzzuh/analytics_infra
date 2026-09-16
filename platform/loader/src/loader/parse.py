"""Envelope parsing.

Product events are schemaless by design (SPEC.md §7): the *envelope* is validated here, the
*payload* is not. Anything that cannot be turned into a raw row goes to the DLQ rather than
failing the batch, because one malformed message must not stall a partition.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from .models import ParsedEvent, ParseFailure, SourceRecord

ENVELOPE_FIELDS = ("event_id", "event_type", "event_time", "received_at")


def _parse_ts(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    ts = datetime.fromisoformat(value)
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts


def _parse_int(value: Any, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be an integer")
    return value


def parse_record(record: SourceRecord) -> ParsedEvent | ParseFailure:
    # A null value is a Kafka tombstone. It is meaningful for CDC topics and meaningless for
    # product events, so here it is a poison message, not a crash.
    if record.value is None:
        return ParseFailure("tombstone_on_event_topic", record)

    try:
        body = json.loads(record.value)
    except json.JSONDecodeError as exc:
        return ParseFailure(f"invalid_json: {exc.msg}", record)

    if not isinstance(body, dict):
        return ParseFailure("envelope_not_object", record)

    missing = [f for f in ENVELOPE_FIELDS if body.get(f) is None]
    if missing:
        return ParseFailure(f"missing_envelope_fields: {','.join(missing)}", record)

    try:
        event_id = UUID(str(body["event_id"]))
        event_time = _parse_ts(body["event_time"])
        received_at = _parse_ts(body["received_at"])
        account_id = _parse_int(body.get("account_id"), "account_id")
        user_id = _parse_int(body.get("user_id"), "user_id")
    except (ValueError, AttributeError, TypeError) as exc:
        return ParseFailure(f"invalid_envelope: {exc}", record)

    event_type = body["event_type"]
    if not isinstance(event_type, str) or not event_type:
        return ParseFailure("invalid_envelope: event_type must be a non-empty string", record)

    payload = body.get("payload", {})
    if not isinstance(payload, dict):
        return ParseFailure("invalid_envelope: payload must be an object", record)

    return ParsedEvent(
        event_id=event_id,
        account_id=account_id,
        user_id=user_id,
        event_type=event_type,
        event_time=event_time,
        received_at=received_at,
        payload=payload,
        record=record,
    )
