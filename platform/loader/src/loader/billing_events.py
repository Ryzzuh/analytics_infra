"""Parsing for billing webhooks arriving through the broker (SPEC.md §4.3).

Webhooks are at-least-once and unordered by nature, so nothing here tries to enforce order or
uniqueness: raw keeps whatever arrived, and staging reconciles it against the daily pull. The
one thing this does enforce is that an event without a provider id is unusable — it cannot be
deduplicated against the batch path, so it cannot be merged with it.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from .models import BillingWebhook, ParseFailure, SourceRecord


def _parse_ts(value: str) -> datetime:
    ts = datetime.fromisoformat(value)
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts


def parse_billing_webhook(record: SourceRecord) -> BillingWebhook | ParseFailure:
    if record.value is None:
        return ParseFailure("tombstone_on_billing_topic", record)

    try:
        body = json.loads(record.value)
    except json.JSONDecodeError as exc:
        return ParseFailure(f"invalid_json: {exc.msg}", record)

    if not isinstance(body, dict):
        return ParseFailure("envelope_not_object", record)

    provider_event_id = body.get("id")
    if not isinstance(provider_event_id, str) or not provider_event_id:
        # Without the provider's id there is nothing to reconcile against the batch pull.
        return ParseFailure("missing_provider_event_id", record)

    event_type = body.get("type")
    if not isinstance(event_type, str) or not event_type:
        return ParseFailure("missing_event_type", record)

    try:
        created_at = _parse_ts(body["created_at"])
        received_at = _parse_ts(body.get("received_at") or body["created_at"])
    except (KeyError, ValueError, TypeError) as exc:
        return ParseFailure(f"invalid_timestamps: {exc}", record)

    account_id = body.get("account_id")
    if account_id is not None and not isinstance(account_id, int):
        return ParseFailure("invalid_account_id", record)

    return BillingWebhook(
        provider_event_id=provider_event_id,
        event_type=event_type,
        account_id=account_id,
        provider_created_at=created_at,
        received_at=received_at,
        payload=body,
        record=record,
    )
