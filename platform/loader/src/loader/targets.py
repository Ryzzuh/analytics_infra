"""Load targets: what a stream parses into, and where it lands.

The ledger, range claiming, replace-on-rerun and erasure filtering are identical for every
stream (SPEC.md §4.4). Only the parsing and the destination table differ, so those are the
only things a target defines.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Protocol

from psycopg import Connection
from psycopg.types.json import Jsonb

from .models import LedgerEntry, ParsedEvent, ParseFailure, SkipRecord, SourceRecord
from .parse import parse_record


class Parsed(Protocol):
    """Anything a target produces from a record and can write."""

    account_id: int | None


@dataclass(frozen=True)
class LoadTarget:
    """One stream's parse/copy/clear behaviour."""

    name: str
    table: str
    parse: Callable[[SourceRecord], Any]
    copy_sql: str
    to_row: Callable[..., tuple]
    sort_key: Callable[[Any], Any]

    def clear(self, conn: Connection, entry: LedgerEntry) -> None:
        """Remove a previous attempt's rows. `loaded_date` prunes to a single partition."""
        conn.execute(
            f"DELETE FROM {self.table} WHERE loaded_date = %s AND ledger_id = %s",
            (entry.loaded_date, entry.id),
        )


# --------------------------------------------------------------------------- product events

PRODUCT_EVENTS_COPY = """
COPY raw.product_events (
    loaded_date, topic, partition_id, kafka_offset, ledger_id, event_id, account_id, user_id,
    event_type, event_time, received_at, kafka_ts, loaded_at, source_path, payload
) FROM STDIN
"""


def _product_event_row(
    ev: ParsedEvent, *, ledger_id: int, loaded_date: date, loaded_at: datetime, source_path: str
) -> tuple:
    r = ev.record
    return (
        loaded_date,
        r.topic,
        r.partition,
        r.offset,
        ledger_id,
        ev.event_id,
        ev.account_id,
        ev.user_id,
        ev.event_type,
        ev.event_time,
        ev.received_at,
        r.timestamp,
        loaded_at,
        source_path,
        Jsonb(ev.payload),
    )


PRODUCT_EVENTS = LoadTarget(
    name="product_events",
    table="raw.product_events",
    parse=parse_record,
    copy_sql=PRODUCT_EVENTS_COPY,
    to_row=_product_event_row,
    sort_key=lambda ev: ev.event_time,
)


# ------------------------------------------------------------------------------------- CDC

CDC_COPY = """
COPY raw.cdc_changes (
    loaded_date, topic, partition_id, kafka_offset, ledger_id, source_table, op, pk,
    account_id, before, after, source_lsn, source_ts, effective_at, is_snapshot,
    kafka_ts, loaded_at, source_path
) FROM STDIN
"""


def _cdc_row(
    ch, *, ledger_id: int, loaded_date: date, loaded_at: datetime, source_path: str
) -> tuple:
    r = ch.record
    return (
        loaded_date,
        r.topic,
        r.partition,
        r.offset,
        ledger_id,
        ch.source_table,
        ch.op,
        ch.pk,
        ch.account_id,
        Jsonb(ch.before) if ch.before is not None else None,
        Jsonb(ch.after) if ch.after is not None else None,
        ch.source_lsn,
        ch.source_ts,
        ch.effective_at,
        ch.is_snapshot,
        r.timestamp,
        loaded_at,
        source_path,
    )


def cdc_target() -> LoadTarget:
    from .cdc import parse_change  # imported here to keep module import order simple

    return LoadTarget(
        name="cdc_changes",
        table="raw.cdc_changes",
        parse=parse_change,
        copy_sql=CDC_COPY,
        to_row=_cdc_row,
        # LSN is the only total order Postgres gives us for changes across a table.
        sort_key=lambda ch: ch.source_lsn,
    )


# ------------------------------------------------------------------------- billing webhooks

BILLING_COPY = """
COPY raw.billing_webhook_events (
    loaded_date, topic, partition_id, kafka_offset, ledger_id, provider_event_id, event_type,
    account_id, provider_created_at, received_at, kafka_ts, loaded_at, source_path, payload
) FROM STDIN
"""


def _billing_row(
    hook, *, ledger_id: int, loaded_date: date, loaded_at: datetime, source_path: str
) -> tuple:
    r = hook.record
    return (
        loaded_date,
        r.topic,
        r.partition,
        r.offset,
        ledger_id,
        hook.provider_event_id,
        hook.event_type,
        hook.account_id,
        hook.provider_created_at,
        hook.received_at,
        r.timestamp,
        loaded_at,
        source_path,
        Jsonb(hook.payload),
    )


def billing_target() -> LoadTarget:
    from .billing_events import parse_billing_webhook

    return LoadTarget(
        name="billing_webhooks",
        table="raw.billing_webhook_events",
        parse=parse_billing_webhook,
        copy_sql=BILLING_COPY,
        to_row=_billing_row,
        sort_key=lambda hook: hook.provider_created_at,
    )


__all__ = [
    "BILLING_COPY",
    "CDC_COPY",
    "PRODUCT_EVENTS",
    "LoadTarget",
    "ParseFailure",
    "SkipRecord",
    "billing_target",
    "cdc_target",
]
