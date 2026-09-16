"""Debezium change-event parsing.

Two subtleties drive this module:

**Tombstones are not data.** A delete produces two messages: the change event itself
(`op="d"`, carrying the `before` image) and then a tombstone — same key, null value — whose
only job is to let log compaction drop the key's history. Storing tombstones would double
every delete, and DLQ-ing them would alarm on normal operation. They are skipped and counted.

**`effective_at` is business time, not wall-clock time.** During the historical replay the
simulator writes a year of changes in minutes, so Debezium's `source.ts_ms` says "now" for a
subscription that actually changed last March. SCD2 validity intervals therefore key off the
`effective_at` column carried in the row itself, with `source.lsn` as the tiebreak, because
LSN is the only total order Postgres gives for committed changes.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from .models import CdcChange, ParseFailure, SkipRecord, SourceRecord

VALID_OPS = {"c", "u", "d", "r"}  # create, update, delete, read (snapshot)


def _ts_from_ms(value: Any) -> datetime:
    return datetime.fromtimestamp(int(value) / 1000, tz=UTC)


def _effective_at(row: dict | None, fallback: datetime) -> datetime:
    """Business time from the row itself, falling back to commit time."""
    if row:
        raw = row.get("effective_at") or row.get("updated_at") or row.get("created_at")
        if isinstance(raw, str):
            try:
                ts = datetime.fromisoformat(raw)
                return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts
            except ValueError:
                pass
        if isinstance(raw, int):  # Debezium microsecond epoch for timestamp columns
            return datetime.fromtimestamp(raw / 1_000_000, tz=UTC)
    return fallback


def _account_id(source_table: str, after: dict | None, before: dict | None, pk: str) -> int | None:
    """Account association, used by the erasure filter (SPEC.md §9.4)."""
    for row in (after, before):
        if row and isinstance(row.get("account_id"), int):
            return row["account_id"]
    if source_table == "accounts":
        try:
            return int(pk)
        except ValueError:
            return None
    return None


def _primary_key(key_bytes: bytes | None, after: dict | None, before: dict | None) -> str | None:
    if key_bytes:
        try:
            key = json.loads(key_bytes)
        except json.JSONDecodeError:
            return None
        if isinstance(key, dict) and key:
            return "|".join(str(key[k]) for k in sorted(key))
        return str(key)
    for row in (after, before):
        if row and "id" in row:
            return str(row["id"])
    return None


def parse_change(record: SourceRecord) -> CdcChange | ParseFailure | SkipRecord:
    if record.value is None:
        # Expected after every delete; not an error and not a row.
        return SkipRecord("tombstone", record)

    try:
        body = json.loads(record.value)
    except json.JSONDecodeError as exc:
        return ParseFailure(f"invalid_json: {exc.msg}", record)

    if not isinstance(body, dict):
        return ParseFailure("envelope_not_object", record)

    # Debezium can wrap the payload when schemas are enabled; accept either shape.
    payload = body.get("payload", body)
    if not isinstance(payload, dict):
        return ParseFailure("payload_not_object", record)

    op = payload.get("op")
    if op not in VALID_OPS:
        return ParseFailure(f"invalid_op: {op!r}", record)

    source = payload.get("source")
    if not isinstance(source, dict):
        return ParseFailure("missing_source_block", record)

    source_table = source.get("table")
    if not isinstance(source_table, str) or not source_table:
        return ParseFailure("missing_source_table", record)

    lsn = source.get("lsn")
    if not isinstance(lsn, int):
        # Without an LSN there is no reliable order, and SCD2 would be guesswork.
        return ParseFailure("missing_source_lsn", record)

    before = payload.get("before")
    after = payload.get("after")
    if before is not None and not isinstance(before, dict):
        return ParseFailure("before_not_object", record)
    if after is not None and not isinstance(after, dict):
        return ParseFailure("after_not_object", record)
    if op == "d" and before is None:
        # REPLICA IDENTITY FULL is what makes this present. Without it a delete carries only
        # the key, and SCD2 cannot close the interval with the values that were live.
        return ParseFailure("delete_without_before_image", record)
    if op in {"c", "u", "r"} and after is None:
        return ParseFailure(f"{op}_without_after_image", record)

    pk = _primary_key(record.key, after, before)
    if pk is None:
        return ParseFailure("missing_primary_key", record)

    source_ts = _ts_from_ms(source.get("ts_ms") or payload.get("ts_ms") or 0)
    # A delete has no business time of its own. The before-image's `effective_at` records when
    # the row last *changed*, not when it was removed, so dating the deletion by it closes the
    # SCD2 version at the very instant that version opened: valid_from == valid_to, a
    # zero-length interval that no `at >= valid_from and at < valid_to` predicate can ever
    # match. The subscription then vanishes from every as-of query for the whole period it was
    # actually alive. Observed on the running stack: two subscriptions present in the OLTP
    # snapshot with no version covering that instant, both deleted after the snapshot was taken.
    #
    # Commit time is the honest answer for a deletion — unlike an insert or update, there is no
    # surviving row to carry a business timestamp. Erasures happen in the present in any case;
    # the historical replay produces no deletes.
    row_for_time = None if op == "d" else after

    return CdcChange(
        source_table=source_table,
        op=op,
        pk=pk,
        account_id=_account_id(source_table, after, before, pk),
        before=before,
        after=after,
        source_lsn=lsn,
        source_ts=source_ts,
        effective_at=_effective_at(row_for_time, source_ts),
        is_snapshot=str(source.get("snapshot", "false")).lower() in {"true", "first", "last"},
        record=record,
    )
