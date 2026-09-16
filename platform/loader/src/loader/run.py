"""The micro-batch load itself.

Invariants this module exists to guarantee (SPEC.md §4.4):

1. Rows and their ledger entry commit in ONE transaction. A crash anywhere before COMMIT
   leaves no trace, so the next run redoes the range cleanly.
2. A crash AFTER commit but before the broker offset commit is harmless, because the next
   run reads its starting point from the ledger, not from the broker.
3. Rerunning a run id re-reads the exact offset range that run recorded and REPLACES its
   rows, so a fixed parser can be reapplied without duplicating data.
4. A rerun whose offsets have expired from the broker fails loudly instead of loading less.
5. Erased accounts can never re-enter, on any path, because the filter is applied at load
   time rather than at purge time only.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, date, datetime

from psycopg import Connection
from psycopg.types.json import Jsonb

from . import ledger
from .errors import OffsetOutOfRange
from .models import LoadResult, ParsedEvent, ParseFailure, PartitionLoad
from .parse import parse_record
from .source import MessageSource

log = logging.getLogger(__name__)

COPY_SQL = """
COPY raw.product_events (
    loaded_date, topic, partition_id, kafka_offset, ledger_id, event_id, account_id, user_id,
    event_type, event_time, received_at, kafka_ts, loaded_at, source_path, payload
) FROM STDIN
"""


def ensure_partition(conn: Connection, day: date) -> None:
    """Create the raw partition for `day` if it does not exist.

    Cheap enough to call every run, and it means a new day never fails the first load.
    """
    name = f"product_events_{day:%Y%m%d}"
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS raw.{name} PARTITION OF raw.product_events "
        f"FOR VALUES FROM ('{day.isoformat()}') TO ('{day.fromordinal(day.toordinal() + 1)}')"
    )


def _copy_rows(
    conn: Connection,
    events: list[ParsedEvent],
    *,
    ledger_id: int,
    loaded_date: date,
    loaded_at: datetime,
    source_path: str,
) -> None:
    with conn.cursor().copy(COPY_SQL) as cp:
        for ev in events:
            r = ev.record
            cp.write_row(
                (
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
            )


def _write_dlq(conn: Connection, failures: list[ParseFailure], ledger_id: int) -> None:
    if not failures:
        return
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO ops.load_dlq "
            "(ledger_id, topic, partition_id, kafka_offset, reason, raw_value) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            [
                (
                    ledger_id,
                    f.record.topic,
                    f.record.partition,
                    f.record.offset,
                    f.reason,
                    f.raw_text,
                )
                for f in failures
            ],
        )


def load_partition(
    conn: Connection,
    source: MessageSource,
    *,
    topic: str,
    partition_id: int,
    dag_run_id: str,
    max_records: int = 50_000,
    source_path: str = "live",
    now: datetime | None = None,
    before_commit: Callable[[], None] | None = None,
) -> PartitionLoad:
    """Load one (topic, partition) for one run. Commits on success, rolls back on any error.

    `before_commit` is a fault-injection seam: chaos scenarios and tests use it to die with
    the transaction open, which is the crash this design exists to survive.
    """
    loaded_at = now or datetime.now(UTC)
    existing = ledger.find_entry(conn, dag_run_id, topic, partition_id)
    replacing = existing is not None

    if replacing:
        start, end = existing.start_offset, existing.end_offset
        loaded_date = existing.loaded_date  # reuse so the delete prunes to one partition
    else:
        watermark = ledger.watermark(conn, topic, partition_id)
        start = watermark if watermark is not None else source.earliest_offset(topic, partition_id)
        end = min(source.end_offset(topic, partition_id), start + max_records)
        loaded_date = loaded_at.date()
        if end <= start:
            return PartitionLoad(topic, partition_id, start, start, skipped=True)

    earliest = source.earliest_offset(topic, partition_id)
    if start < earliest:
        # Live path: data expired before we read it. Rerun path: the range is gone.
        # Either way, loading a shorter range would silently lose data.
        raise OffsetOutOfRange(topic, partition_id, start, earliest)

    result = PartitionLoad(topic, partition_id, start, end, replaced=replacing)

    with conn.transaction():
        if replacing:
            entry = existing
            ledger.clear_entry_data(conn, entry)
        else:
            entry = ledger.claim(
                conn,
                dag_run_id=dag_run_id,
                topic=topic,
                partition_id=partition_id,
                start_offset=start,
                end_offset=end,
                loaded_date=loaded_date,
            )
        ensure_partition(conn, loaded_date)

        events: list[ParsedEvent] = []
        failures: list[ParseFailure] = []
        for record in source.fetch(topic, partition_id, start, end):
            parsed = parse_record(record)
            if isinstance(parsed, ParseFailure):
                failures.append(parsed)
            else:
                events.append(parsed)

        erased = ledger.erased_account_ids(conn)
        if erased:
            kept = [e for e in events if e.account_id not in erased]
            result.erased_skipped = len(events) - len(kept)
            events = kept

        if source_path == "backfill":
            # Restores event_time/load-order correlation so the BRIN index stays useful in the
            # single partition a backfill writes into (SPEC.md §5.2).
            events.sort(key=lambda e: e.event_time)

        _copy_rows(
            conn,
            events,
            ledger_id=entry.id,
            loaded_date=loaded_date,
            loaded_at=loaded_at,
            source_path=source_path,
        )
        _write_dlq(conn, failures, entry.id)
        ledger.finalise(
            conn,
            entry.id,
            row_count=len(events),
            dlq_count=len(failures),
            erased_count=result.erased_skipped,
            bump_attempt=replacing,
        )

        result.ledger_id = entry.id
        result.rows_loaded = len(events)
        result.dlq_rows = len(failures)
        result.attempt = entry.attempt + (1 if replacing else 0)

        if before_commit is not None:
            before_commit()

    # Offsets are advanced only after the database transaction has committed, and a failure
    # here is logged rather than raised: the ledger is already correct.
    try:
        source.commit_offsets(topic, partition_id, end)
    except Exception:  # noqa: BLE001 - monitoring only, never correctness
        log.warning("offset commit failed for %s/%s at %s", topic, partition_id, end, exc_info=True)

    return result


def run_load(
    conn: Connection,
    source: MessageSource,
    *,
    topic: str,
    dag_run_id: str,
    max_records: int = 50_000,
    source_path: str = "live",
    now: datetime | None = None,
    before_commit: Callable[[], None] | None = None,
) -> LoadResult:
    """Load every partition of `topic` for one run."""
    result = LoadResult(dag_run_id=dag_run_id, topic=topic)
    for partition_id in source.partitions(topic):
        result.partitions.append(
            load_partition(
                conn,
                source,
                topic=topic,
                partition_id=partition_id,
                dag_run_id=dag_run_id,
                max_records=max_records,
                source_path=source_path,
                now=now,
                before_commit=before_commit,
            )
        )
    return result
