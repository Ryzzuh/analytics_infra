"""Value types shared by the loader. Deliberately free of psycopg and Kafka imports."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any
from uuid import UUID


@dataclass(frozen=True, slots=True)
class SourceRecord:
    """One message as read from the broker (or a fake)."""

    topic: str
    partition: int
    offset: int
    timestamp: datetime
    key: bytes | None
    value: bytes | None  # None == tombstone


@dataclass(frozen=True, slots=True)
class ParsedEvent:
    event_id: UUID
    account_id: int | None
    user_id: int | None
    event_type: str
    event_time: datetime
    received_at: datetime
    payload: dict[str, Any]
    record: SourceRecord


@dataclass(frozen=True, slots=True)
class ParseFailure:
    reason: str
    record: SourceRecord

    @property
    def raw_text(self) -> str | None:
        if self.record.value is None:
            return None
        return self.record.value.decode("utf-8", errors="replace")[:8192]


@dataclass(frozen=True, slots=True)
class SkipRecord:
    """A message that is neither data nor an error: a CDC tombstone, for instance."""

    reason: str
    record: SourceRecord


@dataclass(frozen=True, slots=True)
class CdcChange:
    source_table: str
    op: str  # c | u | d | r
    pk: str
    account_id: int | None
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    source_lsn: int
    source_ts: datetime
    effective_at: datetime  # business time; see cdc.py
    is_snapshot: bool
    record: SourceRecord


@dataclass(frozen=True, slots=True)
class BillingWebhook:
    provider_event_id: str
    event_type: str
    account_id: int | None
    provider_created_at: datetime
    received_at: datetime
    payload: dict[str, Any]
    record: SourceRecord


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    id: int
    dag_run_id: str
    topic: str
    partition_id: int
    start_offset: int
    end_offset: int
    loaded_date: date
    row_count: int
    dlq_count: int
    erased_count: int
    skipped_count: int
    attempt: int


@dataclass(slots=True)
class PartitionLoad:
    """What one (topic, partition) did in one run."""

    topic: str
    partition_id: int
    start_offset: int
    end_offset: int
    rows_loaded: int = 0
    dlq_rows: int = 0
    erased_skipped: int = 0
    records_skipped: int = 0  # tombstones and other non-data messages
    ledger_id: int | None = None
    attempt: int = 1
    replaced: bool = False
    skipped: bool = False  # nothing new to read


@dataclass(slots=True)
class LoadResult:
    dag_run_id: str
    topic: str
    partitions: list[PartitionLoad] = field(default_factory=list)

    @property
    def rows_loaded(self) -> int:
        return sum(p.rows_loaded for p in self.partitions)

    @property
    def dlq_rows(self) -> int:
        return sum(p.dlq_rows for p in self.partitions)

    @property
    def erased_skipped(self) -> int:
        return sum(p.erased_skipped for p in self.partitions)

    @property
    def records_skipped(self) -> int:
        return sum(p.records_skipped for p in self.partitions)
