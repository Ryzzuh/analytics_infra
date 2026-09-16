"""In-memory MessageSource plus event builders.

Shipped inside the package (not tests/) because the chaos harness and the local CLI use the
same fake to replay scenarios without a broker.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from .errors import OffsetOutOfRange
from .models import SourceRecord


class FakeMessageSource:
    """A log per (topic, partition) with a movable `earliest` to simulate retention."""

    def __init__(self) -> None:
        self._log: dict[tuple[str, int], list[SourceRecord]] = {}
        self._earliest: dict[tuple[str, int], int] = {}
        self.committed: dict[tuple[str, int], int] = {}
        self.commit_should_fail = False

    # -- production side -------------------------------------------------
    def produce(
        self,
        topic: str,
        partition: int,
        value: bytes | None,
        *,
        key: bytes | None = None,
        timestamp: datetime | None = None,
    ) -> SourceRecord:
        log = self._log.setdefault((topic, partition), [])
        offset = self._earliest.get((topic, partition), 0) + len(log)
        record = SourceRecord(
            topic=topic,
            partition=partition,
            offset=offset,
            timestamp=timestamp or datetime.now(UTC),
            key=key,
            value=value,
        )
        log.append(record)
        return record

    def replace_at(self, topic: str, partition: int, offset: int, value: bytes | None) -> None:
        """Rewrite the value at an offset.

        Stands in for "the same range, read by fixed code".
        """
        log = self._log[(topic, partition)]
        index = next(i for i, r in enumerate(log) if r.offset == offset)
        log[index] = SourceRecord(
            topic=topic,
            partition=partition,
            offset=offset,
            timestamp=log[index].timestamp,
            key=log[index].key,
            value=value,
        )

    def expire_through(self, topic: str, partition: int, offset: int) -> None:
        """Drop everything before `offset`, as broker retention would."""
        key = (topic, partition)
        log = self._log.get(key, [])
        self._log[key] = [r for r in log if r.offset >= offset]
        self._earliest[key] = offset

    # -- MessageSource port ----------------------------------------------
    def partitions(self, topic: str) -> list[int]:
        return sorted(p for (t, p) in self._log if t == topic)

    def earliest_offset(self, topic: str, partition: int) -> int:
        return self._earliest.get((topic, partition), 0)

    def end_offset(self, topic: str, partition: int) -> int:
        log = self._log.get((topic, partition), [])
        return log[-1].offset + 1 if log else self._earliest.get((topic, partition), 0)

    def fetch(self, topic: str, partition: int, start: int, end: int) -> Iterator[SourceRecord]:
        earliest = self.earliest_offset(topic, partition)
        if start < earliest:
            raise OffsetOutOfRange(topic, partition, start, earliest)
        for record in self._log.get((topic, partition), []):
            if start <= record.offset < end:
                yield record

    def commit_offsets(self, topic: str, partition: int, offset: int) -> None:
        if self.commit_should_fail:
            raise RuntimeError("broker unavailable for offset commit")
        self.committed[(topic, partition)] = offset


def event_bytes(
    *,
    event_id: UUID | None = None,
    account_id: int | None = 1,
    user_id: int | None = 10,
    event_type: str = "feature_invoked",
    event_time: datetime | str | None = None,
    received_at: datetime | str | None = None,
    payload: dict | None = None,
    **overrides,
) -> bytes:
    """A valid event envelope.

    Timestamps accept a raw string so a test can inject malformed values, which is how the
    DLQ path is exercised.
    """
    now = datetime.now(UTC)

    def _iso(value: datetime | str) -> str:
        return value if isinstance(value, str) else value.isoformat()

    body: dict = {
        "event_id": str(event_id or uuid4()),
        "account_id": account_id,
        "user_id": user_id,
        "event_type": event_type,
        "event_time": _iso(event_time or now),
        "received_at": _iso(received_at or now + timedelta(milliseconds=40)),
        "payload": payload if payload is not None else {"feature": "export"},
    }
    body.update(overrides)
    return json.dumps(body).encode()


def change_bytes(
    *,
    op: str = "u",
    before: dict | None = None,
    after: dict | None = None,
    lsn: int = 1,
    source_table: str = "subscriptions",
    effective_at: datetime | None = None,
    source_ts: datetime | None = None,
    snapshot: bool = False,
) -> bytes:
    """A Debezium change envelope, as the connector emits it with schemas disabled."""
    commit_ts = source_ts or datetime.now(UTC)
    row = before if op == "d" else after
    if row is not None and effective_at is not None:
        row = {**row, "effective_at": effective_at.isoformat()}
        if op == "d":
            before = row
        else:
            after = row
    return json.dumps(
        {
            "before": before,
            "after": after,
            "op": op,
            "ts_ms": int(commit_ts.timestamp() * 1000),
            "source": {
                "db": "app",
                "schema": "public",
                "table": source_table,
                "lsn": lsn,
                "ts_ms": int(commit_ts.timestamp() * 1000),
                "snapshot": "true" if snapshot else "false",
            },
        }
    ).encode()


def tombstone_key(pk: int | str, column: str = "id") -> bytes:
    """The key a tombstone carries: the row's primary key, with a null value."""
    return json.dumps({column: pk}).encode()


class FakeMessageSink:
    """Collects produced messages, and can feed them straight into a FakeMessageSource.

    Lets history generation be tested end to end — generate, load, model — with no broker.
    """

    def __init__(self, source: FakeMessageSource | None = None, partitions: int = 1):
        self.source = source or FakeMessageSource()
        self.partitions = partitions
        self.sent = 0
        self.flushed = 0

    def send(self, topic: str, value: bytes, *, key: bytes | None = None) -> None:
        # Mirrors Kafka's default partitioner closely enough for ordering tests: the same key
        # always lands on the same partition.
        partition = 0 if key is None or self.partitions == 1 else hash(key) % self.partitions
        self.source.produce(topic, partition, value, key=key)
        self.sent += 1

    def flush(self) -> None:
        self.flushed += 1
