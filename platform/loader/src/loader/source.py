"""The message-source port.

The loader only ever reads an explicit [start, end) offset range. That is what makes a rerun
deterministic: it replays the range recorded in the ledger, not "whatever is in the topic now".
Consumer-group offsets are never read back as truth; `commit_offsets` exists purely so lag
metrics look right in the broker's own tooling.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Protocol, runtime_checkable

from .models import SourceRecord


@runtime_checkable
class MessageSource(Protocol):
    def partitions(self, topic: str) -> list[int]: ...

    def earliest_offset(self, topic: str, partition: int) -> int: ...

    def end_offset(self, topic: str, partition: int) -> int:
        """Exclusive high-water mark."""
        ...

    def fetch(self, topic: str, partition: int, start: int, end: int) -> Iterator[SourceRecord]:
        """Records in [start, end). Raises OffsetOutOfRange if `start` is no longer retained."""
        ...

    def commit_offsets(self, topic: str, partition: int, offset: int) -> None:
        """Advance the consumer group. Monitoring only; never read back as truth."""
        ...
