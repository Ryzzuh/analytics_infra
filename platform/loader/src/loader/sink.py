"""The message-sink port: the write side of the broker.

The collector produces through confluent-kafka directly, because it is a service whose whole
job is that one call. History generation is different: it produces millions of messages from
code that has to be runnable, testable and restartable without a broker, so it goes through a
port with an in-memory implementation.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class MessageSink(Protocol):
    def send(self, topic: str, value: bytes, *, key: bytes | None = None) -> None: ...

    def flush(self) -> None:
        """Block until everything sent so far is acknowledged by the broker."""
        ...
