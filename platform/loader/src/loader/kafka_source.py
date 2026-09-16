"""Redpanda/Kafka adapter for the MessageSource port.

Reads by explicit assignment and seek rather than by subscribing, because the loader must be
able to replay an exact offset range rather than "wherever the consumer group happens to be".
The consumer group still exists, so `rpk group describe` and the lag dashboards work, but its
committed offsets are never read back as truth (SPEC.md §4.4).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

from .errors import OffsetOutOfRange
from .models import SourceRecord

DEFAULT_GROUP = "warehouse-loader"
_POLL_TIMEOUT_S = 5.0


class KafkaMessageSource:
    def __init__(self, bootstrap_servers: str, group_id: str = DEFAULT_GROUP):
        from confluent_kafka import Consumer  # imported lazily: tests never need the C library

        self._bootstrap = bootstrap_servers
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "enable.auto.commit": False,  # the ledger decides, never the broker
                "auto.offset.reset": "earliest",
            }
        )

    def partitions(self, topic: str) -> list[int]:
        meta = self._consumer.list_topics(topic, timeout=10).topics[topic]
        if meta.error is not None:
            raise RuntimeError(f"topic metadata error for {topic}: {meta.error}")
        return sorted(meta.partitions)

    def _watermarks(self, topic: str, partition: int) -> tuple[int, int]:
        from confluent_kafka import TopicPartition

        return self._consumer.get_watermark_offsets(
            TopicPartition(topic, partition), timeout=10, cached=False
        )

    def earliest_offset(self, topic: str, partition: int) -> int:
        return self._watermarks(topic, partition)[0]

    def end_offset(self, topic: str, partition: int) -> int:
        return self._watermarks(topic, partition)[1]

    def fetch(self, topic: str, partition: int, start: int, end: int) -> Iterator[SourceRecord]:
        from confluent_kafka import KafkaError, TopicPartition

        low, _high = self._watermarks(topic, partition)
        if start < low:
            raise OffsetOutOfRange(topic, partition, start, low)

        self._consumer.assign([TopicPartition(topic, partition, start)])
        try:
            next_offset = start
            while next_offset < end:
                msg = self._consumer.poll(_POLL_TIMEOUT_S)
                if msg is None:
                    raise RuntimeError(
                        f"{topic}/{partition}: poll timed out at offset {next_offset} "
                        f"before reaching {end}; the claimed range is incomplete"
                    )
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    raise RuntimeError(f"{topic}/{partition}: {msg.error()}")
                _kind, ts_ms = msg.timestamp()
                yield SourceRecord(
                    topic=topic,
                    partition=partition,
                    offset=msg.offset(),
                    timestamp=datetime.fromtimestamp(ts_ms / 1000, tz=UTC),
                    key=msg.key(),
                    value=msg.value(),
                )
                next_offset = msg.offset() + 1
        finally:
            self._consumer.unassign()

    def commit_offsets(self, topic: str, partition: int, offset: int) -> None:
        from confluent_kafka import TopicPartition

        self._consumer.commit(
            offsets=[TopicPartition(topic, partition, offset)], asynchronous=False
        )

    def close(self) -> None:
        self._consumer.close()
