"""Redpanda/Kafka implementation of MessageSink, tuned for bulk history production."""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


class KafkaMessageSink:
    def __init__(self, bootstrap_servers: str, *, linger_ms: int = 200, batch_size: int = 1 << 20):
        from confluent_kafka import Producer

        self._failures: list[str] = []
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                # Bulk settings: history production is throughput-bound, not latency-bound,
                # so batches are large and linger is long compared with the collector's.
                "linger.ms": linger_ms,
                "batch.size": batch_size,
                "compression.type": "zstd",
                "queue.buffering.max.messages": 1_000_000,
            }
        )

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self._failures.append(str(err))

    def send(self, topic: str, value: bytes, *, key: bytes | None = None) -> None:
        from confluent_kafka import KafkaException

        while True:
            try:
                self._producer.produce(
                    topic=topic, value=value, key=key, on_delivery=self._on_delivery
                )
                return
            except BufferError:
                # The local queue is full: the broker is the bottleneck, so wait for it rather
                # than dropping history on the floor.
                self._producer.poll(0.5)
            except KafkaException:
                raise

    def flush(self) -> None:
        self._producer.flush()
        if self._failures:
            failures, self._failures = self._failures, []
            raise RuntimeError(
                f"{len(failures)} messages were not acknowledged; first failure: {failures[0]}"
            )
