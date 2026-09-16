"""Loader failures that must be loud rather than silently degrading."""

from __future__ import annotations


class LoaderError(Exception):
    pass


class OffsetOutOfRange(LoaderError):
    """The offsets a rerun needs have fallen out of broker retention.

    Loading a shorter range instead would silently drop data, so the task fails and the
    operator chooses: rebuild from raw (`dbt --full-refresh`) or accept the gap.
    """

    def __init__(self, topic: str, partition: int, requested: int, earliest: int):
        super().__init__(
            f"{topic}/{partition}: offset {requested} is no longer retained "
            f"(earliest available is {earliest}). Rerun cannot be deterministic."
        )
        self.topic = topic
        self.partition = partition
        self.requested = requested
        self.earliest = earliest


class LedgerGap(LoaderError):
    """The next range does not start where the last committed one ended."""
