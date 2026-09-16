"""Reverse ETL: modelled insight, delivered back to the product (SPEC.md §8)."""

from .sync import SyncResult, idempotency_key, payload_for, sync_account_health

__all__ = ["SyncResult", "idempotency_key", "payload_for", "sync_account_health"]
