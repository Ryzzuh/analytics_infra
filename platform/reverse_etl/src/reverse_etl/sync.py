"""Push `marts.mart_account_health` back into the product (SPEC.md §8).

Everything upstream of here controls both ends of the pipe. This does not: the destination is
someone else's API, with its own rate limit, its own idea of which accounts exist, and its own
bad days. So the interesting content is the failure taxonomy, not the HTTP call.

* **Diff-based.** Only accounts whose payload changed since the last successful send are sent.
  Re-sending everything every run burns the destination's rate limit to say nothing, and a
  destination that rate-limits you for saying nothing will rate-limit you when it matters.
* **4xx is recorded and skipped.** A deleted account returning 404 is the expected outcome of
  an erasure, not an incident, and retrying it forever is how a queue becomes permanently
  wedged behind data that will never be accepted.
* **5xx is retried, then counted.** Transient by definition, so a bounded backoff; if it
  persists it is a real failure and the run should say so.
* **429 is obeyed.** The destination is telling you the rate; arguing with it gets you
  throttled harder.
* **The run fails on an error RATE, not on any error.** One bad account out of two thousand is
  not a failed sync, and treating it as one trains people to ignore the alert.

One request per account rather than batches of fifty. The trade is real: batching means fewer
requests, but a partial failure inside a batch is ambiguous unless the API reports per-item
results, and here every account has its own idempotency key and its own recorded state. Rate
limiting is applied client-side between chunks instead.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx
from psycopg import Connection

log = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 50
DEFAULT_MAX_ERROR_RATE = 0.02
MAX_ATTEMPTS = 4


@dataclass
class SyncResult:
    considered: int = 0
    unchanged: int = 0
    sent: int = 0
    succeeded: int = 0
    client_errors: int = 0  # 4xx: recorded, not retried
    server_errors: int = 0  # 5xx: retried, then counted as failures
    rate_limited: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def error_rate(self) -> float:
        return self.server_errors / self.sent if self.sent else 0.0


class SyncFailed(Exception):
    """Too many accounts failed for the run to be called a success."""


def payload_for(row: dict[str, Any]) -> dict[str, Any]:
    """The contract the product consumes.

    Shaped so a model could replace the rules without the destination noticing: score, band,
    version, and the reasons behind it (SPEC.md §6.3).
    """
    return {
        "account_id": row["account_id"],
        "churn_score": row["churn_score"],
        "churn_band": row["churn_band"],
        "score_version": row["score_version"],
        "reasons": row["reasons"],
        "as_of_date": row["as_of_date"].isoformat(),
    }


def payload_hash(payload: dict[str, Any]) -> str:
    """Stable across runs: sorted keys, no whitespace drift, no timestamps of our own."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def idempotency_key(account_id: int, score_version: str, digest: str) -> str:
    """Identifies the *content* being delivered, so a retry after an ambiguous timeout cannot
    apply the same update twice, while a genuinely new score is a new key."""
    return f"{account_id}:{score_version}:{digest[:16]}"


def _candidates(conn: Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT h.account_id, h.churn_score, h.churn_band, h.score_version, h.reasons,
               h.as_of_date, s.payload_hash, s.status
        FROM marts.mart_account_health h
        LEFT JOIN ops.reverse_etl_sync_state s USING (account_id)
        ORDER BY h.churn_score DESC, h.account_id
        """
    ).fetchall()
    columns = (
        "account_id",
        "churn_score",
        "churn_band",
        "score_version",
        "reasons",
        "as_of_date",
        "previous_hash",
        "previous_status",
    )
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _record(
    conn: Connection,
    *,
    account_id: int,
    digest: str,
    score_version: str,
    status: str,
    status_code: int | None,
    error: str | None,
    now: datetime,
) -> None:
    conn.execute(
        """
        INSERT INTO ops.reverse_etl_sync_state
            (account_id, payload_hash, score_version, status, attempts, last_attempt_at,
             last_success_at, last_status_code, last_error)
        VALUES (%(account_id)s, %(hash)s, %(version)s, %(status)s, 1, %(now)s,
                %(success_at)s, %(code)s, %(error)s)
        ON CONFLICT (account_id) DO UPDATE SET
            payload_hash     = excluded.payload_hash,
            score_version    = excluded.score_version,
            status           = excluded.status,
            attempts         = ops.reverse_etl_sync_state.attempts + 1,
            last_attempt_at  = excluded.last_attempt_at,
            last_success_at  = coalesce(excluded.last_success_at,
                                        ops.reverse_etl_sync_state.last_success_at),
            last_status_code = excluded.last_status_code,
            last_error       = excluded.last_error
        """,
        {
            "account_id": account_id,
            "hash": digest,
            "version": score_version,
            "status": status,
            "now": now,
            "success_at": now if status == "synced" else None,
            "code": status_code,
            "error": error,
        },
    )


def _send_one(
    client: httpx.Client,
    payload: dict[str, Any],
    key: str,
    *,
    sleep: Callable[[float], None],
    result: SyncResult,
) -> httpx.Response:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        response = client.post("/internal/insights", json=payload, headers={"Idempotency-Key": key})
        if response.status_code == 429:
            result.rate_limited += 1
            retry_after = response.headers.get("Retry-After")
            sleep(float(retry_after) if retry_after else min(2**attempt * 0.2, 5.0))
            continue
        if response.status_code >= 500 and attempt < MAX_ATTEMPTS:
            sleep(min(2**attempt * 0.2, 5.0))
            continue
        return response
    return response


def sync_account_health(
    conn: Connection,
    client: httpx.Client,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_error_rate: float = DEFAULT_MAX_ERROR_RATE,
    pause_between_chunks: float = 0.0,
    now: datetime | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> SyncResult:
    """Send changed account health to the product. Raises SyncFailed above the error rate."""
    at = now or datetime.now(UTC)
    result = SyncResult()

    for index, row in enumerate(_candidates(conn)):
        result.considered += 1
        payload = payload_for(row)
        digest = payload_hash(payload)

        # The diff. A previously skipped 4xx counts as "delivered as far as we can", so it is
        # not retried until its content actually changes.
        if row["previous_hash"] == digest and row["previous_status"] in {
            "synced",
            "skipped_client_error",
        }:
            result.unchanged += 1
            continue

        if index and index % chunk_size == 0 and pause_between_chunks:
            sleep(pause_between_chunks)

        key = idempotency_key(row["account_id"], row["score_version"], digest)
        result.sent += 1
        response = _send_one(client, payload, key, sleep=sleep, result=result)

        if response.is_success:
            status, error = "synced", None
            result.succeeded += 1
        elif 400 <= response.status_code < 500:
            status = "skipped_client_error"
            error = f"{response.status_code}: {response.text[:200]}"
            result.client_errors += 1
        else:
            status = "failed"
            error = f"{response.status_code}: {response.text[:200]}"
            result.server_errors += 1
            result.errors.append(f"account {row['account_id']}: {error}")

        _record(
            conn,
            account_id=row["account_id"],
            digest=digest,
            score_version=row["score_version"],
            status=status,
            status_code=response.status_code,
            error=error,
            now=at,
        )

    log.info(
        "reverse ETL: %d considered, %d unchanged, %d sent, %d ok, %d 4xx, %d 5xx",
        result.considered,
        result.unchanged,
        result.sent,
        result.succeeded,
        result.client_errors,
        result.server_errors,
    )

    if result.error_rate > max_error_rate:
        raise SyncFailed(
            f"{result.server_errors}/{result.sent} accounts failed with server errors "
            f"({result.error_rate:.1%} > {max_error_rate:.1%}); first: {result.errors[0]}"
        )
    return result
