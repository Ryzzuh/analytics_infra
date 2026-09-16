"""The product's own API (SPEC.md §8).

This is the reverse-ETL destination, and it is written to behave like somebody else's service
rather than like a cooperative part of the platform: it rate-limits, it 404s for accounts that
no longer exist, and it honours idempotency keys. Those are the behaviours the sync has to
survive, so a mock that accepted everything would make the sync's failure handling untestable.

Insights land in `account_insights`, which is deliberately absent from the Debezium publication
(`db/app/ddl/002_replication.sql`). If it were captured, every score the warehouse wrote would
flow back into the warehouse and feed the model that produced it.
"""

from __future__ import annotations

import json
import os
import time
from collections import deque
from typing import Any

import psycopg
from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.responses import HTMLResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from pydantic import BaseModel, Field

APP_DSN = os.environ.get("APP_DSN", "postgresql://app:app@postgres-app:5432/app")
RATE_LIMIT_PER_MINUTE = int(os.environ.get("INSIGHTS_RATE_LIMIT_PER_MINUTE", 600))

insights_written = Counter("app_insights_written_total", "Insights accepted", ["band"])
insights_rejected = Counter("app_insights_rejected_total", "Insights rejected", ["reason"])

app = FastAPI(title="SaaS App API")

_recent_requests: deque[float] = deque()
# Idempotency keys already applied, with the response that was returned for them. Bounded:
# a real implementation would persist these with a TTL, and the sync's keys change whenever
# the content does.
_idempotency: dict[str, dict[str, Any]] = {}


class Insight(BaseModel):
    account_id: int
    churn_score: int = Field(ge=0, le=100)
    churn_band: str
    score_version: str
    reasons: list[str] = Field(default_factory=list)
    as_of_date: str


def _check_rate_limit() -> None:
    now = time.monotonic()
    while _recent_requests and now - _recent_requests[0] > 60:
        _recent_requests.popleft()
    if len(_recent_requests) >= RATE_LIMIT_PER_MINUTE:
        insights_rejected.labels("rate_limited").inc()
        raise HTTPException(status_code=429, detail="rate limited", headers={"Retry-After": "5"})
    _recent_requests.append(now)


@app.post("/internal/insights")
def write_insight(
    insight: Insight, idempotency_key: str | None = Header(default=None, alias="Idempotency-Key")
) -> dict[str, Any]:
    _check_rate_limit()

    if idempotency_key and idempotency_key in _idempotency:
        # Same content delivered twice — a retry after an ambiguous timeout, not a new fact.
        return _idempotency[idempotency_key]

    with psycopg.connect(APP_DSN, autocommit=True) as conn:
        exists = conn.execute(
            "SELECT 1 FROM accounts WHERE id = %s", (insight.account_id,)
        ).fetchone()
        if not exists:
            # Expected after an erasure. The sync records it and stops trying, rather than
            # retrying forever against an account that will never come back.
            insights_rejected.labels("unknown_account").inc()
            raise HTTPException(status_code=404, detail="unknown account")

        conn.execute(
            """
            INSERT INTO account_insights
                (account_id, churn_score, churn_band, score_version, reasons, synced_at)
            VALUES (%s, %s, %s, %s, %s, now())
            ON CONFLICT (account_id) DO UPDATE SET
                churn_score = excluded.churn_score,
                churn_band = excluded.churn_band,
                score_version = excluded.score_version,
                reasons = excluded.reasons,
                synced_at = excluded.synced_at
            """,
            (
                insight.account_id,
                insight.churn_score,
                insight.churn_band,
                insight.score_version,
                json.dumps(insight.reasons),
            ),
        )

    insights_written.labels(insight.churn_band).inc()
    result = {"account_id": insight.account_id, "stored": True}
    if idempotency_key:
        _idempotency[idempotency_key] = result
    return result


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    """The one product screen worth having: insights arriving back from the warehouse."""
    with psycopg.connect(APP_DSN, autocommit=True) as conn:
        rows = conn.execute(
            """
            SELECT a.id, a.company_name, s.status, i.churn_band, i.churn_score, i.reasons,
                   i.synced_at
            FROM accounts a
            LEFT JOIN account_insights i ON i.account_id = a.id
            LEFT JOIN LATERAL (
                SELECT status FROM subscriptions WHERE account_id = a.id
                ORDER BY effective_at DESC LIMIT 1
            ) s ON true
            ORDER BY i.churn_score DESC NULLS LAST, a.id
            LIMIT 50
            """
        ).fetchall()

    cells = "".join(
        f"<tr><td>{r[0]}</td><td>{r[1]}</td><td>{r[2] or '-'}</td>"
        f"<td class='band {r[3] or 'none'}'>{r[3] or 'not scored'}</td>"
        f"<td>{r[4] if r[4] is not None else '-'}</td>"
        f"<td>{', '.join(r[5]) if r[5] else '-'}</td>"
        f"<td>{r[6].strftime('%Y-%m-%d %H:%M') if r[6] else '-'}</td></tr>"
        for r in rows
    )
    return f"""
    <html><head><title>Acme SaaS - accounts</title><style>
      body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #1a1a1a; }}
      table {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
      th, td {{ text-align: left; padding: 6px 10px; border-bottom: 1px solid #e4e4e7; }}
      th {{ font-weight: 600; color: #52525b; }}
      .band.critical {{ color: #b91c1c; font-weight: 600; }}
      .band.high {{ color: #c2410c; }}
      .band.medium {{ color: #a16207; }}
      .band.low {{ color: #15803d; }}
      .note {{ color: #71717a; font-size: 13px; margin-bottom: 1.5rem; }}
    </style></head><body>
      <h1>Accounts</h1>
      <p class="note">Churn bands are written back by the analytics platform (reverse ETL).
      This table is the product's own view of them.</p>
      <table>
        <tr><th>ID</th><th>Company</th><th>Subscription</th><th>Churn band</th><th>Score</th>
            <th>Reasons</th><th>Last synced</th></tr>
        {cells}
      </table>
    </body></html>
    """


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
