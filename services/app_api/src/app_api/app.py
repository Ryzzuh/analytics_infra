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


# The event types this page can emit, grouped as the collector groups them into topics. Kept in
# step with collector.EVENT_FAMILIES by a test: a button that emits an unknown type would be
# rejected with a 422 the visitor has no way to interpret.
PRODUCT_ACTIONS = [
    ("Open dashboard", "page_view", "session"),
    ("Run a report", "report_exported", "feature_usage"),
    ("Use a feature", "feature_invoked", "feature_usage"),
    ("Call the API", "api_called", "feature_usage"),
    ("Invite a teammate", "invite_sent", "lifecycle"),
    ("Add a seat", "seat_added", "lifecycle"),
    ("View plans", "plan_viewed", "billing_ui"),
    ("Start checkout", "checkout_started", "billing_ui"),
]


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    """The product, as a product: things to click, and the insights that come back.

    The buttons post real events to the real collector, so a click here travels the whole
    pipeline — collector, broker, loader, dbt, reverse ETL — and comes back as a churn band in
    the table below. That round trip is the demo; a screenshot of a dashboard is not.

    Posting to a *relative* /v1/events matters: Caddy proxies it to the collector from the same
    origin this page was served from, so the browser never makes a cross-origin request and the
    collector never needs CORS — which would mean an unauthenticated write path into the
    warehouse.
    """
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
    options = (
        "".join(f"<option value='{r[0]}'>{r[1]} (#{r[0]})</option>" for r in rows[:50])
        or "<option value='1'>no accounts loaded</option>"
    )
    buttons = "".join(
        f"<button data-type='{etype}' data-family='{family}'>{label}"
        f"<span class='ev'>{etype}</span></button>"
        for label, etype, family in PRODUCT_ACTIONS
    )

    return f"""
    <html><head><title>Acme SaaS</title><meta name="viewport"
      content="width=device-width, initial-scale=1"><style>
      :root {{ color-scheme: light; }}
      body {{ font-family: system-ui, sans-serif; margin: 0; color: #18181b; background: #fafafa; }}
      header {{ background: #18181b; color: #fafafa; padding: 1rem 2rem; display: flex;
               align-items: baseline; gap: 1rem; flex-wrap: wrap; }}
      header h1 {{ font-size: 18px; margin: 0; letter-spacing: -0.01em; }}
      header .sub {{ color: #a1a1aa; font-size: 13px; }}
      main {{ padding: 1.5rem 2rem 3rem; max-width: 1100px; }}
      .panel {{ background: #fff; border: 1px solid #e4e4e7; border-radius: 10px;
                padding: 1.25rem; margin-bottom: 1.5rem; }}
      .panel h2 {{ font-size: 14px; text-transform: uppercase; letter-spacing: 0.05em;
                   color: #71717a; margin: 0 0 0.75rem; }}
      .row {{ display: flex; gap: 0.75rem; align-items: center; flex-wrap: wrap;
              margin-bottom: 1rem; }}
      select {{ font: inherit; padding: 6px 10px; border: 1px solid #d4d4d8; border-radius: 6px;
                background: #fff; }}
      .actions {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(190px, 1fr));
                  gap: 0.6rem; }}
      button {{ font: inherit; text-align: left; padding: 10px 12px; border: 1px solid #d4d4d8;
                border-radius: 8px; background: #fff; cursor: pointer; display: flex;
                flex-direction: column; gap: 3px; }}
      button:hover {{ border-color: #18181b; }}
      button:disabled {{ opacity: 0.5; cursor: wait; }}
      .ev {{ font-family: ui-monospace, monospace; font-size: 11px; color: #71717a; }}
      #log {{ font-family: ui-monospace, monospace; font-size: 12px; background: #fafafa;
              border: 1px solid #e4e4e7; border-radius: 8px; padding: 0.75rem; min-height: 4.5rem;
              max-height: 11rem; overflow-y: auto; white-space: pre-wrap; }}
      .ok {{ color: #15803d; }} .err {{ color: #b91c1c; }}
      table {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
      th, td {{ text-align: left; padding: 6px 10px; border-bottom: 1px solid #e4e4e7; }}
      th {{ font-weight: 600; color: #52525b; }}
      .band.critical {{ color: #b91c1c; font-weight: 600; }}
      .band.high {{ color: #c2410c; }}
      .band.medium {{ color: #a16207; }}
      .band.low {{ color: #15803d; }}
      .note {{ color: #71717a; font-size: 13px; margin: 0 0 1rem; }}
    </style></head><body>
      <header>
        <h1>Acme SaaS</h1>
        <span class="sub">a synthetic product, wired to a real pipeline</span>
      </header>
      <main>
        <div class="panel">
          <h2>Use the product</h2>
          <p class="note">Every button posts a real event to the collector. It travels the whole
          pipeline — broker, loader, dbt — and comes back as a churn band in the table below,
          once the transform has run.</p>
          <div class="row">
            <label for="account">Acting as</label>
            <select id="account">{options}</select>
            <label for="count">Events</label>
            <select id="count">
              <option>1</option><option>5</option><option>25</option><option>100</option>
            </select>
          </div>
          <div class="actions">{buttons}</div>
        </div>

        <div class="panel">
          <h2>What the collector said</h2>
          <div id="log">Ready. Clicks are sent to /v1/events on this same origin, which Caddy
proxies to the collector.</div>
        </div>

        <div class="panel">
          <h2>Accounts</h2>
          <p class="note">Churn bands are written back by the analytics platform (reverse ETL).
          This table is the product's own view of them, not the warehouse's.</p>
          <table>
            <tr><th>ID</th><th>Company</th><th>Subscription</th><th>Churn band</th><th>Score</th>
                <th>Reasons</th><th>Last synced</th></tr>
            {cells}
          </table>
        </div>
      </main>

      <script>
        const log = document.getElementById('log');
        const say = (msg, cls) => {{
          const at = new Date().toLocaleTimeString();
          log.innerHTML = `<span class="${{cls || ''}}">${{at}}  ${{msg}}</span>\n` + log.innerHTML;
        }};

        document.querySelectorAll('button[data-type]').forEach(btn => {{
          btn.addEventListener('click', async () => {{
            const type = btn.dataset.type;
            const accountId = parseInt(document.getElementById('account').value, 10);
            const count = parseInt(document.getElementById('count').value, 10);
            // A stable-ish user per account, so events group the way a real session would
            // rather than inventing a new user for every click.
            const userId = accountId * 1000 + 1;
            const events = Array.from({{length: count}}, () => ({{
              event_id: crypto.randomUUID(),
              account_id: accountId,
              user_id: userId,
              event_type: type,
              event_time: new Date().toISOString(),
              payload: {{ feature: 'demo', surface: 'web', plan: 'team', duration_ms: 120 }}
            }}));

            btn.disabled = true;
            try {{
              const res = await fetch('/v1/events', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify({{events}})
              }});
              const body = await res.text();
              if (res.ok) {{
                say(`${{count}} x ${{type}} -> HTTP ${{res.status}} ${{body}}`, 'ok');
              }} else {{
                // Worth surfacing verbatim: a 422 names the field the collector rejected, and a
                // 401 means the passcode prompt was dismissed.
                say(`${{type}} -> HTTP ${{res.status}} ${{body}}`, 'err');
              }}
            }} catch (e) {{
              say(`${{type}} -> ${{e}}`, 'err');
            }} finally {{
              btn.disabled = false;
            }}
          }});
        }});
      </script>
    </body></html>
    """


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
