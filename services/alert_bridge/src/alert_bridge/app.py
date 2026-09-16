"""Alertmanager webhooks to Telegram (SPEC.md §10.2).

Alertmanager has no native Telegram receiver worth using here, and the interesting part is not
the HTTP call anyway — it is what a message says. An alert that arrives as a wall of labels
gets swiped away; one that says what broke, how bad it is, and which runbook to open gets acted
on. So the formatting is the feature.

A separate bot from any personal one: a public demo can generate alerts, and those should not
land in a chat used for anything else.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx
from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

log = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
REPO_URL = os.environ.get("REPO_URL", "https://github.com/Ryzzuh/analytics_infra/blob/main")

notifications = Counter("alert_bridge_notifications_total", "Messages sent", ["status", "outcome"])

app = FastAPI(title="Alert Bridge")

SEVERITY_MARK = {"critical": "🔴", "warning": "🟠", "info": "🔵"}


def format_alert(alert: dict[str, Any]) -> str:
    labels = alert.get("labels", {})
    annotations = alert.get("annotations", {})
    severity = labels.get("severity", "warning")
    resolved = alert.get("status") == "resolved"

    mark = "✅" if resolved else SEVERITY_MARK.get(severity, "⚪")
    headline = f"{mark} <b>{labels.get('alertname', 'Alert')}</b>"
    if resolved:
        headline += " (resolved)"

    lines = [headline]
    if summary := annotations.get("summary"):
        lines.append(summary)

    # Context that makes the message actionable rather than merely alarming.
    context = [f"{k}={v}" for k, v in sorted(labels.items()) if k not in {"alertname", "severity"}]
    if context:
        lines.append(f"<i>{' · '.join(context)}</i>")
    if runbook := annotations.get("runbook"):
        lines.append(f"Runbook: {REPO_URL}/{runbook}")

    return "\n".join(lines)


async def send_to_telegram(text: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        # Unconfigured is a normal local state: log the message rather than failing the
        # webhook, or Alertmanager will retry a notification nobody can receive.
        log.info("telegram not configured; would have sent:\n%s", text)
        return False
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"},
        )
        response.raise_for_status()
    return True


@app.post("/alerts")
async def receive(request: Request) -> dict[str, Any]:
    body = await request.json()
    alerts = body.get("alerts", [])
    sent = 0

    for alert in alerts:
        text = format_alert(alert)
        try:
            if await send_to_telegram(text):
                sent += 1
                notifications.labels(alert.get("status", "firing"), "sent").inc()
            else:
                notifications.labels(alert.get("status", "firing"), "unconfigured").inc()
        except httpx.HTTPError as exc:
            # Never 5xx back to Alertmanager for a downstream problem: it would retry the whole
            # group, and a flapping Telegram API would turn one incident into a flood.
            log.warning("telegram delivery failed: %s", exc)
            notifications.labels(alert.get("status", "firing"), "failed").inc()

    return {"received": len(alerts), "sent": sent}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
