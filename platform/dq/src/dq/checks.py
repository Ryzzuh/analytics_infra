"""Data-quality checks that dbt's own tests cannot express (SPEC.md §10.3).

dbt covers "is this column null / unique / in this set". What it does not cover is the class of
problem where every row is individually valid and the *shape* is wrong: half the usual volume,
a duplicate rate that tripled overnight, data that stopped arriving. Those are the failures
that reach a dashboard and get acted on before anyone notices, so they are the ones worth
alerting on.

Every check writes a row to `ops.dq_results` whether it passed or not. A check that only
records failures cannot answer "when did this start", and the answer to that question is
usually the whole investigation.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from psycopg import Connection

log = logging.getLogger(__name__)

PASS, WARN, FAIL = "pass", "warn", "fail"


@dataclass
class CheckResult:
    check_name: str
    target: str
    status: str
    observed: float | None = None
    threshold: float | None = None
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status == PASS


def _record(conn: Connection, result: CheckResult, checked_at: datetime) -> None:
    conn.execute(
        "INSERT INTO ops.dq_results (checked_at, check_name, target, status, observed, "
        "threshold, details) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (
            checked_at,
            result.check_name,
            result.target,
            result.status,
            result.observed,
            result.threshold,
            json.dumps(result.details),
        ),
    )


def volume_anomaly(
    conn: Connection,
    *,
    now: datetime,
    tolerance: float = 0.5,
    min_history_days: int = 3,
) -> CheckResult:
    """Yesterday's event volume against the median of the preceding week.

    Median, not mean: one traffic spike or one outage day would drag a mean far enough to hide
    the next anomaly behind it.

    Ratios rather than absolute counts, so the check keeps working as the product grows.
    """
    day = (now - timedelta(days=1)).date()
    rows = conn.execute(
        """
        SELECT activity_date, sum(events) AS events
        FROM core.fct_account_activity_daily
        WHERE activity_date > %s - 8 AND activity_date <= %s
        GROUP BY 1 ORDER BY 1
        """,
        (day, day),
    ).fetchall()

    history = {r[0]: float(r[1]) for r in rows}
    observed = history.pop(day, 0.0)

    if len(history) < min_history_days:
        # Not enough history to have an opinion. Saying "pass" would be a lie; saying "fail"
        # would cry wolf on every fresh deployment.
        return CheckResult(
            "volume_anomaly",
            "core.fct_account_activity_daily",
            WARN,
            observed=observed,
            details={"reason": "insufficient history", "days_available": len(history)},
        )

    values = sorted(history.values())
    middle = len(values) // 2
    median = values[middle] if len(values) % 2 else (values[middle - 1] + values[middle]) / 2
    ratio = observed / median if median else 0.0
    status = PASS if (1 - tolerance) <= ratio <= (1 + tolerance) else FAIL

    return CheckResult(
        "volume_anomaly",
        "core.fct_account_activity_daily",
        status,
        observed=ratio,
        threshold=tolerance,
        details={"day": str(day), "events": observed, "median_events": median},
    )


def duplicate_rate(conn: Connection, *, now: datetime, threshold: float = 0.02) -> CheckResult:
    """How much of today's traffic was a client retrying something we already had.

    Duplicates are expected — the collector acks only after the broker does, so a timeout means
    a retry. A rising rate means something changed: a client bug, or an edge that started
    timing out.
    """
    row = conn.execute(
        """
        SELECT coalesce(sum(duplicate_copies), 0), coalesce(sum(events), 0)
        FROM core.fct_account_activity_daily
        WHERE activity_date > %s - 1
        """,
        (now.date(),),
    ).fetchone()
    duplicates, events = float(row[0]), float(row[1])
    rate = duplicates / events if events else 0.0

    return CheckResult(
        "duplicate_rate",
        "core.fct_account_activity_daily",
        PASS if rate <= threshold else FAIL,
        observed=rate,
        threshold=threshold,
        details={"duplicate_copies": duplicates, "events": events},
    )


def quarantine_rate(conn: Connection, *, now: datetime, threshold: float = 0.01) -> CheckResult:
    """Share of recent events the lateness cutoff is holding back (SPEC.md §6.2).

    Some quarantining is the design working. A lot of it means clients are buffering for days,
    or the loader has been stalled long enough that live traffic now looks stale.
    """
    quarantined = conn.execute(
        "SELECT count(*) FROM staging.stg_product_events_quarantined "
        "WHERE loaded_at > %s - interval '1 day'",
        (now,),
    ).fetchone()[0]
    loaded = conn.execute(
        "SELECT count(*) FROM staging.stg_product_events WHERE loaded_at > %s - interval '1 day'",
        (now,),
    ).fetchone()[0]

    total = quarantined + loaded
    rate = quarantined / total if total else 0.0

    return CheckResult(
        "quarantine_rate",
        "staging.stg_product_events",
        PASS if rate <= threshold else FAIL,
        observed=rate,
        threshold=threshold,
        details={"quarantined": quarantined, "loaded": loaded},
    )


def reverse_etl_health(conn: Connection, *, now: datetime, threshold: float = 0.02) -> CheckResult:
    """Share of accounts whose last sync attempt failed with a server error.

    `skipped_client_error` is excluded deliberately: an erased account returning 404 forever is
    the system working, and counting it as a failure would make the metric permanently red.
    """
    row = conn.execute(
        "SELECT count(*) FILTER (WHERE status = 'failed'), "
        "count(*) FILTER (WHERE status <> 'skipped_client_error') "
        "FROM ops.reverse_etl_sync_state"
    ).fetchone()
    failed, considered = float(row[0]), float(row[1])
    rate = failed / considered if considered else 0.0

    return CheckResult(
        "reverse_etl_error_rate",
        "ops.reverse_etl_sync_state",
        PASS if rate <= threshold else FAIL,
        observed=rate,
        threshold=threshold,
        details={"failed": failed, "considered": considered},
    )


FRESHNESS_SOURCES = {
    "raw_product_events": (
        "SELECT max(loaded_at) FROM raw.product_events WHERE source_path = 'live'"
    ),
    "raw_cdc": "SELECT max(loaded_at) FROM raw.cdc_changes",
    "raw_billing": "SELECT max(loaded_at) FROM raw.billing_webhook_events",
    "staging": "SELECT max(loaded_at) FROM staging.stg_product_events",
    "marts": "SELECT max(as_of_date)::timestamptz FROM marts.mart_account_health",
    "reverse_etl": "SELECT max(last_success_at) FROM ops.reverse_etl_sync_state",
}


def freshness(conn: Connection, *, now: datetime) -> list[CheckResult]:
    """Age of each layer against its SLO (SPEC.md §10.1).

    A layer that has never produced anything is reported as a failure rather than skipped: on a
    fresh deployment "no data yet" and "the loader is broken" look identical from the outside,
    and the one that matters is the second.
    """
    targets = {
        row[0]: row[1]
        for row in conn.execute("SELECT layer, target_seconds FROM ops.freshness_slo").fetchall()
    }
    results = []

    for layer, query in FRESHNESS_SOURCES.items():
        target_seconds = targets.get(layer)
        if target_seconds is None:
            continue
        try:
            latest = conn.execute(query).fetchone()[0]
        except Exception as exc:  # noqa: BLE001 - a missing relation is a real finding
            results.append(
                CheckResult(
                    "freshness", layer, FAIL, details={"error": str(exc).splitlines()[0][:200]}
                )
            )
            continue

        if latest is None:
            results.append(
                CheckResult(
                    "freshness",
                    layer,
                    FAIL,
                    threshold=target_seconds,
                    details={"reason": "no data in this layer"},
                )
            )
            continue

        age = (now - latest).total_seconds()
        results.append(
            CheckResult(
                "freshness",
                layer,
                PASS if age <= target_seconds else FAIL,
                observed=age,
                threshold=target_seconds,
                details={"latest": latest.isoformat()},
            )
        )

    return results


def run_checks(conn: Connection, *, now: datetime | None = None) -> list[CheckResult]:
    """Run every check and record the results, passes included."""
    at = now or datetime.now(UTC)
    results: list[CheckResult] = [
        volume_anomaly(conn, now=at),
        duplicate_rate(conn, now=at),
        quarantine_rate(conn, now=at),
        reverse_etl_health(conn, now=at),
        *freshness(conn, now=at),
    ]

    for result in results:
        _record(conn, result, at)

    failed = [r for r in results if r.status == FAIL]
    log.info("dq: %d checks, %d failing", len(results), len(failed))
    return results
