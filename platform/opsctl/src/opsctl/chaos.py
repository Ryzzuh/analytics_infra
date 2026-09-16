"""The chaos scenarios (SPEC.md §9.2).

Each scenario is a triple — inject, symptom, recover — because a demo that only breaks things
proves half a point. The half that matters to a platform engineer is that the platform notices
and comes back.

Two design rules hold throughout:

* **Every injection opens a chaos window.** Alertmanager inhibits paging while one is open, so
  a visitor with the demo passcode cannot wake anybody at 3am. The corollary is the valuable
  part: an alert that pages with *no* window open is a genuine incident (SPEC.md §10.2).
* **Nothing here touches the database directly to fake a symptom.** A scenario that writes a
  drift finding by hand proves the alert fires, not that the detector works. Injections act on
  the real system — stop a container, change what the simulator emits, produce malformed
  messages — and everything downstream is the platform genuinely reacting.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from psycopg import Connection
from psycopg.rows import tuple_row

log = logging.getLogger(__name__)


class Executor(Protocol):
    """Whatever can act on the running stack.

    Three verbs rather than "run this shell command", for two reasons. It is typed — a scenario
    cannot ask for something the executor does not support — and it does not assume the caller
    has a docker CLI, a compose file, or the project directory. The Console runs inside a
    container with only the Docker socket, which the original shell-out design could not use.
    """

    def start(self, service: str) -> str: ...

    def stop(self, service: str) -> str: ...

    def exec(self, service: str, *command: str) -> str: ...


@dataclass(frozen=True)
class Scenario:
    key: str
    title: str
    what_breaks: str
    expected_symptoms: list[str]
    recovery: str
    runbook: str
    # How long the window stays open if nobody closes it. Past this the platform is expected to
    # have recovered, and leaving it open would suppress paging indefinitely.
    max_duration: timedelta = timedelta(minutes=30)
    inject: Callable[..., dict[str, Any]] = field(repr=False, default=None)
    recover: Callable[..., dict[str, Any]] = field(repr=False, default=None)


# --------------------------------------------------------------------------- windows


def open_window(conn: Connection, scenario: str, *, opened_by: str = "console") -> int:
    # An explicit row factory, because callers differ: the Console uses dict rows, the checks
    # and tests use tuples, and a library function that depends on the caller's choice breaks
    # in exactly one of them.
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "INSERT INTO ops.chaos_windows (scenario, opened_by) VALUES (%s, %s) RETURNING id",
            (scenario, opened_by),
        )
        window_id = cur.fetchone()[0]
    log.info("chaos window %s opened for %s", window_id, scenario)
    return window_id


def close_window(conn: Connection, window_id: int, *, notes: str | None = None) -> None:
    conn.execute(
        "UPDATE ops.chaos_windows SET closed_at = now(), notes = %s "
        "WHERE id = %s AND closed_at IS NULL",
        (notes, window_id),
    )


def open_windows(conn: Connection) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT id, scenario, opened_at, opened_by FROM ops.chaos_windows "
            "WHERE closed_at IS NULL ORDER BY opened_at"
        )
        rows = cur.fetchall()
    return [{"id": r[0], "scenario": r[1], "opened_at": r[2], "opened_by": r[3]} for r in rows]


def expire_stale_windows(conn: Connection, *, now: datetime | None = None) -> int:
    """Close windows nobody closed.

    Without this, one abandoned scenario suppresses paging forever — the failure mode where the
    safety mechanism becomes the outage.
    """
    at = now or datetime.now(UTC)
    closed = 0
    for window in open_windows(conn):
        scenario = SCENARIOS.get(window["scenario"])
        limit = scenario.max_duration if scenario else timedelta(minutes=30)
        if at - window["opened_at"] > limit:
            close_window(conn, window["id"], notes="expired automatically")
            closed += 1
    if closed:
        log.warning("closed %d stale chaos window(s)", closed)
    return closed


# --------------------------------------------------------------------------- scenarios


def inject_connector_outage(conn: Connection, executor: Executor, **_: Any) -> dict[str, Any]:
    """Stop Kafka Connect and let the consequences unfold on their own.

    Nothing is faked: the slot stops being read, WAL accumulates because Postgres must keep it
    for a consumer that has not confirmed, and the platform's own alerts fire in order — CDC
    stalled first, then the WAL warning, then critical. If it runs long enough the cap
    invalidates the slot, which is the scenario's real payload (SPEC.md §9.1).
    """
    executor.stop("connect")
    return {"stopped": "connect"}


def recover_connector_outage(conn: Connection, executor: Executor, **_: Any) -> dict[str, Any]:
    """Restart Connect, and re-sync properly if the slot was lost while it was down."""
    executor.start("connect")

    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SELECT count(*) FROM ops.cdc_gaps WHERE resolved_at IS NULL")
        slot_lost = cur.fetchone()[0] > 0

    if not slot_lost:
        return {"restarted": "connect", "resnapshot": False}

    # The slot was invalidated, so restarting alone leaves a hole: Debezium resumes from a
    # position whose WAL is gone. An incremental snapshot fills it (SPEC.md §9.1).
    executor.exec(
        "postgres-app",
        "psql",
        "-U",
        "app",
        "-d",
        "app",
        "-c",
        "INSERT INTO debezium_signal (id, type, data) VALUES "
        "(gen_random_uuid()::text, 'execute-snapshot', "
        '\'{"data-collections": ["public.subscriptions", "public.accounts"]}\')',
    )
    return {"restarted": "connect", "resnapshot": True}


def inject_schema_drift(conn: Connection, executor: Executor, **_: Any) -> dict[str, Any]:
    """Ship an 'app release' that renames plan_id to plan_code.

    The simulator starts emitting the new shape; nothing rejects it, because payloads are
    schemaless by design. The drift detector notices within the hour, and because a model
    declares the old field required, that model's staging build fails — and only that one.
    """
    executor.exec("simulator", "sh", "-c", "touch /tmp/rename_plan_field")
    return {"released": "plan_id -> plan_code"}


def recover_schema_drift(conn: Connection, executor: Executor, **_: Any) -> dict[str, Any]:
    """Roll the release back and resolve the finding.

    In a real incident the fix is usually forward — teach the staging model to read both shapes
    during the transition — but a demo that could not be re-run would be worth little.
    """
    executor.exec("simulator", "sh", "-c", "rm -f /tmp/rename_plan_field")
    conn.execute(
        "UPDATE ops.drift_findings SET resolved_at = now() "
        "WHERE resolved_at IS NULL AND json_path IN ('plan', 'plan_code')"
    )
    return {"rolled_back": True}


def inject_late_duplicate_storm(
    conn: Connection,
    executor: Executor,
    *,
    events: int = 5000,
    days_late: int = 3,
    duplicate_rate: float = 0.3,
    **_: Any,
) -> dict[str, Any]:
    """A fleet of devices comes back online and flushes days of buffered events, twice.

    Exercises two different mechanisms at once: dedup (same event_id arriving repeatedly) and
    the lateness cutoff (events older than the tolerance are held back rather than rewriting a
    closed period).
    """
    executor.exec(
        "simulator",
        "python",
        "-m",
        "simulator.storm",
        f"--events={events}",
        f"--days-late={days_late}",
        f"--duplicate-rate={duplicate_rate}",
    )
    return {"events": events, "days_late": days_late, "duplicate_rate": duplicate_rate}


def recover_late_duplicate_storm(conn: Connection, executor: Executor, **_: Any) -> dict[str, Any]:
    """Nothing to restart: the platform absorbed what it could and held back the rest.

    Whether to absorb the quarantined remainder is a decision, not a cleanup step — it changes
    numbers people may already have seen (SPEC.md §6.2), so it is reported rather than done.
    """
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SELECT count(*) FROM staging.stg_product_events_quarantined")
        held = cur.fetchone()[0]
    return {
        "quarantined": held,
        "note": "absorb with `dbt build --select stg_product_events+ --full-refresh` if the "
        "change to closed periods is acceptable",
    }


def inject_poison_message(
    conn: Connection, executor: Executor, *, count: int = 5, **_: Any
) -> dict[str, Any]:
    """Put messages on the topic that cannot be parsed, plus a tombstone on an event topic.

    The point is what does NOT happen: the partition keeps moving, the rest of the batch loads,
    and the bad messages land in the DLQ with their reason rather than stalling the loader or
    being silently dropped.
    """
    executor.exec("simulator", "python", "-m", "simulator.storm", "--poison", f"--events={count}")
    return {"poison_messages": count}


def recover_poison_message(conn: Connection, executor: Executor, **_: Any) -> dict[str, Any]:
    """Nothing to fix: the DLQ is the recovery. This reports what it caught."""
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT reason, count(*) FROM ops.load_dlq GROUP BY reason ORDER BY count(*) DESC"
        )
        rows = cur.fetchall()
    return {"dlq": dict(rows)}


SCENARIOS: dict[str, Scenario] = {
    scenario.key: scenario
    for scenario in [
        Scenario(
            key="connector_outage",
            title="Connector outage → slot invalidation",
            what_breaks="Kafka Connect is stopped, so nothing reads the replication slot.",
            expected_symptoms=[
                "CdcStalled fires within ~10 minutes",
                "Retained WAL climbs; SlotWalRetainedWarning, then Critical",
                "Past the 5 GB cap Postgres invalidates the slot and CDC has a gap",
                "The app keeps serving: the cap protects the shared disk",
            ],
            recovery="Restart Connect. If the slot was lost, re-register with snapshot.mode=never "
            "and run an incremental snapshot; the SCD2 reconciliation test proves it healed.",
            runbook="docs/runbooks/slot-invalidation.md",
            max_duration=timedelta(minutes=45),
            inject=inject_connector_outage,
            recover=recover_connector_outage,
        ),
        Scenario(
            key="schema_drift",
            title="Schema drift release",
            what_breaks="The app renames plan_id to plan_code. Nothing rejects it.",
            expected_symptoms=[
                "Drift detector records field_removed on `plan`, marked blocking",
                "DriftBlocking fires",
                "stg_product_events fails; other event families keep flowing",
            ],
            recovery="Roll the release back, or teach staging to read both shapes during the "
            "transition, then resolve the finding.",
            runbook="docs/runbooks/schema-drift.md",
            inject=inject_schema_drift,
            recover=recover_schema_drift,
        ),
        Scenario(
            key="late_duplicate_storm",
            title="Late and duplicate event storm",
            what_breaks="Offline devices flush three-day-old events, many of them twice.",
            expected_symptoms=[
                "duplicate_rate rises; staging keeps first arrival per event_id",
                "Events past the tolerance are held in quarantine, not folded in",
                "QuarantineRateHigh fires if it persists for an hour",
            ],
            recovery="Nothing to restart. Absorbing the quarantined remainder is a decision, "
            "because it changes numbers already reported.",
            runbook="docs/runbooks/quarantine.md",
            inject=inject_late_duplicate_storm,
            recover=recover_late_duplicate_storm,
        ),
        Scenario(
            key="poison_message",
            title="Poison message and loader crash",
            what_breaks="Unparseable messages and a tombstone on an event topic.",
            expected_symptoms=[
                "Rows appear in ops.load_dlq with their reason",
                "The partition still advances; the rest of the batch loads",
                "No duplicates: the ledger and the rows commit together",
            ],
            recovery="Automatic. Fix the parser and rerun the task to reprocess the same offset "
            "range if the messages were valid after all.",
            runbook="docs/runbooks/loader-stalled.md",
            max_duration=timedelta(minutes=15),
            inject=inject_poison_message,
            recover=recover_poison_message,
        ),
    ]
}
