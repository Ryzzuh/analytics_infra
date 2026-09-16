"""Detect schema drift in schemaless event payloads (SPEC.md §7).

The trade this pays for: events are accepted without payload validation, so a client shipping a
new field is never rejected — and neither is a client that stops sending one. The second case
is the dangerous one. Nothing fails; a column simply becomes null for new rows, and a mart
quietly reports zero.

So drift is detected against *observed history* rather than a declared schema, and the response
is tiered (SPEC.md §7):

* a new field is logged and notified — it breaks nothing;
* a field that disappears, or changes type, **blocks** only if a model declares it as required;
* everything else keeps flowing, because one event family's problem must not stall the rest.

"Required" comes from the dbt manifest — `meta.required_fields` on the models themselves — so
the registry cannot drift away from the SQL that depends on it.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from psycopg import Connection

log = logging.getLogger(__name__)

FIELD_ADDED = "field_added"
FIELD_REMOVED = "field_removed"
TYPE_CHANGED = "type_changed"


@dataclass(frozen=True)
class DriftFinding:
    event_type: str
    json_path: str
    change: str
    blocking: bool
    details: dict[str, Any]


def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object"


def _paths(payload: dict[str, Any], prefix: str = "") -> dict[str, str]:
    """Flatten a payload to {path: json_type}, one level of nesting deep.

    Deliberately shallow: deeply nested blobs produce a combinatorial number of paths, and the
    fields models actually read are at the top.
    """
    flat: dict[str, str] = {}
    for key, value in payload.items():
        path = f"{prefix}{key}"
        flat[path] = _json_type(value)
        if isinstance(value, dict) and not prefix:
            flat.update(_paths(value, prefix=f"{path}."))
    return flat


def observe_shapes(
    conn: Connection, *, since: datetime, until: datetime | None = None
) -> dict[tuple[str, str], str]:
    """Record the shape of everything loaded in a window, and return it.

    Reads raw rather than staging: staging has already picked the fields it knows about, so by
    then a new field is invisible and a missing one has become a null column.
    """
    end = until or datetime.now(UTC)
    rows = conn.execute(
        "SELECT event_type, payload FROM raw.product_events "
        "WHERE loaded_at >= %s AND loaded_at < %s",
        (since, end),
    ).fetchall()

    observed: dict[tuple[str, str, str], int] = {}
    for event_type, payload in rows:
        for path, json_type in _paths(payload).items():
            observed[(event_type, path, json_type)] = (
                observed.get((event_type, path, json_type), 0) + 1
            )

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO ops.event_shape (event_type, json_path, json_type, first_seen, last_seen,
                                         occurrences)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_type, json_path, json_type) DO UPDATE SET
                last_seen = excluded.last_seen,
                occurrences = ops.event_shape.occurrences + excluded.occurrences
            """,
            [(et, path, jt, since, end, count) for (et, path, jt), count in observed.items()],
        )

    return _dominant_types(observed)


def _dominant_types(observed: dict[tuple[str, str, str], int]) -> dict[tuple[str, str], str]:
    """The most common type per (event_type, path).

    One stray null in an hour of strings is not a type change, and reporting it as one is how a
    detector earns itself an exception list.
    """
    best: dict[tuple[str, str], tuple[str, int]] = {}
    for (event_type, path, json_type), count in observed.items():
        key = (event_type, path)
        if key not in best or count > best[key][1]:
            best[key] = (json_type, count)
    return {key: json_type for key, (json_type, _count) in best.items()}


def required_fields(manifest_path: Path) -> dict[str, list[str]]:
    """Map field name -> models that declare it required, read from the dbt manifest.

    The manifest rather than a hand-kept list: a field's importance is declared next to the SQL
    that reads it, so the two cannot disagree.
    """
    manifest = json.loads(Path(manifest_path).read_text())
    required: dict[str, list[str]] = {}

    for node in manifest.get("nodes", {}).values():
        for field in node.get("config", {}).get("meta", {}).get("required_fields", []) or node.get(
            "meta", {}
        ).get("required_fields", []):
            required.setdefault(field, []).append(node["name"])

    return required


def detect_drift(
    conn: Connection,
    *,
    manifest_path: Path | None = None,
    window: timedelta = timedelta(hours=1),
    now: datetime | None = None,
    min_baseline_occurrences: int = 20,
) -> list[DriftFinding]:
    """Compare the current window against the recorded baseline and record what changed."""
    end = now or datetime.now(UTC)
    start = end - window
    current = observe_shapes(conn, since=start, until=end)

    baseline_rows = conn.execute(
        "SELECT event_type, json_path, json_type, occurrences, last_seen FROM ops.event_shape "
        "WHERE first_seen < %s",
        (start,),
    ).fetchall()

    # A field seen a handful of times is not a baseline: it is noise, and treating it as one
    # produces a "field removed" finding every time an optional field is absent for an hour.
    baseline = {
        (event_type, path): (json_type, occurrences)
        for event_type, path, json_type, occurrences, _last_seen in baseline_rows
        if occurrences >= min_baseline_occurrences
    }

    required = required_fields(manifest_path) if manifest_path else {}
    findings: list[DriftFinding] = []
    seen_event_types = {event_type for event_type, _path in current}

    for (event_type, path), json_type in current.items():
        if (event_type, path) not in baseline:
            findings.append(
                DriftFinding(
                    event_type,
                    path,
                    FIELD_ADDED,
                    blocking=False,  # a new field breaks nothing by existing
                    details={"json_type": json_type},
                )
            )
        elif baseline[(event_type, path)][0] != json_type:
            field = path.split(".")[-1]
            findings.append(
                DriftFinding(
                    event_type,
                    path,
                    TYPE_CHANGED,
                    blocking=field in required,
                    details={
                        "was": baseline[(event_type, path)][0],
                        "now": json_type,
                        "required_by": required.get(field, []),
                    },
                )
            )

    for (event_type, path), (json_type, _occurrences) in baseline.items():
        # Only judge event types that are still arriving: a family that has gone quiet is a
        # freshness problem, and reporting every one of its fields as "removed" would bury it.
        if event_type in seen_event_types and (event_type, path) not in current:
            field = path.split(".")[-1]
            findings.append(
                DriftFinding(
                    event_type,
                    path,
                    FIELD_REMOVED,
                    blocking=field in required,
                    details={"was": json_type, "required_by": required.get(field, [])},
                )
            )

    _record(conn, findings, end)
    blocking = [f for f in findings if f.blocking]
    log.info("drift: %d findings, %d blocking", len(findings), len(blocking))
    return findings


def _record(conn: Connection, findings: list[DriftFinding], detected_at: datetime) -> None:
    if not findings:
        return
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO ops.drift_findings (detected_at, event_type, json_path, change, "
            "blocking, details) VALUES (%s, %s, %s, %s, %s, %s)",
            [
                (
                    detected_at,
                    f.event_type,
                    f.json_path,
                    f.change,
                    f.blocking,
                    json.dumps(f.details),
                )
                for f in findings
            ],
        )
