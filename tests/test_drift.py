"""Schema-drift detection: the cost of accepting schemaless payloads (SPEC.md §7)."""

from __future__ import annotations

import itertools
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from drift import detect_drift, observe_shapes, required_fields

NOW = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)
BASELINE_AT = NOW - timedelta(days=2)


@pytest.fixture
def raw_events(conn):
    """Insert straight into raw: the loader has its own tests, and this is about shapes."""
    offsets = itertools.count()

    def add(
        payload: dict, *, event_type: str = "feature_invoked", loaded_at: datetime, count: int = 1
    ):
        conn.execute(
            "CREATE TABLE IF NOT EXISTS raw.product_events_shape_test PARTITION OF "
            "raw.product_events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"
        )
        conn.execute(
            "INSERT INTO ops.load_ledger (dag_run_id, topic, partition_id, start_offset, "
            "end_offset, loaded_date) VALUES (%s, 't', 0, 0, 1, %s) ON CONFLICT DO NOTHING",
            (f"run-{loaded_at.isoformat()}", loaded_at.date()),
        )
        ledger_id = conn.execute(
            "SELECT id FROM ops.load_ledger ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]
        for _ in range(count):
            conn.execute(
                "INSERT INTO raw.product_events (loaded_date, topic, partition_id, kafka_offset, "
                "ledger_id, event_id, account_id, event_type, event_time, received_at, kafka_ts, "
                "loaded_at, source_path, payload) VALUES (%s, 't', 0, %s, %s, %s, 1, %s, %s, %s, "
                "%s, %s, 'live', %s)",
                (
                    loaded_at.date(),
                    next(offsets),
                    ledger_id,
                    uuid4(),
                    event_type,
                    loaded_at,
                    loaded_at,
                    loaded_at,
                    loaded_at,
                    json.dumps(payload),
                ),
            )

    return add


@pytest.fixture
def manifest(tmp_path) -> Path:
    """A dbt manifest declaring which fields models actually depend on."""
    path = tmp_path / "manifest.json"
    path.write_text(
        json.dumps(
            {
                "nodes": {
                    "model.analytics_infra.stg_product_events": {
                        "name": "stg_product_events",
                        "config": {"meta": {"required_fields": ["feature", "surface", "plan"]}},
                    }
                }
            }
        )
    )
    return path


def establish_baseline(conn, raw_events, payload: dict, *, event_type="feature_invoked", count=40):
    """Insert history AND observe it, because that is how the baseline actually forms: the
    hourly job records what it sees, run after run. A baseline nobody observed is not one."""
    raw_events(payload, event_type=event_type, loaded_at=BASELINE_AT, count=count)
    observe_shapes(
        conn, since=BASELINE_AT - timedelta(hours=1), until=BASELINE_AT + timedelta(hours=1)
    )


def findings(conn) -> list[tuple]:
    return conn.execute(
        "SELECT event_type, json_path, change, blocking FROM ops.drift_findings ORDER BY json_path"
    ).fetchall()


def test_required_fields_come_from_the_dbt_manifest(manifest):
    """Declared next to the SQL that reads them, so the registry cannot drift from the models."""
    assert required_fields(manifest) == {
        "feature": ["stg_product_events"],
        "surface": ["stg_product_events"],
        "plan": ["stg_product_events"],
    }


def test_a_steady_shape_produces_no_findings(conn, raw_events, manifest):
    payload = {"feature": "export", "surface": "web", "plan": "team"}
    establish_baseline(conn, raw_events, payload)
    raw_events(payload, loaded_at=NOW - timedelta(minutes=5), count=30)

    result = detect_drift(conn, manifest_path=manifest, now=NOW)

    assert result == []


def test_a_new_field_is_noted_but_blocks_nothing(conn, raw_events, manifest):
    """Clients adding fields is normal; rejecting it is what schemaless ingest exists to avoid."""
    establish_baseline(conn, raw_events, {"feature": "export", "surface": "web", "plan": "team"})
    raw_events(
        {"feature": "export", "surface": "web", "plan": "team", "experiment": "b"},
        loaded_at=NOW - timedelta(minutes=5),
        count=30,
    )

    result = detect_drift(conn, manifest_path=manifest, now=NOW)

    assert [(f.json_path, f.change, f.blocking) for f in result] == [
        ("experiment", "field_added", False)
    ]


def test_a_renamed_field_a_model_depends_on_blocks(conn, raw_events, manifest):
    """The worked example from the spec: without this, revenue-by-plan quietly goes to zero for
    new rows while every row remains individually valid."""
    establish_baseline(conn, raw_events, {"feature": "export", "surface": "web", "plan": "team"})
    raw_events(
        {"feature": "export", "surface": "web", "plan_code": "team"},
        loaded_at=NOW - timedelta(minutes=5),
        count=30,
    )

    result = detect_drift(conn, manifest_path=manifest, now=NOW)

    by_path = {f.json_path: f for f in result}
    assert by_path["plan"].change == "field_removed"
    assert by_path["plan"].blocking is True
    assert by_path["plan"].details["required_by"] == ["stg_product_events"]
    assert by_path["plan_code"].blocking is False  # the replacement is just a new field


def test_a_field_nothing_depends_on_disappearing_does_not_block(conn, raw_events, manifest):
    establish_baseline(
        conn, raw_events, {"feature": "export", "surface": "web", "plan": "team", "debug_id": "x"}
    )
    raw_events(
        {"feature": "export", "surface": "web", "plan": "team"},
        loaded_at=NOW - timedelta(minutes=5),
        count=30,
    )

    result = detect_drift(conn, manifest_path=manifest, now=NOW)

    assert [(f.json_path, f.blocking) for f in result] == [("debug_id", False)]


def test_a_type_change_on_a_required_field_blocks(conn, raw_events, manifest):
    """string -> array is the classic: every downstream cast starts failing or silently nulls."""
    establish_baseline(conn, raw_events, {"feature": "export", "surface": "web", "plan": "team"})
    raw_events(
        {"feature": ["export", "share"], "surface": "web", "plan": "team"},
        loaded_at=NOW - timedelta(minutes=5),
        count=30,
    )

    result = detect_drift(conn, manifest_path=manifest, now=NOW)

    finding = next(f for f in result if f.json_path == "feature")
    assert (finding.change, finding.blocking) == ("type_changed", True)
    assert finding.details == {
        "was": "string",
        "now": "array",
        "required_by": ["stg_product_events"],
    }


def test_one_stray_null_is_not_a_type_change(conn, raw_events, manifest):
    """A detector that fires on this earns itself an exception list, and then it is ignored."""
    establish_baseline(conn, raw_events, {"feature": "export", "surface": "web", "plan": "team"})
    raw_events(
        {"feature": "export", "surface": "web", "plan": "team"},
        loaded_at=NOW - timedelta(minutes=5),
        count=29,
    )
    raw_events(
        {"feature": None, "surface": "web", "plan": "team"},
        loaded_at=NOW - timedelta(minutes=4),
        count=1,
    )

    result = detect_drift(conn, manifest_path=manifest, now=NOW)

    assert result == []


def test_a_rarely_seen_field_is_not_treated_as_a_baseline(conn, raw_events, manifest):
    """Otherwise every optional field produces a "removed" finding the first quiet hour."""
    raw_events(
        {"feature": "export", "surface": "web", "plan": "team", "rare": 1},
        loaded_at=BASELINE_AT,
        count=3,
    )
    establish_baseline(conn, raw_events, {"feature": "export", "surface": "web", "plan": "team"})
    raw_events(
        {"feature": "export", "surface": "web", "plan": "team"},
        loaded_at=NOW - timedelta(minutes=5),
        count=30,
    )

    result = detect_drift(conn, manifest_path=manifest, now=NOW)

    assert [f.json_path for f in result] == []


def test_an_event_family_that_went_quiet_is_not_reported_as_drift(conn, raw_events, manifest):
    """It is a freshness problem, and reporting each of its fields as removed would bury that."""
    establish_baseline(
        conn,
        raw_events,
        {"feature": "export", "surface": "web", "plan": "team"},
        event_type="report_exported",
    )
    establish_baseline(conn, raw_events, {"feature": "export", "surface": "web", "plan": "team"})
    raw_events(
        {"feature": "export", "surface": "web", "plan": "team"},
        loaded_at=NOW - timedelta(minutes=5),
        count=30,
    )

    result = detect_drift(conn, manifest_path=manifest, now=NOW)

    assert [f.event_type for f in result] == []


def test_findings_are_recorded_for_the_console_and_the_alert(conn, raw_events, manifest):
    establish_baseline(conn, raw_events, {"feature": "export", "surface": "web", "plan": "team"})
    raw_events(
        {"feature": "export", "surface": "web"}, loaded_at=NOW - timedelta(minutes=5), count=30
    )

    detect_drift(conn, manifest_path=manifest, now=NOW)

    assert findings(conn) == [("feature_invoked", "plan", "field_removed", True)]


def test_shapes_are_observed_from_raw_not_staging(conn, raw_events):
    """Staging has already picked the fields it knows about: by then a new field is invisible."""
    raw_events({"feature": "export", "nested": {"a": 1}}, loaded_at=NOW - timedelta(minutes=5))

    shapes = observe_shapes(conn, since=NOW - timedelta(hours=1), until=NOW)

    assert shapes[("feature_invoked", "feature")] == "string"
    assert shapes[("feature_invoked", "nested")] == "object"
    assert shapes[("feature_invoked", "nested.a")] == "number"
