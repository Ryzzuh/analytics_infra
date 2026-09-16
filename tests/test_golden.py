"""Golden snapshots, restore ordering and catch-up windows (SPEC.md §9.3)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from loader.testing import FakeMessageSink
from opsctl import GapTooLarge, GoldenManifest, catch_up_window, restore_plan
from opsctl.golden import REQUIRED_VOLUMES
from simulator.backfill import catch_up_history

T0 = datetime(2026, 9, 1, 2, 0, tzinfo=UTC)


def manifest(**overrides) -> GoldenManifest:
    payload = {
        "golden_at": T0,
        "created_at": T0 + timedelta(minutes=4),
        "volumes": dict.fromkeys(REQUIRED_VOLUMES, "sha256:abc"),
        "sizes_bytes": dict.fromkeys(REQUIRED_VOLUMES, 5_000_000_000),
        "stack_version": "abc1234",
    }
    payload.update(overrides)
    return GoldenManifest(**payload)


def test_manifest_round_trips():
    restored = GoldenManifest.from_json(manifest().to_json())
    assert restored == manifest()


def test_a_partial_snapshot_is_rejected(tmp_path):
    """Restoring the databases without Redpanda puts the connector's stored offsets at a
    position the restored log does not match — CDC then breaks, or silently skips."""
    partial = manifest(volumes={"warehouse-data": "sha256:abc", "app-data": "sha256:def"})

    assert set(partial.missing_volumes()) == {"redpanda-data", "airflow-data"}
    with pytest.raises(ValueError, match="CDC offsets"):
        partial.validate()


def test_catch_up_window_starts_where_the_snapshot_ends():
    now = T0 + timedelta(days=1, hours=3)

    start, end = catch_up_window(manifest(), now)

    assert (start, end) == (T0, now)


def test_a_stale_snapshot_is_refused_rather_than_replayed():
    """Catching up runs at live density: a week-old snapshot means ~12M events and ~15 minutes
    of restore, so past the limit the platform says rebuild instead of grinding through it."""
    with pytest.raises(GapTooLarge) as exc:
        catch_up_window(manifest(), T0 + timedelta(days=7))

    assert exc.value.gap.days == 7
    assert "rebuild history" in str(exc.value)


def test_a_snapshot_dated_in_the_future_is_an_error():
    with pytest.raises(ValueError, match="clocks disagree"):
        catch_up_window(manifest(), T0 - timedelta(hours=1))


def test_restore_plan_orders_the_steps_that_must_not_be_swapped():
    steps = restore_plan(manifest(), T0 + timedelta(days=1, hours=6))
    joined = " | ".join(steps)

    stop = next(i for i, s in enumerate(steps) if s.startswith("stop every service"))
    swap = next(i for i, s in enumerate(steps) if s.startswith("restore volumes"))
    register = next(i for i, s in enumerate(steps) if "snapshot.mode=never" in s)
    catch_up = next(i for i, s in enumerate(steps) if s.startswith("catch up"))

    assert stop < swap < register < catch_up
    assert "1d 6h" in joined  # the plan states how much history it is about to replay


def test_catch_up_fills_only_the_gap():
    """A week-shaped hole in every mart is what this exists to prevent."""
    sink = FakeMessageSink()
    window = (T0, T0 + timedelta(hours=6))

    stats = catch_up_history(sink, window=window, accounts=3, live_rate=0.05, seed=5)

    times = []
    for (_topic, _partition), records in sink.source._log.items():
        for record in records:
            times.append(record.value)
    assert stats["events"] > 0
    assert all(t.endswith(".backfill") for (t, _p) in sink.source._log)
    # Events land inside the window, not before it or after now.
    import json as _json

    parsed = [datetime.fromisoformat(_json.loads(v)["event_time"]) for v in times]
    assert min(parsed) >= window[0]
    assert max(parsed) < window[1]


def test_an_empty_window_produces_nothing():
    sink = FakeMessageSink()

    stats = catch_up_history(sink, window=(T0, T0), accounts=3)

    assert stats["events"] == 0
    assert sink.sent == 0
