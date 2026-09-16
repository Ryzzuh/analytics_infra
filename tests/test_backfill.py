"""History replay: backfill topics, the lateness cutoff, and its exemption (SPEC.md §6.2)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from loader import load_partition
from loader.testing import FakeMessageSink, FakeMessageSource, event_bytes
from simulator.backfill import DensityProfile, estimate_events, generate_event_history
from test_dbt_staging import dbt, dbt_env  # noqa: F401  (fixture reuse)

pytestmark = pytest.mark.dbt  # these invoke dbt for real

BACKFILL_TOPIC = "product.feature_usage.backfill"
LIVE_TOPIC = "product.feature_usage"
NOW = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)


def load(conn, source, topic, *, run_id, source_path="live"):
    return load_partition(
        conn,
        source,
        topic=topic,
        partition_id=0,
        dag_run_id=run_id,
        source_path=source_path,
        now=NOW,
    )


# --------------------------------------------------------------------- generation


def test_density_ramps_towards_the_present():
    """Uniform density is the trap: 20/s for a year is ~630M events (~300 GB in Postgres),
    and 1/s for a year makes the recent weeks look dead (SPEC.md §6.2)."""
    profile = DensityProfile(baseline_rate=1.0, peak_rate=20.0, ramp_days=14)
    end = NOW

    assert profile.rate_at(end - timedelta(days=200), end) == 1.0
    assert profile.rate_at(end - timedelta(days=7), end) == pytest.approx(10.5, rel=0.01)
    assert profile.rate_at(end - timedelta(hours=1), end) == pytest.approx(19.94, rel=0.01)


def test_volume_can_be_estimated_before_generating_anything():
    """At ~500 bytes a row, the profile decides whether the warehouse is 20 GB or 300 GB, so
    it has to be answerable without producing 42 million messages first."""
    start, end = NOW - timedelta(days=365), NOW
    thin = estimate_events(DensityProfile(1.0, 20.0, 14), start, end)
    dense = estimate_events(DensityProfile(5.0, 20.0, 14), start, end)

    assert 35_000_000 < thin < 50_000_000
    assert dense > thin


def test_history_goes_to_backfill_topics_keyed_by_account():
    sink = FakeMessageSink(partitions=1)

    stats = generate_event_history(
        sink,
        start=NOW - timedelta(days=3),
        end=NOW,
        accounts=5,
        profile=DensityProfile(baseline_rate=0.01, peak_rate=0.05, ramp_days=1),
        seed=3,
    )

    topics = {t for (t, _p) in sink.source._log}
    assert stats["events"] > 0
    assert topics and all(t.endswith(".backfill") for t in topics)
    assert sink.flushed >= 1  # nothing is left unacknowledged in the producer queue


# --------------------------------------------------------------------- cutoff behaviour


def test_backfill_is_exempt_from_the_cutoff(conn, dbt_env):  # noqa: F811
    """The reason backfill has its own topics at all: by load lag, every replayed event is
    months late, and a single shared path would quarantine the entire history."""
    source = FakeMessageSource()
    for days_old in (400, 200, 30):
        source.produce(
            BACKFILL_TOPIC,
            0,
            event_bytes(
                account_id=1,
                event_time=NOW - timedelta(days=days_old),
                received_at=NOW - timedelta(days=days_old) + timedelta(milliseconds=40),
            ),
        )

    load(conn, source, BACKFILL_TOPIC, run_id="backfill-1", source_path="backfill")
    conn.commit()
    dbt("build", "--select", "stg_product_events stg_product_events_quarantined", env=dbt_env)

    assert conn.execute("SELECT count(*) FROM staging.stg_product_events").fetchone()[0] == 3
    assert (
        conn.execute("SELECT count(*) FROM staging.stg_product_events_quarantined").fetchone()[0]
        == 0
    )


def test_a_genuinely_stale_live_event_is_held_back_not_folded_in(conn, dbt_env):  # noqa: F811
    """Quietly rewriting a closed period is how reported numbers change under people who
    already acted on them.

    Staged as it happens in production: the model already exists, and the stale event turns up
    afterwards. (A model's FIRST build is not incremental, and absorbs whatever raw holds —
    same rule as a full refresh, and equally deliberate.)
    """
    source = FakeMessageSource()
    source.produce(LIVE_TOPIC, 0, event_bytes(account_id=1, event_time=NOW - timedelta(hours=1)))
    load(conn, source, LIVE_TOPIC, run_id="live-1")
    dbt("build", "--select", "stg_product_events stg_product_events_quarantined", env=dbt_env)

    source.produce(LIVE_TOPIC, 0, event_bytes(account_id=1, event_time=NOW - timedelta(days=45)))
    load(conn, source, LIVE_TOPIC, run_id="live-2")
    dbt("build", "--select", "stg_product_events stg_product_events_quarantined", env=dbt_env)

    assert conn.execute("SELECT count(*) FROM staging.stg_product_events").fetchone()[0] == 1
    quarantined = conn.execute(
        "SELECT load_lag_days FROM staging.stg_product_events_quarantined"
    ).fetchall()
    assert len(quarantined) == 1
    assert quarantined[0][0] > 44


def test_a_full_refresh_absorbs_quarantined_events(conn, dbt_env):  # noqa: F811
    """Absorbing them is a decision someone makes, not something that happens by default."""
    source = FakeMessageSource()
    source.produce(LIVE_TOPIC, 0, event_bytes(account_id=1, event_time=NOW - timedelta(hours=1)))
    load(conn, source, LIVE_TOPIC, run_id="live-1")
    dbt("build", "--select", "stg_product_events stg_product_events_quarantined", env=dbt_env)

    source.produce(LIVE_TOPIC, 0, event_bytes(account_id=1, event_time=NOW - timedelta(days=45)))
    load(conn, source, LIVE_TOPIC, run_id="live-2")
    dbt("build", "--select", "stg_product_events stg_product_events_quarantined", env=dbt_env)
    held = conn.execute("SELECT count(*) FROM staging.stg_product_events").fetchone()[0]
    still_held = conn.execute(
        "SELECT count(*) FROM staging.stg_product_events_quarantined"
    ).fetchone()[0]

    dbt(
        "build",
        "--select",
        "stg_product_events stg_product_events_quarantined",
        "--full-refresh",
        env=dbt_env,
    )
    absorbed = conn.execute("SELECT count(*) FROM staging.stg_product_events").fetchone()[0]
    remaining = conn.execute(
        "SELECT count(*) FROM staging.stg_product_events_quarantined"
    ).fetchone()[0]

    assert (held, still_held) == (1, 1)
    # Absorbed, and the quarantine list empties: it reports what is actually held back.
    assert (absorbed, remaining) == (2, 0)


def test_backfill_rows_are_written_in_event_time_order(conn):
    """A year of history lands in one load-date partition, so without this the BRIN index on
    event_time is useless for exactly the partition that holds most of the data."""
    source = FakeMessageSource()
    for days_old in (10, 300, 120, 2, 240):
        source.produce(
            BACKFILL_TOPIC, 0, event_bytes(account_id=1, event_time=NOW - timedelta(days=days_old))
        )

    load(conn, source, BACKFILL_TOPIC, run_id="backfill-1", source_path="backfill")

    times = [
        r[0]
        for r in conn.execute("SELECT event_time FROM raw.product_events ORDER BY ctid").fetchall()
    ]
    assert times == sorted(times)


def test_generated_history_loads_and_models_end_to_end(conn, dbt_env):  # noqa: F811
    """Generator -> sink -> topics -> loader -> staging, on the same code the live path uses."""
    sink = FakeMessageSink(partitions=1)
    generate_event_history(
        sink,
        start=NOW - timedelta(days=20),
        end=NOW,
        accounts=4,
        profile=DensityProfile(baseline_rate=0.02, peak_rate=0.2, ramp_days=5),
        seed=11,
    )

    for topic in {t for (t, _p) in sink.source._log}:
        load(conn, sink.source, topic, run_id=f"backfill-{topic}", source_path="backfill")
    conn.commit()
    dbt("build", "--select", "stg_product_events", env=dbt_env)

    spread, rows = conn.execute(
        "SELECT max(event_time) - min(event_time), count(*) FROM staging.stg_product_events"
    ).fetchone()
    assert rows > 10
    assert spread > timedelta(days=14)  # history, not a single day's worth
    assert (
        conn.execute(
            "SELECT count(*) FROM staging.stg_product_events WHERE source_path <> 'backfill'"
        ).fetchone()[0]
        == 0
    )
