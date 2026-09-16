"""The invariants M1 exists to prove. Each test maps to a numbered claim in run.py's docstring."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from conftest import TOPIC, ledger_rows, raw_count
from loader import OffsetOutOfRange, load_partition, run_load
from loader.errors import LedgerGap
from loader.testing import event_bytes


def produce(source, n: int, *, partition: int = 0, account_id: int = 1, **kw) -> None:
    for _ in range(n):
        source.produce(TOPIC, partition, event_bytes(account_id=account_id, **kw))


def test_watermark_advances_without_gaps_or_overlap(conn, source):
    produce(source, 10)
    first = load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1")
    produce(source, 5)
    second = load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-2")

    assert (first.start_offset, first.end_offset) == (0, 10)
    assert (second.start_offset, second.end_offset) == (10, 15)
    assert raw_count(conn) == 15
    assert (
        conn.execute("SELECT count(DISTINCT kafka_offset) FROM raw.product_events").fetchone()[0]
        == 15
    )


def test_nothing_new_is_a_no_op(conn, source):
    produce(source, 3)
    load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1")
    again = load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-2")

    assert again.skipped is True
    assert len(ledger_rows(conn)) == 1  # an empty run leaves no ledger noise


def test_crash_before_commit_leaves_no_trace(conn, source):
    """Invariant 1: rows and ledger entry are one transaction."""
    produce(source, 8)

    def die():
        raise RuntimeError("worker killed mid-batch")

    with pytest.raises(RuntimeError, match="worker killed"):
        load_partition(
            conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1", before_commit=die
        )

    assert raw_count(conn) == 0
    assert ledger_rows(conn) == []
    assert source.committed == {}  # offsets never advanced either

    recovered = load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1")
    assert recovered.rows_loaded == 8
    assert raw_count(conn) == 8


def test_crash_after_commit_before_offset_commit_does_not_duplicate(conn, source):
    """Invariant 2: the ledger, not the broker, decides where the next run starts."""
    produce(source, 6)
    source.commit_should_fail = True

    result = load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1")
    assert result.rows_loaded == 6
    assert source.committed == {}  # the broker still thinks nothing was consumed

    source.commit_should_fail = False
    produce(source, 2)
    second = load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-2")

    assert second.start_offset == 6  # resumed from the ledger, not from offset 0
    assert raw_count(conn) == 8


def test_rerun_replaces_rather_than_duplicating(conn, source):
    """Invariant 3: clearing a task re-reads its recorded range and replaces its rows."""
    produce(source, 12)
    first = load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1")
    produce(source, 4)  # arrived after the first run claimed its range

    rerun = load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1")

    assert rerun.replaced is True
    assert (rerun.start_offset, rerun.end_offset) == (first.start_offset, first.end_offset)
    assert rerun.rows_loaded == 12  # the 4 newer messages are NOT swept in
    assert raw_count(conn) == 12
    assert len(ledger_rows(conn)) == 1
    assert ledger_rows(conn)[0][-1] == 2  # attempt bumped


def test_rerun_applies_a_parser_fix_to_the_same_range(conn, source):
    """The reason reruns exist: reload a range through corrected code."""
    good = [event_bytes(account_id=1) for _ in range(3)]
    broken = (
        b'{"event_id": "not-a-uuid", "event_type": "x", "event_time": "2026-01-01T00:00:00Z",'
        b' "received_at": "2026-01-01T00:00:00Z"}'
    )
    for value in [*good, broken]:
        source.produce(TOPIC, 0, value)

    load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1")
    assert raw_count(conn) == 3
    assert conn.execute("SELECT count(*) FROM ops.load_dlq").fetchone()[0] == 1

    # Simulate the fix landing: the same offset, now parseable by corrected code.
    source.replace_at(TOPIC, 0, 3, event_bytes(account_id=1))
    load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1")

    assert raw_count(conn) == 4
    assert conn.execute("SELECT count(*) FROM ops.load_dlq").fetchone()[0] == 0


def test_rerun_outside_retention_fails_loudly(conn, source):
    """Invariant 4: silently loading less would be worse than failing."""
    produce(source, 10)
    load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1")
    source.expire_through(TOPIC, 0, 5)

    with pytest.raises(OffsetOutOfRange) as exc:
        load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1")

    assert (exc.value.requested, exc.value.earliest) == (0, 5)
    assert raw_count(conn) == 10  # the failed rerun changed nothing


def test_live_load_detects_data_expiring_past_the_watermark(conn, source):
    """A stalled loader that falls behind retention has LOST data, and must say so.

    (A first-ever load has no watermark to compare against, so it legitimately starts at
    whatever the broker still holds.)
    """
    produce(source, 4)
    load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1")
    produce(source, 6)
    source.expire_through(TOPIC, 0, 6)  # offsets 4 and 5 were never read and are now gone

    with pytest.raises(OffsetOutOfRange) as exc:
        load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-2")

    assert (exc.value.requested, exc.value.earliest) == (4, 6)


def test_erased_accounts_cannot_re_enter_on_any_path(conn, source):
    """Invariant 5: erasure is enforced at load time, so replay cannot resurrect data."""
    produce(source, 3, account_id=1)
    produce(source, 2, account_id=99)
    load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1")
    assert raw_count(conn) == 5

    conn.execute("INSERT INTO ops.erasure_requests (account_id) VALUES (99)")
    conn.execute("DELETE FROM raw.product_events WHERE account_id = 99")
    conn.commit()

    rerun = load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1")

    assert rerun.erased_skipped == 2
    assert raw_count(conn, account_id=99) == 0
    assert raw_count(conn) == 3


def test_poison_messages_go_to_the_dlq_without_stalling_the_partition(conn, source):
    source.produce(TOPIC, 0, event_bytes())
    source.produce(TOPIC, 0, None)  # tombstone on an event topic
    source.produce(TOPIC, 0, b"{not json")
    source.produce(TOPIC, 0, event_bytes(event_time="yesterday"))
    source.produce(TOPIC, 0, event_bytes())

    result = load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1")

    assert (result.rows_loaded, result.dlq_rows) == (2, 3)
    reasons = {r[0] for r in conn.execute("SELECT reason FROM ops.load_dlq").fetchall()}
    assert any("tombstone" in r for r in reasons)
    assert any("invalid_json" in r for r in reasons)
    assert any("invalid_envelope" in r for r in reasons)
    assert result.end_offset == 5  # the partition still advanced past the poison


def test_max_records_caps_a_batch_and_the_next_run_continues(conn, source):
    produce(source, 25)
    first = load_partition(
        conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1", max_records=10
    )
    second = load_partition(
        conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-2", max_records=10
    )

    assert (first.end_offset, second.start_offset, second.end_offset) == (10, 10, 20)
    assert raw_count(conn) == 20


def test_ledger_refuses_a_gap(conn, source):
    from loader import ledger

    produce(source, 5)
    load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1")

    with pytest.raises(LedgerGap):
        with conn.transaction():
            ledger.claim(
                conn,
                dag_run_id="run-2",
                topic=TOPIC,
                partition_id=0,
                start_offset=7,  # skips offsets 5 and 6
                end_offset=9,
                loaded_date=datetime.now(UTC).date(),
            )


def test_all_partitions_load_in_one_run(conn, source):
    produce(source, 4, partition=0)
    produce(source, 6, partition=1)
    produce(source, 2, partition=2)

    result = run_load(conn, source, topic=TOPIC, dag_run_id="run-1")

    assert result.rows_loaded == 12
    assert {p.partition_id for p in result.partitions} == {0, 1, 2}
    assert len(ledger_rows(conn)) == 3


def test_rows_land_in_the_load_date_partition(conn, source):
    """Replace-by-ledger prunes to one partition only if a run writes one load date."""
    produce(source, 3)
    day = datetime(2026, 3, 4, 9, 30, tzinfo=UTC)
    load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1", now=day)

    assert conn.execute("SELECT count(*) FROM raw.product_events_20260304").fetchone()[0] == 3


def test_backfill_rows_are_sorted_by_event_time(conn, source):
    """Preserves BRIN correlation when a year of history lands in one load partition."""
    base = datetime(2026, 1, 1, tzinfo=UTC)
    for days in (200, 5, 120, 1, 60):
        source.produce(TOPIC, 0, event_bytes(event_time=base + timedelta(days=days)))

    load_partition(
        conn,
        source,
        topic=f"{TOPIC}",
        partition_id=0,
        dag_run_id="backfill-1",
        source_path="backfill",
    )

    times = [
        r[0]
        for r in conn.execute("SELECT event_time FROM raw.product_events ORDER BY ctid").fetchall()
    ]
    assert times == sorted(times)


def test_dedup_is_left_to_staging_and_raw_keeps_every_copy(conn, source):
    """Raw is the audit of what actually arrived, duplicates included (SPEC.md §4.4)."""
    duplicated_id = uuid4()
    for _ in range(3):
        source.produce(TOPIC, 0, event_bytes(event_id=duplicated_id))

    load_partition(conn, source, topic=TOPIC, partition_id=0, dag_run_id="run-1")

    assert raw_count(conn) == 3
    assert (
        conn.execute("SELECT count(DISTINCT event_id) FROM raw.product_events").fetchone()[0] == 1
    )
