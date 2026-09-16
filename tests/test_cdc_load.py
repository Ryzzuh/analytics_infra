"""CDC ingestion: Debezium envelopes into raw.cdc_changes."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from conftest import ledger_rows
from loader import load_partition
from loader.cdc import parse_change
from loader.models import CdcChange, ParseFailure, SkipRecord
from loader.targets import cdc_target
from loader.testing import FakeMessageSource, change_bytes, tombstone_key

CDC_TOPIC = "cdc.app.public.subscriptions"
TARGET = cdc_target()


def load(conn, source, run_id="run-1", **kw):
    return load_partition(
        conn, source, topic=CDC_TOPIC, partition_id=0, dag_run_id=run_id, target=TARGET, **kw
    )


def cdc_rows(conn) -> list[dict]:
    return [
        dict(
            zip(
                ("op", "pk", "account_id", "before", "after", "lsn", "effective_at"),
                r,
                strict=True,
            )
        )
        for r in conn.execute(
            "SELECT op, pk, account_id, before, after, source_lsn, effective_at "
            "FROM raw.cdc_changes ORDER BY source_lsn"
        ).fetchall()
    ]


def test_insert_update_delete_land_as_three_rows(conn, source):
    base = datetime(2026, 3, 1, tzinfo=UTC)
    trial = {"id": 5, "account_id": 77, "status": "trial", "seats": 3}
    active = {**trial, "status": "active"}

    source.produce(CDC_TOPIC, 0, change_bytes(op="c", after=trial, lsn=100, effective_at=base))
    source.produce(
        CDC_TOPIC,
        0,
        change_bytes(
            op="u", before=trial, after=active, lsn=200, effective_at=base + timedelta(hours=2)
        ),
    )
    source.produce(
        CDC_TOPIC,
        0,
        change_bytes(op="d", before=active, lsn=300, effective_at=base + timedelta(hours=5)),
    )

    result = load(conn, source)

    rows = cdc_rows(conn)
    assert result.rows_loaded == 3
    assert [r["op"] for r in rows] == ["c", "u", "d"]
    assert rows[1]["before"]["status"] == "trial"  # the update is diffable
    assert rows[2]["before"]["status"] == "active"  # the delete knows what was live
    assert rows[2]["after"] is None
    assert all(r["account_id"] == 77 for r in rows)


def test_tombstones_are_skipped_not_stored_and_not_dlq(conn, source):
    """A tombstone follows every delete. Storing it would double-count the delete; DLQ-ing it
    would alarm on entirely normal operation."""
    deleted = {"id": 5, "account_id": 77, "status": "cancelled"}
    source.produce(CDC_TOPIC, 0, change_bytes(op="d", before=deleted, lsn=300))
    source.produce(CDC_TOPIC, 0, None, key=tombstone_key(5))

    result = load(conn, source)

    assert (result.rows_loaded, result.records_skipped, result.dlq_rows) == (1, 1, 0)
    assert ledger_rows(conn)[0][-2] == 1  # skipped_count recorded on the ledger
    assert result.end_offset == 2  # the partition still advances past the tombstone


def test_delete_without_before_image_is_rejected(conn, source):
    """The symptom of REPLICA IDENTITY not being FULL. Silently accepting it would produce
    SCD2 intervals closed with unknown values."""
    source.produce(CDC_TOPIC, 0, change_bytes(op="d", before=None, lsn=300))

    result = load(conn, source)

    assert (result.rows_loaded, result.dlq_rows) == (0, 1)
    reason = conn.execute("SELECT reason FROM ops.load_dlq").fetchone()[0]
    assert reason == "delete_without_before_image"


def test_change_without_lsn_is_rejected(conn, source):
    """Without an LSN there is no total order, so SCD2 ordering would be guesswork."""
    body = json.loads(change_bytes(op="u", before={"id": 1}, after={"id": 1}, lsn=10))
    del body["source"]["lsn"]
    source.produce(CDC_TOPIC, 0, json.dumps(body).encode())

    result = load(conn, source)

    assert result.dlq_rows == 1
    assert conn.execute("SELECT reason FROM ops.load_dlq").fetchone()[0] == "missing_source_lsn"


def test_effective_at_comes_from_the_row_not_the_commit_time():
    """During historical replay the commit time is 'now' for a change that happened last
    March. SCD2 must use business time, or a year of history collapses into today."""
    replayed_at = datetime(2026, 9, 16, 10, 0, tzinfo=UTC)
    happened_at = datetime(2026, 3, 4, 9, 30, tzinfo=UTC)
    record = FakeMessageSource().produce(
        CDC_TOPIC,
        0,
        change_bytes(
            op="u",
            before={"id": 1, "status": "trial"},
            after={"id": 1, "status": "active"},
            lsn=10,
            effective_at=happened_at,
            source_ts=replayed_at,
        ),
    )

    change = parse_change(record)

    assert isinstance(change, CdcChange)
    assert change.effective_at == happened_at
    assert change.source_ts == replayed_at


def test_snapshot_reads_are_flagged(conn, source):
    """Snapshot rows are current state, not history: the initial snapshot of a year-old
    subscription says 'cancelled' and carries no earlier transitions (SPEC.md §6.1)."""
    source.produce(
        CDC_TOPIC,
        0,
        change_bytes(
            op="r", after={"id": 9, "account_id": 1, "status": "cancelled"}, lsn=1, snapshot=True
        ),
    )

    load(conn, source)

    assert conn.execute("SELECT is_snapshot FROM raw.cdc_changes").fetchone()[0] is True


def test_cdc_rerun_replaces_and_erasure_applies(conn, source):
    for lsn, account in ((10, 1), (20, 2), (30, 2)):
        source.produce(
            CDC_TOPIC,
            0,
            change_bytes(op="c", after={"id": lsn, "account_id": account}, lsn=lsn),
        )
    load(conn, source)
    assert len(cdc_rows(conn)) == 3

    conn.execute("INSERT INTO ops.erasure_requests (account_id) VALUES (2)")
    conn.commit()
    rerun = load(conn, source)  # same run id: replace in place

    assert rerun.replaced is True
    assert rerun.erased_skipped == 2
    assert [r["account_id"] for r in cdc_rows(conn)] == [1]
    assert len(ledger_rows(conn)) == 1


def test_malformed_and_unknown_ops_are_rejected(conn, source):
    source.produce(CDC_TOPIC, 0, b"{oops")
    source.produce(CDC_TOPIC, 0, json.dumps({"op": "z", "source": {"lsn": 1}}).encode())

    result = load(conn, source)

    assert (result.rows_loaded, result.dlq_rows) == (0, 2)


def test_parse_accepts_schema_wrapped_envelopes():
    """Debezium wraps the body in `payload` when schemas are enabled; both shapes are real."""
    inner = json.loads(change_bytes(op="c", after={"id": 1, "account_id": 3}, lsn=5))
    wrapped = FakeMessageSource().produce(
        CDC_TOPIC, 0, json.dumps({"schema": {"type": "struct"}, "payload": inner}).encode()
    )

    change = parse_change(wrapped)

    assert isinstance(change, CdcChange)
    assert change.pk == "1"


@pytest.mark.parametrize("value", [b"null", b"[]"])
def test_non_object_envelopes_are_failures_not_skips(value):
    record = FakeMessageSource().produce(CDC_TOPIC, 0, value)
    parsed = parse_change(record)
    assert isinstance(parsed, ParseFailure)
    assert not isinstance(parsed, SkipRecord)
