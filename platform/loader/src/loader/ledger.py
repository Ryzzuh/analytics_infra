"""Offset ledger: the source of truth for what has been consumed.

Every function here takes a live connection and does NOT commit. Callers run them inside the
same transaction as the COPY, which is the whole point: data and ledger entry commit together
or not at all.
"""

from __future__ import annotations

from datetime import date

from psycopg import Connection

from .errors import LedgerGap
from .models import LedgerEntry

_ENTRY_COLUMNS = """
    id, dag_run_id, topic, partition_id, start_offset, end_offset,
    loaded_date, row_count, dlq_count, erased_count, attempt
"""


def _row_to_entry(row: tuple) -> LedgerEntry:
    return LedgerEntry(*row)


def watermark(conn: Connection, topic: str, partition_id: int) -> int | None:
    """Highest committed end_offset for a partition, or None if never loaded.

    Uncommitted work is invisible here by construction, so there is no 'in progress' state
    to reconcile after a crash.
    """
    row = conn.execute(
        "SELECT max(end_offset) FROM ops.load_ledger WHERE topic = %s AND partition_id = %s",
        (topic, partition_id),
    ).fetchone()
    return row[0] if row else None


def find_entry(
    conn: Connection, dag_run_id: str, topic: str, partition_id: int
) -> LedgerEntry | None:
    row = conn.execute(
        f"SELECT {_ENTRY_COLUMNS} FROM ops.load_ledger "
        "WHERE dag_run_id = %s AND topic = %s AND partition_id = %s",
        (dag_run_id, topic, partition_id),
    ).fetchone()
    return _row_to_entry(row) if row else None


def claim(
    conn: Connection,
    *,
    dag_run_id: str,
    topic: str,
    partition_id: int,
    start_offset: int,
    end_offset: int,
    loaded_date: date,
) -> LedgerEntry:
    """Insert the ledger entry for a new range, failing if it would leave a gap."""
    current = watermark(conn, topic, partition_id)
    if current is not None and start_offset != current:
        raise LedgerGap(
            f"{topic}/{partition_id}: claim starts at {start_offset} but the ledger ends at "
            f"{current}. Refusing to skip or re-read data implicitly."
        )
    row = conn.execute(
        "INSERT INTO ops.load_ledger "
        "(dag_run_id, topic, partition_id, start_offset, end_offset, loaded_date) "
        f"VALUES (%s, %s, %s, %s, %s, %s) RETURNING {_ENTRY_COLUMNS}",
        (dag_run_id, topic, partition_id, start_offset, end_offset, loaded_date),
    ).fetchone()
    return _row_to_entry(row)


def finalise(
    conn: Connection,
    entry_id: int,
    *,
    row_count: int,
    dlq_count: int,
    erased_count: int,
    bump_attempt: bool = False,
) -> None:
    conn.execute(
        "UPDATE ops.load_ledger SET row_count = %s, dlq_count = %s, erased_count = %s, "
        "attempt = attempt + %s, updated_at = now() WHERE id = %s",
        (row_count, dlq_count, erased_count, 1 if bump_attempt else 0, entry_id),
    )


def clear_entry_data(conn: Connection, entry: LedgerEntry) -> None:
    """Delete everything a previous attempt of this entry wrote.

    `loaded_date` is in the predicate so the planner prunes to the single raw partition the
    entry wrote into: a rerun touches one day's data, not the whole table.
    """
    conn.execute(
        "DELETE FROM raw.product_events WHERE loaded_date = %s AND ledger_id = %s",
        (entry.loaded_date, entry.id),
    )
    conn.execute("DELETE FROM ops.load_dlq WHERE ledger_id = %s", (entry.id,))


def erased_account_ids(conn: Connection) -> set[int]:
    return {r[0] for r in conn.execute("SELECT account_id FROM ops.erasure_requests").fetchall()}
