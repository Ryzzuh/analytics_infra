"""Independent snapshot of source state, for reconciliation (SPEC.md §6.1).

Everything else in the warehouse is derived from the CDC change log, so the change log cannot
check itself: if a slot was invalidated and changes were lost, the derived data is perfectly
self-consistent and perfectly wrong. This reads current state straight from the source
database instead, which is the one comparison that can detect it.

Deliberately dumb: no incremental logic, no dependence on LSNs or offsets. The whole point is
that it shares no machinery with the path it is checking.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from psycopg import Connection

SNAPSHOT_TABLES = {
    "subscriptions": "SELECT id, account_id, plan_code, status, seats, mrr_cents, "
    "started_at, ended_at FROM subscriptions",
    "accounts": "SELECT id, company_name, country FROM accounts",
}


def take_snapshot(
    app_conn: Connection,
    warehouse_conn: Connection,
    *,
    tables: dict[str, str] | None = None,
    now: datetime | None = None,
) -> dict[str, int]:
    """Copy current source state into raw.oltp_snapshots. Returns rows per table."""
    snapshot_at = now or datetime.now(UTC)
    counts: dict[str, int] = {}

    for table, query in (tables or SNAPSHOT_TABLES).items():
        with app_conn.cursor() as cur:
            cur.execute(query)
            columns = [c.name for c in cur.description]
            rows = cur.fetchall()

        with warehouse_conn.cursor().copy(
            "COPY raw.oltp_snapshots (snapshot_at, source_table, pk, row_data) FROM STDIN"
        ) as cp:
            for row in rows:
                record = dict(zip(columns, row, strict=True))
                cp.write_row(
                    (
                        snapshot_at,
                        table,
                        str(record["id"]),
                        json.dumps(record, default=str),
                    )
                )
        counts[table] = len(rows)

    warehouse_conn.commit()
    return counts


def prune_snapshots(warehouse_conn: Connection, keep: int = 7) -> int:
    """Keep the most recent N snapshots per table; they are only useful while recent."""
    deleted = warehouse_conn.execute(
        """
        DELETE FROM raw.oltp_snapshots s
        WHERE s.snapshot_at NOT IN (
            SELECT snapshot_at FROM (
                SELECT DISTINCT snapshot_at FROM raw.oltp_snapshots
                ORDER BY snapshot_at DESC LIMIT %s
            ) recent
        )
        """,
        (keep,),
    ).rowcount
    warehouse_conn.commit()
    return deleted
