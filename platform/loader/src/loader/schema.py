"""Apply the warehouse DDL. Plain .sql files, applied in filename order, all idempotent."""

from __future__ import annotations

from pathlib import Path

from psycopg import Connection

DDL_DIR = Path(__file__).resolve().parents[4] / "db" / "warehouse" / "ddl"


def apply_ddl(conn: Connection, ddl_dir: Path | None = None) -> list[str]:
    applied = []
    for path in sorted((ddl_dir or DDL_DIR).glob("*.sql")):
        conn.execute(path.read_text())
        applied.append(path.name)
    conn.commit()
    return applied
