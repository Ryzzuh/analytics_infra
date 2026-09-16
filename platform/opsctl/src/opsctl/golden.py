"""Golden snapshots: reset to a known-good state without breaking CDC (SPEC.md §9.3).

Restoring a database while Debezium's stored offsets still point at a WAL position from the
future leaves CDC either broken or — worse — silently skipping changes. So a golden snapshot is
not "a database backup": it is a **cold copy of every volume, taken together**, so the slot
position, the connector's offsets in Redpanda, and the warehouse's load ledger are guaranteed
to agree when they come back.

The second problem is that golden data ages. A snapshot taken on day T0 and restored on T0+60
leaves a sixty-day hole in every mart, subscriptions that missed their renewals, and freshness
checks that think the world stopped. So a restore is always followed by a catch-up: the gap
between the snapshot and now is replayed through the backfill path.

This module holds the parts worth testing — what a snapshot records, how big a gap may be, and
what a restore does in what order. The shell script does the docker work.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

# Volumes that must be captured together. Omitting any one of them is how a restore ends up
# internally inconsistent: Redpanda holds the connector's offsets and the topics themselves,
# so restoring the databases without it puts CDC at a position the log no longer matches.
REQUIRED_VOLUMES = (
    "warehouse-data",
    "app-data",
    "redpanda-data",
    "airflow-data",
)

# Deviation from SPEC.md §9.3, which paired a weekly golden rebuild with restore-plus-catch-up.
# The arithmetic does not support it: catching up runs at the live rate (20/s), so a 7-day gap
# is ~12.1M events, ~6 GB, and roughly 15 minutes of generating and loading — during which the
# public instance is mid-restore. A day-old snapshot is ~1.7M events, ~2 minutes.
#
# So golden is rebuilt DAILY and the gap limit is 2 days. Past that, replaying at live density
# is slower than rebuilding history properly, and the platform says so instead of grinding.
DEFAULT_MAX_GAP = timedelta(days=2)


class GapTooLarge(Exception):
    """The snapshot is too old to catch up from.

    Catching up replays the gap at live density, so a huge gap stops being a quick restore and
    becomes a slower, noisier version of regenerating history from scratch. Past the limit,
    rebuilding is both faster and more honest than pretending a restore happened.
    """

    def __init__(self, gap: timedelta, limit: timedelta):
        super().__init__(
            f"golden snapshot is {gap.days} days old, limit is {limit.days}; "
            "rebuild history instead of catching up"
        )
        self.gap = gap
        self.limit = limit


@dataclass(frozen=True)
class GoldenManifest:
    """What a snapshot is, and the business time it represents.

    `golden_at` is the important field: it is the point in simulated history the data ends at,
    and therefore the start of the catch-up window. It is recorded rather than inferred,
    because inferring it from the data ("max event_time, probably") would quietly shift every
    time a late event landed just before the snapshot was taken.
    """

    golden_at: datetime
    created_at: datetime
    volumes: dict[str, str]  # volume name -> sha256 of its archive
    sizes_bytes: dict[str, int] = field(default_factory=dict)
    stack_version: str = "unknown"

    def to_json(self) -> str:
        payload = asdict(self)
        payload["golden_at"] = self.golden_at.isoformat()
        payload["created_at"] = self.created_at.isoformat()
        return json.dumps(payload, indent=2, sort_keys=True)

    @classmethod
    def from_json(cls, raw: str) -> GoldenManifest:
        payload = json.loads(raw)
        return cls(
            golden_at=datetime.fromisoformat(payload["golden_at"]),
            created_at=datetime.fromisoformat(payload["created_at"]),
            volumes=payload["volumes"],
            sizes_bytes=payload.get("sizes_bytes", {}),
            stack_version=payload.get("stack_version", "unknown"),
        )

    def write(self, path: Path) -> None:
        path.write_text(self.to_json())

    @classmethod
    def read(cls, path: Path) -> GoldenManifest:
        return cls.from_json(path.read_text())

    def missing_volumes(self) -> list[str]:
        return [name for name in REQUIRED_VOLUMES if name not in self.volumes]

    def validate(self) -> None:
        missing = self.missing_volumes()
        if missing:
            raise ValueError(
                f"golden snapshot is missing {', '.join(missing)}: restoring a partial set "
                "leaves CDC offsets disagreeing with the databases"
            )


def catch_up_window(
    manifest: GoldenManifest,
    now: datetime,
    *,
    max_gap: timedelta = DEFAULT_MAX_GAP,
) -> tuple[datetime, datetime]:
    """The stretch of history a restore has to replay: [golden_at, now)."""
    gap = now - manifest.golden_at
    if gap < timedelta(0):
        raise ValueError("golden snapshot is dated in the future; clocks disagree")
    if gap > max_gap:
        raise GapTooLarge(gap, max_gap)
    return manifest.golden_at, now


def restore_plan(manifest: GoldenManifest, now: datetime) -> list[str]:
    """The ordered steps of a restore, as the Console shows them and the script performs them.

    Order is not cosmetic. The stack must be fully stopped before volumes are swapped, or a
    running Postgres writes into a half-restored data directory; connectors must be registered
    with `snapshot.mode=never` after the restore, or Debezium re-snapshots the whole database
    on top of data that already contains it; and the catch-up runs last, because it produces
    into topics the restored connector has to already be reading correctly.
    """
    manifest.validate()
    start, end = catch_up_window(manifest, now)
    return [
        "stop every service (a running Postgres would write into a half-restored volume)",
        f"restore volumes together: {', '.join(REQUIRED_VOLUMES)}",
        "verify archive checksums against the manifest",
        "start databases and the broker, and wait for health",
        "register connectors with snapshot.mode=never at the restored slot position",
        f"catch up {(end - start).days}d {(end - start).seconds // 3600}h "
        f"({start:%Y-%m-%d %H:%M} -> {end:%Y-%m-%d %H:%M}) through the backfill path",
        "run a full dbt build so marts reflect the restored history plus the catch-up",
    ]
