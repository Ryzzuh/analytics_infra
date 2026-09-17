# Golden snapshot and restore

The reset path (SPEC.md §9.3, ADR 0003). A cold snapshot of every volume, restored together,
with the gap since the snapshot refilled from the backfill topics.

```bash
./infra/golden/golden.sh snapshot    # stack down, all four volumes archived, stack back up
./infra/golden/golden.sh restore     # volumes replaced, connector re-registered, gap caught up
```

Snapshots go to `.golden/` with a manifest recording `golden_at`, the stack's git revision, and
a sha256 per volume. `plan-restore` refuses a snapshot older than the catch-up limit or missing
a volume, so a restore fails before touching anything rather than halfway through.

## Why all four volumes, cold

`warehouse-data`, `app-data`, `redpanda-data`, `airflow-data` are archived with the stack
stopped, together. A snapshot taken while Postgres is running restores into recovery. One taken
without Redpanda restores the databases to a position the connector's stored offsets no longer
match. They are one consistent set or they are nothing.

## Observed on a real run (2026-09-17)

First run of this path. The snapshot is quick; everything interesting is in the restore.

| Volume | Archive |
|---|---|
| `warehouse-data` | 19.0 MB |
| `airflow-data` | 10.3 MB |
| `app-data` | 6.8 MB |
| `redpanda-data` | 5.2 MB |

Restore, with the warehouse fingerprinted beforehand:

| Measure | Before snapshot | Before restore | After restore |
|---|---|---|---|
| `raw.product_events` | 45,613 | 46,013 | **45,613** |
| latest load | 01:40:04 | 01:55 | **01:40:04** |
| `ops.load_dlq` | 5 | 5 | 5 |
| `core.dim_subscription` | 2,464 | 2,464 | 2,464 |

The 400 events loaded after the snapshot are gone, which is the point of a restore. The slot came
back `active` and the connector `RUNNING` at the restored position, registered with
`snapshot.mode=never` so it resumes rather than re-snapshotting the whole source on top of
itself.

Catch-up wrote 36,000 events to the `*.backfill` topics, and this is the number worth keeping:

    raw.product_events                 82,063   (36,000 of them source_path='backfill')
    staging.stg_product_events_quarantined      1,262 — unchanged

Thirty-six thousand events dated in the past arrived and **not one was quarantined**. The
quarantine count is exactly what it was before the restore, all of it from an earlier late-event
storm. That is the whole reason backfill has its own topics: replayed history is indistinguishable
from late data by timestamp alone, and routing it separately lets the cutoff keep protecting
closed periods without rejecting a legitimate reload (SPEC.md §6.2).

A full `transform` passed afterwards, `assert_scd2_matches_oltp_snapshot` included, which is the
part that matters: the restored warehouse is consistent, not merely populated.

## What the first real run broke

Every one of these was invisible until the path actually ran, and three of the four left the
stack part-way down — this script stops the stack as its first act, so anything that fails
afterwards fails at the worst possible moment.

| Failure | Cause |
|---|---|
| `sh: zstd: not found` | alpine ships `tar` but not the compressor |
| `curl: (56) Recv failure` | `up -d connect` returns before the JVM's REST API listens |
| control plane dead after `compose start` | it bind-mounted `golden.sh`, and replacing the file breaks a single-file mount |
| `jq: command not found` | an undeclared dependency, discovered mid-restore |

The shape they share is worth noticing: none is a flaw in the *design* of the reset path. They
are all assumptions about the environment that held on a developer's machine and nowhere else,
and they could only ever be found by running the thing on a host that had never run it.

## If a restore fails part-way

The archives are not touched by a failed restore, so re-running it is safe: volumes are replaced
from the same files and the script is idempotent from the top. Bring the stack back with
`docker compose -f infra/compose/docker-compose.yml --profile full up -d` first if the failure
left it stopped, so you can see what state the databases are actually in before overwriting them
again.
