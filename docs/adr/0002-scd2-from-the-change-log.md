# ADR 0002 — History comes from the CDC change log, dated by business time

**Status:** accepted (M2)

## Context

The warehouse needs type-2 history of subscriptions: what plan, status and seat count an
account had at any past moment.

The default answer in a dbt project is `dbt snapshot`: run periodically, compare current state
against the last snapshot, close and open rows on change. It needs no CDC at all.

There is also a timestamp problem. The platform seeds twelve months of history by *replaying*
the simulator against an empty database (SPEC.md §6.2), so the commit time of a change that
"happened" last March is today.

## Decision

**History is derived from the Debezium change log**, not from dbt snapshots.

Ordering and dating use different columns, deliberately:

- **`source_lsn` orders changes.** It is the only total order Postgres offers over committed
  changes, and it is still correct when several changes share a timestamp.
- **`effective_at` dates the intervals.** It is business time, carried in the row itself, so a
  year of history replayed in ten minutes still produces correctly dated versions.

Supporting decisions:

- **`REPLICA IDENTITY FULL` on every captured data table.** Without it an `UPDATE` carries no
  old values and a `DELETE` carries only the primary key, so an interval cannot be closed with
  what was actually live. A delete arriving with no before image is sent to the DLQ rather
  than modelled, because it is the symptom of this being misconfigured.
- **Deletes close an interval; they do not open a version.** A deleted row has no state to
  describe. The version it ended is flagged `ended_by_delete`.
- **Updates that change nothing tracked produce no version.** CDC captures every column
  update, including touched timestamps; versioning those would inflate the dimension with rows
  that differ only by clock.
- **Rebuilt in full, not incrementally.** The change log is small at this scale, and an
  incremental SCD2 would have to reopen and re-close intervals for late changes.
- **Reconciled against an independent snapshot.** Everything else is derived from the change
  log, so the change log cannot check itself: if a slot was invalidated and changes were lost,
  the derived data is self-consistent and wrong. `raw.oltp_snapshots` is read straight from
  the source and compared daily.

## Consequences

- A `trial → active → cancelled` sequence inside one day produces three versions. A daily
  snapshot would produce one row saying "cancelled", and the trial would never have existed.
- Rows from Debezium's initial snapshot (`op = 'r'`) are current state with no prior history,
  so they are flagged `is_initial_snapshot` and must be excluded from any "time in status"
  measure. This is the honest version of what happens when a real company adopts CDC.
- More WAL per update, from `REPLICA IDENTITY FULL`. Accepted: the change log *is* the
  history here, and it cannot be re-derived later.
- The reconciliation test stays silent in two states — no snapshot taken yet, and a known open
  gap in `ops.cdc_gaps` — because a red test meaning "not checked yet" or "recovery already in
  progress" teaches people to ignore it.
- Open intervals end at `9999-12-31 00:00:00+00`, not `'infinity'`. Postgres handles infinity
  correctly, but psycopg raises `DataError` reading it back (Python `datetime` has no
  infinity), and reverse ETL reads this column. The time-of-day is midnight UTC so that a
  session east of UTC does not render it into year 10000 and overflow the same way.

## Deviation from the spec

SPEC.md §5.2 described `raw.cdc_<table>` per captured table. The implementation uses a single
`raw.cdc_changes` with a `source_table` column: per-table raw would mean new DDL, a new
partition set and a new ledger stream for every table added to the publication, for a change
log that is a few hundred thousand rows a year. Staging splits it back out per table.
