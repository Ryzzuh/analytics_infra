{{ config(materialized='table', schema='core') }}

-- Slowly-changing dimension (type 2) built from the CDC change log (SPEC.md §6.1).
--
-- Why not dbt snapshots: a snapshot compares current state at each run, so a subscription that
-- went trial -> active -> cancelled between two runs appears once, as 'cancelled'. The change
-- log has every transition, which is the entire reason CDC is in this platform.
--
-- Two ordering columns, doing different jobs:
--   * source_lsn orders the changes. It is the only total order Postgres gives for committed
--     changes, and it is correct even when several changes share a timestamp.
--   * effective_at dates the intervals. It is business time carried in the row, so a year of
--     history replayed in ten minutes still produces correctly dated versions.
--
-- Materialised as a table and rebuilt in full: the change log is small (a few hundred thousand
-- rows a year at this scale), and an incremental SCD2 would have to reopen and re-close
-- intervals for late-arriving changes, which is a lot of machinery for no gain here.

with changes as (
    select * from {{ ref('stg_subscription_changes') }}
),

-- Two window passes, in this order, and the order is the whole point.
--
-- Deciding whether a change *starts* a version means comparing it with the change before it,
-- so `lag` runs over every change. Deciding where a version *ends* means finding the next
-- surviving version, so `lead` must run only over the rows that survive.
--
-- Doing both in one pass — as this model originally did — closes each version at the next
-- *change* rather than the next *version*. A write that touches nothing modelled then ends the
-- current version at its own timestamp and opens nothing, so the timeline acquires a hole
-- until the next real change, and a subscription whose latest write was a no-op has no current
-- row at all. On real data that was 74 of 500 subscriptions with no current version, and an
-- overlap check cannot see it: gaps are not overlaps.
classified as (
    select
        *,
        lag(status) over w           as prior_status,
        lag(plan_code) over w        as prior_plan_code,
        lag(seats) over w            as prior_seats,
        lag(mrr_cents) over w        as prior_mrr_cents
    from changes
    window w as (partition by subscription_id order by source_lsn)
),

boundaries as (
    select *
    from classified
    where
        -- Deletes are kept here so they can close the preceding interval, and excluded from
        -- the final select: they end a version without opening one.
        op = 'd'
        -- CDC captures every column update, including ones that change nothing we track
        -- (a touched updated_at, say). Emitting a version for those would inflate the
        -- dimension with duplicates that differ only by timestamp.
        or op in ('c', 'r')
        or status is distinct from prior_status
        or plan_code is distinct from prior_plan_code
        or seats is distinct from prior_seats
        or mrr_cents is distinct from prior_mrr_cents
),

versions as (
    select
        *,
        lead(effective_at) over w    as next_effective_at,
        lead(op) over w              as next_op
    from boundaries
    window w as (partition by subscription_id order by source_lsn)
)

select
    {{ surrogate_key(['subscription_id', 'source_lsn']) }} as subscription_version_key,
    subscription_id,
    account_id,
    plan_code,
    status,
    seats,
    mrr_cents,
    started_at,
    ended_at,
    effective_at                                    as valid_from,
    -- Open intervals end at the maximum representable timestamp rather than 'infinity'.
    -- Postgres handles infinity correctly, but no Python client can read it back: datetime
    -- has no infinity, so psycopg raises DataError. Reverse ETL and the Console read this
    -- column, so the value has to survive the trip. It still sorts correctly and still needs
    -- no null handling in range predicates.
    coalesce(next_effective_at, {{ end_of_time() }}) as valid_to,
    next_op is null                                 as is_current,
    -- coalesce, because `next_op = 'd'` is NULL — not false — for the last version of every
    -- subscription, which is exactly the set of rows anything downstream cares about. A
    -- consumer writing the obvious `where is_current and not ended_by_delete` then gets NULL,
    -- WHERE discards it, and the result is silently empty. That is how the CDC reconciliation
    -- test came to compare the source against nothing and report all 500 subscriptions as
    -- never captured.
    coalesce(next_op = 'd', false)                  as ended_by_delete,
    -- Rows from the initial snapshot are current state, not history: nothing before them is
    -- knowable, so anything measuring "time in status" must exclude them (SPEC.md §6.1).
    is_snapshot                                     as is_initial_snapshot,
    source_lsn,
    source_ts,
    source_path
from versions
where op <> 'd'
