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

sequenced as (
    select
        *,
        lead(effective_at) over w    as next_effective_at,
        lead(op) over w              as next_op,
        lag(status) over w           as prior_status,
        lag(plan_code) over w        as prior_plan_code,
        lag(seats) over w            as prior_seats,
        lag(mrr_cents) over w        as prior_mrr_cents
    from changes
    window w as (partition by subscription_id order by source_lsn)
),

versions as (
    select *
    from sequenced
    where
        -- Deletes close the previous interval; they do not open a version of their own.
        op <> 'd'
        -- CDC captures every column update, including ones that change nothing we track
        -- (a touched updated_at, say). Emitting a version for those would inflate the
        -- dimension with duplicates that differ only by timestamp.
        and (
            op in ('c', 'r')
            or status is distinct from prior_status
            or plan_code is distinct from prior_plan_code
            or seats is distinct from prior_seats
            or mrr_cents is distinct from prior_mrr_cents
        )
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
    next_op = 'd'                                   as ended_by_delete,
    -- Rows from the initial snapshot are current state, not history: nothing before them is
    -- knowable, so anything measuring "time in status" must exclude them (SPEC.md §6.1).
    is_snapshot                                     as is_initial_snapshot,
    source_lsn,
    source_ts,
    source_path
from versions
