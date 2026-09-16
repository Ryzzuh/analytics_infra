-- Reconciliation: the CDC-derived history must agree with an independent snapshot of current
-- OLTP state (SPEC.md §6.1).
--
-- This is the check that catches a slot invalidation having lost changes. Everything else in
-- the pipeline is derived from the change log, so the change log cannot check itself: the
-- snapshot is loaded separately, straight from the source database.
--
-- Rows returned = failures.

with latest_snapshot as (
    select
        pk::bigint                          as subscription_id,
        row_data ->> 'status'               as status,
        (row_data ->> 'seats')::integer     as seats,
        row_data ->> 'plan_code'            as plan_code
    from {{ source('raw', 'oltp_snapshots') }}
    where source_table = 'subscriptions'
      and snapshot_at = (
          select max(snapshot_at) from {{ source('raw', 'oltp_snapshots') }}
          where source_table = 'subscriptions'
      )
),

current_versions as (
    select subscription_id, status, seats, plan_code
    from {{ ref('dim_subscription') }}
    where is_current
      and not ended_by_delete
),

-- Two states where this test must stay silent rather than go red:
--
--   * No snapshot has been taken yet (fresh deploy, or just after a reset). That is "not
--     checked", not "disagrees", and a red test meaning the former trains people to ignore it.
--   * A gap the platform already knows about — slot invalidated, re-snapshot requested — is
--     reported by ops.cdc_gaps. Failing here too is noise during a recovery that is already
--     alarming.
verdict_possible as (
    select
        exists (
            select 1 from {{ source('raw', 'oltp_snapshots') }}
            where source_table = 'subscriptions'
        ) as have_snapshot,
        not exists (
            select 1 from {{ source('ops', 'cdc_gaps') }} where resolved_at is null
        ) as no_open_gap
)

select
    coalesce(s.subscription_id, c.subscription_id) as subscription_id,
    s.status    as snapshot_status,
    c.status    as scd2_status,
    s.seats     as snapshot_seats,
    c.seats     as scd2_seats,
    s.plan_code as snapshot_plan_code,
    c.plan_code as scd2_plan_code
from latest_snapshot s
full outer join current_versions c using (subscription_id)
cross join verdict_possible v
where v.have_snapshot
  and v.no_open_gap
  and (
      s.subscription_id is null          -- in the warehouse but not in the source
      or c.subscription_id is null       -- in the source but never captured
      or s.status is distinct from c.status
      or s.seats is distinct from c.seats
      or s.plan_code is distinct from c.plan_code
  )
