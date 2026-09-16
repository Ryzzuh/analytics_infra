{{ config(materialized='view') }}

-- Daily webhook reliability: the metric that says whether the fast path can be trusted
-- (SPEC.md §4.3).
--
-- Only days that have actually been reconciled are counted. Today's events are mostly newer
-- than the last daily pull, so including them would report a miss rate of nearly 100% every
-- morning and teach everyone to ignore it.

select
    date_trunc('day', provider_created_at)::date                as billing_day,
    count(*)                                                    as events,
    count(*) filter (where arrived_by = 'both')                 as both_paths,
    count(*) filter (where webhook_missed)                      as webhook_missed,
    count(*) filter (where webhook_deliveries > 1)              as duplicate_deliveries,
    round(
        count(*) filter (where webhook_missed)::numeric / nullif(count(*), 0), 4
    )                                                           as webhook_miss_rate
from {{ ref('stg_billing_events') }}
where not not_yet_reconciled
group by 1
