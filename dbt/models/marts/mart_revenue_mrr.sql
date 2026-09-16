{{ config(materialized='table', schema='marts') }}

-- Daily MRR and subscription counts by plan.
--
-- Built from fct_subscription_daily rather than from current state, so a cancellation does not
-- retroactively erase the revenue that existed before it.

select
    date_day,
    plan_code,
    count(*) filter (where status in ('active', 'past_due'))        as paying_subscriptions,
    count(*) filter (where status = 'trial')                        as trial_subscriptions,
    count(*) filter (where status = 'past_due')                     as past_due_subscriptions,
    count(*) filter (where status = 'cancelled')                    as cancelled_subscriptions,
    sum(mrr_cents) filter (where status in ('active', 'past_due'))  as mrr_cents,
    sum(seats) filter (where status in ('active', 'past_due'))      as paid_seats,
    -- Days whose subscriptions all came from the initial snapshot have no real history behind
    -- them, so a "growth vs last month" reading of them would be meaningless.
    bool_and(is_initial_snapshot)                                   as all_from_initial_snapshot
from {{ ref('fct_subscription_daily') }}
group by 1, 2
