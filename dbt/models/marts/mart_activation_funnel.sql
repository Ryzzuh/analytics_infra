{{ config(materialized='table', schema='marts') }}

-- Signup cohorts by week, with the two steps that matter early: did they use the product, and
-- did they convert from trial.
--
-- Cohorts are dated by the account's created_at (business time), so replayed history lands in
-- the week it actually happened.

with accounts as (
    select
        account_id,
        created_at,
        date_trunc('week', created_at)::date as cohort_week
    from {{ ref('dim_account') }}
    where not from_initial_snapshot   -- no signup date worth trusting for these
),

first_usage as (
    select
        a.account_id,
        min(f.activity_date) as first_active_date
    from accounts a
    join {{ ref('fct_account_activity_daily') }} f using (account_id)
    where f.feature_invocations > 0
    group by 1
),

converted as (
    select
        s.account_id,
        min(s.valid_from) as first_active_subscription_at
    from {{ ref('dim_subscription') }} s
    where s.status = 'active'
    group by 1
)

select
    a.cohort_week,
    count(*)                                                            as signups,
    count(u.account_id)                                                 as ever_active,
    count(*) filter (
        where u.first_active_date <= (a.created_at + interval '14 days')::date
    )                                                                   as activated_within_14d,
    count(c.account_id)                                                 as ever_converted,
    count(*) filter (
        where c.first_active_subscription_at <= a.created_at + interval '30 days'
    )                                                                   as converted_within_30d
from accounts a
left join first_usage u using (account_id)
left join converted c using (account_id)
group by 1
