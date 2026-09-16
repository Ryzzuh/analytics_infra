{{ config(materialized='table', schema='marts') }}

-- Churn-risk score per account: the payload reverse ETL writes back to the product
-- (SPEC.md §6.3, §8).
--
-- Rule-based and transparent on purpose. The platform is the thing being demonstrated, and a
-- model would put a training pipeline, versioned artefacts and leakage questions between the
-- reader and the infrastructure. The contract (`score`, `band`, `score_version`, `reasons`)
-- is shaped so a model could replace the rules without the sync changing.
--
-- Every component carries its reason. A score with no explanation is unusable by the people
-- who receive it, and untestable by the people who build it.

{% set score_version = 'rules-v1' %}
{% set window_days = 28 %}

with reference as (
    -- "Today" comes from the data, not from now(): a test seeding March data must score March,
    -- and a reset instance must not report every account as dormant for a year.
    select coalesce(max(activity_date), current_date) as as_of_date
    from {{ ref('fct_account_activity_daily') }}
),

subscriptions as (
    select
        account_id,
        max(mrr_cents) filter (where is_current)                as mrr_cents,
        max(seats) filter (where is_current)                    as seats,
        (array_agg(status order by valid_from desc))[1]         as status,
        bool_or(status = 'past_due' and is_current)             as is_past_due,
        bool_or(status = 'cancelled' and is_current)            as is_cancelled
    from {{ ref('dim_subscription') }}
    group by 1
),

recent_activity as (
    select
        f.account_id,
        sum(f.events)                                           as events_recent,
        max(f.active_users)                                     as peak_active_users_recent,
        max(f.activity_date)                                    as last_active_date,
        sum(f.feature_invocations)                              as feature_invocations_recent
    from {{ ref('fct_account_activity_daily') }} f, reference r
    where f.activity_date > r.as_of_date - {{ window_days }}
    group by 1
),

prior_activity as (
    select
        f.account_id,
        sum(f.events)                                           as events_prior
    from {{ ref('fct_account_activity_daily') }} f, reference r
    where f.activity_date <= r.as_of_date - {{ window_days }}
      and f.activity_date >  r.as_of_date - {{ window_days * 2 }}
    group by 1
),

payments as (
    select
        account_id,
        sum(failure_count)                                      as payment_failures,
        count(*) filter (where status = 'payment_failed')        as unpaid_invoices
    from {{ ref('fct_invoices') }}
    group by 1
),

tickets as (
    select
        t.account_id,
        count(*)                                                as tickets_recent,
        count(*) filter (where t.severity = 'high')              as high_severity_recent
    from {{ ref('stg_support_ticket_changes') }} t, reference r
    where t.op <> 'd'
      and t.opened_at > (r.as_of_date - {{ window_days }})::timestamptz
    group by 1
),

assembled as (
    select
        a.account_id,
        a.company_name,
        r.as_of_date,
        s.status                                                as subscription_status,
        s.mrr_cents,
        s.seats,
        coalesce(ra.events_recent, 0)                           as events_recent,
        coalesce(pa.events_prior, 0)                            as events_prior,
        ra.last_active_date,
        r.as_of_date - ra.last_active_date                      as days_since_active,
        coalesce(ra.peak_active_users_recent, 0)                as active_users,
        coalesce(p.payment_failures, 0)                         as payment_failures,
        coalesce(t.tickets_recent, 0)                           as tickets_recent,
        coalesce(t.high_severity_recent, 0)                     as high_severity_tickets,
        coalesce(s.is_past_due, false)                          as is_past_due,
        coalesce(s.is_cancelled, false)                         as is_cancelled,
        case
            when s.seats is null or s.seats = 0 then null
            else coalesce(ra.peak_active_users_recent, 0)::numeric / s.seats
        end                                                     as seat_utilisation
    from {{ ref('dim_account') }} a
    cross join reference r
    left join subscriptions s using (account_id)
    left join recent_activity ra using (account_id)
    left join prior_activity pa using (account_id)
    left join payments p using (account_id)
    left join tickets t using (account_id)
    where not a.is_deleted   -- an erased account has nothing to be scored, or to receive it
),

scored as (
    select
        *,
        -- Usage collapse: heaviest single signal, because it precedes almost every churn.
        case
            when events_prior = 0 then 0
            when events_recent = 0 then 35
            when events_recent::numeric / events_prior < 0.4 then 25
            when events_recent::numeric / events_prior < 0.7 then 12
            else 0
        end                                                     as usage_drop_points,
        case
            when days_since_active is null then 20              -- never active at all
            when days_since_active > 21 then 20
            when days_since_active > 14 then 12
            when days_since_active > 7 then 5
            else 0
        end                                                     as dormancy_points,
        case
            when seat_utilisation is null then 0
            when seat_utilisation < 0.2 then 15
            when seat_utilisation < 0.5 then 8
            else 0
        end                                                     as seat_waste_points,
        least(payment_failures * 8, 20)                         as payment_points,
        least(high_severity_tickets * 6 + tickets_recent * 2, 15) as support_points,
        case when is_past_due then 10 else 0 end                as past_due_points
    from assembled
),

totalled as (
    select
        *,
        least(
            usage_drop_points + dormancy_points + seat_waste_points
            + payment_points + support_points + past_due_points,
            100
        ) as churn_score
    from scored
)

select
    account_id,
    company_name,
    as_of_date,
    subscription_status,
    mrr_cents,
    seats,
    seat_utilisation,
    events_recent,
    events_prior,
    days_since_active,
    payment_failures,
    tickets_recent,
    churn_score,
    case
        when is_cancelled     then 'churned'
        when churn_score >= 60 then 'critical'
        when churn_score >= 35 then 'high'
        when churn_score >= 15 then 'medium'
        else 'low'
    end                                                         as churn_band,
    '{{ score_version }}'                                       as score_version,
    -- The reasons the score is what it is, strongest first, so the product can say something
    -- specific rather than "this account looks risky".
    (
        select coalesce(jsonb_agg(reason order by points desc), '[]'::jsonb)
        from (
            values
                ('usage_drop', usage_drop_points),
                ('dormant', dormancy_points),
                ('seats_unused', seat_waste_points),
                ('payment_failures', payment_points),
                ('support_load', support_points),
                ('past_due', past_due_points)
        ) as r(reason, points)
        where points > 0
    )                                                           as reasons
from totalled
