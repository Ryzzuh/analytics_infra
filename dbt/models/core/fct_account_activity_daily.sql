{{
  config(
    materialized='incremental',
    unique_key=['account_id', 'activity_date'],
    incremental_strategy='delete+insert',
  )
}}

-- Daily product-usage aggregate per account: the grain every churn and engagement question
-- actually needs, and small enough to rebuild a window of cheaply.
--
-- Selection is by `loaded_at` and aggregation by `event_time`, which is the whole late-data
-- design in two lines: a three-day-old event loaded today must update the day it HAPPENED,
-- not today. The lookback covers the tolerance window so those days get rebuilt.

{% set lookback_days = var('lookback_days', 3) %}

with events as (
    select *
    from {{ ref('stg_product_events') }}
    {% if is_incremental() %}
    where loaded_at >= (
        select coalesce(max(last_loaded_at), '-infinity'::timestamptz) from {{ this }}
    ) - interval '{{ lookback_days }} days'
    {% endif %}
),

-- Late events reopen their own day, so the whole day is recomputed from raw rather than
-- incremented: adding to an existing aggregate would double-count on a rerun.
affected_days as (
    select distinct account_id, event_time::date as activity_date
    from events
)

select
    a.account_id,
    a.activity_date,
    count(*)                                                        as events,
    count(distinct e.user_id)                                       as active_users,
    count(distinct e.feature) filter (where e.feature is not null)  as distinct_features,
    count(*) filter (where e.event_type = 'session_start')           as sessions,
    count(*) filter (where e.event_type = 'feature_invoked')         as feature_invocations,
    count(*) filter (where e.is_late_beyond_tolerance)               as late_events,
    sum(e.duplicate_copies)                                          as duplicate_copies,
    min(e.event_time)                                                as first_event_at,
    max(e.event_time)                                                as last_event_at,
    max(e.loaded_at)                                                 as last_loaded_at
from affected_days a
join {{ ref('stg_product_events') }} e
    on e.account_id = a.account_id
   and e.event_time::date = a.activity_date
group by 1, 2
