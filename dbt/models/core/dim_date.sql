{{ config(materialized='table') }}

-- Date spine. Needed because "how many active subscriptions on day X" cannot be answered from
-- SCD2 intervals alone: a subscription that changed nothing for six months has one row, not
-- 180, and days with no activity have no rows anywhere to hang a zero on.
--
-- Bounds come from a var rather than from min(valid_from) in the data. Deriving them would put
-- the spine downstream of the dimensions that join to it, so every subscription change would
-- rebuild it, and the graph would read as though dates depend on subscriptions.

{% set spine_start = var('spine_start_date', '2025-01-01') %}

select
    date_day::date                                       as date_day,
    extract(isodow from date_day)::integer               as day_of_week,
    date_trunc('week', date_day)::date                   as week_start,
    date_trunc('month', date_day)::date                  as month_start,
    extract(isodow from date_day) in (6, 7)              as is_weekend
from generate_series(
    '{{ spine_start }}'::date,
    current_date + 30,
    interval '1 day'
) as date_day
