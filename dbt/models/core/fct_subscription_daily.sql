{{ config(materialized='table') }}

-- One row per subscription per day it existed: SCD2 intervals flattened onto the date spine.
--
-- `distinct on` picks the version live at the END of each day. A subscription that went
-- trial -> active -> cancelled in one day matches three versions for that date, and daily
-- reporting needs exactly one: the state it finished the day in. The intra-day transitions
-- are not lost, they are in dim_subscription, which is where they belong.

select distinct on (d.date_day, s.subscription_id)
    d.date_day,
    s.subscription_id,
    s.account_id,
    s.plan_code,
    s.status,
    s.seats,
    s.mrr_cents,
    s.is_initial_snapshot,
    s.valid_from                                        as version_started_at
from {{ ref('dim_date') }} d
join {{ ref('dim_subscription') }} s
    on d.date_day >= s.valid_from::date
   and d.date_day <  s.valid_to::date
order by d.date_day, s.subscription_id, s.valid_from desc
