{{ config(materialized='view') }}

-- Events the cutoff held back: live events that reached the warehouse more than the tolerance
-- window after they happened (SPEC.md §6.2).
--
-- They are not lost — raw has them, and this is where they are counted, alerted on and
-- inspected before someone decides whether to absorb them with a full refresh.
--
-- Defined as an anti-join against staging, not by re-stating the cutoff rule: what matters is
-- whether an event is actually being held back, not whether it matches a predicate. An initial
-- build and a `--full-refresh` both absorb late events deliberately, and after either, this
-- list empties — which is the honest answer to "what is the platform currently withholding?"

{% set lookback_days = var('lookback_days', 3) %}

select
    event_id,
    account_id,
    event_type,
    topic,
    event_time,
    received_at,
    loaded_at,
    source_path,
    extract(epoch from (loaded_at - event_time)) / 86400    as load_lag_days,
    date_trunc('day', event_time)::date                     as would_have_landed_on
from {{ source('raw', 'product_events') }} r
where source_path = 'live'
  and (loaded_at - event_time) > interval '{{ lookback_days }} days'
  and not exists (
      select 1 from {{ ref('stg_product_events') }} s where s.event_id = r.event_id
  )
