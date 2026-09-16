{{
  config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
  )
}}

-- Typed, deduplicated view of raw product events.
--
-- Raw keeps every copy that arrived (it is the audit of what was actually received); this is
-- where duplicates are resolved, keeping the FIRST arrival per event_id so the result does
-- not change when a later duplicate shows up (SPEC.md §4.4).
--
-- Selection is by `loaded_at`, not `event_time`: an event that arrived late still has to be
-- picked up by the run that loaded it. Reprocessing covers the lookback window so
-- late-but-tolerable events land in the day they happened.
--
-- Two different lags, measuring different things and both worth keeping:
--   * client_lag  = received_at - event_time. How long the device sat on the event. A phone
--     that was offline for three days shows up here.
--   * load_lag    = loaded_at   - event_time. How stale the event was when it reached the
--     warehouse. This is the one the incremental models care about, because it says whether
--     a day that was already built is about to change.
--
-- The CUTOFF uses load_lag. Beyond tolerance, a live event is held back rather than folded in,
-- because quietly rewriting a closed period is how reported numbers change under people who
-- already acted on them. Held events are visible in `stg_product_events_quarantined`, counted
-- as a metric, and absorbed by an explicit `--full-refresh` — a decision, not a default.
--
-- Backfill is exempt by construction. Replayed history is months old by load_lag and would be
-- quarantined in its entirety, which is exactly why it arrives on separate topics and carries
-- `source_path = 'backfill'` (SPEC.md §6.2).

{% set lookback_days = var('lookback_days', 3) %}

with source as (
    select *
    from {{ source('raw', 'product_events') }}
    {% if is_incremental() %}
    where loaded_at >= (select coalesce(max(loaded_at), '-infinity'::timestamptz) from {{ this }})
                        - interval '{{ lookback_days }} days'
    {% endif %}
),

ranked as (
    select
        *,
        row_number() over (
            partition by event_id
            order by kafka_ts, topic, partition_id, kafka_offset
        ) as arrival_rank,
        count(*) over (partition by event_id) as arrival_count
    from source
)

select
    event_id,
    account_id,
    user_id,
    event_type,
    topic,
    event_time,
    received_at,
    kafka_ts,
    loaded_at,
    source_path,
    arrival_count - 1                                   as duplicate_copies,
    extract(epoch from (received_at - event_time))      as client_lag_seconds,
    extract(epoch from (loaded_at - event_time))        as load_lag_seconds,
    (loaded_at - event_time) > interval '{{ lookback_days }} days' as is_late_beyond_tolerance,
    payload ->> 'feature'                               as feature,
    payload ->> 'surface'                               as surface,
    payload ->> 'plan'                                  as plan,
    (payload ->> 'duration_ms')::bigint                 as duration_ms,
    payload
from ranked
where arrival_rank = 1
{% if is_incremental() %}
  -- The cutoff. A full refresh deliberately omits this, which is how quarantined events are
  -- absorbed once someone decides to accept the change to closed periods.
  and not (
      source_path = 'live'
      and (loaded_at - event_time) > interval '{{ lookback_days }} days'
  )
{% endif %}
