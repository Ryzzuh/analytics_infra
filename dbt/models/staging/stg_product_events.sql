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
-- picked up by the run that loaded it. The lateness POLICY (what counts as too late) lives in
-- the cutoff below, and reprocessing covers the lookback window so late-but-tolerable events
-- land in the right day.

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
    extract(epoch from (received_at - event_time))      as arrival_lag_seconds,
    (received_at - event_time) > interval '{{ lookback_days }} days' as is_late_beyond_tolerance,
    payload ->> 'feature'                               as feature,
    payload ->> 'surface'                               as surface,
    payload ->> 'plan'                                  as plan,
    (payload ->> 'duration_ms')::bigint                 as duration_ms,
    payload
from ranked
where arrival_rank = 1
