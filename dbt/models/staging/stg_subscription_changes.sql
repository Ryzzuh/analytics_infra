{{ config(materialized='view') }}

-- Typed view of the subscription change log.
--
-- Deduplicated on (pk, source_lsn): an LSN identifies one committed change, so two rows
-- sharing one is always a redelivery, never two real changes. This is the CDC equivalent of
-- the event_id dedup in stg_product_events.

with deduped as (
    select
        *,
        row_number() over (
            partition by pk, source_lsn
            order by loaded_at, kafka_offset
        ) as arrival_rank
    from {{ source('raw', 'cdc_changes') }}
    where source_table = 'subscriptions'
),

typed as (
    select
        pk::bigint                                          as subscription_id,
        op,
        is_snapshot,
        source_lsn,
        source_ts,
        effective_at,
        loaded_at,
        source_path,
        -- For a delete the current values are in `before`: `after` is null by definition.
        coalesce(after, before)                             as row_state,
        before
    from deduped
    where arrival_rank = 1
)

select
    subscription_id,
    op,
    is_snapshot,
    source_lsn,
    source_ts,
    effective_at,
    loaded_at,
    source_path,
    (row_state ->> 'account_id')::bigint                    as account_id,
    row_state ->> 'plan_code'                               as plan_code,
    row_state ->> 'status'                                  as status,
    (row_state ->> 'seats')::integer                        as seats,
    (row_state ->> 'mrr_cents')::integer                    as mrr_cents,
    (row_state ->> 'started_at')::timestamptz               as started_at,
    (row_state ->> 'ended_at')::timestamptz                 as ended_at,
    before ->> 'status'                                     as previous_status
from typed
