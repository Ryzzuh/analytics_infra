{{ config(materialized='view') }}

-- Typed view of the subscription change log.
--
-- Deduplicated on (pk, source_lsn): an LSN identifies one committed change, so two rows
-- sharing one is always a redelivery, never two real changes. This is the CDC equivalent of
-- the event_id dedup in stg_product_events.

with changes as (
    {{ cdc_stream('subscriptions') }}
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
        row_state,
        before
    from changes
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
