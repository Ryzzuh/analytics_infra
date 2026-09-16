{{ config(materialized='table') }}

-- The two billing paths, merged (SPEC.md §4.3).
--
-- Webhooks are the fast path and are unreliable by nature: at-least-once, unordered, and
-- absent entirely while the receiver is down. The daily pull is slow and complete. Neither is
-- sufficient alone, so both are kept and joined on the provider's own event id — the only
-- identifier both paths agree on.
--
-- `arrived_by` is the useful output. `batch_only` means the webhook never came, which is the
-- number that says whether the fast path can be trusted this week.

with webhooks as (
    -- At-least-once delivery means duplicates are normal, so collapse to first arrival.
    select
        provider_event_id,
        min(event_type)             as event_type,
        min(account_id)             as account_id,
        min(provider_created_at)    as provider_created_at,
        min(received_at)            as webhook_received_at,
        count(*)                    as webhook_deliveries
    from {{ source('raw', 'billing_webhook_events') }}
    group by provider_event_id
),

batch as (
    select
        provider_event_id,
        event_type,
        account_id,
        provider_created_at,
        pulled_at,
        payload
    from {{ source('raw', 'billing_events_batch') }}
)

select
    coalesce(w.provider_event_id, b.provider_event_id)      as provider_event_id,
    coalesce(w.event_type, b.event_type)                    as event_type,
    coalesce(w.account_id, b.account_id)                    as account_id,
    coalesce(w.provider_created_at, b.provider_created_at)  as provider_created_at,
    w.webhook_received_at,
    b.pulled_at                                             as batch_pulled_at,
    coalesce(w.webhook_deliveries, 0)                       as webhook_deliveries,
    case
        when w.provider_event_id is not null and b.provider_event_id is not null then 'both'
        when w.provider_event_id is not null then 'webhook_only'
        else 'batch_only'
    end                                                     as arrived_by,
    -- The webhook never arrived. Not an error in itself — it is why the daily pull exists —
    -- but a rising rate means the fast path is degrading.
    w.provider_event_id is null                             as webhook_missed,
    -- Seen only by webhook so far: expected for events newer than the last pull, and a
    -- reconciliation gap if it persists.
    b.provider_event_id is null                             as not_yet_reconciled,
    b.payload
from webhooks w
full outer join batch b using (provider_event_id)
