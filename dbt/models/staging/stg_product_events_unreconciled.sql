{{ config(materialized='view') }}

-- Events dated before the account they name was created: a contradiction the source cannot
-- produce, so the warehouse refuses them.
--
-- Deliberately NOT a list of events whose account is unknown. An unknown id proves nothing —
-- the CDC stream may not have caught up, or the account may have been erased — and withholding
-- on absence of evidence would let a lagging load quietly destroy real events.
--
-- Three generators each invented account ids from a hard-coded range — the live stream
-- (1..SIM_ACCOUNTS), the history replay (1..2000) and the chaos storm (1..500). All three wrote
-- to the same table, so fixing one at a time changed nothing until the last was done. A
-- September replay wrote history for accounts created in October; those accounts then arrived
-- holding usage they never had, with no recent activity to match it, and the usage-collapse
-- rule read the gap as a customer who had stopped using the product.
--
-- Anti-join against staging rather than a restatement of the rule, matching
-- `stg_product_events_quarantined`: what matters is whether a row is actually being withheld,
-- not whether it matches a predicate. If the rule changes, this follows it.
--
-- Unlike the lateness quarantine, a `--full-refresh` does NOT absorb these. Late events are
-- true facts that arrived inconveniently; these are not facts at all, and no decision to
-- accept a change to a closed period should let them in.

with account_changes as (
    {{ cdc_stream('accounts') }}
),

account_exists as (
    select
        pk::bigint                                      as account_id,
        min((row_state ->> 'created_at')::timestamptz)  as created_at
    from account_changes
    where row_state ->> 'created_at' is not null
    group by 1
)

select
    r.event_id,
    r.account_id,
    r.event_type,
    r.topic,
    r.event_time,
    r.loaded_at,
    r.source_path,
    a.created_at                                            as account_created_at,
    'event predates account'                                as reason
from {{ source('raw', 'product_events') }} r
join account_exists a on a.account_id = r.account_id
where not exists (
    select 1 from {{ ref('stg_product_events') }} s where s.event_id = r.event_id
)
-- Restricted to reconciliation failures. The anti-join alone would also catch events the
-- lateness cutoff is holding back, which are true facts withheld for a different reason and
-- already listed in `stg_product_events_quarantined`.
and r.event_time < a.created_at
