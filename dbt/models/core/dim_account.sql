{{ config(materialized='table') }}

-- Accounts as current state (type 1), not SCD2.
--
-- Deliberate asymmetry with dim_subscription: nothing downstream asks what an account's
-- company name or country was in March, whereas every revenue and churn question depends on
-- what a subscription's status and plan were at a point in time. The account change log is
-- still in `raw`, so history remains recoverable if a question ever needs it.

with current_accounts as (
    {{ current_rows(ref('stg_account_changes'), 'account_id') }}
),

deleted as (
    select account_id, min(effective_at) as deleted_at
    from {{ ref('stg_account_changes') }}
    where op = 'd'
    group by 1
)

select
    a.account_id,
    a.company_name,
    a.country,
    a.created_at,
    a.effective_at                                      as last_changed_at,
    a.is_snapshot                                       as from_initial_snapshot,
    d.deleted_at,
    d.account_id is not null                            as is_deleted
from current_accounts a
left join deleted d using (account_id)
