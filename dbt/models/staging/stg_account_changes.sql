{{ config(materialized='view') }}

-- Typed view of the accounts change log, deletes included: a hard delete here is a GDPR
-- erasure, and the `before` image is how the warehouse knows which account vanished.

with changes as (
    {{ cdc_stream('accounts') }}
)

select
    pk::bigint                                  as account_id,
    op,
    is_snapshot,
    source_lsn,
    source_ts,
    effective_at,
    loaded_at,
    row_state ->> 'company_name'                as company_name,
    row_state ->> 'country'                     as country,
    (row_state ->> 'created_at')::timestamptz   as created_at
from changes
