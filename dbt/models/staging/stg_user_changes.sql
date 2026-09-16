{{ config(materialized='view') }}

with changes as (
    {{ cdc_stream('users') }}
)

select
    pk::bigint                                  as user_id,
    op,
    source_lsn,
    effective_at,
    loaded_at,
    (row_state ->> 'account_id')::bigint        as account_id,
    row_state ->> 'role'                        as role,
    (row_state ->> 'last_login_at')::timestamptz as last_login_at,
    (row_state ->> 'created_at')::timestamptz   as created_at
from changes
