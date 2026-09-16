{{ config(materialized='view') }}

with changes as (
    {{ cdc_stream('support_tickets') }}
)

select
    pk::bigint                                  as ticket_id,
    op,
    source_lsn,
    effective_at,
    loaded_at,
    (row_state ->> 'account_id')::bigint        as account_id,
    row_state ->> 'severity'                    as severity,
    (row_state ->> 'opened_at')::timestamptz    as opened_at,
    (row_state ->> 'closed_at')::timestamptz    as closed_at
from changes
