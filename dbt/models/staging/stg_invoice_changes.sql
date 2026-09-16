{{ config(materialized='view') }}

with changes as (
    {{ cdc_stream('invoices') }}
)

select
    pk::bigint                                  as invoice_id,
    op,
    source_lsn,
    effective_at,
    loaded_at,
    (row_state ->> 'account_id')::bigint        as account_id,
    row_state ->> 'provider_invoice_id'         as provider_invoice_id,
    (row_state ->> 'amount_cents')::integer     as amount_cents,
    row_state ->> 'status'                      as status,
    (row_state ->> 'issued_at')::timestamptz    as issued_at,
    (row_state ->> 'paid_at')::timestamptz      as paid_at,
    -- A payment that failed and was never retried looks identical to one still open unless
    -- the transition itself is kept, which is why this reads the change log and not state.
    before ->> 'status'                         as previous_status
from changes
