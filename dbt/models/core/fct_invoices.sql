{{ config(materialized='table') }}

-- Current invoice state, plus the payment-failure history that current state alone destroys:
-- an invoice that failed twice and then paid ends as 'paid' with nothing to show for it, and
-- failed payments are a churn signal.

with current_invoices as (
    {{ current_rows(ref('stg_invoice_changes'), 'invoice_id') }}
),

failures as (
    select
        invoice_id,
        count(*)                as failure_count,
        max(effective_at)       as last_failed_at
    from {{ ref('stg_invoice_changes') }}
    where status = 'payment_failed'
      and previous_status is distinct from 'payment_failed'
    group by 1
)

select
    i.invoice_id,
    i.account_id,
    i.provider_invoice_id,
    i.amount_cents,
    i.status,
    i.issued_at,
    i.paid_at,
    coalesce(f.failure_count, 0)                        as failure_count,
    f.last_failed_at,
    i.paid_at is not null                               as is_paid
from current_invoices i
left join failures f using (invoice_id)
