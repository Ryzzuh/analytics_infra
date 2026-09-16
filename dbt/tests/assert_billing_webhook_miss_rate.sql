-- The fast path is allowed to miss events — that is why the daily pull exists — but not most
-- of them. A day where a quarter of webhooks never arrived means the provider or the receiver
-- is broken, not merely unreliable.
--
-- Severity is a warning below that: a red test for the platform's normal, designed-for
-- behaviour would train people to ignore it.

{{ config(severity='error') }}

select
    billing_day,
    events,
    webhook_missed,
    webhook_miss_rate
from {{ ref('stg_billing_reconciliation') }}
where events >= 20             -- a 1-in-2 miss rate on 2 events is noise, not signal
  and webhook_miss_rate > 0.25
