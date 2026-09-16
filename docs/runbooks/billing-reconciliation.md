# Billing webhook miss rate high
**Symptom:** `BillingWebhookMissRateHigh` — more than a quarter of reconciled billing events
never arrived by webhook.

**Missing some is normal.** That is why the daily pull exists: webhooks are at-least-once and
best-effort. Missing most of them is a broken receiver or provider.

```sql
SELECT billing_day, events, webhook_missed, webhook_miss_rate
FROM staging.stg_billing_reconciliation ORDER BY billing_day DESC LIMIT 7;
```

**Diagnose**

1. Is the receiver up and acking? `curl -s localhost:8002/health`, then check
   `webhook_rejected_total` — a `broker_nack` count means the receiver is correctly refusing to
   acknowledge what Redpanda would not take.
2. Is the provider's behaviour set to drop? On this demo it is deliberately configurable:
   `curl -s localhost:8001/internal/behaviour`.

**No data is lost while this fires** — the daily pull backfills everything the webhooks missed.
What is lost is timeliness: billing data becomes up to a day stale rather than near-real-time.
