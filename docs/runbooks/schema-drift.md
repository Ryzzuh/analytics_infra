# Schema drift blocking a model
**Symptom:** `DriftBlocking` — a field a model depends on has disappeared or changed type.

```sql
SELECT event_type, json_path, change, details, detected_at
FROM ops.drift_findings WHERE resolved_at IS NULL AND blocking;
```

**What has already happened:** the affected event type's staging model is failing, and only that
subtree. Other event families keep flowing — that separation is the point of per-family topics.

**Fix**

1. Confirm the producer change is intentional (a rename usually is; a type change usually is not).
2. Update the staging model to read both shapes during the transition, e.g.
   `coalesce(payload ->> 'plan_code', payload ->> 'plan_id')`.
3. Update `meta.required_fields` in the model's schema.yml so the registry matches the SQL.
4. Mark the finding resolved.

**Do not** delete the drift finding to clear the alert. The blocked model is the safety
mechanism working: without it, revenue-by-plan quietly goes to zero for new rows.
