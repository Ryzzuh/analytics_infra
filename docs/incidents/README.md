# Incident write-ups

One per chaos scenario (SPEC.md §9.2). Each breaks something real and recovers it — nothing here
fakes a symptom, so every alert and every number below is the platform genuinely reacting.

| Scenario | What it proves |
|---|---|
| [Connector outage → slot invalidation](01-connector-outage.md) | The WAL cap protects the shared disk, and an invalidated slot is recoverable and verifiable |
| [Schema drift release](02-schema-drift.md) | A renamed field is caught before it silently zeroes a mart, and blocks only the model that needs it |
| [Late and duplicate event storm](03-late-duplicate-storm.md) | Duplicates resolve visibly; late events land in the day they happened, or are held back rather than rewriting closed periods |
| [Poison message and loader crash](04-poison-message.md) | One bad message cannot stall a partition, and a crash mid-batch cannot duplicate |

Run them from the Console (demo passcode required). Each says what to expect before you press
the button, and recovery is a button too.
