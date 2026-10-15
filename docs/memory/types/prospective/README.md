# Prospective memory

Prospective memory links a future reminder/follow-up to an existing owned memory. Scheduling is explicit; consolidation does not automatically infer a calendar action or create reminders from prose.

PostgreSQL stores schedule, lease/attempt/retry state and outcomes. Hosts poll `nextral.reminders.due`; `nextral.reminders.list` inspects state without dispatching. There is no autonomous installed cron daemon or required Redis reminder queue.

Real delivery uses `NEXTRAL_REMINDER_WEBHOOK_URL` and optional `NEXTRAL_REMINDER_WEBHOOK_TOKEN`. HTTP acknowledgement determines completion; a title cannot simulate a failure or success. Rust applies allowed channel, privacy, priority, deduplication, quiet hours in the supplied IANA timezone, expiry and retry policy. Lifecycle timestamps accept RFC3339 or epoch-second strings; persisted scheduling times use epoch seconds.

The host owns downstream action idempotency and user consent. Completion of webhook delivery is not proof that a human read the reminder or an external task finished. See [prospective payloads](../../contracts/prospective-contract.md).
