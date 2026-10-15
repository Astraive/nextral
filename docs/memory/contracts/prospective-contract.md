# Reminder payload and delivery guide

## Schedule

```json
{
  "tenant_id": "example",
  "user_id": "alice",
  "source_memory_id": "atlas-database",
  "kind": "follow_up",
  "title": "Check migration progress",
  "due_at": "2026-10-16T09:00:00Z",
  "timezone": "Asia/Kolkata",
  "channel": "webhook",
  "allowed_channels": ["webhook"],
  "priority": "normal",
  "quiet_hours": {"start_hour": 22, "end_hour": 8}
}
```

The source must exist in the same tenant/user scope. Kinds are `follow_up`, `commitment`, `task`; priorities are `low`, `normal`, `high`. Optional fields include `expires_at` and `trace_id`. Timestamp inputs support RFC3339 and epoch-second strings; persisted scheduling timestamps use epoch seconds. Quiet hours are interpreted in the named IANA timezone, not the server's local timezone.

## Execute and inspect

`nextral.reminders.due` requires scope, `due_at_or_before`, `actor`, `retry_delay_seconds`, with optional `max_retries`, `dispatch_policy_version`, `retry_strategy_id`, `trace_id`, `privacy_scope`, and `lease_seconds`. The host owns polling cadence. `nextral.reminders.list` reads state without dispatching.

Configure `NEXTRAL_REMINDER_WEBHOOK_URL` and optional `NEXTRAL_REMINDER_WEBHOOK_TOKEN` for actual delivery. Delivery completion requires the real HTTP acknowledgement, not a magic title or a synthesized receipt. Durable attempt records include outcome/HTTP status/receipt. Quiet hours, channel/privacy gates, expiry, priority, dedupe, lease recovery and bounded retries control dispatch. Downstream handlers should be idempotent across possible retries; a webhook acknowledgement does not prove human receipt.

Persisted states include `draft`, `scheduled`, `due`, `dispatched`, `completed`, `failed`, `retry_scheduled`, `cancelled`, `expired`. Terminal reasons distinguish delivery, retry exhaustion, unavailable source, privacy/channel denial and expiry. Inspect status, attempts, terminal reason, errors and receipts instead of assuming every state is reachable through a separate public mutation API. Forget invalidates reminders derived from the exact owned source.
