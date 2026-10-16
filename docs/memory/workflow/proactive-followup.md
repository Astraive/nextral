# Workflow: proactive follow-up

1. Host extraction identifies a commitment and ingests an owned source memory with explicit model outputs.
2. Host resolves due time, timezone, privacy and channel consent, then schedules the reminder explicitly.
3. A host-owned scheduler invokes `nextral.reminders.due` with scope, cutoff, actor and retry policy.
4. Rust claims durable delivery work, checks source availability, privacy/channel policy, priority, quiet hours, dedupe and expiry.
5. The configured webhook receives a real HTTP request; acknowledgement or error determines the persisted attempt outcome and next state.
6. Inspect `reminders.list` for status, retries, terminal reason and receipts without sending again.

Set `NEXTRAL_REMINDER_WEBHOOK_URL` and optional `NEXTRAL_REMINDER_WEBHOOK_TOKEN`. No magic title simulates delivery. A restarted host can recover durable state/expired leases; downstream handlers must remain idempotent under retries. Completion means acknowledged delivery, not human engagement or execution of an arbitrary agent workflow.

A package import does not install cron, extract future actions automatically, or generate follow-up prose. See [reminder payloads](../contracts/prospective-contract.md).
