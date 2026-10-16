# Runtime and host control

## Process and trust boundary

The package dispatcher owns a process-wide configured `RuntimeStore`. Configure before operations or provide `NEXTRAL_CONFIG`. Production dispatch uses `ProductionStore`; explicit `test_memory` is test-only. Python/Node wrappers serialize native requests and do not implement a second memory system.

The host authenticates embedded/CLI/MCP callers and supplies tenant/user scope. Network services add verified JWT scope binding or privileged service-token authorization. Do not expose raw package dispatch as an unauthenticated remote endpoint.

## Request loop

1. Append a message with tenant, user, session and stable message identity.
2. Persist the warm transcript and refresh the bounded Redis tail.
3. Supply a query vector/entities and request working context.
4. Receive session tail, summary, policies and retrieved memories with token accounting.
   The response also includes `degraded_reasons`: Redis unavailability falls back to the canonical warm transcript rather than silently returning an empty tail.
5. Let the host run its own agent/model/tool loop and discard transient reasoning as appropriate.

No internal `MemoryAgent`, Claude call, prompt runner, or `asyncio` pipeline is implied by these steps.

## Worker loop

The host chooses idle/close/deep/retention scheduling. `nextral.jobs.enqueue` persists work, `nextral.jobs.run` claims due jobs with durable leases and records retry/terminal/completion state, and `jobs.list/get` expose it. Expired leases allow recovery after process loss. Workers consume host outputs carried in consolidation requests, not an internal extraction provider.

Claimed progress/completion writes require a live matching lease and an unchanged persisted request. Existing terminal state or a request scrubbed by forget wins over stale worker state; the worker cannot overwrite cancellation or restore removed consolidation payloads.

`nextral.outbox.run` drains external-store operations left pending after canonical transactions. Reminder delivery uses its own due scan and durable dispatch state; call `nextral.reminders.due` on a host-owned schedule. A package import does not start these loops.

## Replay and failure

Use stable session/version/candidate provenance for unchanged consolidation inputs; changed input requires a new version. Preserve explicit failures and inspect receipts rather than synthesizing success. Batch writes report item-level results, not cross-item transactions. Batch reads with a repeated fingerprint key still observe fresh canonical state.

## Experiment control

Promotion requires a newly created candidate and comparable measured baseline/candidate replay evidence matching the active/candidate policies. Promotion saves the prior lane, policy version and active experiment identity. Rollback requires a nonblank reason, a saved rollback point and the currently active promoted experiment; it restores that snapshot, not a fixed stable/default policy. Created, superseded and already rolled-back candidates cannot change runtime state through rollback.

See [worker operations](../operations/consolidation-and-retention.md) and [recovery](../operations/incidents-and-recovery.md).
