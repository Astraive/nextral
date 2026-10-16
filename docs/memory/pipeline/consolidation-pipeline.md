# Consolidation pipeline

The host supplies `{tenant_id,user_id,session_id,lane,policy,version,summary,candidates,chunk_messages}`. Model extraction and embedding happen outside Nextral. Use `lane: fast` or `deep`; `chunk_messages` defaults to 100.

## Fast lane

Load the session, produce a nonempty host summary, and supply explicit ingestion candidates with provenance and vectors. Rust checks scope/source linkage, ingests accepted candidates, persists the summary and records durable job/provenance results. It does not generate a summary from an omitted field.

## Deep lane

Candidates correspond to deterministic contiguous message chunks: join each message as `role: content` with newline separators. Supply matching ordered source message IDs and vectors for those exact chunks. Rust validates the correspondence, writes accepted memory, and uploads the actual transcript archive to S3. An object key alone is not archive success.

## Scheduling, replay and recovery

The host decides when a session is idle/closed or eligible for deep work; no built-in nightly or seven-day schedule is promised. Persist asynchronous work with `jobs.enqueue` and process it through `jobs.run`. Stable session/lane/version and source keys fence replay; changed input requires a new version. Inspect completion, archive/store receipts and durable failures; run outbox recovery as needed.

See [worker runbook](../operations/consolidation-and-retention.md) for leases and retention.
