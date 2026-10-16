# Incidents and recovery

## Triage

1. Identify affected tenant/user scopes and the failing operation; avoid raw content in incident logs.
2. Distinguish liveness (`/healthz`) from real store readiness (`/readyz`) and from operation-level success.
3. Inspect canonical state, job status/attempts/leases, outbox backlog and required store receipts.
4. Restore the actual dependency or credentials before replay. Keep terminal reasons visible.

## Retrieval outages

Enabled vector and graph paths run in parallel. A surviving path can return degraded results; all enabled paths failing returns an explicit error. The host may decide to use session-only context but must not relabel failed retrieval as a successful memory hit. After recovery, check canonical hydration/privacy filtering and representative queries, not only index counts.

## Interrupted writes and workers

Canonical/audit/outbox mutation is transactional in PostgreSQL, not atomic across all stores. Restore the failed vector/graph/archive dependency, invoke `outbox.run`, and inspect acknowledgements. Due jobs with expired durable leases can be reclaimed by `jobs.run`; completed IDs and retry state support resumption. Do not manually declare a job completed to hide a missing external effect.

For consolidation, retain the same host input and version when replaying unchanged work. Regenerate new output only under a new version. Host model outages are host responsibilities; Nextral will not synthesize embeddings or summaries as a fallback.

## Deletion failure

Keep the request incomplete until required store receipts confirm removal/redaction. Replay pending cleanup and inspect exact source-derived references, including sessions/summary/archive state and reminders. Never perform blanket substring replacement or delete unrelated manual policies to make a check pass.

Archive manifests with missing legacy source provenance require an integrity-checked, scope-validated object read before source matching; recovery persists the derived source IDs. An unavailable/corrupt object is a cleanup failure, except `NotFound` is acknowledged as tombstoned. Same-key marked tombstones make verified reads return `NotFound`; conditional immutable uploads prevent late resurrection. This does not prove erasure of versions/backups. Forget-scrubbed job requests and terminal job states must survive stale worker completion; claimed writes fence both under the live lease.

## Schema deployment

Run explicit migrations with the schema-owning role before using a restricted runtime role. Migration history must be a checksum-matching, contiguous prefix of the application's ordered migrations; unknown versions, gaps and changed checksums block migration and readiness. Concurrent migrators serialize under a PostgreSQL transaction advisory lock.

Do not delete or rewrite history to force readiness. Restore the matching application/schema version or investigate the incompatible state before retrying. Upgrade verification uses isolated databases with a populated legacy transcript, concurrent migrators and a cross-owner canonical write race.

## Reembed interruption

Resume the same scoped plan with missing vectors and inspect checkpoint/validation status. Do not switch a global collection alias as an improvised repair. See [reembed runbook](reembed-and-backfill.md).

Backups and restoration drills remain deployment obligations. Record root cause, scope, observed effects, recovery actions and missing alerts; successful health responses alone do not prove restored data integrity.
