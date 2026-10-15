# Consolidation and retention workers

## Enqueue durable work

`nextral.jobs.enqueue` accepts tenant/user/session scope, `trigger` (`idle`, `close`, `deep`, `retention`), `run_at`, optional `idle_seconds`, `consolidation`, `memory_ids`, `redact`, and `max_attempts`. Idle work requires positive `idle_seconds`. Idle/close use a fast consolidation request; deep uses deep. The embedded consolidation scope/session/lane must match the job. Host candidates, vectors and fast summary must already be supplied.

Retention jobs require explicit nonempty `memory_ids`, no consolidation payload, and optional `redact`. There is no automatic global date-based purge or partition drop. The host chooses eligibility and legal retention policy.

## Run and recover

`nextral.jobs.run` takes `{tenant_id,user_id,now,limit?,lease_seconds?}`. It claims due work using durable leases, persists attempts/retry state, and records completed memory IDs plus errors. `jobs.list/get` inspect persisted jobs. Restarts recover due work and expired claims; choose a lease duration that accommodates actual store latency and monitor retry/backlog growth.

`nextral.outbox.run` processes durable external-store work left pending after canonical/audit/outbox transactions. Do not mark a deletion or consolidation repaired until required receipts acknowledge effects and job/outbox state agrees. Retry reports and receipts are evidence; an object shaped like a report is not automatically success.

## Consolidation checklist

1. Select scoped transcript and version; generate host outputs outside Nextral.
2. Fast: require nonempty summary and explicit candidate provenance.
3. Deep: use deterministic role-prefixed contiguous chunks and matching source IDs; default `chunk_messages` is 100.
4. Enqueue or explicitly execute the consolidation request.
5. Inspect accepted/rejected candidates, persisted summary, archive acknowledgement and store receipts.
6. Drain pending external work and investigate terminal failures. Changed host input needs a new version.

## Retention checklist

1. Select owned memory IDs according to the host's policy and legal constraints.
2. Persist a retention job; run due workers.
3. Inspect per-item completion/errors and required backend receipts.
4. Confirm canonical deletion/redaction and exact-source cleanup before closing the request.

Redis TTL is a cache policy, not a canonical retention policy. PostgreSQL partitioning, graph confidence aging and S3 lifecycle are deployment design choices, not automatically enabled package features.
