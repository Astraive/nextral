# Scoped reembedding and backfill

## Plan versus execution

`nextral.reembed.plan` takes `tenant_id`, `source_collection`, `shadow_collection`, `target_embedding_provider`, and `target_embedding_model`. It produces a plan, not embeddings, a completed migration, or a global alias operation.

`nextral.jobs.reembed.execute` accepts:

- `plan`: the returned plan
- `user_id`: the owned user scope within the plan tenant
- `vectors`: host-generated `{memory_id,vector}` entries
- `target_dimension`: explicit target dimension
- `cutover`: optional explicit request to switch validated reads
- `rollback`: optional explicit request to restore the prior scoped route

## Migration runbook

1. Preserve the source collection and create a plan with a distinct shadow destination and target provider/model.
2. Generate actual target vectors in the host for active owned source records.
3. Execute with explicit dimension; inspect durable checkpoints, counts, receipts and errors.
4. A partial vector set reports `awaiting_vectors`; resume the same plan with missing vectors. Do not treat partial progress as completed.
5. Validate all scoped target records and run representative retrieval/quality comparisons.
6. Request cutover explicitly only after validation. The effective route persists for the tenant **and user**, leaving peer scopes unchanged.
7. Retain the source until the deployment's rollback window ends. Request `rollback` explicitly if required and verify the original scoped route.

This is not a global Qdrant alias swap, an internal embedding-provider invocation, or an instant dimension rewrite of unrelated users. Subsequent host queries must supply vectors compatible with the effective scoped route. Preserve plan/state across process restarts; source changes can invalidate prior migration assumptions and must be resolved rather than ignored.

## Transcript backfill

Select sessions explicitly, generate host candidates/vectors, and use versioned fast/deep consolidation with deterministic source provenance. Deep input must match role-prefixed contiguous transcript chunks and actual source IDs. Replays preserve unchanged identities/versions; changed input uses a new version. Inspect real archive and indexing receipts, drain recovery work, and measure retrieval quality before declaring backfill complete.
