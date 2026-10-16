# Ingestion pipeline

## Session append is not memory extraction

`nextral.session.append` records a scoped message in the full PostgreSQL transcript and refreshes the Redis last-20 tail. Appending text does not automatically call a model, embed a fact, infer a reminder, or invoke retrieval.

## Explicit durable candidate ingestion

1. The host supplies tenant/user scope, optional stable ID/session provenance, content/type, source IDs, scores, privacy, entities/tags, typed graph hints and embedding.
2. Rust validates identifiers, content, scores, policy thresholds and production vector/provider dimensions. Rejection can be a normal `status: rejected` result.
3. Canonical memory, audit and outbox state commit together in PostgreSQL.
4. External vector/graph work produces real receipts; durable outbox processing repairs incomplete effects.
5. Retrieval later reloads active canonical state instead of trusting stale index content.

The request is an executable write, not a schema response. Use Rust CLI `schema nextral.memory.ingest` or Python `memory ingest-schema` for inspection. See [record payloads](../contracts/memory-record-schema.md).

A stable explicit identity is useful for replay. Do not assume an undocumented message-hash fallback or transactional all-or-nothing batch behavior. Preserve returned validation errors, receipts and trace IDs without logging sensitive bodies.
