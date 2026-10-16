# Semantic memory

Semantic records hold facts, decisions, preferences, goals and notes. PostgreSQL is authoritative for content, identity, ownership, privacy and lifecycle; Qdrant holds matching host-supplied vectors in `stores.qdrant_collection`. Names such as `memories` or `knowledge` are not implicit collection defaults.

The host provides importance/confidence, entities, tags, graph hints and embedding. Policy thresholds may reject a candidate as a normal response. Production validates vectors against provider/model/dimension configuration; a configured endpoint is not invoked internally.

Retrieval uses a host query vector and blends configured similarity/recency/importance/access weights. Canonical active-row hydration enforces current privacy and deletion state before results are selected under a token estimate. Updates and forget must be visible on subsequent batch reads, even with a reused unchanged request fingerprint key.

See [ingestion](../../pipeline/ingestion-pipeline.md) and [retrieval](../../pipeline/retrieval-pipeline.md).
