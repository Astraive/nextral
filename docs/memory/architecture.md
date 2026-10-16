# Nextral memory architecture

Nextral stores host-prepared memory and assembles scoped context. It is not an autonomous agent, model provider, extraction service, or a background scheduler installed by importing a package.

## Memory topology

| Type | Runtime representation | Persistence |
| --- | --- | --- |
| Working | Request-local context assembled for the host | Ephemeral; the host owns model context and disposal |
| Session | Messages, hot tail, host summary | PostgreSQL full transcript plus Redis last 20 messages |
| Episodic | Event/episode memory records with provenance | PostgreSQL canonical rows, Qdrant vectors, S3 transcript archive |
| Semantic | Facts, preferences, decisions | PostgreSQL canonical rows and Qdrant vectors |
| Relational | Memory entities and typed graph hints | Canonical source plus Neo4j nodes/edges |
| Procedural | Explicit scoped policies and procedural records | PostgreSQL with Redis policy cache |
| Prospective | Owned source memory plus scheduled reminder | PostgreSQL durable reminder/attempt state and delivery integration |

`production_stores` uses all five external stores. `test_memory` is an explicitly selected process-local test backend; its lexical retrieval and receipts are not production evidence. See [package boundaries](../package-production.md).

## Write and recovery boundary

The host supplies embeddings, scores, entities, graph hints and consolidation outputs. Ingestion validates policy, records canonical/audit/outbox state transactionally in PostgreSQL, and performs external indexing with acknowledged receipts. PostgreSQL canonical lifecycle state is authoritative; there is no atomic transaction spanning all stores. Durable outbox work repairs partial external failures. Callers must inspect status, receipts and pending/retry work rather than equating an HTTP acceptance response with universal store completion.

Use explicit schema-owner migration before startup. Runtime operations use tenant/user-scoped RLS with a restricted no-DDL role. Authentication and scope authorization remain required at the host boundary.

## Session and consolidation lifecycle

Every accepted session append persists the full warm transcript; Redis bounds the hot tail to 20, with configured TTL. It is not necessary to wait for overflow before PostgreSQL durability. Context assembly combines the tail, stored host summary, enabled scoped policies and retrieved memories within a token estimate/budget.

Fast consolidation needs a nonempty host `summary` and explicit candidates. Deep consolidation validates deterministic role-prefixed transcript chunks and matching contiguous source IDs, embeds using supplied candidate vectors, and uploads the real archive. Lane and version fence replay. Applications enqueue idle/close/deep/retention jobs and invoke workers; no implicit nightly extraction or model call occurs.

## Retrieval

The host supplies `query_vector` and entities. Enabled vector and graph paths run in parallel. The merge deduplicates IDs, loads active canonical candidates in one tenant/user/privacy-scoped query, applies configured scoring weights and token fitting, and atomically records access without reindexing vectors. One failed enabled path produces degraded results from the survivor; all enabled paths failing is an explicit error.

Batch read idempotency keys fence request fingerprints, not raw response-body caches. Reusing an unchanged key must still read current canonical state after updates, forget, or privacy changes.

## Governance and operations

Forget operates on an exact owned memory ID and scrubs exact source references across derived state. It is not substring replacement or query/category deletion; unrelated manual policies survive. Required store acknowledgements and durable recovery state govern completion.

Reembedding persists progress and an effective vector route for a specific tenant and user. Hosts provide vectors; partial runs resume before validated cutover, and rollback is explicit. It is not a global Qdrant alias swap.

- [Runtime control](architecture/runtime.md)
- [Storage and lifecycle](architecture/storage-and-lifecycle.md)
- [Retrieval and graph](architecture/retrieval-and-graph.md)
- [Types](types/README.md), [pipelines](pipeline/README.md), [payloads](contracts/README.md), [workflows](workflow/README.md)
- [Workers, recovery, SLO targets and measured research](operations/README.md)

The SVG diagrams in this folder are conceptual design illustrations, not authoritative API schemas, measured latency charts, or promises that automatic model/scheduler components exist. Current source contracts and these implementation guides define executable behavior.
