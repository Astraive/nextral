# Retrieval and graph architecture

## Parallel candidate discovery

Hosts provide `query_text`, `query_vector`, entities, tenant/user identity, privacy scope, top-k, hop bound and token budget. Production semantic search uses the supplied vector; Nextral does not embed the query. Graph matching uses supplied entities rather than a hidden extraction call.

Enabled Qdrant and Neo4j paths execute in parallel. Merge deduplicates canonical memory IDs and hydrates candidates in one scoped active-row query. This prevents stale, deleted, wrong-user or out-of-privacy index payloads from becoming authoritative content. Access counters/timestamps are updated atomically without rewriting vector indexes.

A surviving path can return degraded results with reasons. Full retrieval failure occurs when all enabled paths fail; a deliberately disabled path is not an outage. Hosts must inspect status and safety decision, not only whether an `items` array exists.

## Ranking and budgets

Configured scoring weights blend semantic similarity, exponential recency, importance and normalized access. The example configuration weights are 0.5/0.2/0.2/0.1; they are not a fixed universal contract. The response contains score breakdowns, source path and token estimates. Token fitting bounds estimated memory contribution, not the actual tokenizer cost of the entire host prompt.

Repeated batch-read keys fence request fingerprints, never replay a stored raw retrieval body. Current canonical content and authorization must be read on every call.

## Graphify and provenance

Hosts supply entities and typed `graph_hints`. Public graphify replays an active scoped canonical `memory_id` with optional `hints`, persisting graph intent/audit/outbox without rewriting the memory. Nonempty supplied hints are authoritative for relationships; otherwise deterministic content heuristics apply, never a hidden model call. Matching positive/`NOT_` relations on the same directed scoped endpoints are logical contradictions; `USES` and `OWNS` remain compatible. Graphify constructs canonical nodes/edges, validates enum/confidence fields, and replaces source evidence through scoped Neo4j identities. Replaying deterministic writes must not duplicate identities; forgetting a source removes its evidence without deleting unrelated relationships or manual policies.

See [graph payloads](../contracts/graphify-contract.md), [retrieval payloads](../contracts/retrieval-contract.md), and [measured replay](../operations/slos-alerting-and-capacity.md). Candidate limits and hop bounds are resource controls, not evidence of a 40 ms production SLO.
