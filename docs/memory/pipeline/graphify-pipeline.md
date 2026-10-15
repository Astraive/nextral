# Graphify pipeline

1. Receive `tenant_id`, `user_id`, `memory_id` and optional typed `hints`; fetch the active canonical memory in that scope.
2. Validate node labels, relationship enum, direction and confidence.
3. Canonicalize node/edge identities and attach source-memory evidence.
4. Persist graph intent, audit and outbox transactionally; attempt scoped source replacement in Neo4j.
5. Return flattened graph output, `status` and `write_receipts`; retain external failure work for recovery.

The public `nextral.graph.graphify` tool rejects a full `record` payload and does not rewrite canonical content. Nonempty supplied hints are authoritative for relationships; deterministic content heuristics run only without them. There is no hidden LLM extraction stage. Matching positive/`NOT_` relations on the same directed scoped endpoints are contradictions; compatible types such as `USES` and `OWNS` are not. See [actual payload fields and receipt semantics](../contracts/graphify-contract.md).

Replays use deterministic scoped merges rather than blindly creating duplicates. Forget targets exact source evidence, so unrelated memories sharing an entity or relationship remain. An external graph write failure is not proof that canonical memory was lost; inspect outbox state and replay acknowledged work before declaring recovery complete.
