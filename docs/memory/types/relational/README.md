# Relational memory

Relational memory records explicit entity connections in scoped Neo4j graph state. Hosts supply entities and typed directed `graph_hints` with confidence; Graphify canonicalizes identities and merges nodes/edges with source-memory evidence. There is no built-in Claude extraction call.

Use the declared node/relationship enums in [graph payloads](../../contracts/graphify-contract.md), rather than assuming any arbitrary label or relationship is accepted. Source references connect graph evidence to the canonical memory record and its tenant/user scope.

Graph retrieval matches host-provided entities and performs bounded traversal. Its candidate IDs join vector candidates and are reloaded from canonical active records before use. A graph path failure can degrade to a surviving enabled vector path; neither path is a substitute for canonical privacy/lifecycle checks.

Forget removes the exact source contribution. Shared relationships with unrelated source evidence must not be erased just because one memory is forgotten.
