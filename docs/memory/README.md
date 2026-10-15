# Memory guides

Nextral supports seven memory categories, with request-local working context and six durable kinds under the configured production backend. The host owns model execution and extraction.

## Reading map

- [Architecture](architecture.md): canonical state, external indexes and execution boundaries
- [Types](types/README.md): working, session, episodic, semantic, relational, procedural and prospective
- [Runtime/storage/retrieval](architecture/README.md)
- [Payload guides](contracts/README.md)
- [Pipelines](pipeline/README.md)
- [Workflows](workflow/README.md)
- [Operations](operations/README.md): durable workers, recovery, scoped reembedding and measurement

`memory_taxonomy.svg`, `memory_lifecycle.svg` and `graphify_dual_retrieval.svg` illustrate design concepts. Labels describing automated model extraction, scheduler timing, or latency in older illustrations are not current API or performance guarantees. Use the linked implementation guides and actual schemas.
