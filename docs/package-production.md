# Package-first production runtime

Nextral ships as pip and npm packages with CLI tools. The packages expose the
same Rust core through Python, Node, CLI, MCP, and optional HTTP/gRPC/GraphQL
service modes.

## Required production stores

- PostgreSQL: canonical memory index, sessions, reminders, audit, jobs, outbox.
- Redis: hot session tail, cache, leases, invalidation.
- Qdrant: vector storage and semantic retrieval.
- Neo4j: relational memory graph.
- MinIO/S3: immutable transcript and source archive.

Nextral does not require or ship n8n or ClickHouse integrations.

## Model providers

Embedding, extraction, and reranking providers are configured. Model names,
dimensions, endpoints, API key environment variable names, and provider kinds
must come from config. Source code must not hardcode provider names or models.

Supported provider kinds are:

- `open_ai_compatible`
- `http`
- `external_callback`
- `test`

`test` providers are for tests and demos only.

## Configuration

Start from `examples/config.production.example.json` and replace placeholders
with deployment values or environment-variable expansion handled by the host
application.

Validate a config through the Python package CLI:

```bash
nextral config validate examples/config.production.example.json
```

The validator checks required stores, provider settings, score bounds, retrieval
policy, cache TTLs, and production-vs-test backend compatibility.

## Optional service modes

The package may run network APIs for applications that want a service boundary:

- HTTP REST contract: `contracts/http/openapi.json`
- gRPC contract: `contracts/grpc/nextral.proto`
- GraphQL contract: `contracts/graphql/schema.graphql`

These service modes must use the same configured runtime and store adapters as
the package APIs. They are optional and must not become a separate required SaaS.

## Observability

Structured logs, metrics, and traces are dev-first. Production observability is
enabled only when configured. Logs must not include raw sensitive memory content.
