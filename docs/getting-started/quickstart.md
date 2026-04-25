# Quickstart

Nextral is a package-first memory runtime. Install the Python or npm package,
provide production store/provider config, and use the CLI or language bindings
from your app.

## Validate Config

```bash
nextral config validate examples/config.production.example.json
```

The production backend requires PostgreSQL, Redis, Qdrant, Neo4j, MinIO/S3,
embedding provider config, extraction provider config, cache policy, auth, and
retrieval policy. Example values are placeholders; production values must be
supplied by the host application or deployment environment.

## Inspect Runtime Shapes

```bash
nextral memory ingest
nextral memory smoke
nextral jobs reembed-plan examples/reembed.example.json
```

`memory smoke` uses the explicit testkit backend. It is not a hidden production
default.

## Optional Service Modes

```bash
nextral-api plan http examples/config.production.example.json
nextral-api plan grpc examples/config.production.example.json
nextral-api plan graphql examples/config.production.example.json
nextral-mcp tools
```

HTTP, gRPC, GraphQL, and MCP are optional package-provided service surfaces over
the same Rust runtime.

## Architecture Docs

- `docs/architecture/project-structure.md`
- `docs/memory/README.md`
- `docs/package-production.md`

