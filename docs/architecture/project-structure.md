# Project structure (Nextral runtime-neutral layout)

```text
nextral/
├── src/
│   ├── adapters/           # production store implementations
│   │   ├── postgres.rs
│   │   ├── redis.rs
│   │   ├── qdrant.rs
│   │   ├── neo4j.rs
│   │   ├── s3.rs
│   │   └── transport.rs
│   ├── api/                # HTTP/gRPC/GraphQL server
│   ├── config/             # configuration types and validation
│   ├── contracts/          # shared error types
│   ├── domain/             # core domain types
│   │   ├── memory.rs
│   │   ├── graph.rs
│   │   ├── reminder.rs
│   │   ├── session.rs
│   │   ├── policy.rs
│   │   ├── audit.rs
│   │   └── runtime_policy.rs
│   ├── graph/              # graph utilities and re-exports
│   ├── ingestion.rs        # memory ingestion pipeline
│   ├── memory/             # memory record types and re-exports
│   ├── package/            # MCP tools, smoke tests, batch operations
│   ├── planner.rs          # operation planning
│   ├── ports/              # trait definitions for adapters
│   ├── prospective.rs      # reminder types and re-exports
│   ├── providers/          # provider traits (embedding, extraction, reranker)
│   ├── retrieval/          # retrieval scoring and execution
│   ├── runtime/            # orchestration and runtime execution
│   ├── scoring/            # lexical and multi-factor scoring
│   ├── store.rs            # test store re-exports
│   ├── testkit/            # TestMemoryStore for in-memory testing
│   ├── topology.rs         # memory type to store role mapping
│   └── lib.rs
├── bindings/
│   ├── python/
│   │   ├── nextral/
│   │   ├── src/
│   │   ├── pyproject.toml
│   │   └── Cargo.toml
│   └── node/
│       ├── src/
│       ├── package.json
│       ├── Cargo.toml
│       └── index.ts
├── apps/
│   ├── api/                # HTTP API server binary
│   ├── cli/                # CLI application
│   ├── mcp/                # MCP tool server
│   ├── web/                # optional web frontend (planned)
│   └── examples/           # usage examples (planned)
├── contracts/
│   ├── http/openapi.json   # HTTP API contract
│   ├── grpc/nextral.proto  # gRPC service definition
│   └── graphql/schema.graphql # GraphQL schema
├── migrations/             # database migration files
├── tests/                  # integration tests (planned)
├── docs/
├── scripts/
├── Cargo.toml
├── package.json
├── pyproject.toml
├── README.md
└── CHANGELOG.md
```

## Runtime-neutral boundary

- The Rust core in `src/` is canonical and language-agnostic.
- Core APIs use Rust-native types (`Vec<T>`, structs, enums) and `thiserror` for domain errors.
- FFI crates in `bindings/python` and `bindings/node` map those errors into runtime-native exceptions.

## Async strategy

- Internal concurrency and orchestration are handled in the Rust runtime module (Tokio-based).
- Python bindings bridge async work into `asyncio` with `pyo3-async-runtimes`.
- Node bindings expose async Rust work as Promise-based APIs through napi-rs.

## Serialization strategy

- Shared graph/memory payloads are represented as Serde-compatible Rust types in the core.
- Bindings convert those payloads into runtime-native objects without duplicating business logic.
