# Nextral

**Nextral** is a package-first memory runtime for agents, LLM applications, and tool-driven systems.

It provides a canonical Rust core with Python and Node.js bindings, CLI tools, MCP support, and optional HTTP, gRPC, and GraphQL service surfaces. Nextral is designed to be embedded directly into applications or exposed as a service boundary when needed.

## What Nextral does

Nextral provides a production-ready memory layer for applications that need to store, retrieve, score, relate, and operate on long-lived memory.

It supports:

* Structured memory records
* Semantic retrieval
* Graph-based relationships
* Reminder and session workflows
* Runtime-neutral package APIs
* CLI and MCP tool-call workflows
* Optional HTTP, gRPC, and GraphQL APIs
* Production storage adapters for real deployment environments

Nextral does **not** manage LLM providers directly. Host applications and agents provide embeddings, extraction outputs, and model-specific behavior. Nextral focuses on memory runtime, persistence, retrieval, graph operations, and tool access.

## Architecture

```text
nextral/
├── src/                 # canonical Rust core and runtime modules
├── bindings/
│   ├── python/          # PyO3 bridge + Python wrappers
│   └── node/            # napi-rs bridge + TypeScript wrappers
├── apps/                # CLI, MCP, API, examples, and native consumers
├── tests/
├── docs/
├── scripts/
├── Cargo.toml
├── package.json
├── pyproject.toml
└── README.md
```

The Rust core in `src/` is the source of truth. Language bindings expose the same runtime behavior to Python and Node.js without duplicating business logic.

## Core principles

* **Package-first:** Nextral is designed to be installed and used as a package.
* **Runtime-neutral:** The same Rust core powers Python, Node.js, CLI, MCP, and service modes.
* **LLM-agnostic:** Model providers are configured externally. Nextral does not hardcode model names, endpoints, dimensions, or provider-specific assumptions.
* **Production-store ready:** PostgreSQL, Redis, Qdrant, Neo4j, and MinIO/S3 are used for production persistence.
* **Service-optional:** HTTP, gRPC, and GraphQL APIs are available when an application wants a network boundary, but they are not required.

## Repository layout

```text
src/
├── memory/              # memory records, taxonomy, and memory operations
├── retrieval/           # retrieval planning and execution
├── graph/               # graph nodes, edges, canonicalization, relationships
├── scoring/             # retrieval scoring logic
├── runtime/             # orchestration and runtime execution
├── contracts/           # shared contracts and schemas
└── lib.rs

bindings/
├── python/              # Python package and PyO3 bridge
└── node/                # Node.js package and napi-rs bridge

apps/
├── cli/                 # CLI application
├── mcp/                 # MCP tool server
├── web/                 # optional service host surfaces
└── examples/            # usage examples
```

## Build

### Rust workspace

```bash
cargo build --workspace
```

### Python package

```bash
pip install -e bindings/python
```

### Node package

```bash
npm --workspace bindings/node run build
```

## Quick smoke test

```bash
nextral memory smoke
```

## Configuration

Production configuration should start from:

```text
examples/config.production.example.json
```

Validate a production config with:

```bash
nextral config validate examples/config.production.example.json
```

The validator checks required stores, provider settings, retrieval policy, score bounds, cache TTLs, and production-vs-test backend compatibility.

## Production stores

A production Nextral deployment uses:

| Store      | Purpose                                                              |
| ---------- | -------------------------------------------------------------------- |
| PostgreSQL | Canonical memory index, sessions, reminders, audit, jobs, and outbox |
| Redis      | Hot session tail, cache, leases, and invalidation                    |
| Qdrant     | Vector storage and semantic retrieval                                |
| Neo4j      | Relational memory graph                                              |
| MinIO/S3   | Immutable transcript and source archive                              |

Nextral does not require n8n or ClickHouse.

## Model providers

Embedding, extraction, and reranking providers are configured externally.

Provider configuration must define:

* Provider kind
* Model name
* Dimensions
* Endpoint
* API key environment variable
* Any runtime-specific provider settings

Supported provider kinds:

```text
open_ai_compatible
http
external_callback
test
```

The `test` provider kind is intended for tests and demos only.

## Optional service modes

Nextral can run network APIs for applications that need a service boundary.

Available contracts:

```text
contracts/http/openapi.json
contracts/grpc/nextral.proto
contracts/graphql/schema.graphql
```

These service modes use the same configured runtime and store adapters as the package APIs. They are optional and should not become a separate required SaaS layer.

## CLI

Common commands:

```bash
nextral config validate examples/config.production.example.json
nextral memory smoke
```

Additional CLI commands are documented in:

```text
docs/cli/README.md
```

## MCP support

Nextral includes MCP support so agents and LLM applications can use memory operations through tool calls.

The MCP app is located at:

```text
apps/mcp/
```

## Documentation

Start here:

```text
docs/README.md
```

Important docs:

```text
docs/architecture/project-structure.md
docs/getting-started/installation.md
docs/getting-started/quickstart.md
docs/memory/README.md
docs/package-production.md
```

Memory architecture docs:

```text
docs/memory/architecture.md
docs/memory/types/README.md
docs/memory/pipeline/README.md
docs/memory/contracts/README.md
docs/memory/workflow/README.md
docs/memory/operations/README.md
```

## Local integration verification

Use the Docker-based end-to-end script to verify the production store stack locally:

```powershell
.\scripts\e2e-docker.ps1
```

This verifies the required production stores and package surfaces, including PostgreSQL, Redis, Qdrant, Neo4j, MinIO/S3, CLI, and Node package paths.

## Development status

Nextral currently includes:

* Canonical Rust domain models
* Seven memory types represented in core logic
* Retrieval scoring logic
* Runtime topology and operation planning
* Native graph utilities
* Production adapters for PostgreSQL, Redis, Qdrant, Neo4j, and S3-compatible storage
* Python CLI paths
* MCP/API execution paths
* Long-running service host mode
* Test runtime coverage

See the roadmap and pending task tracker for current implementation notes:

```text
docs/pending.md
```

## License

Add your license here.

```text
MIT OR Apache-2.0
```

## Project status

Nextral is under active development. APIs, contracts, and package surfaces may change before a stable release.
