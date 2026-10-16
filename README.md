# Nextral

Nextral is a package-first memory runtime: a canonical Rust core with Python and Node.js bindings, a Rust CLI, MCP stdio, and optional HTTP, gRPC, and GraphQL hosts. It persists and retrieves host-supplied memory; it does not call LLM or embedding providers.

## Start here

- [Installation](docs/getting-started/installation.md) and [quickstart](docs/getting-started/quickstart.md)
- [Supported capabilities and packages](docs/package-production.md#supported-capabilities-and-packages)
- [Configuration, security, and deployment](docs/package-production.md)
- [CLI and MCP examples](docs/cli/README.md)
- [Python API](bindings/python/README.md) and [Node.js API](bindings/node/README.md)
- [Memory architecture](docs/memory/architecture.md) and [documentation index](docs/README.md)

## Runtime boundaries

Choose a backend explicitly: `production_stores` for PostgreSQL, Redis, Qdrant, Neo4j, and private S3-compatible storage, or `test_memory` for isolated, process-local tests. Configure the package programmatically or supply `NEXTRAL_CONFIG`; there is no implicit in-memory application backend.

The host supplies embeddings, query vectors, extracted entities/graph hints, consolidation candidates, and summaries. Provider configuration describes those inputs; it is not an SDK that invokes a model. PostgreSQL is authoritative for tenant/user-scoped canonical state. External indexes and archives have acknowledged receipts and durable recovery work, not a cross-store transaction.

Run schema migration with an owner role before starting applications. The runtime database role needs no DDL rights. Embedded packages, CLI, and stdio MCP are trusted host interfaces; network APIs verify JWT claims or accept a privileged service token. See the [trust boundaries](docs/package-production.md#authentication-and-trust-boundaries).

## Build from checkout

```sh
cargo build --workspace
python -m pip install -e bindings/python
npm ci
npm --workspace bindings/node run build
```

These require Rust and the platform linker. Package release artifacts have a narrower supported platform matrix than arbitrary Rust targets; see the package guide. Registry installation commands assume an appropriate artifact is available, not that this checkout has been published.

Version-tag pushes verify the full release matrix and upload attested artifacts without publishing. Registry/GitHub publication requires an explicit maintainer dispatch: `gh workflow run release.yml --ref vVERSION -F publish=true`. See [release verification and publication](docs/package-production.md#release-verification-and-publication) for prerequisites.

## Usage

The Rust CLI executable is built as `nextral` (under Cargo's configured target directory):

```sh
nextral config validate examples/config.production.example.json
nextral schema nextral.memory.ingest
nextral --config runtime.json memory ingest request.json
nextral --config runtime.json memory list '{"tenant_id":"example","user_id":"alice"}'
nextral memory smoke
```

`runtime.json` and `request.json` above are host-created deployment/request files. Ingestion executes a write; schema inspection is a separate command. The smoke command deliberately uses the isolated testkit and does not verify production stores. Python installs a different `nextral` command; use `python -m nextral` to disambiguate it and consult the CLI guide for syntax differences.

## Deployment and evidence

```powershell
.\scripts\e2e-docker.ps1
```

The integration harness provisions the five-store stack and exercises package and service paths. This is an instruction to run it, not a blanket claim of production readiness. Deployment SLOs are [targets requiring local measurement](docs/memory/operations/slos-alerting-and-capacity.md). Replay research records actual host outcomes and process-roundtrip latency, not synthetic severity scores or a production latency guarantee. Optional telemetry emits real Prometheus and OTLP/HTTP data only when enabled.

APIs may change before a stable release. [Historical release notes](docs/releases/0.0.1.md) describe the old scaffold, not the current runtime.
