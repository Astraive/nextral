# Changelog

## Unreleased

- Preserved local runtime, adapter, binding, and CI work and integrated upstream isolation fixes.
- Restored the local five-store stack with pinned reachable images, loopback-only ports, and a working MinIO readiness probe.
- Added persisted canonical-memory graph replay, explicit positive/`NOT_` contradictions, and authoritative graph hints.
- Exposed binding experiment/safety controls, restoring prior runtime state on reasoned active-experiment rollback.
- Added explicit private-CA JWKS trust and provenance-aware archive cleanup with same-key tombstones, immutable uploads and scrubbed-job fencing.

## 0.1.0

- Added production package runtime contracts for all seven memory systems.
- Structured the Rust core into domain, config, providers, ports, adapters, runtime, API, package, and testkit modules.
- Added package-visible E2E smoke flow, re-embed planning, config validation, and ingest request shape helpers.
- Added optional HTTP/gRPC/GraphQL startup planning and MCP tool-surface inspection.
- Added production adapter surfaces for PostgreSQL, Redis, Qdrant, Neo4j, and MinIO/S3.
- Added 157 tests covering domain, config, scoring, transport, API, MCP, store, intelligence, and evaluation.
- Added batch operations (ingest, retrieve, forget) with idempotency support.
- Added structured logging via tracing in all app binaries.
- Added feature flags for optional adapter compilation.
- Removed n8n and ClickHouse as required dependencies.

## 0.0.2

- Rebranded project namespace from `neuros` to `nextral`.
- Reorganized repository into a monorepo with a canonical Rust core in `src/`.
- Added runtime-neutral FFI scaffolding:
  - `bindings/python` (PyO3 + maturin layout)
  - `bindings/node` (napi-rs layout)
- Added native app surfaces under `apps/cli`, `apps/mcp`, and `apps/api`.
- Updated top-level and architecture documentation for the new structure.

## 0.0.1

- Initialized info-only publish surfaces for PyPI and npm packages.
- Initialized `neuros` Python package metadata and build configuration.
- Added CLI entry point with `--help`, `--version`, and placeholder command groups.
- Added architecture scaffolding for memory, tools, file ingestion, storage, and LangChain integration namespaces.
- Added docs-first project documentation and release notes.
- No runtime features implemented in this release.
