# Changelog

## 0.1.0

- Added production package runtime contracts for all seven memory systems.
- Structured the Rust core into domain, config, providers, ports, adapters, runtime, API, package, and testkit modules.
- Added package-visible E2E smoke flow, re-embed planning, config validation, and ingest request shape helpers.
- Added optional HTTP/gRPC/GraphQL startup planning and MCP tool-surface inspection.
- Added production adapter surfaces for PostgreSQL, Redis, Qdrant, Neo4j, and MinIO/S3.
- Removed n8n and ClickHouse as required dependencies.

## 0.0.1 (nextral info-only release)

- Initialized info-only publish surfaces for:
  - PyPI package `nextral`
  - npm package `nextral`
- Set package metadata/version to `0.0.1` for Python and Node release channels.
- Kept runtime implementation surfaces scaffolded while shipping metadata-first publish targets.

## 0.0.2

- Rebranded project namespace from `neuros` to `nextral`.
- Reorganized repository into a monorepo with a canonical Rust core in `src/`.
- Added runtime-neutral FFI scaffolding:
  - `bindings/python` (PyO3 + maturin layout)
  - `bindings/node` (napi-rs layout)
- Added native app surfaces under `apps/cli` and `apps/mcp`.
- Updated top-level and architecture documentation for the new structure.

## 0.0.1

- Initialized `neuros` Python package metadata and build configuration.
- Added CLI entry point with `--help`, `--version`, and placeholder command groups.
- Added architecture scaffolding for memory, tools, file ingestion, storage, and LangChain integration namespaces.
- Added docs-first project documentation and release notes.
- No runtime features implemented in this release.

