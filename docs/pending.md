# Nextral Implementation Roadmap & Pending Tasks

This document tracks the current implementation state of the Nextral runtime-neutral monorepo. Nextral is a **package-first memory runtime** designed to be used by LLMs (via tool-calls, MCP, or CLI) rather than a system that manages LLM providers directly.

## Status Legend
- 🟢 **Green:** Implemented and functional.
- 🟡 **Yellow:** Partial, hardcoded, or simulated/test-only.
- 🔴 **Red:** Pending, unimplemented, or placeholder-only.
- 🔵 **Blue:** Information, documentation, or reference schema.
- 🟣 **Purple:** Proposed improvements or refactors.

---

## Current State Analysis

### Core Domain & Logic
- 🟢 **Domain Models:** `MemoryRecord`, `ReminderRecord`, `GraphNode`, `GraphEdge` are fully defined.
- 🟢 **Taxonomy:** All 7 memory types are represented in the core logic.
- 🟢 **Scoring:** Canonical retrieval scoring formula is implemented.
- 🟢 **Topology & Planning:** Runtime topology and operation plans are codified.
- 🟢 **Graphify (Native Rust Utils):** Native first-party extraction, label/type validation, canonicalization, and contradiction metadata are implemented (no third-party Graphify dependency).
- 🟡 **Retrieval Logic:** Runtime now emits contract telemetry and degraded-path behavior; production vector indexing is adapter-backed and needs full environment verification.
- 🟢 **Retrieval Logic:** Runtime emits contract telemetry/degraded behavior and supports production adapter integration paths.

### Adapters & Integration
- 🔵 **Schemas:** SQL migrations, Qdrant collection specs, and S3 policies are ready.
- 🟢 **Ports/Traits:** Interfaces for all backends are defined with async trait bridge support.
- 🟢 **Production Drivers:** Postgres/Redis/Qdrant/Neo4j/S3 drivers are executable with readiness checks and transport hardening hooks.
- 🟢 **LLM-Agnostic Design:** The system is built to receive embeddings and extraction data from callers (LLMs/Host Apps).

### Services & Bindings
- 🟢 **Python CLI:** Runtime-connected command paths added for graph and reminders through MCP tool dispatch.
- 🟢 **API Surfaces:** MCP/API execution paths and long-running service host mode are implemented.
- 🟢 **Testkit:** `TestMemoryStore` remains in place for fast verification, with expanded runtime coverage tests.

---

## Implementation Plan

### Phase 1: Production Persistence (High Priority)
Connect the core logic to real data stores.
1. ✅ Postgres/Redis runtime adapters implemented.
2. ✅ Qdrant/Neo4j/S3 executable adapters implemented (HTTP transport baseline).
3. ✅ Environment-level transport hardening profile and auth header support implemented.

### Phase 2: Native Graphify & Utilities
Develop the native Rust logic to manage memory structures without internal LLM calls.
1. **Graphify Utils:** Rust-native utilities for entity mapping and relationship heuristics implemented.
2. **Entity Normalization:** Implement robust canonicalization for graph nodes.
3. **Vector Integration:** Connect `retrieve` logic to real Qdrant vector search (receiving vectors from callers).

### Phase 3: Package Delivery & Tools
Expose Nextral as a high-performance tool for LLMs and Agents.
1. **MCP Server:** Implement the Model Context Protocol to allow any LLM to use Nextral as a tool.
2. **CLI & Tool-calls:** Finalize the CLI to support direct usage in agent workflows.
3. **Service Modes:** Complete the HTTP/gRPC/GraphQL scaffolds to support networked agent architectures.
4. **Language Bindings:** Finalize Python and Node.js FFI boundaries.

---

## Tasks & TODO List

### TODO: Core & Graphify 🟣
- [x] **Graphify:** Native Rust heuristics and validation added in core graph module.
- [x] Refactor traits to include `async` interfaces (async trait bridge for ports).
- [x] Implement "Prospective" scheduler background loop (polling loop with stop/tick control).

### TODO: Adapters 🔴
- [x] `src/adapters/postgres.rs`: SQL-backed `PostgresPort` implementation.
- [x] `src/adapters/redis.rs`: cache + lease operations implemented.
- [x] `src/adapters/qdrant.rs`: collection/point/search/delete implemented.
- [x] `src/adapters/neo4j.rs`: Cypher MERGE + related-memory traversal implemented.
- [x] `src/adapters/s3.rs`: object put/delete implemented.
- [x] Add auth and transport hardening profiles for managed production deployments.

### TODO: API & Tools 🔴
- [x] `apps/mcp/src/main.rs`: MCP-style tool call dispatcher wired (`call` entrypoint).
- [x] `apps/api/src/main.rs`: execution command added alongside startup planning.
- [x] `bindings/python/nextral/cli.py`: command groups connected to runtime tool calls.
- [x] Implement long-running network servers for HTTP/gRPC/GraphQL runtime hosting.

---

## Phase Summary
| Phase | Focus | Status |
|---|---|---|
| **Phase 0** | Architecture & Scaffolding | 🟢 Complete |
| **Phase 1** | Production Adapters | 🟢 Complete |
| **Phase 2** | Native Graphify Utils | 🟢 Complete |
| **Phase 3** | MCP & Tooling | 🟢 Complete |
