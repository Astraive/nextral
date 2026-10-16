# Project structure

| Path | Responsibility |
| --- | --- |
| `src/domain/` | Memory, graph, session, policy, reminder and audit types |
| `src/runtime/` | Storage orchestration, sessions, retrieval, lifecycle jobs, governance, research and telemetry |
| `src/adapters/` | PostgreSQL, Redis, Qdrant, authenticated Neo4j and S3 transport |
| `src/config/` | Configuration types, environment resolution and validation |
| `src/package/` | Configured package dispatcher and native JSON boundary |
| `src/api/` | HTTP, gRPC and GraphQL implementation and authentication |
| `src/testkit/` | Explicit in-memory test store |
| `bindings/python/nextral/` | Python dictionary API, errors, types and installed CLI |
| `bindings/python/src/` | PyO3 native bridge |
| `bindings/node/api.js`, `api.d.ts` | Canonical Node object API and TypeScript declarations |
| `bindings/node/src/` | Private napi-rs transport; not a public generated-index API |
| `bindings/node/native/` | Collected platform `.node` outputs, not committed source |
| `apps/cli/`, `apps/mcp/`, `apps/api/` | Rust executable entry points |
| `contracts/` | HTTP OpenAPI, gRPC proto, GraphQL schema |
| `migrations/` | Store/schema provisioning resources |
| `examples/` | Production configuration, reembed request, measured replay host/runner |
| `fixtures/evaluation/` | Representative replay inputs and expectations |
| `scripts/` | Integration, telemetry and release packaging tools |
| `docs/` | Public usage and operations guides |

`apps/web/` and `apps/examples/` are not required runtime entry points. Use the implemented binaries and root `examples/` rather than assuming an application exists in those directories.

## Runtime boundary

Rust owns business logic and Serde contracts. Python and Node serialize objects and translate native errors, preserving host vectors and extraction results. Most package calls are synchronous; they are not uniformly asyncio/Promise APIs. Python `serve` blocks with the GIL released; Node `serve` runs on a native async worker and returns a Promise. Both have explicit shutdown controls.

The configured runtime is process-wide. Production calls use external persistence; test memory must be explicitly selected. See [deployment and capability boundaries](../package-production.md).
