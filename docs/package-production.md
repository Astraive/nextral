# Packages and deployment

## Supported capabilities and packages

| Surface | Supported behavior | Boundary |
| --- | --- | --- |
| Rust core | Canonical memory, scoped retrieval, sessions, policies, graph, reminders, consolidation, batches, durable jobs/outbox, scoped reembedding, replay research | Requires explicit configured backend and host model outputs |
| Python `nextral` | Native Rust operations through dictionary requests; CLI; blocking service hosts with `stop_serving()` | CPython 3.10+, abi3; no PyPy or free-threaded promise |
| npm `nextral` | Object API in `api.js`, types in `api.d.ts`; native operations; async `serve` and `stopServing()` | Node 22+; most other calls are synchronous; no public generated-index or raw JSON API |
| Rust binaries | `nextral`, `nextral-mcp`, `nextral-api` | Built by Cargo; npm does not install these binaries |
| MCP | Newline-delimited JSON-RPC stdio with tool discovery and execution | Trusted local host interface, not a remote authenticated MCP endpoint |
| HTTP / gRPC / GraphQL | Real Axum / tonic-prost / async-graphql listeners | Contract-specific operations, not an unrestricted remote tool dispatcher |
| Production backend | PostgreSQL, Redis, Qdrant REST, authenticated Neo4j HTTP, SigV4 S3 | External stores are not installed by pip/npm |
| `test_memory` | Explicit process-local test backend and lexical test retrieval | No durability, provider execution, production isolation or performance evidence |

Release artifacts target Windows x64 MSVC, macOS x64/arm64, and glibc Linux x64/arm64. Node release baselines are glibc 2.35 x64 and 2.39 arm64. No Windows arm64, 32-bit, musl/Alpine, Android, FreeBSD, WASM, PyPy, or free-threaded Python artifact support is promised. See binding READMEs for native artifact collection/build commands. Registry installation assumes a published artifact matching the host; source builds require Rust and a linker. A local single-platform npm tarball is not the complete release matrix.

### Artifact verification

`scripts/verify-release.py` resolves Cargo's configured target directory, including `CARGO_TARGET_DIR` and machine-local Cargo configuration. Crate packaging uses `<target>/crate-package`, clean extracted Rust consumers use `<target>/crate-consumer`, and publication verification uses `<target>/crate-publish`. Package archives are under `<target>/crate-package/package`.

Keep these contexts separate from workspace/native builds: Cargo verification of an extracted same-version crate can otherwise leave native builds using the archive's core artifacts rather than current checkout sources. The integration runner builds its Python wheel under `<target>/python-native`. Pre-existing checkout-native binaries are not release evidence; verify clean installed wheels and npm tarballs outside the checkout.

## Configuration and stores

Start with `examples/config.production.example.json`. Replace deployment placeholders and set referenced environment variables. Rust resolves `${NAME}` configuration references; credential fields ending in `_env` name environment variables rather than containing secrets. Container DNS names in the example are not valid host addresses automatically.

Select `backend: "production_stores"` or explicitly opt into `"test_memory"`. Packages use programmatic configuration or `NEXTRAL_CONFIG`; configure once before work, since package runtime state is process-wide. Required production stores include `stores.qdrant_collection`, not only a Qdrant URL.

| Store | Responsibility |
| --- | --- |
| PostgreSQL | Canonical rows, full session transcript, summaries, policies, reminders, audit, durable jobs/leases, outbox, migration state |
| Redis | Bounded hot session tail, cache, invalidation and coordination |
| Qdrant | Host-supplied vectors and scoped similarity search |
| Neo4j | Scoped nodes/relationships and source evidence over authenticated HTTP |
| S3 / MinIO | Private transcript/source archives with acknowledged uploads |

Neo4j authentication uses `NEXTRAL_NEO4J_USER` (default `neo4j`) and required `NEXTRAL_NEO4J_PASSWORD`; do not embed credentials in endpoint URLs. The adapter uses transactional HTTP, not a Bolt client. `neo4j://`/`bolt://` endpoint forms are converted to HTTP, with port 7687 mapped to 7474 (TLS forms to HTTPS/7473); supply the actual reachable HTTP endpoint when using custom ports.

Validate configuration separately from connectivity and schema provisioning:

```sh
nextral config validate examples/config.production.example.json
nextral --config owner-config.json database migrate '{}'
```

The migration command above is the Rust CLI. Python uses `python -m nextral db migrate owner-config.json`. Run migration with a schema-owning role. Then use a restricted application role with tenant/user RLS and required table/sequence permissions but no schema ownership or DDL privileges. Do not make a superuser, table owner, or RLS-bypass role the application role. Provision store-side collections/indexes/buckets and network credentials for the deployment; validation alone does not do this or prove connectivity.

## Host-owned model inputs

Provider kinds `open_ai_compatible`, `http`, `external_callback`, and `test` describe configuration; Nextral does not invoke model endpoints. Supply ingestion `embedding`, retrieval `query_vector`, entities, typed `graph_hints`, consolidation `candidates`, and a fast-lane `summary` from the host. Production vectors must match the configured provider/model/dimension. Configuring a reranker is not evidence of an external reranking call. Test lexical matches are not embedding-provider coverage.

## Authentication and trust boundaries

Network APIs verify RS256/ES256 JWT signatures with HTTPS JWKS, issuer and audience, and bind request tenant/user/actor scope to authenticated claims (`sub` for user identity). An opaque service token from `auth.service_token_env` is privileged cross-scope automation: protect it as an administrative credential, not a normal tenant token. Put listeners behind appropriate TLS/network controls; bind addresses do not provide transport encryption.

For a private JWKS issuer CA, configure the name of an environment variable containing the PEM certificate (not a file path):

```json
{
  "auth": {
    "issuer": "https://identity.example",
    "audience": "nextral",
    "jwks_url": "https://identity.example/.well-known/jwks.json",
    "jwks_ca_pem_env": "NEXTRAL_JWKS_CA_PEM",
    "service_token_env": "NEXTRAL_SERVICE_TOKEN"
  }
}
```

Set `NEXTRAL_JWKS_CA_PEM` to the private CA's PEM text before starting network hosts; omit `jwks_ca_pem_env` for ordinary public trust roots. The explicit certificate adds a trust root without disabling HTTPS certificate/hostname verification. A missing, empty or invalid configured certificate fails startup; redirects are not followed.

Embedded Python/Node, Rust package calls, CLI and stdio MCP trust the invoking host to authorize scope. Supplying `tenant_id` is not authentication. Keep these interfaces inside the host trust boundary. Tenant/user PostgreSQL RLS is defense in depth, not a substitute for authenticating callers. Health liveness is not readiness: `/healthz` means alive; `/readyz` probes actual configured stores.

## Service hosting

```sh
nextral-api serve all runtime.json
python -m nextral serve all runtime.json
nextral-mcp --config runtime.json stdio
```

`plan` prints a startup plan; `serve` runs listeners until shutdown. HTTP, gRPC, and GraphQL expose the operations in `contracts/http/openapi.json`, `contracts/grpc/nextral.proto`, and `contracts/graphql/schema.graphql`. They are optional hosts over the same runtime. Python `serve` blocks with the GIL released; call `stop_serving()` from another thread. Node `serve` returns a pending Promise; `stopServing()` requests graceful shutdown.

## Telemetry and deployment evidence

Telemetry is opt-in through `observability.enabled`, `prometheus_bind`, and/or `otlp_endpoint`. The implementation exports real Prometheus metrics and OTLP/HTTP JSON metrics/spans; it does not require an exporter when disabled. Do not log raw memory bodies or attach tenant/user identifiers as metric labels. Protect telemetry endpoints as deployment infrastructure. See [measurement and replay](memory/operations/slos-alerting-and-capacity.md).

`scripts/e2e-docker.ps1` and `scripts/e2e-docker.sh` invoke the real-store integration harness; `scripts/telemetry-smoke.py` checks received wire telemetry using official OpenTelemetry protobuf classes. These are runnable evidence-producing tools, not an assertion that every deployment passes them. Establish your own readiness, recovery, load and SLO evidence before production use.
