# Nextral for Node.js

The `nextral` package exposes one object-based JavaScript API, with matching TypeScript declarations in `api.d.ts`. Business logic, persistence, lifecycle rules, scoring, and validation execute in Rust. The native JSON transport is private: there is no public `mcpCall`, JSON-string variant, or alternative TypeScript runtime implementation.

## Install and platform support

```sh
npm install nextral
```

Node.js 22 or later is required. Release artifacts target Windows x64 (MSVC), macOS x64/arm64, and glibc Linux x64/arm64. The release build baselines are glibc 2.35 on x64 and 2.39 on arm64. Windows arm64, 32-bit platforms, Alpine/musl, Android, FreeBSD, and WASM are not supported.

The npm release contains all five platform binaries in `native/`; installation does not download code, run a shell, search `PATH`, or compile Rust. It also works with `npm install --ignore-scripts`. The loader selects a package-local artifact using Node's platform, architecture, and runtime report. Missing or incompatible artifacts fail explicitly; there is no JavaScript memory-store fallback.

Both module systems use the same implementation:

```js
const { configureRuntime, ingestMemory, retrieve } = require('nextral');
// Or: import { configureRuntime, ingestMemory, retrieve } from 'nextral';
```

## Configure explicitly

Configuration and memory state are process-wide, not per imported module. Configure once before operations. Without an explicit configuration, Rust reads the path in `NEXTRAL_CONFIG`; it never silently chooses the test store. The following configuration deliberately opts into the in-memory **test** backend:

```js
const policy = { min_importance_score: 0, min_confidence_score: 0 };
configureRuntime({
  backend: 'test_memory',
  embedding: { kind: 'test', model: 'example', dimension: 3 },
  extraction: { kind: 'test', model: 'example' },
  ingestion_policy: policy,
  retrieval_policy: {
    privacy_scope: ['private'], token_budget: 1800, top_k_vector: 12,
    max_graph_hops: 2,
    scoring_weights: { semantic_similarity: 0.5, recency: 0.2, importance: 0.2, access: 0.1 },
  },
  cache: { key_prefix: 'example', session_ttl_seconds: 7200, retrieval_ttl_seconds: 120, policy_ttl_seconds: 300 },
  service: {}, auth: {}, observability: { enabled: false },
});

const scope = { tenant_id: 'example', user_id: 'alice' };
const accepted = ingestMemory({
  ...scope, id: 'atlas-database', content: 'Atlas uses PostgreSQL',
  content_type: 'decision', memory_type: 'semantic', source_type: 'manual',
  source_message_ids: [], importance_score: 1, confidence_score: 1,
  entities: ['Atlas', 'PostgreSQL'], tags: ['architecture'], privacy_level: 'private',
  graph_hints: [], policy,
});
if (accepted.status !== 'accepted') throw new Error(accepted.validation_errors.join('; '));
const response = retrieve({
  ...scope, query_text: 'Atlas database', entities: ['Atlas'],
  token_budget: 1800, privacy_scope: ['private'], top_k_vector: 12, max_graph_hops: 2,
});
console.log(response.items);
```

For production use `backend: 'production_stores'`, configure `stores` with PostgreSQL, Redis, Qdrant (including `qdrant_collection`), Neo4j, and S3 connection information, and describe embedding/extraction inputs. `RuntimeConfig` describes the complete object. Provider configuration does not invoke model endpoints. Credential fields ending in `_env` name environment variables; the host supplies other configured connection values. No datastore or model provider is installed by this npm package.

Run `migrateDatabase()` explicitly under a schema-owning configuration before starting applications, then use a restricted tenant/user-RLS runtime role without DDL rights. Embedded Node calls trust the host to authorize scope; `tenant_id` alone is not authentication. See the [package and security boundaries](../../docs/package-production.md).

## Host-owned model inputs

Supply model output from your own host rather than an SDK-specific callback:

- `ingestMemory(request)` accepts `embedding: number[]`, entities, tags, scores, and typed `graph_hints`.
- `retrieve(request)`, `queryGraph(request)`, and `workingContext(request)` accept `query_vector: number[]`.
- `consolidate(request)` accepts `summary` and `candidates: IngestMemoryRequest[]`, including each candidate's vector and provenance.

Objects are JSON-encoded without renaming, stripping, normalizing, or replacing host vector/extraction fields. Use ordinary numeric arrays, not typed arrays or pre-encoded JSON. Non-finite numbers are rejected before JSON could silently convert them to `null`; production dimension and provider validation remain in Rust. Test-memory lexical retrieval is not evidence that a production embedding provider was exercised.

For fast consolidation, supply a nonempty host summary and explicit candidates referencing source message IDs. Deep consolidation requires one candidate per deterministic transcript chunk: each chunk joins `role: content` lines with `\n`; source IDs must identify those same contiguous messages. `chunk_messages` defaults to 100. Give changed input a new `version`; replaying a version uses Rust's durable job/provenance rules.

## Operations

All operations take typed objects; field names follow the Rust wire contract (`tenant_id`, `query_vector`, and so on). Optional values may be omitted or set to `null` where declared. Most functions are synchronous and can block on storage IO; use a worker thread for latency-sensitive Node servers. `serve` alone runs on a native async worker.

| Area | Public functions |
| --- | --- |
| Configuration and health | `configureRuntime`, `validateConfig`, `health`, `lexicalScore`, `migrateDatabase` |
| Memory | `ingestMemory`, `retrieve`, `getMemory`, `updateMemory`, `listMemories`, `memoryStats`, `searchByTag`, `searchByEntity`, `forgetMemory` |
| Sessions | `appendSessionMessage`, `workingContext` |
| Procedural policies | `upsertPolicy`, `listPolicies`, `removePolicy` |
| Graph | `queryGraph`, `graphify` |
| Consolidation | `consolidate` |
| Reminders | `scheduleReminder`, `executeDueReminders`, `listReminders` |
| Batches | `batchIngest`, `batchRetrieve`, `batchForget` |
| Reembedding | `planReembed`, `executeReembed` |
| Durable jobs and outbox | `enqueueJob`, `runJobs`, `listJobs`, `getJob`, `runOutbox` |
| Research/control | `createExperiment`, `promoteExperiment`, `rollbackExperiment`, `experimentStatus`, `getSafetyPolicy`, `setSafetyPolicy`, `scoredSearch` |
| Serving | `serve`, `stopServing` |

`workingContext` returns stored session tail, summary, procedural policies, retrieved memories, and an inclusive token estimate. Reminder scheduling links to an owned source memory. Set `NEXTRAL_REMINDER_WEBHOOK_URL` for real delivery and optionally `NEXTRAL_REMINDER_WEBHOOK_TOKEN` for authentication; a title does not simulate success or failure. Privacy scope, priority, quiet hours in the named IANA timezone, expiry, deduplication, and retries are enforced by Rust. Lifecycle timestamps accept epoch-second strings or RFC3339; persisted scheduling timestamps use epoch seconds.

Durable jobs accept `trigger: 'idle' | 'close' | 'deep' | 'retention'`. Consolidation jobs carry explicit host outputs in `consolidation`; retention jobs carry explicit `memory_ids` and `redact`. `enqueueJob` persists the request; `runJobs` claims due work with leases and records retries/completion. `getJob` and `listJobs` expose persisted results. `executeReembed` takes a `plan`, `user_id`, `target_dimension`, and per-memory host `vectors`; `cutover` and `rollback` are explicit flags. `runOutbox` retries acknowledged external-store work. Inspect reports and receipts rather than treating a returned object as a fabricated success.

Reembedding persists partial progress (`awaiting_vectors`) until target validation can finish; explicit cutover/rollback changes only the tenant/user vector route, not a global Qdrant alias. Hosts must supply query vectors compatible with that effective route.

`graphify({ tenant_id, user_id, memory_id, hints })` loads an active canonical memory; a full `record` is rejected. It persists graph intent/audit/outbox and returns flattened graph output with `status: 'accepted'` and `write_receipts`, without rewriting canonical content. Inspect receipts and pending outbox work rather than assuming Neo4j completion. Nonempty hints are authoritative for relationships; otherwise deterministic heuristics apply. A matching positive/`NOT_` relation on the same directed scoped endpoints is a contradiction; `USES` plus `OWNS` is compatible. See the [graph replay contract](../../docs/memory/contracts/graphify-contract.md). Forget scrubs exact owned source references rather than arbitrary substrings, so unrelated manual policies remain.

`promoteExperiment({ experiment_id, evidence })` requires comparable measured baseline/candidate replay evidence for the active/candidate policies. `rollbackExperiment({ experiment_id, reason })` requires a nonblank reason and the currently active promoted experiment with a saved rollback point; it restores the previous lane, policy version and active experiment identity, not a fixed stable default. Created, superseded or already rolled-back candidates cannot be rolled back. `experimentStatus()` returns registry state; passing `{ experiment_id }` returns that candidate or `null`. `getSafetyPolicy()` reads the action map; `setSafetyPolicy({ severity, action })` updates it. `scoredSearch` accepts the retrieval request and returns the scored retrieval response.

```js
const { forgetMemory, NextralError } = require('nextral');
try {
  const result = forgetMemory({
    tenant_id: 'example', user_id: 'alice', memory_id: 'atlas-database',
    actor: 'alice', reason: 'requested deletion', redact: true,
  });
  // Check completion and individual backend receipts, not merely absence of an exception.
  console.log(result.status, result.receipts);
} catch (error) {
  if (error instanceof NextralError) console.error(error.code, error.message);
  else throw error;
}
```

`NextralError` preserves Rust's `code` and `message` and retains the native error in `cause`. Policy rejection is a normal ingestion response with `status: 'rejected'`, not necessarily an exception. Batch responses report `total`, `succeeded`, `failed`, `errors`, and successful `results`; they are not transactional all-or-nothing promises.

Batch-read idempotency keys fence request fingerprints without persisting/replaying raw retrieval bodies. Repeated reads observe current canonical content, privacy and deletion state after mutations.

## Serve the same runtime

```js
const { serve } = require('nextral');
// config.service supplies the desired bind addresses.
await serve(config, ['http', 'grpc', 'graphql']);
```

Modes are `http`, `grpc`, `graphql`, or `all`. The promise remains pending while listeners run and resolves after native shutdown with `{ status: 'stopped', binds: [...] }`. Startup errors reject. Call `stopServing()` from another callback to request graceful shutdown of active hosts in this process, including on Windows. Native Ctrl-C handling (and SIGTERM on Unix) also controls shutdown; the JSON bridge is not a JavaScript server simulation.

Network hosts verify JWT signatures/issuer/audience and bind tenant/user/actor claims, or accept a privileged cross-scope service token. Protect that token and listener transport; it is not an ordinary user credential. `/healthz` is liveness, whereas `/readyz` probes actual stores.

## Build, collect, and release

From `bindings/node` in the complete Rust checkout, with the Rust toolchain and platform linker installed:

```sh
npm ci
npm run build -- --target x86_64-pc-windows-msvc
npm pack
```

Use the corresponding target for each platform:

| Rust target | Native artifact |
| --- | --- |
| `x86_64-pc-windows-msvc` | `native/nextral.win32-x64-msvc.node` |
| `x86_64-apple-darwin` | `native/nextral.darwin-x64.node` |
| `aarch64-apple-darwin` | `native/nextral.darwin-arm64.node` |
| `x86_64-unknown-linux-gnu` | `native/nextral.linux-x64-gnu.node` |
| `aarch64-unknown-linux-gnu` | `native/nextral.linux-arm64-gnu.node` |

`npm run build:debug -- --target TARGET` omits release optimization. Both commands use the pinned napi CLI and Cargo's locked dependency graph, write binaries only under `native/`, disable napi's generated JavaScript loader, and write private generated declarations under `native/`. Neither overwrites `api.js` or `api.d.ts`. Generated binaries are not committed.

A release job builds all five targets, uploads only the named `.node` artifacts, then starts with an empty `native/` directory in the assembly job:

```sh
npm ci
npm run artifacts:collect -- PATH_TO_DOWNLOADED_ARTIFACTS
npm run release:check
npm pack
npm publish nextral-0.1.0.tgz --access public
```

The collector rejects duplicate artifact names. `release:check` requires the complete supported matrix; normal `npm pack` requires the current host artifact so a source-only or wrong-platform package cannot accidentally pass its prepack hook. Package version changes must update both Cargo/npm versions and lockfiles before a tagged release. An isolated installation of a locally built single-platform tarball supports that platform only; published releases must pass the all-platform guard.

## Consumer checks

The test files exercise the real native runtime, not mocked exports. Run these after building:

```sh
npm test
npm run test:types
```

To exercise the packed/install layout, install the tarball into a new directory with `npm install PATH_TO_TARBALL --ignore-scripts`, then set `NEXTRAL_PACKAGE` to its absolute `node_modules/nextral` directory and run `node --test PATH_TO_CHECKOUT/bindings/node/test/consumer.test.cjs`.

The production-vector scenario requires `NEXTRAL_NODE_PRODUCTION_CONFIG` pointing to a real production configuration, live external stores, and host-vector provider settings. Without that explicit environment variable it is skipped, not counted as production coverage. It checks vector-driven ranking against conflicting lexical content and invalid dimensions, then forgets its records. The always-on scenarios cover native ingest/retrieve/update/redaction, tenant isolation, structured errors, policy rejection, session/batch state, consolidation provenance/summary erasure, durable retention-job deletion, a real HTTP listener with graceful shutdown, and CommonJS/ESM parity. These are commands to run, not a claim that any checks have already passed.
