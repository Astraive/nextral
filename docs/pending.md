# Project completion checklist

Canonical tracker: https://github.com/Astraive/nextral/issues/19

## Baseline

- [x] Preserve existing changes on `chore/project-baseline` and merge PR #18.
- [x] Run integrated workspace tests (177 passed), strict Clippy, and actual CLI testkit smoke.
- [x] Route local Cargo builds to `D:/target` using machine-local Cargo configuration.
- [x] Review architecture/contracts and discover relevant skills through Vercel Skills.

The baseline was not production completion: its dispatcher used an in-memory store and had protocol, durability, lifecycle and packaging gaps. Subsequent implementation remains subject to the acceptance checklist below; code presence alone does not close an item.

## Phase 1: Correctness and isolation

### 1.1 — fix(core): enforce tenant and privacy isolation ([#20](https://github.com/Astraive/nextral/issues/20))

- [ ] Reject cross-owner memory overwrites and scope graph traversals at every hop.
- [ ] Filter private/sensitive/restricted records before ranking across stores.
- [ ] Prove tenant and user collisions cannot read, mutate, or suppress another owner.

### 1.2 — fix(core): enforce canonical write invariants ([#21](https://github.com/Astraive/nextral/issues/21))

- [ ] Apply server/configured score floors without caller bypass.
- [ ] Validate finite scores, required identities, vectors and supported schema versions.
- [ ] Keep rejected writes out of durable/index/graph state with audit reasons.

### 1.3 — fix(contracts): unify time and vector metadata semantics ([#59](https://github.com/Astraive/nextral/issues/59))

- [ ] Accept documented RFC3339 timestamps and compare/score time consistently.
- [ ] Preserve canonical-to-vector point identity and required payload metadata.
- [ ] Verify schema compatibility and cross-surface round trips.

## Phase 2: Production runtime and durability

### 2.1 — feat(runtime): compose configured production stores ([#22](https://github.com/Astraive/nextral/issues/22))

- [ ] Construct one configured production runtime over required store adapters.
- [ ] Make testkit explicit rather than a silent production fallback.
- [ ] Prove data survives process restart and is shared by package surfaces.

### 2.2 — feat(storage): persist canonical memory operations ([#23](https://github.com/Astraive/nextral/issues/23))

- [ ] Implement tenant/user-scoped get/list/update/count/tags/entities against PostgreSQL.
- [ ] Preserve timestamps, metadata and ownership through round trips.
- [ ] Verify concurrent conflicting writes do not reassign ownership.

### 2.3 — feat(storage): implement durable session memory ([#24](https://github.com/Astraive/nextral/issues/24))

- [ ] Write complete session messages to PostgreSQL and a TTL-bounded Redis hot tail.
- [ ] Make idempotency scoped to tenant/user/session and durable across restart.
- [ ] Recover context from warm storage when hot cache is unavailable.

### 2.4 — feat(retrieval): accept host embeddings for vector search ([#25](https://github.com/Astraive/nextral/issues/25))

- [ ] Accept host-provided vectors with configured model/dimension metadata.
- [ ] Write and query actual Qdrant vectors without internal model-provider assumptions.
- [ ] Reject dimension/nonfinite mismatches before writes and prove semantic retrieval.

### 2.5 — feat(retrieval): run bounded hybrid retrieval in parallel ([#26](https://github.com/Astraive/nextral/issues/26))

- [ ] Execute vector and graph paths concurrently with bounded candidates/hops.
- [ ] Deduplicate, normalize canonical scoring and fit a hard context budget.
- [ ] Return correct degradation markers for one failed path and explicit error for both.

### 2.6 — feat(graph): persist scoped graph evidence ([#27](https://github.com/Astraive/nextral/issues/27))

- [ ] Use deterministic canonical merge keys and approved node/relationship types.
- [ ] Preserve multiple sources, confirmation times and contradiction evidence.
- [ ] Replay graphify safely and verify bounded tenant-isolated traversal.

### 2.7 — feat(storage): secure immutable source archives ([#28](https://github.com/Astraive/nextral/issues/28))

- [ ] Persist immutable source/transcript objects with computed checksums.
- [ ] Enforce tenant-scoped keys and authenticated S3-compatible transport.
- [ ] Record archive receipts and make redaction/tombstone behavior observable.

### 2.8 — feat(jobs): persist replayable multi-store work ([#29](https://github.com/Astraive/nextral/issues/29))

- [ ] Persist jobs and outbox work with claim/lease, bounded retry and terminal reason.
- [ ] Resume interrupted graphify/consolidation/forget work without duplicate effects.
- [ ] Expose backlog and failure state for recovery instead of silent success.

### 2.9 — fix(storage): enforce transport and credential policies ([#30](https://github.com/Astraive/nextral/issues/30))

- [ ] Honor configured TLS/auth/timeouts for every production adapter.
- [ ] Reject incompatible insecure production configurations.
- [ ] Do not leak credentials or raw sensitive content in diagnostics.

### 2.10 — feat(storage): commit canonical changes with outbox atomically ([#55](https://github.com/Astraive/nextral/issues/55))

- [ ] Commit memory mutations, audits and outbox intent in one transaction.
- [ ] Roll back related rows on audit/outbox failure.
- [ ] Recover external writes from committed durable intent.

### 2.11 — feat(security): enforce authenticated tenant boundaries ([#56](https://github.com/Astraive/nextral/issues/56))

- [ ] Bind authenticated requests to authorized tenant/user scope.
- [ ] Provide database RLS and least-privilege tenant transaction context.
- [ ] Reject cross-tenant scope forgery.

### 2.12 — feat(migrations): track schema versions and safe deployment ([#57](https://github.com/Astraive/nextral/issues/57))

- [ ] Apply ordered checksum-verified migrations under an advisory lock.
- [ ] Record history and reject conflicting or unknown state.
- [ ] Verify concurrent startup and data-preserving schema upgrades.

## Phase 3: Memory lifecycles and governance

### 3.1 — feat(memory): preserve ephemeral working context ([#31](https://github.com/Astraive/nextral/issues/31))

- [ ] Assemble bounded session/retrieval context without persisting scratch state.
- [ ] Budget every context layer consistently.
- [ ] Prove temporary working state is absent from durable memories.

### 3.2 — feat(memory): complete fast consolidation ([#32](https://github.com/Astraive/nextral/issues/32))

- [ ] Consume host extraction/summary outputs and full scoped transcript.
- [ ] Persist accepted candidates, summary and graphify work idempotently.
- [ ] Persist retry/terminal reasons and keep failed sessions eligible for recovery.

### 3.3 — feat(memory): complete deep consolidation and retention ([#33](https://github.com/Astraive/nextral/issues/33))

- [ ] Select aged sessions and deterministically chunk/version outputs.
- [ ] Persist embeddings, archive receipts and replay checkpoints.
- [ ] Apply configured retention without prematurely deleting unarchived source data.

### 3.4 — feat(memory): load scoped procedural policy ([#34](https://github.com/Astraive/nextral/issues/34))

- [ ] Persist per-user procedural preferences and load them before response context.
- [ ] Cache/invalidate policy consistently through Redis.
- [ ] Keep procedural controls distinct from ad-hoc semantic recall.

### 3.5 — feat(reminders): implement real idempotent dispatch ([#35](https://github.com/Astraive/nextral/issues/35))

- [ ] Replace title-based simulated outcomes with host callbacks or configured webhooks.
- [ ] Persist scoped dedupe/leases, attempt outcomes and bounded retry scheduling.
- [ ] Enforce source privacy/channel/quiet-hours policy and audit transitions.

### 3.6 — feat(governance): propagate forget across stores ([#36](https://github.com/Astraive/nextral/issues/36))

- [ ] Apply canonical tombstone plus vector removal, graph unlink, cache invalidation and archive manifest.
- [ ] Report per-store acknowledgement and incomplete/failed propagation truthfully.
- [ ] Resume failures and prove forgotten content is not returned by any surface.

### 3.7 — feat(migrations): execute reembedding and backfill ([#37](https://github.com/Astraive/nextral/issues/37))

- [ ] Execute host-vector shadow collection writes with durable checkpoints.
- [ ] Measure source/target counts and replay quality before read cutover.
- [ ] Keep previous collection and support explicit rollback.

### 3.8 — feat(runtime): schedule durable consolidation and retention work ([#58](https://github.com/Astraive/nextral/issues/58))

- [ ] Schedule idle/close and aged-session consolidation through durable jobs.
- [ ] Persist lane/version/checkpoints, retries and terminal reasons.
- [ ] Retain unarchived source data and expose recovery work.

## Phase 4: Protocol and CLI surfaces

### 4.1 — feat(http): conform to the OpenAPI contract ([#38](https://github.com/Astraive/nextral/issues/38))

- [ ] Serve documented health/readiness, ingest, retrieval, forget and reminder paths.
- [ ] Honor path identities and stable request/response schemas.
- [ ] Map invalid/not-found/conflict/unavailable errors to correct HTTP statuses.

### 4.2 — feat(grpc): serve generated memory and reminder RPCs ([#39](https://github.com/Astraive/nextral/issues/39))

- [ ] Implement real HTTP/2 gRPC for every declared MemoryService and ReminderService method.
- [ ] Use the configured shared runtime with correct field/error mapping.
- [ ] Exercise generated client calls including invalid/scoped requests.

### 4.3 — feat(graphql): execute the declared schema ([#40](https://github.com/Astraive/nextral/issues/40))

- [ ] Execute every declared query and mutation with typed argument validation.
- [ ] Resolve memory/retrieval/reminders against the shared runtime.
- [ ] Verify schema introspection, variables, selections and errors via a real client.

### 4.4 — feat(mcp): implement stdio protocol lifecycle ([#41](https://github.com/Astraive/nextral/issues/41))

- [ ] Handle JSON-RPC initialize, initialized notification, ping and shutdown/EOF.
- [ ] List invocable tools with input JSON Schemas and accurate annotations.
- [ ] Dispatch tools/call with IDs, structured results and protocol/tool errors.

### 4.5 — feat(api): manage service lifecycle and readiness ([#42](https://github.com/Astraive/nextral/issues/42))

- [ ] Separate liveness from actual configured backend readiness.
- [ ] Run selected protocol listeners with graceful shutdown and explicit startup failure.
- [ ] Enforce configured API auth/tenant boundary and bounded request handling.

### 4.6 — feat(cli): complete runtime command parity ([#43](https://github.com/Astraive/nextral/issues/43))

- [ ] Make memory ingest execute writes rather than print a schema.
- [ ] Expose the documented memory/session/graph/reminder/batch/jobs commands and generic dispatch.
- [ ] Return nonzero on invalid commands and keep stdout machine-readable.

## Phase 5: Python and Node packages

### 5.1 — feat(node): ship one typed JavaScript API ([#44](https://github.com/Astraive/nextral/issues/44))

- [ ] Ship a single documented object-based API with matching TypeScript declarations.
- [ ] Expose runtime configuration, memory/session/graph/reminder/batch/jobs operations and structured errors.
- [ ] Build from source and consume the packed package from an isolated project.

### 5.2 — feat(node): package native artifacts portably ([#45](https://github.com/Astraive/nextral/issues/45))

- [ ] Declare supported Node/OS/architecture matrix and ship corresponding native artifacts.
- [ ] Eliminate accidental source-only loader assumptions and unsafe shell probes.
- [ ] Install and exercise package artifacts on supported CI platforms.

### 5.3 — feat(python): expose complete native runtime API ([#46](https://github.com/Astraive/nextral/issues/46))

- [ ] Expose configured runtime operations through Python without duplicated business logic.
- [ ] Map native failures to structured Python errors.
- [ ] Install a built wheel into a clean environment and execute memory/session/reminder workflows.

### 5.4 — feat(python): align installed CLI with documented commands ([#47](https://github.com/Astraive/nextral/issues/47))

- [ ] Provide documented memory/session/graph/reminder/batch/jobs/config commands.
- [ ] Start actual service hosts when serving is requested.
- [ ] Verify installed console entrypoint exit codes and JSON outputs.

### 5.5 — feat(python): build supported wheels reproducibly ([#48](https://github.com/Astraive/nextral/issues/48))

- [ ] Declare supported Python/OS/architecture matrix and build matching wheels.
- [ ] Verify imports and operations outside the repository source path.
- [ ] Package license/types/docs metadata and avoid checked-in local native binaries.

## Phase 6: Research, operations and release

### 6.1 — feat(eval): measure golden retrieval and safety outcomes ([#49](https://github.com/Astraive/nextral/issues/49))

- [ ] Replace severity-derived fabricated metrics with replay measurements.
- [ ] Provide deterministic golden recall, contradiction, reminder and destructive-event cases.
- [ ] Gate experiment promotion on observed quality and latency evidence.

### 6.2 — feat(observability): expose operational runtime signals ([#50](https://github.com/Astraive/nextral/issues/50))

- [ ] Emit configured stage latency/candidate counts, job age/retry and forget completion signals.
- [ ] Keep observability opt-in and redact raw sensitive content.
- [ ] Document measured baselines and alert inputs without claiming unmeasured SLOs.

### 6.3 — fix(integration): restore reproducible production store verification ([#51](https://github.com/Astraive/nextral/issues/51))

- [ ] Use reachable pinned store images and provision migrations idempotently.
- [ ] Fix SQL/fixture errors and verify actual runtime writes rather than only direct fixture inserts.
- [ ] Exercise all seven memory types with Python, Node, CLI and network protocols.

### 6.4 — ci(workspace): verify package and protocol contracts ([#52](https://github.com/Astraive/nextral/issues/52))

- [ ] Run Rust formatting/lint/tests and installed Python/Node behavioral checks on supported platforms.
- [ ] Run production storage and protocol integration with reliable cleanup.
- [ ] Respect external Cargo target directories and reproducible lockfiles.

### 6.5 — ci(release): automate verifiable package artifacts ([#53](https://github.com/Astraive/nextral/issues/53))

- [ ] Build version-consistent Rust/Python/Node release artifacts with provenance/checksums.
- [ ] Configure registry publishing through supported authentication without committing secrets.
- [ ] Install release artifacts in clean environments and run consumer smoke workflows.

Registry authorization is tracked in [#75](https://github.com/Astraive/nextral/issues/75). GitHub environments `pypi`, `npm`, and `crates-io` now accept only `v*` tags. No repository or repository-available organization registry secrets were present, and the current crates.io job requires `CARGO_REGISTRY_TOKEN`. PyPI/npm account-owned trusted-publisher authorization is not observable through GitHub APIs. No release tag has been created.

### 6.6 — docs(project): reconcile contracts and completion evidence ([#54](https://github.com/Astraive/nextral/issues/54))

- [ ] Publish phased issue checklist and supported capability matrix.
- [ ] Replace stale complete/production-ready claims with verified evidence.
- [ ] Document migration, configuration, recovery, research evaluation and package usage accurately.

## Regression acceptance

- [x] [#60](https://github.com/Astraive/nextral/issues/60): fence stale scheduled-job writes after source forget; verify terminal cancellation and scrubbed request survive a second PostgreSQL connection.
- [x] [#61](https://github.com/Astraive/nextral/issues/61): retain archive source provenance across failed consolidation; recover legacy manifests and block late uploads after source-only forget.
- [x] [#62](https://github.com/Astraive/nextral/issues/62): give distinct accepted same-ID mutations distinct audit events; reject conflicting explicit event IDs and roll back canonical/outbox changes atomically.
- [ ] [#63](https://github.com/Astraive/nextral/issues/63): distinguish explicit negation from compatible graph relationships and measure the representative replay suite.
- [ ] [#64](https://github.com/Astraive/nextral/issues/64): honor the configured traversal maximum through native and production graph paths.
- [x] [#65](https://github.com/Astraive/nextral/issues/65): classify malformed tool payloads as invalid input consistently across native consumers.
- [x] [#67](https://github.com/Astraive/nextral/issues/67): replace yanked transitive cryptography releases and verify locked workspace/package behavior.
- [ ] [#68](https://github.com/Astraive/nextral/issues/68): apply requested replay-policy provenance and measure contradictions through canonical persisted graph replay.
- [x] [#69](https://github.com/Astraive/nextral/issues/69): supply and bundle portable manylinux TLS dependencies, then clean-install both architecture wheels.
- [x] [#70](https://github.com/Astraive/nextral/issues/70): correct redundant async-macro attributes and pass latest-stable strict Clippy without suppression.
- [x] [#71](https://github.com/Astraive/nextral/issues/71): preserve checked trace sequence allocation on supported Rust versions without deprecated atomic APIs.
- [x] [#72](https://github.com/Astraive/nextral/issues/72): normalize external artifact paths before strict upload validation.
- [x] [#73](https://github.com/Astraive/nextral/issues/73): build checksum-verified official MinIO and mc sources when former registry releases are unavailable.
- [x] [#74](https://github.com/Astraive/nextral/issues/74): isolate extracted-crate and native Cargo caches, then verify a current installed Python wheel.
- [ ] [#76](https://github.com/Astraive/nextral/issues/76): complete authenticated protocol smoke without incidental HTTP error-label pinning.

## Current verification boundary

The latest local run passed strict all-feature workspace Clippy and 278 Rust tests (six real-store tests ignored). The rebuilt CLI `memory smoke` passed against explicit `test_memory`. A fresh isolated Python wheel passed its installed consumer suite, including the formerly failing configuration error classification; the current npm tarball passed its clean consumer suite. Node declarations passed.

| Local installed-artifact check | Result | Boundary |
| --- | --- | --- |
| Python consumer suite | 13 passed, 1 skipped | Production-only case skipped |
| Node consumer suite | 9 passed, 2 skipped | Production vectors and measured controls skipped |
| Python HTTP/Prometheus/OTLP smoke | Passed, including graceful shutdown | Explicit `test_memory`; not production-store evidence |

Workflow lint, daemon-free Compose configuration validation, script syntax and configured Cargo-context resolution passed. These do not prove source-built fixtures or production runtime behavior.

Isolated local `crate-package` and `crate-smoke` runs passed after the cache repair: both publishable crates verified, extracted workspace targets/examples/API compiled, and the clean Rust consumer ingested and retrieved its accepted memory. Archives and consumer builds remained in their separate configured D-drive target contexts.

The local Docker daemon remains unresponsive; verification uses isolated GitHub runners instead. The third [CI run](https://github.com/Astraive/nextral/actions/runs/38808491198), at `765e18e`, completed all non-production jobs and reached real-store authenticated protocol checks:

| Isolated gate | Result |
| --- | --- |
| Non-production jobs, including latest-stable Rust and release bundle checks | 46 passed |
| Real-store Rust regressions | 6 passed |
| Installed Python suite with production scenario | 14 passed, none skipped |
| Installed Node suite with production vectors and measured controls | 11 passed, none skipped |

All five stores became ready, including source-built MinIO. The remaining failure was an auth-smoke assertion demanding the incidental HTTP error label `forbidden` after correctly receiving HTTP 403. Production telemetry follows auth and remains unexecuted. Graph smoke precedes auth; its completion follows from harness reachability [INFERENCE], but its captured JSON was not printed after the later failure. The integrated run is not yet green.

Draft [PR #66](https://github.com/Astraive/nextral/pull/66) carries the integrated implementation. Its first isolated [CI run](https://github.com/Astraive/nextral/actions/runs/38799231532) exposed Rust 1.99 async-macro Clippy errors and missing manylinux OpenSSL build dependencies. These are tracked repairs, not waived checks.

The second isolated [CI run](https://github.com/Astraive/nextral/actions/runs/38802757818), at `a2e26c5`, produced these completed gates:

| Gate | Result |
| --- | --- |
| Native package build jobs | 5 passed |
| Installed Python wheel jobs | 25 passed |
| Installed npm consumer jobs | 10 passed |

The run still failed: latest-stable Rust deprecated the atomic update API, artifact uploads rejected parent-directory path components, and the MinIO image was unauthorized. Crate packaging and its clean Rust consumer ran successfully before the upload failure. Production and measured research checks did not run successfully. A subsequent local Python configuration regression exposed shared Cargo-cache contamination; source-package verification must not replace native workspace artifacts.

## Integration contracts

- Rust owns business logic; bindings and protocols call the same configured runtime.
- Host applications supply embeddings and extraction/model outputs; Nextral does not choose or invoke LLM providers.
- Test memory is explicit; production configuration must never silently select it.
- Failed or partial writes, dispatch, deletion, migration and jobs must not report fabricated completion.
- Every issue closes only after its observable acceptance criteria have current evidence.
- Commit subjects use `type(scope): message`; no co-author trailers or empty quota commits.

## Research and engineering references

- [Canonical memory architecture](memory/architecture.md) and its linked type, pipeline, contract, workflow and operations documents.
- [Package production requirements](package-production.md).
- [Apollo Rust best-practices skill](https://skills.sh/apollographql/skills/rust-best-practices), discovered with `npx skills find rust --owner apollographql`; reuse the installed local skill.
- [Vercel Skills directory](https://skills.sh/). Low-install binding search results were not installed without stronger evidence.
- [MCP specification](https://modelcontextprotocol.io/specification/) for actual protocol implementation.
