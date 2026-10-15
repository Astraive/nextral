# Nextral for Python

The `nextral` package exposes the canonical Rust runtime through dictionary requests and native errors. Python does not implement a second memory store, call a model provider, or replace host vectors/extraction outputs.

## Install and platforms

```sh
python -m pip install nextral
```

Registry installation requires an available matching release artifact. Wheels target CPython 3.10+ using PyO3 abi3 on Windows x64 MSVC, macOS x64/arm64, and glibc Linux x64/arm64. PyPy, free-threaded Python, musl/Alpine and other targets are not promised. Source/editable builds require Rust, maturin build dependencies and a platform linker:

```sh
python -m pip install -e bindings/python
```

Run that command from the complete repository checkout. The package does not install PostgreSQL, Redis, Qdrant, Neo4j, S3/MinIO or an embedding service. See the [capability/package matrix](../../docs/package-production.md#supported-capabilities-and-packages).

## Configure and execute

Configuration is process-wide. Call `configure_runtime` before operations or provide `NEXTRAL_CONFIG`; no implicit in-memory application backend exists. Choose `production_stores` with all required store configuration (including `qdrant_collection`), or explicitly select `test_memory` only for isolated process-local tests.

The example reads host-created configuration and real host model output files:

```python
import json
from pathlib import Path
import nextral

config = json.loads(Path("runtime.json").read_text(encoding="utf-8"))
nextral.configure_runtime(config)

# Host-prepared request: content, scope, scores, policy, metadata and embedding.
request = json.loads(Path("ingest.json").read_text(encoding="utf-8"))
accepted = nextral.ingest(request)
if accepted["status"] != "accepted":
    raise ValueError(accepted["validation_errors"])

# Host-prepared query includes a matching query_vector and explicit privacy scope.
query = json.loads(Path("query.json").read_text(encoding="utf-8"))
response = nextral.retrieve(query)
print(response["status"], response["items"])
```

Use [ingestion](../../docs/memory/contracts/memory-record-schema.md) and [retrieval](../../docs/memory/contracts/retrieval-contract.md) payload guides for those files. `ingest_request_schema()` returns a template; `ingest(request)` executes ingestion. Provider configuration describes host model inputs rather than invoking endpoints. Ordinary numeric lists are JSON-encoded with non-finite values rejected; supply actual `embedding`, `query_vector`, entities, graph hints, consolidation candidates and summary.

Apply schema migration explicitly using an owner configuration (`migrate_database()` after configuring that role, or `python -m nextral db migrate owner-config.json`), then start runtime applications with a restricted no-DDL RLS role. Do not migrate automatically on every request or use the schema owner as the application role.

## Public operations

| Area | Functions |
| --- | --- |
| Configuration/inspection | `configure_runtime`, `validate_config`, `health`, `lexical_score`, `ingest_request_schema`, `e2e_smoke`, `adapter_smoke`, `migrate_database` |
| Memory | `ingest`, `retrieve`, `get_memory`, `update_memory`, `list_memories`, `memory_stats`, `search_by_tag`, `search_by_entity`, `forget` |
| Sessions | `append_session`, `session_context` |
| Policies | `upsert_policy`, `list_policies`, `remove_policy` |
| Graph | `graph_query`, `graphify` |
| Consolidation | `consolidate` |
| Reminders | `schedule_reminder`, `due_reminders`, `list_reminders` |
| Batches | `batch_ingest`, `batch_retrieve`, `batch_forget` |
| Durable jobs/outbox | `enqueue_job`, `run_jobs`, `list_jobs`, `get_job`, `run_outbox` |
| Reembedding | `reembed_plan`, `execute_reembed` |
| Research/control | `scored_search`, `create_experiment`, `promote_experiment`, `rollback_experiment`, `experiment_status`, `safety_policy`, `set_safety_policy` |
| Hosts/dispatch | `serve`, `stop_serving`, `mcp_call` |

Most functions synchronously call Rust and may block on store IO. They are not asyncio coroutines; use a host worker thread/process as appropriate. `mcp_call(tool, payload)` is a trusted native tool interface, not a remote authenticated MCP client.

`graphify({"tenant_id": ..., "user_id": ..., "memory_id": ..., "hints": [...]})` fetches an active scoped canonical memory and rejects a full `record`. It persists graph intent/audit/outbox without rewriting content and returns flattened graph output plus `status: "accepted"` and `write_receipts`; receipts and pending recovery determine external completion. Nonempty supplied hints are authoritative for relationships; otherwise deterministic heuristics apply. Matching positive/`NOT_` relations on identical directed scoped endpoints are contradictions; `USES` and `OWNS` are compatible. See the [graph replay contract](../../docs/memory/contracts/graphify-contract.md).

Fast consolidation requires a nonempty host summary and candidates. Deep candidates must match deterministic `role: content` transcript chunks and source IDs; default `chunk_messages` is 100. Changed input requires a new version.

`promote_experiment` requires comparable measured baseline/candidate replay evidence for the active/candidate policies. `rollback_experiment` requires `experiment_id`, a nonblank `reason`, and the currently active promoted experiment with a saved rollback point. It restores the previous lane, policy version and active experiment identity; created, superseded or already rolled-back candidates cannot be rolled back.

Durable jobs carry explicit host consolidation outputs or retention `memory_ids`; `run_jobs` claims due work with leases and persists retries/results. `run_outbox` retries pending acknowledged external-store work. Reembedding consumes per-memory host vectors, persists partial progress and switches only the tenant/user route after validated explicit cutover; rollback is explicit. See the [operations guides](../../docs/memory/operations/README.md).

Reminder delivery requires a real configured webhook (`NEXTRAL_REMINDER_WEBHOOK_URL`, optional `NEXTRAL_REMINDER_WEBHOOK_TOKEN`). Hosts invoke due scans; quiet hours/timezone, expiry, privacy, priority, dedupe and retries are runtime policy, not title-based simulated outcomes.

## Mutation and failure semantics

`NextralError` subclasses preserve native `code` and `message`; specific classes include `InvalidInputError`, `NotFoundError`, `ConflictError`, `StorageError` and `SerializationError`. Policy rejection can be a normal response rather than an exception. Inspect status, errors and required backend receipts.

Batch responses contain `total`, `succeeded`, `failed`, `errors` and successful `results`; they are not all-or-nothing transactions. Batch-read keys fence request fingerprints without caching raw response bodies: later reads see current canonical updates/privacy/deletion state. Forget targets exact owned source references, not arbitrary substrings; unrelated manual policies remain.

## Blocking hosts and programmatic shutdown

```python
from concurrent.futures import ThreadPoolExecutor
import nextral

# config has the required bind addresses, backend, stores and auth settings.
with ThreadPoolExecutor(max_workers=1) as executor:
    running = executor.submit(nextral.serve, config, ["http", "grpc", "graphql"])
    try:
        # Host waits/works here; this example waits for operator input.
        input("Press Enter to stop Nextral services\n")
    finally:
        nextral.stop_serving()
    result = running.result()
```

`serve(config, modes)` blocks until shutdown but releases the GIL, allowing another Python thread to call `stop_serving()`. Modes are `http`, `grpc`, `graphql` or `all`. Startup errors propagate; the returned report describes stopped hosts, not a serve plan. Request shutdown after listener startup; a prior stop signal is not a substitute for a readiness check. Native Ctrl-C and Unix SIGTERM are also handled. `/healthz` is liveness and `/readyz` probes stores.

Network hosts verify JWT signatures/issuer/audience/scope or accept a privileged service token. Embedded Python and its CLI trust the invoking host to authorize tenant/user scope. See [trust boundaries](../../docs/package-production.md#authentication-and-trust-boundaries).

## CLI and evidence

```sh
python -m nextral --help
python -m nextral memory ingest-schema
python -m nextral --config runtime.json memory ingest ingest.json
python -m nextral serve all runtime.json
```

The Python-installed `nextral` command differs from the Cargo-built binary; use `python -m nextral` to select this parser. See [CLI differences](../../docs/cli/README.md#python-installed-cli-differs).

`e2e_smoke()` deliberately uses the native isolated testkit, not production persistence. Real-store integration uses repository `scripts/e2e-docker.ps1`/`.sh`; telemetry wire validation uses `scripts/telemetry-smoke.py`. Research promotion consumes measured baseline/candidate replay evidence, not fabricated severity metrics; process-roundtrip replay timings include host startup/setup and are not production SLOs. No test result or registry publication is implied by these instructions.
