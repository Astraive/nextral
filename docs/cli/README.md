# CLI, MCP, and service reference

## Rust CLI

The Cargo-built `nextral` binary accepts `--config FILE` (overriding `NEXTRAL_CONFIG`). Requests accept inline JSON, a file path, `@FILE`, or `-` for stdin. Examples use POSIX-style quoting; on other shells prefer JSON files.

```sh
nextral --help
nextral help memory
nextral tools
nextral schema nextral.memory.ingest
nextral config validate runtime.json
nextral --config runtime.json memory ingest ingest.json
nextral --config runtime.json memory retrieve query.json
nextral --config runtime.json call nextral.memory.list '{"tenant_id":"t","user_id":"u"}'
```

The named command groups execute the corresponding tools:

| Group | Operations |
| --- | --- |
| `memory` | `ingest`, `retrieve`, `get`, `list`, `update`, `stats`, `search-by-tag`, `search-by-entity`, `forget` |
| `session` | `append`, `context` |
| `policy` | `upsert`, `list`, `remove` |
| `graph` | `query`, `graphify` |
| `reminders` | `schedule`, `due`, `list` |
| `consolidation` | `run` |
| `batch` | `ingest`, `retrieve`, `forget` |
| `jobs` | `enqueue`, `run`, `list`, `get`, `reembed-plan`, `reembed-execute` |
| `outbox` | `run` |
| `database` | `migrate` |
| `runtime` | `scored-search` |
| `experiments` | `create`, `promote`, `rollback`, `status` |
| `safety` | `get`, `set` |

Use `schema TOOL` for actual request fields, not invented minimal payloads. `memory ingest` without input does not print a schema. Other commands include `health`, the explicit testkit `memory smoke`, and `adapters smoke REQUEST` for a real adapter request. Do not confuse `health` metadata with the API's real-store readiness probe.

## Python-installed CLI differs

`python -m nextral` selects the installed Python bridge even if the Rust binary has the same name on `PATH`. It supports the memory/session/policy/graph/reminder/consolidation/batch/job/outbox operations, but is not a parser alias for the Rust binary:

```sh
python -m nextral --help
python -m nextral memory ingest-schema
python -m nextral --config runtime.json memory ingest ingest.json
python -m nextral db migrate owner-config.json
python -m nextral serve all runtime.json
```

Python requests accept JSON objects, file paths, or `-`; `@FILE` is Rust CLI syntax. Python's `mcp call` accepts a native envelope containing string `tool` and JSON-encoded string `payload_json`; Rust uses `call TOOL REQUEST` instead. Do not use the old Rust `mcp call` alias. Python research/control functions are available programmatically; inspect that API rather than assuming Rust command parity.

## MCP stdio JSON-RPC

```sh
nextral-mcp --config runtime.json stdio
```

Send one JSON-RPC object per line; stdout is protocol output and diagnostics go to stderr. A client initializes, sends the initialized notification, then discovers and calls tools:

```json
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"example","version":"1"}}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"nextral.memory.list","arguments":{"tenant_id":"t","user_id":"u"}}}
```

This is a trusted host transport, not JWT-authenticated remote access. Restrict which processes/users can invoke it and which tools an agent may call.

## Network services

```sh
nextral-api plan all runtime.json
nextral-api readiness runtime.json
nextral-api serve all runtime.json
```

`serve` runs real HTTP/gRPC/GraphQL listeners; `plan` and readiness output are not listeners. REST endpoints include `POST /v1/memories`, `/v1/retrieval/query`, `/v1/memories/{memory_id}/forget`, and `/v1/reminders`. Send request objects matching the contract and `Authorization: Bearer TOKEN`. For example, with a scoped JWT or privileged automation token in `TOKEN`:

```sh
curl -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" --data-binary @query.json http://127.0.0.1:8080/v1/retrieval/query
```

`/healthz` is liveness; `/readyz` probes stores. GraphQL uses `/graphql`; gRPC uses `contracts/grpc/nextral.proto`. Contracts and network JWT/service-token boundaries are described in the [package guide](../package-production.md). Not every package tool is a public REST operation.
