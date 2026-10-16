# Quickstart

## 1. Configure a backend

For durable use, copy `examples/config.production.example.json` to a deployment file, replace placeholders, configure credentials and reachable stores, and set `stores.qdrant_collection`. The host must supply model outputs; provider configuration does not call a provider. For isolated tests only, explicitly select `test_memory` as shown in the [Node example](../../bindings/node/README.md#configure-explicitly).

## 2. Validate and migrate

Using the Rust CLI:

```sh
nextral config validate runtime.json
nextral --config owner-config.json database migrate '{}'
```

Use a schema-owning migration role, then a restricted no-DDL runtime role with tenant/user RLS. Python's migration spelling is `python -m nextral db migrate owner-config.json`. Validation is not migration, store provisioning, or a readiness check.

## 3. Execute actual operations

```sh
nextral schema nextral.memory.ingest
nextral --config runtime.json memory ingest ingest.json
nextral --config runtime.json memory retrieve query.json
nextral --config runtime.json memory get '{"tenant_id":"example","user_id":"alice","memory_id":"atlas-database"}'
```

Create `ingest.json` from the schema and supply your own content, embedding, metadata, scores and policy; create `query.json` with a matching `query_vector` and explicit tenant/user/privacy scope. The [Python](../../bindings/python/README.md) and [Node](../../bindings/node/README.md) guides show programmatic calls. `memory ingest` is not a schema command. Python's template command is `python -m nextral memory ingest-schema`.

For the deliberately isolated native testkit flow:

```sh
nextral memory smoke
```

This does not exercise production persistence or an embedding provider.

## 4. Optional hosts and integration

```sh
nextral-api serve all runtime.json
nextral-mcp --config runtime.json stdio
```

The API command starts real listeners. `nextral-api plan all runtime.json` only prints a plan. MCP uses JSON-RPC `tools/list`, not a `nextral-mcp tools` subcommand. See [CLI and protocol examples](../cli/README.md).

For real-store integration with Docker, run `scripts/e2e-docker.ps1` on Windows or `scripts/e2e-docker.sh` on a shell host. The harness provisions fixtures and exercises durable operations; run it against disposable infrastructure, not user data. Its scope is not a production readiness certification.
