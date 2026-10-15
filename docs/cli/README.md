# CLI Reference

The Nextral CLI provides command-line access to memory operations, configuration, and service management.

## Commands

### Configuration

```bash
nextral config validate <config.json>
```

Validates a JSON configuration file against all Nextral constraints.

### Memory Operations

```bash
nextral memory smoke                    # run E2E smoke test
nextral memory ingest                   # show ingest request schema
nextral memory retrieve '<json>'        # retrieve memories matching query
nextral memory forget '<json>'          # forget/redact a memory
nextral memory list '<json>'            # list memories for a user
```

### Session Operations

```bash
nextral session append '<json>'         # append a message to a session
nextral session context '<json>'        # assemble working context
```

### Reminder Operations

```bash
nextral reminders due '<json>'          # execute due reminders
nextral reminders schedule '<json>'     # schedule a new reminder
```

### Consolidation

```bash
nextral consolidation run '<json>'       # consolidate a session into memory
```

### Batch Operations

```bash
nextral batch ingest '<json>'           # batch ingest multiple memories
nextral batch retrieve '<json>'         # batch retrieve across queries
nextral batch forget '<json>'           # batch forget multiple memories
```

### Adapter Smoke Test

```bash
nextral adapters smoke <request.json>   # verify production adapter connectivity
```

### Reembed

```bash
nextral jobs reembed-plan <request.json> # plan vector re-embedding
```

### Health

```bash
nextral health                          # print health status
```

### MCP

```bash
nextral mcp call '<json>'               # execute an MCP tool call directly
```

## JSON Input

Commands accepting `<json>` accept either:
- An inline JSON string: `'{"tenant_id":"t","user_id":"u"}'`
- A path to a JSON file: `request.json`

## Examples

```bash
# Retrieve memories about PostgreSQL
nextral memory retrieve '{"tenant_id":"t","user_id":"u","query_text":"PostgreSQL","entities":[],"token_budget":1800,"privacy_scope":["private"],"top_k_vector":12,"max_graph_hops":2}'

# List all memories for a user
nextral memory list '{"tenant_id":"t","user_id":"u"}'

# Batch ingest
nextral batch ingest '{"items":[{"tenant_id":"t","user_id":"u","content":"fact","content_type":"note","memory_type":"semantic","source_type":"manual","source_message_ids":[],"importance_score":0.5,"entities":[],"tags":[],"privacy_level":"private","graph_hints":[],"policy":{"min_importance_score":0.0,"min_confidence_score":0.0}}]}'
```
