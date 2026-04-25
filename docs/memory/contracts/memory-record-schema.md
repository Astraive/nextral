# Memory Record Schema Contract

## 1. Canonical memory payload (store-agnostic)

```json
{
  "id": "mem_2f84d7f7-5e4e-47af-8f31-8db2bcde2c22",
  "user_id": "usr_2a6f06f7-6c4d-4efd-b6a2-6ed31e0ac2ef",
  "session_id": "sess_88dd1b55-bfdf-4f98-90bb-e12348b42d9c",
  "content": "Decided to use PostgreSQL for Project Atlas backend",
  "content_type": "decision",
  "memory_type": "semantic",
  "source_type": "realtime",
  "source_message_ids": ["msg_01", "msg_02"],
  "importance_score": 0.85,
  "confidence_score": 0.92,
  "embedding_model": "text-embedding-3-small",
  "embedding_dim": 1536,
  "entities": ["PostgreSQL", "Project Atlas"],
  "tags": ["database", "architecture"],
  "privacy_level": "private",
  "created_at": "2026-04-10T19:30:00Z",
  "updated_at": "2026-04-10T19:30:00Z",
  "last_accessed_at": null,
  "access_count": 0,
  "status": "active",
  "schema_version": "1.0.0"
}
```

## 2. Field definitions

| Field | Type | Required | Notes |
|---|---|---|---|
| `id` | UUID/string | yes | stable identity across all stores |
| `user_id` | UUID/string | yes | isolation partition key |
| `session_id` | UUID/string | no | source session linkage |
| `content` | text | yes | canonical memory text |
| `content_type` | enum | yes | `decision`, `preference`, `goal`, `fact`, `task`, `event`, `note`, `commitment`, `pattern` |
| `memory_type` | enum | yes | `working`, `session`, `episodic`, `semantic`, `relational`, `procedural`, `prospective` |
| `source_type` | enum | yes | `realtime`, `fast_lane`, `deep_lane`, `import`, `manual` |
| `importance_score` | float | yes | 0-1 bounded |
| `confidence_score` | float | no | extractor/model confidence 0-1 |
| `entities` | string[] | no | normalized entity names |
| `privacy_level` | enum | yes | `private`, `sensitive`, `shared`, `restricted` |
| `status` | enum | yes | `active`, `soft_deleted`, `redacted`, `archived` |
| `schema_version` | semver | yes | migration/compat guard |

## 3. Store mapping contract

| Store | ID | Required mapped fields |
|---|---|---|
| PostgreSQL index | `id` | user/session/source/content_type/status/privacy/timestamps |
| Qdrant payload | `id` | user/content_type/privacy/importance/entities/timestamps |
| Neo4j edge metadata | `source_memory_id=id` | confidence, created_at, last_confirmed_at |
| PostgreSQL event/audit rows | `memory_id=id` | write/read/delete events and timings |

## 4. PostgreSQL schema requirements

- unique index on `id`
- composite index `(user_id, created_at desc)`
- composite index `(user_id, content_type, created_at desc)`
- filtered index for active rows (`status = 'active'`)
- GIN/FTS index for `content` where required

## 5. Qdrant requirements

- vector dimension and metric are collection-level contract
- payload must include `user_id`, `content_type`, `privacy_level`, `importance_score`
- writes must include deterministic point ID = memory `id`

## 6. Validation rules

1. reject writes with missing required fields
2. clamp and validate score fields to `[0.0, 1.0]`
3. reject unknown enum values
4. reject empty content
5. enforce schema version compatibility

## 7. Lifecycle transitions

```text
active -> soft_deleted -> redacted
active -> archived
```

Invalid transitions are rejected and logged.

## 8. Deletion and redaction contract

Forget flow minimum requirements:

1. canonical index row marked `soft_deleted` or `redacted`
2. Qdrant point removal or payload redaction
3. graph unlink/redaction for derived relationships
4. audit event persisted with actor, reason, timestamp

## 9. Versioning strategy

- `schema_version` follows semver
- minor changes: backward-compatible optional fields
- major changes: migration required before write
- readers must tolerate known older versions via compatibility adapters

<!-- memory-expansion-2026-04-10 -->

## Builder Addendum: Expanded Control Surface

This addendum extends the document with practical implementation controls for the Tony memory runtime.

| Control surface | Default posture | Why it matters |
|---|---|---|
| Candidate precision | threshold-gated writes | reduces low-signal memory pollution |
| Recall diversity | vector + graph blending | improves answer richness and grounding |
| Durability | multi-store receipts + audit trail | prevents silent memory loss |
| Cost efficiency | token-budget fitting and pruning | preserves quality under context limits |

```mermaid
flowchart LR
    A[Input signal] --> B[Memory classification]
    B --> C[Store writes and indexing]
    C --> D[Hybrid retrieval]
    D --> E[Context budget fit]
    E --> F[Response + post-hooks]
```
