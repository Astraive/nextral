# Ingestion and canonical memory records

## Ingestion request

This is an illustrative host-prepared request. The three-dimensional vector is valid only for a configuration that explicitly uses that dimension; replace it with the host model output in a real deployment.

```json
{
  "id": "atlas-database",
  "tenant_id": "example",
  "user_id": "alice",
  "session_id": null,
  "content": "Atlas uses PostgreSQL",
  "content_type": "decision",
  "memory_type": "semantic",
  "source_type": "manual",
  "source_message_ids": [],
  "importance_score": 0.9,
  "confidence_score": 0.95,
  "entities": ["Atlas", "PostgreSQL"],
  "tags": ["architecture"],
  "privacy_level": "private",
  "graph_hints": [],
  "policy": {"min_importance_score": 0.35, "min_confidence_score": 0.6},
  "embedding": [1.0, 0.0, 0.0]
}
```

The request supplies content and policy, not server-owned access counters, status or timestamps. Accepted ingestion returns status, trace, canonical ID, validation information and receipts; threshold rejection is a normal result. Inspect status rather than treating a returned object as proof of acceptance.

## Canonical fields

| Fields | Ownership and meaning |
| --- | --- |
| `id`, `tenant_id`, `user_id` | Stable identity and isolation; IDs are nonempty, at most 256 characters and exclude separators/control characters |
| `session_id`, `source_type`, `source_message_ids` | Host provenance used for consolidation, recovery and exact-source deletion |
| `content`, `content_type`, `memory_type` | Host text and validated taxonomy |
| `importance_score`, `confidence_score` | Finite bounded 0–1 signals; invalid scores are rejected, not silently clamped |
| `embedding_provider`, `embedding_model`, `embedding_dim`, `embedding` | Configured provider metadata plus host vector; configuration does not execute the provider |
| `extraction_provider`, `extraction_model` | Recorded extraction configuration, not proof of a model invocation |
| `entities`, `tags`, `privacy_level` | Host metadata; privacy enum is `private`, `sensitive`, `shared`, `restricted` |
| `created_at`, `updated_at`, `last_accessed_at`, `access_count` | Runtime lifecycle/access state |
| `status`, `schema_version` | Canonical lifecycle/schema metadata; not a promise of arbitrary compatibility adapters |

Content types are `decision`, `preference`, `goal`, `fact`, `task`, `event`, `note`, `commitment`, `pattern`. Memory types are `working`, `session`, `episodic`, `semantic`, `relational`, `procedural`, `prospective`; request-local working context is not automatically persisted by context assembly. Source types are `realtime`, `fast_lane`, `deep_lane`, `import`, `manual`. Lifecycle statuses include `active`, `soft_deleted`, `redacted`, `archived`; use supported operations rather than directly mutating status rows.

## Store mapping and migration

PostgreSQL canonical rows are scoped by tenant and user; never treat a globally unique `id` alone as authorization. Qdrant point IDs are deterministically adapter-mapped from scoped identity and payloads retain public IDs. Neo4j source evidence references canonical memory IDs. Audit/outbox record durable mutation and recovery work.

Use explicit schema-owner migration and restricted runtime RLS permissions. Collection dimension/metric and graph indexes are deployment resources, not ad hoc DDL executed by normal requests. See [deployment](../../package-production.md).
