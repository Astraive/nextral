# Storage and lifecycle

| Backend | Authoritative or derived role |
| --- | --- |
| PostgreSQL | Authoritative scoped canonical records, full messages, summaries, policies, reminders, audit, jobs, leases, outbox and reembed state |
| Redis | Hot last-20-message tail, policy/cache entries and invalidation; TTL is configured |
| Qdrant | Derived vectors with tenant/user/privacy metadata in the explicitly configured collection |
| Neo4j | Derived scoped graph with exact source-memory evidence |
| S3/MinIO | Private transcript/source archive bodies with acknowledged uploads |

Working context is request-local host state, not an additional durable store. All session messages go to PostgreSQL on append, not only after Redis overflow. Redis eviction or TTL does not define warm retention. A deep consolidation archive is a real object, not merely a planned URI.

## Schema and consistency

Apply explicit migrations as a schema owner; the application role must not require DDL or bypass tenant/user RLS. Canonical/audit/outbox writes share the PostgreSQL transaction. External stores are not in that transaction: read canonical state to reject deleted or unauthorized stale index candidates, and replay pending external work through the outbox.

Cold does not mean Qdrant or graph state is the canonical source of memory content. A successful vector hit must still resolve an active owned canonical record.

## Retention and deletion

Redis TTL bounds hot cache lifetime. Durable retention is explicit: enqueue scoped retention jobs carrying selected `memory_ids` and `redact`; inspect persisted results and per-store receipts. There is no default 90-day purge, automatic graph confidence aging, or universally enabled table partitioning. A deployment may add managed partitions/object lifecycle only when compatible with schema, recovery and deletion obligations.

Forget removes/redacts the owned canonical record and exact dependent references, including relevant vectors, graph evidence, session/summary/archive-derived material and reminder state. Arbitrary substring erasure would corrupt unrelated content and is not the contract. See [forget workflow](../workflow/forget-and-redaction.md).

Archive manifests persist source provenance before upload; legacy manifests recover missing source IDs from verified scoped archive bodies during cleanup. Cleanup replaces a matched original object at its same key with a marked tombstone, which verified reads expose as `NotFound`. Conditional immutable uploads fence late workers from overwriting tombstones. These guarantees do not erase historical bucket versions, backups or independent copies; see the [forget workflow](../workflow/forget-and-redaction.md).

Back up canonical state, private archives and configuration together with the information needed to rebuild indexes. Test restoration and worker replay on disposable infrastructure; multi-store receipts do not replace backups.
