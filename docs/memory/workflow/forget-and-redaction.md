# Workflow: exact-source forget and redaction

1. Authenticate and authorize the request at the host/network boundary.
2. Resolve an exact owned `memory_id` with tenant/user scope; include `actor`, `reason` and `redact` in the forget request.
3. Record canonical lifecycle, audit and durable recovery intent transactionally.
4. Remove/redact the relevant vector, graph/source evidence and exact derived references in session/summary/archive/reminder state.
5. Inspect required backend receipts and pending/retry work before declaring completion.

The API is not query/category deletion and does not replace arbitrary matching substrings. Deleting memory ID `m` must not erase unrelated policies containing the letter m. Exact owned source references drive cleanup; manual policies without that provenance remain. Shared graph evidence from other source memories remains too.

Batch forget reports per-item outcomes rather than an all-or-nothing transaction. After deletion or redaction, retrieval—including a batch read with a reused unchanged request fingerprint key—loads current active canonical state instead of replaying old response text.

Archive cleanup uses persisted provenance: manifests record tenant/user, optional memory/session IDs, ordered source message IDs, receipt and pending/stored upload state before/after upload. Source IDs come from explicit `source_message_ids` and message IDs in the archived body, so cleanup can match a source even without a canonical session. For legacy manifests missing source IDs, recovery integrity-checks and scope-validates the archived body, then persists recovered provenance. A missing/tombstoned legacy object is acknowledged as tombstoned; corrupt or inaccessible bodies remain failures rather than silently proving cleanup.

S3 cleanup replaces the original object at the **same key** with a metadata-marked tombstone; verified reads return `NotFound`, not the marker as transcript data. Immutable uploads use conditional `If-None-Match: *`: unchanged existing content can be acknowledged, but a late upload cannot replace a tombstone or different content. This is active-object cleanup and resurrection fencing, not forensic erasure of bucket versions, backups, replicas or independent host copies; deployment retention policies still govern those.

Forget also scrubs source-bearing scheduled consolidation requests and cancels nonterminal jobs while retaining already-terminal status. Claimed worker progress/completion writes atomically require a live matching lease and unchanged request, and preserve current terminal or scrubbed state instead of overwriting it with a stale worker snapshot. A stale completion cannot restore a deleted request body or reverse cancellation.

If a required external operation fails, retain incomplete state and replay durable outbox work after restoring the dependency. Preserve the actor/reason, status, timestamps and actual receipts without logging deleted sensitive content as diagnostic text.
