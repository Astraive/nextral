# Retrieval pipeline

1. Host supplies query text/vector, entities and scope; Rust does not embed the query.
2. Enabled vector and graph paths execute in parallel with their configured limits.
3. Merge candidate IDs, deduplicate, then fetch active canonical records in a single tenant/user/privacy-scoped query.
4. Compute configured weighted similarity, recency, importance and access scores.
5. Fit selected items to the token estimate budget, with score/source breakdowns.
6. Atomically update access metadata without vector reindexing and return status, safety decision and telemetry.

One enabled path failing yields degraded results if another survives. All enabled paths failing yields an error; do not invent session-only success as though retrieval succeeded. Hosts may explicitly choose their own fallback behavior.

Batch retrieval keys validate request fingerprints, not stored raw response bodies. Every read observes current canonical content and lifecycle/privacy state after mutations. Never turn idempotency into stale memory replay.

Latency/count telemetry reflects actual stages. Optional exported telemetry and replay reports are separate from performance guarantees; see [measurement](../operations/slos-alerting-and-capacity.md).
