# Memory payload guides

- [Ingestion and canonical records](memory-record-schema.md)
- [Retrieval request/response](retrieval-contract.md)
- [Graph hints and persisted canonical replay](graphify-contract.md)
- [Reminder scheduling and execution](prospective-contract.md)

Rust Serde types and the MCP tool catalog define executable wire fields. Rust CLI `schema TOOL` returns the catalog input schema; Python `ingest_request_schema()` returns an ingestion template, not a write result. Network service contracts live under root `contracts/` and expose only their declared subset of operations.

Always include tenant/user scope. IDs are stable canonical references, not necessarily UUIDs; external vector IDs are adapter-mapped and must not be assumed identical to public string IDs. Embedded callers remain responsible for authorization.
