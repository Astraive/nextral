# Graphify payload guide

## Host-supplied hint

Ingestion accepts `graph_hints` entries of this shape:

```json
{
  "from_label": "Project",
  "from_name": "Atlas",
  "relationship_type": "USES",
  "to_label": "Technology",
  "to_name": "PostgreSQL",
  "confidence": 0.95
}
```

Allowed labels are `Person`, `Project`, `Technology`, `Organization`, `Entity`, `Concept`. Allowed positive relationship types are `WORKS_ON`, `USES`, `OWNS`, `DECIDED`, `RELATED_TO`, `DEPENDS_ON`; each also accepts its explicit `NOT_` form, such as `NOT_USES`. Confidence is finite and bounded to 0–1; invalid values are rejected. Names/merge keys are canonicalized; relationships retain direction and source-memory evidence.

## Canonical replay and persistence

The public `nextral.graph.graphify` request contains `tenant_id`, `user_id`, `memory_id`, and optional `hints` (an array of the shape above). It rejects a full `record` payload and loads the active canonical memory in that exact scope; missing, inactive or foreign-scope memory returns `NotFound`. Replay does not rewrite canonical content. At most 256 hints and 256 canonical entities are accepted.

Nonempty supplied hints are authoritative for relationships: deterministic content heuristics run only when hints are omitted or empty. Canonical entities still contribute nodes. No hidden model extraction occurs. A positive relation and its matching `NOT_` relation on the same scoped, directed canonical endpoints produce a logical contradiction; `USES` plus `OWNS`, different endpoints, and reversed directions do not. Duplicate same-polarity evidence is not a contradiction.

The flattened response contains `status: "accepted"`, `write_receipts`, `trace_id`, `nodes`, `relationships`, `contradictions`, `confidence_calibration_version`, `contradiction_class`, and `evidence_count`. Nodes carry scope, label/name/canonical key and confidence. Relationships carry scoped endpoints, `relationship_type`, `source_memory_ids`, confidence and timestamps. These are not the old nested `{from,type,to}` extractor example.

Production replay transactionally commits graph intent, audit and outbox work in PostgreSQL, then attempts external application. `accepted` means durable intent, not necessarily a completed Neo4j write: inspect `write_receipts` for `neo4j:source:replaced` and drain pending work with `nextral.outbox.run`. Deterministic source replacement preserves other memories' shared evidence. Stale canonical snapshots and forgotten sources cannot restore old graph evidence; forget removes exact source contributions.
