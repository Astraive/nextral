CREATE CONSTRAINT nextral_entity_key IF NOT EXISTS
FOR (n:NextralEntity)
REQUIRE (n.tenant_id, n.user_id, n.label, n.canonical_name) IS UNIQUE;

CREATE INDEX nextral_entity_lookup IF NOT EXISTS
FOR (n:NextralEntity)
ON (n.tenant_id, n.user_id, n.canonical_name);

CREATE INDEX nextral_relationship_source IF NOT EXISTS
FOR ()-[r:NEXTRAL_RELATES_TO]-()
ON (r.tenant_id, r.user_id, r.relationship_type, r.source_memory_ids);
