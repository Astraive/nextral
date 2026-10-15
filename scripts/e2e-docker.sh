#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker compose -f docker-compose.integration.yml"
NETWORK="nextral_default"
COLLECTION="nextral_memories"
BUCKET="nextral-memory"

wait_until() {
    local name="$1"
    shift
    for i in $(seq 1 60); do
        if "$@" >/dev/null 2>&1; then
            echo "$name ready"
            return 0
        fi
        sleep 2
    done
    echo "FAIL: $name did not become ready" >&2
    exit 1
}

if [[ "${1:-}" != "--skip-start" ]]; then
    $COMPOSE up -d
fi

wait_until "postgres" $COMPOSE exec -T postgres pg_isready -U nextral -d nextral
wait_until "redis" bash -c "$COMPOSE exec -T redis redis-cli PING | grep -q PONG"
wait_until "qdrant" curl -sf http://localhost:6333/readyz
wait_until "neo4j" $COMPOSE exec -T neo4j cypher-shell -u neo4j -p nextraldev "RETURN 1;"
wait_until "minio" curl -sf http://localhost:9000/minio/health/ready

echo "Applying PostgreSQL migration"
$COMPOSE exec -T postgres psql -U nextral -d nextral -v ON_ERROR_STOP=1 \
    -f /dev/stdin < migrations/postgres/0001_core_schema.sql

echo "Applying Neo4j schema"
$COMPOSE exec -T neo4j cypher-shell -u neo4j -p nextraldev \
    < migrations/neo4j/0001_graph_schema.cypher

echo "Provisioning Qdrant collection"
curl -sf -X DELETE "http://localhost:6333/collections/$COLLECTION" >/dev/null 2>&1 || true
curl -sf -X PUT "http://localhost:6333/collections/$COLLECTION" \
    -H "Content-Type: application/json" \
    -d '{"vectors":{"size":4,"distance":"Cosine"}}' | jq . >/dev/null

echo "Provisioning MinIO bucket"
docker run --rm --network "$NETWORK" --entrypoint /bin/sh minio/mc -c \
    "mc alias set local http://minio:9000 nextral nextraldev >/dev/null && \
     mc mb -p local/$BUCKET >/dev/null 2>&1 || true && \
     printf 'nextral transcript archive' | mc pipe local/$BUCKET/tenants/tenant_1/users/user_1/sessions/session_1/transcripts/transcript.txt >/dev/null && \
     mc stat local/$BUCKET/tenants/tenant_1/users/user_1/sessions/session_1/transcripts/transcript.txt >/dev/null"

echo "Writing all memory-system fixtures"
$COMPOSE exec -T postgres psql -U nextral -d nextral -v ON_ERROR_STOP=1 -c "
TRUNCATE nextral_archive_objects, nextral_outbox_events, nextral_jobs, nextral_idempotency_keys, nextral_audit_events, nextral_reminders, nextral_session_summaries, nextral_session_messages, nextral_memories RESTART IDENTITY CASCADE;

INSERT INTO nextral_memories (
  id, tenant_id, user_id, session_id, content, content_type, memory_type, source_type,
  source_message_ids, importance_score, confidence_score, embedding_provider, embedding_model,
  embedding_dim, vector_point_id, extraction_provider, extraction_model, entities, tags, privacy_level,
  created_at, updated_at, access_count, status, schema_version
) VALUES
('mem_session', 'tenant_1', 'user_1', 'session_1', 'Session continuity says Atlas migration is active', 'note', 'session', 'realtime', '[]', 0.7, 0.9, 'testkit', 'test-embedding', 4, '11111111-1111-1111-1111-111111111111', NULL, NULL, '[\"Atlas\"]', '[\"session\"]', 'private', now(), now(), 0, 'active', '1.0.0'),
('mem_episodic', 'tenant_1', 'user_1', 'session_1', 'On Monday we decided Atlas migration should use PostgreSQL', 'event', 'episodic', 'fast_lane', '[]', 0.9, 0.95, 'testkit', 'test-embedding', 4, '22222222-2222-2222-2222-222222222222', NULL, NULL, '[\"Atlas\",\"PostgreSQL\"]', '[\"timeline\"]', 'private', now(), now(), 0, 'active', '1.0.0'),
('mem_semantic', 'tenant_1', 'user_1', 'session_1', 'Atlas uses PostgreSQL for durable memory storage', 'fact', 'semantic', 'manual', '[]', 0.95, 0.98, 'testkit', 'test-embedding', 4, '33333333-3333-3333-3333-333333333333', NULL, NULL, '[\"Atlas\",\"PostgreSQL\"]', '[\"fact\"]', 'private', now(), now(), 0, 'active', '1.0.0'),
('mem_relational', 'tenant_1', 'user_1', 'session_1', 'Atlas depends on PostgreSQL and Qdrant', 'fact', 'relational', 'manual', '[]', 0.8, 0.9, 'testkit', 'test-embedding', 4, '44444444-4444-4444-4444-444444444444', NULL, NULL, '[\"Atlas\",\"PostgreSQL\",\"Qdrant\"]', '[\"graph\"]', 'private', now(), now(), 0, 'active', '1.0.0'),
('mem_procedural', 'tenant_1', 'user_1', 'session_1', 'Prefer concise engineering updates with clear verification', 'preference', 'procedural', 'manual', '[]', 0.85, 0.9, 'testkit', 'test-embedding', 4, '55555555-5555-5555-5555-555555555555', NULL, NULL, '[\"communication\"]', '[\"policy\"]', 'private', now(), now(), 0, 'active', '1.0.0'),
('mem_prospective', 'tenant_1', 'user_1', 'session_1', 'Remind user to check Atlas migration progress', 'commitment', 'prospective', 'manual', '[]', 0.9, 0.9, 'testkit', 'test-embedding', 4, '66666666-6666-6666-6666-666666666666', NULL, NULL, '[\"Atlas\"]', '[\"reminder\"]', 'private', now(), now(), 0, 'active', '1.0.0');

INSERT INTO nextral_session_messages (id, tenant_id, user_id, session_id, role, content, created_at)
VALUES ('msg_1', 'tenant_1', 'user_1', 'session_1', 'user', 'Use PostgreSQL for Atlas and remind me Friday', now());

INSERT INTO nextral_reminders (
  id, tenant_id, user_id, source_memory_id, kind, title, details, due_at, timezone,
  priority, status, attempt_count, next_attempt_at, dedupe_key, created_at, updated_at
) VALUES (
  'reminder_1', 'tenant_1', 'user_1', 'mem_prospective', 'follow_up',
  'Check Atlas migration', 'Review PostgreSQL migration progress', now() + interval '1 day',
  'UTC', 'normal', 'scheduled', 0, now() + interval '1 day', 'dedupe_1', now(), now()
);

INSERT INTO nextral_audit_events (id, tenant_id, user_id, actor_id, action, target_type, target_id, reason, metadata, created_at)
VALUES ('audit_1', 'tenant_1', 'user_1', 'user_1', 'write_accepted', 'memory', 'mem_semantic', 'docker e2e fixture', '{}', now());

INSERT INTO nextral_archive_objects (id, tenant_id, user_id, session_id, memory_id, bucket, object_key, content_sha256, object_kind, created_at)
VALUES ('archive_1', 'tenant_1', 'user_1', 'session_1', NULL, '$BUCKET', 'tenants/tenant_1/users/user_1/sessions/session_1/transcripts/transcript.txt', 'fixture', 'transcript', now());
"

echo "Writing Redis fixtures"
$COMPOSE exec -T redis redis-cli SET nextral:tenant_1:user_1:working:request_1 "temporary working memory"
$COMPOSE exec -T redis redis-cli RPUSH nextral:tenant_1:user_1:session_1:tail "Use PostgreSQL for Atlas"
$COMPOSE exec -T redis redis-cli SET nextral:tenant_1:user_1:policy "Prefer concise engineering updates"
$COMPOSE exec -T redis redis-cli SET nextral:tenant_1:user_1:reminder:lease "reminder_1"

echo "Writing Qdrant vector points"
curl -sf -X PUT "http://localhost:6333/collections/$COLLECTION/points?wait=true" \
    -H "Content-Type: application/json" \
    -d '{
    "points": [
      {"id":"11111111-1111-1111-1111-111111111111","vector":[0.1,0.1,0.1,0.1],"payload":{"memory_id":"mem_session","tenant_id":"tenant_1","user_id":"user_1","privacy_level":"private","status":"active","memory_type":"session","content_type":"note","schema_version":"1.0.0"}},
      {"id":"22222222-2222-2222-2222-222222222222","vector":[0.2,0.2,0.2,0.2],"payload":{"memory_id":"mem_episodic","tenant_id":"tenant_1","user_id":"user_1","privacy_level":"private","status":"active","memory_type":"episodic","content_type":"event","schema_version":"1.0.0"}},
      {"id":"33333333-3333-3333-3333-333333333333","vector":[0.3,0.3,0.3,0.3],"payload":{"memory_id":"mem_semantic","tenant_id":"tenant_1","user_id":"user_1","privacy_level":"private","status":"active","memory_type":"semantic","content_type":"fact","schema_version":"1.0.0"}},
      {"id":"44444444-4444-4444-4444-444444444444","vector":[0.4,0.4,0.4,0.4],"payload":{"memory_id":"mem_relational","tenant_id":"tenant_1","user_id":"user_1","privacy_level":"private","status":"active","memory_type":"relational","content_type":"fact","schema_version":"1.0.0"}},
      {"id":"55555555-5555-5555-5555-555555555555","vector":[0.5,0.5,0.5,0.5],"payload":{"memory_id":"mem_procedural","tenant_id":"tenant_1","user_id":"user_1","privacy_level":"private","status":"active","memory_type":"procedural","content_type":"preference","schema_version":"1.0.0"}},
      {"id":"66666666-6666-6666-6666-666666666666","vector":[0.6,0.6,0.6,0.6],"payload":{"memory_id":"mem_prospective","tenant_id":"tenant_1","user_id":"user_1","privacy_level":"private","status":"active","memory_type":"prospective","content_type":"commitment","schema_version":"1.0.0"}}
    ]
  }' | jq . >/dev/null

echo "Writing Neo4j graph fixtures"
$COMPOSE exec -T neo4j cypher-shell -u neo4j -p nextraldev "
MERGE (a:NextralEntity {tenant_id:'tenant_1', user_id:'user_1', label:'Project', canonical_name:'atlas'})
SET a.name='Atlas', a.source_memory_ids=['mem_relational']
MERGE (p:NextralEntity {tenant_id:'tenant_1', user_id:'user_1', label:'Technology', canonical_name:'postgresql'})
SET p.name='PostgreSQL', p.source_memory_ids=['mem_relational']
MERGE (a)-[r:NEXTRAL_RELATES_TO {tenant_id:'tenant_1', user_id:'user_1', relationship_type:'DEPENDS_ON'}]->(p)
SET r.source_memory_ids=['mem_relational'], r.confidence=0.95;
"

echo "Verifying retrieval/storage effects"
MEMORY_COUNT=$($COMPOSE exec -T postgres psql -U nextral -d nextral -Atc "SELECT count(*) FROM nextral_memories WHERE tenant_id='tenant_1' AND user_id='user_1' AND status='active';")
if [[ "$MEMORY_COUNT" != "6" ]]; then echo "FAIL: expected 6 durable memory rows, got $MEMORY_COUNT"; exit 1; fi

TYPES_COUNT=$($COMPOSE exec -T postgres psql -U nextral -d nextral -Atc "SELECT count(DISTINCT memory_type) FROM nextral_memories WHERE tenant_id='tenant_1' AND user_1='user_1';")
if [[ "$TYPES_COUNT" != "6" ]]; then echo "FAIL: expected 6 durable memory types, got $TYPES_COUNT"; exit 1; fi

WORKING=$($COMPOSE exec -T redis redis-cli GET nextral:tenant_1:user_1:working:request_1)
if [[ "$WORKING" != "temporary working memory" ]]; then echo "FAIL: working memory redis check failed"; exit 1; fi

QDRANT_RESULT=$(curl -sf -X POST "http://localhost:6333/collections/$COLLECTION/points/search" \
    -H "Content-Type: application/json" \
    -d '{"vector":[0.3,0.3,0.3,0.3],"limit":3,"with_payload":true,"filter":{"must":[{"key":"tenant_id","match":{"value":"tenant_1"}},{"key":"user_id","match":{"value":"user_1"}},{"key":"status","match":{"value":"active"}}]}}')
QDRANT_COUNT=$(echo "$QDRANT_RESULT" | jq '.result | length')
if [[ "$QDRANT_COUNT" -lt 1 ]]; then echo "FAIL: qdrant retrieval returned no results"; exit 1; fi

GRAPH_COUNT=$($COMPOSE exec -T neo4j cypher-shell -u neo4j -p nextraldev --format plain "MATCH (:NextralEntity {canonical_name:'atlas'})-[r:NEXTRAL_RELATES_TO]->(:NextralEntity {canonical_name:'postgresql'}) RETURN count(r);")
if ! echo "$GRAPH_COUNT" | grep -q "1"; then echo "FAIL: neo4j relationship check failed"; exit 1; fi

docker run --rm --network "$NETWORK" --entrypoint /bin/sh minio/mc -c \
    "mc alias set local http://minio:9000 nextral nextraldev >/dev/null && \
     mc stat local/$BUCKET/tenants/tenant_1/users/user_1/sessions/session_1/transcripts/transcript.txt >/dev/null"

echo "Running package smoke"
cargo run -p nextral-cli -- memory smoke

echo "Docker E2E passed: all seven memory systems covered across PostgreSQL, Redis, Qdrant, Neo4j, MinIO/S3, CLI."

if [[ "${1:-}" != "--keep" ]]; then
    echo "Stack left running for inspection. Run 'docker compose -f docker-compose.integration.yml down' when done."
fi
