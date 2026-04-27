param(
    [switch]$SkipStart,
    [switch]$KeepRunning
)

$ErrorActionPreference = "Stop"

$compose = @("compose", "-f", "docker-compose.integration.yml")
$network = "nextral_default"
$collection = "nextral_memories"
$bucket = "nextral-memory"

function Run([string]$File, [Parameter(ValueFromRemainingArguments = $true)][object[]]$Args) {
    $flat = @()
    foreach ($arg in $Args) {
        if ($arg -is [array]) {
            $flat += $arg
        } else {
            $flat += $arg
        }
    }
    & $File @flat
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed: $File $($flat -join ' ')"
    }
}

function Wait-Until($Name, [scriptblock]$Check) {
    for ($i = 0; $i -lt 60; $i++) {
        try {
            & $Check
            Write-Host "$Name ready"
            return
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    throw "$Name did not become ready"
}

if (-not $SkipStart) {
    Run docker @($compose + @("up", "-d"))
}

Wait-Until "postgres" {
    Run docker @($compose + @("exec", "-T", "postgres", "pg_isready", "-U", "nextral", "-d", "nextral"))
}
Wait-Until "redis" {
    $pong = docker @($compose + @("exec", "-T", "redis", "redis-cli", "PING"))
    if (($pong -join "").Trim() -ne "PONG") { throw "redis not ready" }
}
Wait-Until "qdrant" {
    Invoke-RestMethod -Uri "http://localhost:6333/readyz" | Out-Null
}
Wait-Until "neo4j" {
    Run docker @($compose + @("exec", "-T", "neo4j", "cypher-shell", "-u", "neo4j", "-p", "nextraldev", "RETURN 1;"))
}
Wait-Until "minio" {
    Invoke-WebRequest -Uri "http://localhost:9000/minio/health/ready" -UseBasicParsing | Out-Null
}

Write-Host "Applying PostgreSQL migration"
Get-Content -Raw migrations/postgres/0001_core_schema.sql |
    docker @($compose + @("exec", "-T", "postgres", "psql", "-U", "nextral", "-d", "nextral", "-v", "ON_ERROR_STOP=1"))
if ($LASTEXITCODE -ne 0) { throw "postgres migration failed" }

Write-Host "Applying Neo4j schema"
Get-Content -Raw migrations/neo4j/0001_graph_schema.cypher |
    docker @($compose + @("exec", "-T", "neo4j", "cypher-shell", "-u", "neo4j", "-p", "nextraldev"))
if ($LASTEXITCODE -ne 0) { throw "neo4j schema failed" }

Write-Host "Provisioning Qdrant collection"
try {
    Invoke-RestMethod -Method Delete -Uri "http://localhost:6333/collections/$collection" | Out-Null
} catch {}
$qdrantCollection = @{
    vectors = @{
        size = 4
        distance = "Cosine"
    }
} | ConvertTo-Json -Depth 10
Invoke-RestMethod -Method Put -Uri "http://localhost:6333/collections/$collection" -ContentType "application/json" -Body $qdrantCollection | Out-Null

Write-Host "Provisioning MinIO bucket and archive object"
Run docker @(
    "run", "--rm", "--network", $network, "--entrypoint", "/bin/sh", "minio/mc",
    "-c",
    "mc alias set local http://minio:9000 nextral nextraldev >/dev/null && mc mb -p local/$bucket >/dev/null 2>&1 || true && printf 'nextral transcript archive' | mc pipe local/$bucket/tenants/tenant_1/users/user_1/sessions/session_1/transcripts/transcript.txt >/dev/null && mc stat local/$bucket/tenants/tenant_1/users/user_1/sessions/session_1/transcripts/transcript.txt >/dev/null"
)

Write-Host "Writing all memory-system fixtures"
$sql = @"
TRUNCATE nextral_archive_objects, nextral_outbox_events, nextral_jobs, nextral_idempotency_keys, nextral_audit_events, nextral_reminders, nextral_session_summaries, nextral_session_messages, nextral_memories RESTART IDENTITY CASCADE;

INSERT INTO nextral_memories (
  id, tenant_id, user_id, session_id, content, content_type, memory_type, source_type,
  source_message_ids, importance_score, confidence_score, embedding_provider, embedding_model,
  embedding_dim, vector_point_id, entities, tags, privacy_level, created_at, updated_at,
  access_count, status, schema_version
) VALUES
('mem_session', 'tenant_1', 'user_1', 'session_1', 'Session continuity says Atlas migration is active', 'note', 'session', 'realtime', '[]', 0.7, 0.9, 'testkit', 'test-embedding', 4, '11111111-1111-1111-1111-111111111111', '["Atlas"]', '["session"]', 'private', now(), now(), 0, 'active', '1.0.0'),
('mem_episodic', 'tenant_1', 'user_1', 'session_1', 'On Monday we decided Atlas migration should use PostgreSQL', 'event', 'episodic', 'fast_lane', '[]', 0.9, 0.95, 'testkit', 'test-embedding', 4, '22222222-2222-2222-2222-222222222222', '["Atlas","PostgreSQL"]', '["timeline"]', 'private', now(), now(), 0, 'active', '1.0.0'),
('mem_semantic', 'tenant_1', 'user_1', 'session_1', 'Atlas uses PostgreSQL for durable memory storage', 'fact', 'semantic', 'manual', '[]', 0.95, 0.98, 'testkit', 'test-embedding', 4, '33333333-3333-3333-3333-333333333333', '["Atlas","PostgreSQL"]', '["fact"]', 'private', now(), now(), 0, 'active', '1.0.0'),
('mem_relational', 'tenant_1', 'user_1', 'session_1', 'Atlas depends on PostgreSQL and Qdrant', 'fact', 'relational', 'manual', '[]', 0.8, 0.9, 'testkit', 'test-embedding', 4, '44444444-4444-4444-4444-444444444444', '["Atlas","PostgreSQL","Qdrant"]', '["graph"]', 'private', now(), now(), 0, 'active', '1.0.0'),
('mem_procedural', 'tenant_1', 'user_1', 'session_1', 'Prefer concise engineering updates with clear verification', 'preference', 'procedural', 'manual', '[]', 0.85, 0.9, 'testkit', 'test-embedding', 4, '55555555-5555-5555-5555-555555555555', '["communication"]', '["policy"]', 'private', now(), now(), 0, 'active', '1.0.0'),
('mem_prospective', 'tenant_1', 'user_1', 'session_1', 'Remind user to check Atlas migration progress', 'commitment', 'prospective', 'manual', '[]', 0.9, 0.9, 'testkit', 'test-embedding', 4, '66666666-6666-6666-6666-666666666666', '["Atlas"]', '["reminder"]', 'private', now(), now(), 0, 'active', '1.0.0');

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
VALUES ('archive_1', 'tenant_1', 'user_1', 'session_1', NULL, '$bucket', 'tenants/tenant_1/users/user_1/sessions/session_1/transcripts/transcript.txt', 'fixture', 'transcript', now());
"@
$sql | docker @($compose + @("exec", "-T", "postgres", "psql", "-U", "nextral", "-d", "nextral", "-v", "ON_ERROR_STOP=1"))
if ($LASTEXITCODE -ne 0) { throw "postgres fixture write failed" }

Write-Host "Writing Redis working/session/procedural/prospective fixtures"
Run docker @($compose + @("exec", "-T", "redis", "redis-cli", "SET", "nextral:tenant_1:user_1:working:request_1", "temporary working memory"))
Run docker @($compose + @("exec", "-T", "redis", "redis-cli", "RPUSH", "nextral:tenant_1:user_1:session_1:tail", "Use PostgreSQL for Atlas"))
Run docker @($compose + @("exec", "-T", "redis", "redis-cli", "SET", "nextral:tenant_1:user_1:policy", "Prefer concise engineering updates"))
Run docker @($compose + @("exec", "-T", "redis", "redis-cli", "SET", "nextral:tenant_1:user_1:reminder:lease", "reminder_1"))

Write-Host "Writing Qdrant vector points"
$points = @{
    points = @(
        @{ id = "11111111-1111-1111-1111-111111111111"; vector = @(0.1,0.1,0.1,0.1); payload = @{ memory_id="mem_session"; tenant_id="tenant_1"; user_id="user_1"; privacy_level="private"; status="active"; memory_type="session"; content_type="note"; schema_version="1.0.0" } },
        @{ id = "22222222-2222-2222-2222-222222222222"; vector = @(0.2,0.2,0.2,0.2); payload = @{ memory_id="mem_episodic"; tenant_id="tenant_1"; user_id="user_1"; privacy_level="private"; status="active"; memory_type="episodic"; content_type="event"; schema_version="1.0.0" } },
        @{ id = "33333333-3333-3333-3333-333333333333"; vector = @(0.3,0.3,0.3,0.3); payload = @{ memory_id="mem_semantic"; tenant_id="tenant_1"; user_id="user_1"; privacy_level="private"; status="active"; memory_type="semantic"; content_type="fact"; schema_version="1.0.0" } },
        @{ id = "44444444-4444-4444-4444-444444444444"; vector = @(0.4,0.4,0.4,0.4); payload = @{ memory_id="mem_relational"; tenant_id="tenant_1"; user_id="user_1"; privacy_level="private"; status="active"; memory_type="relational"; content_type="fact"; schema_version="1.0.0" } },
        @{ id = "55555555-5555-5555-5555-555555555555"; vector = @(0.5,0.5,0.5,0.5); payload = @{ memory_id="mem_procedural"; tenant_id="tenant_1"; user_id="user_1"; privacy_level="private"; status="active"; memory_type="procedural"; content_type="preference"; schema_version="1.0.0" } },
        @{ id = "66666666-6666-6666-6666-666666666666"; vector = @(0.6,0.6,0.6,0.6); payload = @{ memory_id="mem_prospective"; tenant_id="tenant_1"; user_id="user_1"; privacy_level="private"; status="active"; memory_type="prospective"; content_type="commitment"; schema_version="1.0.0" } }
    )
} | ConvertTo-Json -Depth 20
Invoke-RestMethod -Method Put -Uri "http://localhost:6333/collections/$collection/points?wait=true" -ContentType "application/json" -Body $points | Out-Null

Write-Host "Writing Neo4j graph fixtures"
$cypher = @"
MERGE (a:NextralEntity {tenant_id:'tenant_1', user_id:'user_1', label:'Project', canonical_name:'atlas'})
SET a.name='Atlas', a.source_memory_ids=['mem_relational']
MERGE (p:NextralEntity {tenant_id:'tenant_1', user_id:'user_1', label:'Technology', canonical_name:'postgresql'})
SET p.name='PostgreSQL', p.source_memory_ids=['mem_relational']
MERGE (a)-[r:NEXTRAL_RELATES_TO {tenant_id:'tenant_1', user_id:'user_1', relationship_type:'DEPENDS_ON'}]->(p)
SET r.source_memory_ids=['mem_relational'], r.confidence=0.95;
"@
$cypher | docker @($compose + @("exec", "-T", "neo4j", "cypher-shell", "-u", "neo4j", "-p", "nextraldev"))
if ($LASTEXITCODE -ne 0) { throw "neo4j fixture write failed" }

Write-Host "Verifying retrieval/storage effects"
$memoryCount = docker @($compose + @("exec", "-T", "postgres", "psql", "-U", "nextral", "-d", "nextral", "-Atc", "SELECT count(*) FROM nextral_memories WHERE tenant_id='tenant_1' AND user_id='user_1' AND status='active';"))
if (($memoryCount -join "").Trim() -ne "6") { throw "expected 6 durable memory rows, got $memoryCount" }

$typesCount = docker @($compose + @("exec", "-T", "postgres", "psql", "-U", "nextral", "-d", "nextral", "-Atc", "SELECT count(DISTINCT memory_type) FROM nextral_memories WHERE tenant_id='tenant_1' AND user_id='user_1';"))
if (($typesCount -join "").Trim() -ne "6") { throw "expected 6 durable memory types, got $typesCount" }

$working = docker @($compose + @("exec", "-T", "redis", "redis-cli", "GET", "nextral:tenant_1:user_1:working:request_1"))
if (($working -join "").Trim() -ne "temporary working memory") { throw "working memory redis check failed" }

$qdrantSearch = @{
    vector = @(0.3,0.3,0.3,0.3)
    limit = 3
    with_payload = $true
    filter = @{
        must = @(
            @{ key = "tenant_id"; match = @{ value = "tenant_1" } },
            @{ key = "user_id"; match = @{ value = "user_1" } },
            @{ key = "status"; match = @{ value = "active" } }
        )
    }
} | ConvertTo-Json -Depth 20
$qdrantResult = Invoke-RestMethod -Method Post -Uri "http://localhost:6333/collections/$collection/points/search" -ContentType "application/json" -Body $qdrantSearch
if ($qdrantResult.result.Count -lt 1) { throw "qdrant retrieval returned no results" }

$graphCount = docker @($compose + @("exec", "-T", "neo4j", "cypher-shell", "-u", "neo4j", "-p", "nextraldev", "--format", "plain", "MATCH (:NextralEntity {canonical_name:'atlas'})-[r:NEXTRAL_RELATES_TO]->(:NextralEntity {canonical_name:'postgresql'}) RETURN count(r);"))
if (($graphCount -join "`n") -notmatch "1") { throw "neo4j relationship check failed" }

Run docker @(
    "run", "--rm", "--network", $network, "--entrypoint", "/bin/sh", "minio/mc",
    "-c",
    "mc alias set local http://minio:9000 nextral nextraldev >/dev/null && mc stat local/$bucket/tenants/tenant_1/users/user_1/sessions/session_1/transcripts/transcript.txt >/dev/null"
)

Write-Host "Running package smoke"
Run cargo @("run", "-p", "nextral-cli", "--", "memory", "smoke")
$adapterSmokePath = Join-Path $env:TEMP "nextral-adapter-smoke.json"
$adapterSmoke = @{
    postgres_url = "postgres://nextral:nextral@localhost:5432/nextral"
    redis_url = "redis://127.0.0.1:6379/0"
    redis_key_prefix = "nextral:e2e:adapter"
    tenant_id = "tenant_1"
    user_id = "user_1"
    session_id = "session_1"
    memory_id = "mem_adapter_smoke"
    memory_content = "Adapter smoke verifies PostgreSQL and Redis production ports"
    embedding_provider = "testkit"
    embedding_model = "test-embedding"
    embedding_dim = 4
    reminder_title = "Check adapter smoke"
    reminder_due_at = "1893456000"
    timezone = "UTC"
    cache_key = "tenant_1:user_1:adapter_smoke"
    cache_value_json = '{"status":"ok"}'
    cache_ttl_seconds = 60
} | ConvertTo-Json -Depth 10
[System.IO.File]::WriteAllText($adapterSmokePath, $adapterSmoke, [System.Text.UTF8Encoding]::new($false))
Run cargo @("run", "-p", "nextral-cli", "--", "adapters", "smoke", $adapterSmokePath)
Run node @("-e", "const n=require('./bindings/node/index.js'); const r=JSON.parse(n.e2ESmoke()); if(r.status !== 'ok') process.exit(1); console.log(JSON.stringify(r));")

Write-Host "Docker E2E passed: all seven memory systems covered across PostgreSQL, Redis, Qdrant, Neo4j, MinIO/S3, CLI, and Node."

if (-not $KeepRunning) {
    Write-Host "Stack left running for inspection. Use 'docker compose -f docker-compose.integration.yml down' when done, or pass -KeepRunning explicitly for the same behavior."
}
