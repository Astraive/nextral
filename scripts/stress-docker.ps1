param(
    [switch]$SkipStart,
    [int]$PerDurableType = 120,
    [int]$WorkingEntries = 180,
    [int]$ArchiveObjects = 72
)

$ErrorActionPreference = "Stop"

$compose = @("compose", "-f", "docker-compose.integration.yml")
$network = "nextral_default"
$collection = "nextral_stress_memories"
$bucket = "nextral-memory-stress"
$expectedDurable = $PerDurableType * 6

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

function Assert-Equal($Name, [string]$Actual, [string]$Expected) {
    if ($Actual.Trim() -ne $Expected) {
        throw "$Name expected $Expected, got $Actual"
    }
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

Write-Host "Applying migrations"
Get-Content -Raw migrations/postgres/0001_core_schema.sql |
    docker @($compose + @("exec", "-T", "postgres", "psql", "-U", "nextral", "-d", "nextral", "-v", "ON_ERROR_STOP=1"))
if ($LASTEXITCODE -ne 0) { throw "postgres migration failed" }

Get-Content -Raw migrations/neo4j/0001_graph_schema.cypher |
    docker @($compose + @("exec", "-T", "neo4j", "cypher-shell", "-u", "neo4j", "-p", "nextraldev"))
if ($LASTEXITCODE -ne 0) { throw "neo4j schema failed" }

Write-Host "Resetting stress collection and object bucket"
try {
    Invoke-RestMethod -Method Delete -Uri "http://localhost:6333/collections/$collection" | Out-Null
} catch {}
$qdrantCollection = @{ vectors = @{ size = 4; distance = "Cosine" } } | ConvertTo-Json -Depth 10
Invoke-RestMethod -Method Put -Uri "http://localhost:6333/collections/$collection" -ContentType "application/json" -Body $qdrantCollection | Out-Null

Run docker @(
    "run", "--rm", "--network", $network, "--entrypoint", "/bin/sh", "minio/mc",
    "-c",
    "mc alias set local http://minio:9000 nextral nextraldev >/dev/null && mc rb --force local/$bucket >/dev/null 2>&1 || true && mc mb -p local/$bucket >/dev/null"
)

Write-Host "Seeding PostgreSQL with $expectedDurable durable memories"
$sql = @"
TRUNCATE nextral_archive_objects, nextral_outbox_events, nextral_jobs, nextral_idempotency_keys, nextral_audit_events, nextral_reminders, nextral_session_summaries, nextral_session_messages, nextral_memories RESTART IDENTITY CASCADE;

WITH specs(memory_type, content_type, source_type) AS (
  VALUES
    ('session', 'note', 'realtime'),
    ('episodic', 'event', 'fast_lane'),
    ('semantic', 'fact', 'manual'),
    ('relational', 'fact', 'manual'),
    ('procedural', 'preference', 'manual'),
    ('prospective', 'commitment', 'manual')
),
series AS (
  SELECT specs.*, generate_series(1, $PerDurableType) AS n
  FROM specs
)
INSERT INTO nextral_memories (
  id, tenant_id, user_id, session_id, content, content_type, memory_type, source_type,
  source_message_ids, importance_score, confidence_score, embedding_provider, embedding_model,
  embedding_dim, vector_point_id, entities, tags, privacy_level, created_at, updated_at,
  access_count, status, schema_version
)
SELECT
  format('stress_%s_%s', memory_type, n),
  format('tenant_%s', ((n - 1) % 3) + 1),
  format('user_%s', ((n - 1) % 12) + 1),
  format('session_%s', ((n - 1) % 24) + 1),
  format('Stress %s memory %s for Atlas PostgreSQL Qdrant Neo4j retrieval', memory_type, n),
  content_type,
  memory_type,
  source_type,
  jsonb_build_array(format('msg_%s', n)),
  0.50 + ((n % 50)::real / 100),
  0.60 + ((n % 40)::real / 100),
  'testkit',
  'stress-embedding',
  4,
  format('stress_point_%s_%s', memory_type, n),
  jsonb_build_array('Atlas', memory_type, format('Entity%s', n % 20)),
  jsonb_build_array(memory_type, 'stress'),
  CASE WHEN n % 5 = 0 THEN 'shared' ELSE 'private' END,
  now() - (n || ' minutes')::interval,
  now(),
  n % 17,
  'active',
  '1.0.0'
FROM series;

INSERT INTO nextral_session_messages (id, tenant_id, user_id, session_id, role, content, created_at)
SELECT
  format('stress_msg_%s', n),
  format('tenant_%s', ((n - 1) % 3) + 1),
  format('user_%s', ((n - 1) % 12) + 1),
  format('session_%s', ((n - 1) % 24) + 1),
  CASE WHEN n % 2 = 0 THEN 'assistant' ELSE 'user' END,
  format('Stress session message %s asks about Atlas storage retrieval', n),
  now() - (n || ' seconds')::interval
FROM generate_series(1, $PerDurableType) AS n;

INSERT INTO nextral_reminders (
  id, tenant_id, user_id, source_memory_id, kind, title, details, due_at, timezone,
  priority, status, attempt_count, next_attempt_at, dedupe_key, created_at, updated_at
)
SELECT
  format('stress_reminder_%s', n),
  format('tenant_%s', ((n - 1) % 3) + 1),
  format('user_%s', ((n - 1) % 12) + 1),
  format('stress_prospective_%s', n),
  CASE WHEN n % 3 = 0 THEN 'task' WHEN n % 3 = 1 THEN 'follow_up' ELSE 'commitment' END,
  format('Stress reminder %s', n),
  'Verify prospective memory due flow',
  now() + (n || ' minutes')::interval,
  'UTC',
  CASE WHEN n % 4 = 0 THEN 'high' ELSE 'normal' END,
  'scheduled',
  0,
  now() + (n || ' minutes')::interval,
  format('stress_dedupe_%s', n),
  now(),
  now()
FROM generate_series(1, $PerDurableType) AS n;

INSERT INTO nextral_audit_events (id, tenant_id, user_id, actor_id, action, target_type, target_id, reason, metadata, created_at)
SELECT
  format('stress_audit_%s', n),
  format('tenant_%s', ((n - 1) % 3) + 1),
  format('user_%s', ((n - 1) % 12) + 1),
  'stress-seed',
  'write_accepted',
  'memory',
  format('stress_semantic_%s', n),
  'stress fixture',
  jsonb_build_object('batch', 'large-memory-stress'),
  now()
FROM generate_series(1, $PerDurableType) AS n;

INSERT INTO nextral_archive_objects (id, tenant_id, user_id, session_id, memory_id, bucket, object_key, content_sha256, object_kind, created_at)
SELECT
  format('stress_archive_%s', n),
  format('tenant_%s', ((n - 1) % 3) + 1),
  format('user_%s', ((n - 1) % 12) + 1),
  format('session_%s', ((n - 1) % 24) + 1),
  NULL,
  '$bucket',
  format('stress/tenant_%s/session_%s/archive_%s.txt', ((n - 1) % 3) + 1, ((n - 1) % 24) + 1, n),
  format('stress-sha-%s', n),
  'transcript',
  now()
FROM generate_series(1, $ArchiveObjects) AS n;
"@
$sql | docker @($compose + @("exec", "-T", "postgres", "psql", "-U", "nextral", "-d", "nextral", "-v", "ON_ERROR_STOP=1"))
if ($LASTEXITCODE -ne 0) { throw "postgres stress seed failed" }

Write-Host "Seeding Redis with $WorkingEntries working/cache entries"
$redisCommands = New-Object System.Text.StringBuilder
for ($i = 1; $i -le $WorkingEntries; $i++) {
    $tenant = "tenant_$((($i - 1) % 3) + 1)"
    $user = "user_$((($i - 1) % 12) + 1)"
    [void]$redisCommands.AppendLine("SET nextral:stress:$tenant`:$user`:working:request_$i working-memory-$i")
    [void]$redisCommands.AppendLine("RPUSH nextral:stress:$tenant`:$user`:session_tail session-message-$i")
    [void]$redisCommands.AppendLine("SETEX nextral:stress:$tenant`:$user`:policy 600 procedural-policy-$i")
    [void]$redisCommands.AppendLine("SETEX nextral:stress:$tenant`:$user`:due_lease:$i 600 reminder-$i")
}
$redisCommands.ToString() | docker @($compose + @("exec", "-T", "redis", "redis-cli", "--pipe"))
if ($LASTEXITCODE -ne 0) { throw "redis stress seed failed" }

Write-Host "Seeding Qdrant with $expectedDurable vector points"
$points = New-Object System.Collections.Generic.List[object]
$pointId = 1
$types = @(
    @{ memory_type = "session"; content_type = "note" },
    @{ memory_type = "episodic"; content_type = "event" },
    @{ memory_type = "semantic"; content_type = "fact" },
    @{ memory_type = "relational"; content_type = "fact" },
    @{ memory_type = "procedural"; content_type = "preference" },
    @{ memory_type = "prospective"; content_type = "commitment" }
)
foreach ($type in $types) {
    for ($i = 1; $i -le $PerDurableType; $i++) {
        $tenant = "tenant_$((($i - 1) % 3) + 1)"
        $user = "user_$((($i - 1) % 12) + 1)"
        $base = ($i % 100) / 100.0
        $points.Add(@{
            id = $pointId
            vector = @($base, ($base + 0.1), ($base + 0.2), ($base + 0.3))
            payload = @{
                memory_id = "stress_$($type.memory_type)_$i"
                tenant_id = $tenant
                user_id = $user
                privacy_level = $(if ($i % 5 -eq 0) { "shared" } else { "private" })
                status = "active"
                memory_type = $type.memory_type
                content_type = $type.content_type
                schema_version = "1.0.0"
            }
        })
        $pointId += 1
    }
}
$qdrantPayload = @{ points = $points } | ConvertTo-Json -Depth 20
Invoke-RestMethod -Method Put -Uri "http://localhost:6333/collections/$collection/points?wait=true" -ContentType "application/json" -Body $qdrantPayload | Out-Null

Write-Host "Seeding Neo4j with $PerDurableType relational paths"
$cypher = @"
MATCH (n:NextralEntity) WHERE n.tenant_id STARTS WITH 'tenant_' DETACH DELETE n;
UNWIND range(1, $PerDurableType) AS n
WITH n, 'tenant_' + toString(((n - 1) % 3) + 1) AS tenant, 'user_' + toString(((n - 1) % 12) + 1) AS user
MERGE (project:NextralEntity {tenant_id: tenant, user_id: user, label: 'Project', canonical_name: 'atlas_' + toString(n)})
SET project.name = 'Atlas ' + toString(n), project.source_memory_ids = ['stress_relational_' + toString(n)]
MERGE (store:NextralEntity {tenant_id: tenant, user_id: user, label: 'Store', canonical_name: 'postgresql_' + toString(n)})
SET store.name = 'PostgreSQL ' + toString(n), store.source_memory_ids = ['stress_relational_' + toString(n)]
MERGE (project)-[r:NEXTRAL_RELATES_TO {tenant_id: tenant, user_id: user, relationship_type: 'USES'}]->(store)
SET r.source_memory_ids = ['stress_relational_' + toString(n)], r.confidence = 0.91, r.last_confirmed_at = datetime();
"@
$cypher | docker @($compose + @("exec", "-T", "neo4j", "cypher-shell", "-u", "neo4j", "-p", "nextraldev"))
if ($LASTEXITCODE -ne 0) { throw "neo4j stress seed failed" }

Write-Host "Seeding MinIO with $ArchiveObjects archive objects"
$archiveScript = "mc alias set local http://minio:9000 nextral nextraldev >/dev/null;"
for ($i = 1; $i -le $ArchiveObjects; $i++) {
    $tenantIndex = (($i - 1) % 3) + 1
    $sessionIndex = (($i - 1) % 24) + 1
    $archiveScript += " printf 'stress archive $i' | mc pipe local/$bucket/stress/tenant_$tenantIndex/session_$sessionIndex/archive_$i.txt >/dev/null;"
}
Run docker @("run", "--rm", "--network", $network, "--entrypoint", "/bin/sh", "minio/mc", "-c", $archiveScript)

Write-Host "Verifying stress seed across storage backends"
$memoryCount = docker @($compose + @("exec", "-T", "postgres", "psql", "-U", "nextral", "-d", "nextral", "-Atc", "SELECT count(*) FROM nextral_memories WHERE id LIKE 'stress_%';"))
Assert-Equal "postgres memory count" (($memoryCount -join "").Trim()) "$expectedDurable"

$typeCount = docker @($compose + @("exec", "-T", "postgres", "psql", "-U", "nextral", "-d", "nextral", "-Atc", "SELECT count(DISTINCT memory_type) FROM nextral_memories WHERE id LIKE 'stress_%';"))
Assert-Equal "postgres memory type count" (($typeCount -join "").Trim()) "6"

$reminderCount = docker @($compose + @("exec", "-T", "postgres", "psql", "-U", "nextral", "-d", "nextral", "-Atc", "SELECT count(*) FROM nextral_reminders WHERE id LIKE 'stress_reminder_%';"))
Assert-Equal "postgres reminder count" (($reminderCount -join "").Trim()) "$PerDurableType"

$redisWorkingCount = docker @($compose + @("exec", "-T", "redis", "redis-cli", "--raw", "KEYS", "nextral:stress:*:working:*"))
$redisWorkingLines = @($redisWorkingCount | Where-Object { $_.Trim() -ne "" })
if ($redisWorkingLines.Count -ne $WorkingEntries) {
    throw "redis working-memory count expected $WorkingEntries, got $($redisWorkingLines.Count)"
}

$countPayload = @{
    exact = $true
    filter = @{ must = @(@{ key = "status"; match = @{ value = "active" } }) }
} | ConvertTo-Json -Depth 10
$qdrantCount = Invoke-RestMethod -Method Post -Uri "http://localhost:6333/collections/$collection/points/count" -ContentType "application/json" -Body $countPayload
if ([int]$qdrantCount.result.count -ne $expectedDurable) {
    throw "qdrant count expected $expectedDurable, got $($qdrantCount.result.count)"
}

$qdrantSearch = @{
    vector = @(0.2, 0.3, 0.4, 0.5)
    limit = 20
    with_payload = $true
    filter = @{ must = @(
        @{ key = "tenant_id"; match = @{ value = "tenant_1" } },
        @{ key = "status"; match = @{ value = "active" } }
    ) }
} | ConvertTo-Json -Depth 20
$qdrantResult = Invoke-RestMethod -Method Post -Uri "http://localhost:6333/collections/$collection/points/search" -ContentType "application/json" -Body $qdrantSearch
if ($qdrantResult.result.Count -lt 20) { throw "qdrant search returned fewer than 20 results" }

$graphCount = docker @($compose + @("exec", "-T", "neo4j", "cypher-shell", "-u", "neo4j", "-p", "nextraldev", "--format", "plain", "MATCH ()-[r:NEXTRAL_RELATES_TO {relationship_type:'USES'}]->() RETURN count(r);"))
if (($graphCount -join "`n") -notmatch "$PerDurableType") { throw "neo4j stress relationship count failed: $graphCount" }

Run docker @(
    "run", "--rm", "--network", $network, "--entrypoint", "/bin/sh", "minio/mc",
    "-c",
    "mc alias set local http://minio:9000 nextral nextraldev >/dev/null && test `$(mc ls --recursive local/$bucket/stress | wc -l) -eq $ArchiveObjects"
)

Write-Host "Stress E2E passed: $expectedDurable durable memories, $WorkingEntries working/cache entries, $PerDurableType graph relationships, $ArchiveObjects archives."
