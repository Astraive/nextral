-- Nextral production memory schema.
-- Values such as retention windows, cache TTLs, model names, and service URLs live in runtime config.

CREATE TABLE IF NOT EXISTS nextral_memories (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT,
    content TEXT NOT NULL,
    content_type TEXT NOT NULL,
    memory_type TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_message_ids JSONB NOT NULL DEFAULT '[]',
    importance_score REAL NOT NULL CHECK (importance_score >= 0 AND importance_score <= 1),
    confidence_score REAL CHECK (confidence_score >= 0 AND confidence_score <= 1),
    embedding_provider TEXT NOT NULL,
    embedding_model TEXT NOT NULL,
    embedding_dim INTEGER NOT NULL CHECK (embedding_dim > 0),
    vector_point_id TEXT,
    entities JSONB NOT NULL DEFAULT '[]',
    tags JSONB NOT NULL DEFAULT '[]',
    privacy_level TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    last_accessed_at TIMESTAMPTZ,
    access_count BIGINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    schema_version TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nextral_memories_user_time
    ON nextral_memories (tenant_id, user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_nextral_memories_user_type_time
    ON nextral_memories (tenant_id, user_id, content_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_nextral_memories_active
    ON nextral_memories (tenant_id, user_id, status)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_nextral_memories_privacy
    ON nextral_memories (tenant_id, user_id, privacy_level, status);

CREATE INDEX IF NOT EXISTS idx_nextral_memories_fts
    ON nextral_memories USING GIN (to_tsvector('simple', content));

CREATE TABLE IF NOT EXISTS nextral_session_messages (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    source_metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nextral_session_messages_tail
    ON nextral_session_messages (tenant_id, user_id, session_id, created_at DESC);

CREATE TABLE IF NOT EXISTS nextral_session_summaries (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    summary TEXT NOT NULL,
    source_message_start_id TEXT,
    source_message_end_id TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nextral_session_summaries_session
    ON nextral_session_summaries (tenant_id, user_id, session_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS nextral_reminders (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    source_memory_id TEXT NOT NULL REFERENCES nextral_memories(id),
    kind TEXT NOT NULL,
    title TEXT NOT NULL,
    details TEXT NOT NULL DEFAULT '',
    due_at TIMESTAMPTZ NOT NULL,
    timezone TEXT NOT NULL,
    priority TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ,
    dedupe_key TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nextral_reminders_due
    ON nextral_reminders (tenant_id, user_id, status, next_attempt_at);

CREATE TABLE IF NOT EXISTS nextral_audit_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT,
    reason TEXT NOT NULL,
    request_id TEXT,
    trace_id TEXT,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nextral_audit_target
    ON nextral_audit_events (tenant_id, target_type, target_id, created_at DESC);

CREATE TABLE IF NOT EXISTS nextral_idempotency_keys (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_nextral_idempotency_unique
    ON nextral_idempotency_keys (tenant_id, operation, request_hash);

CREATE TABLE IF NOT EXISTS nextral_jobs (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    payload JSONB NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    locked_by TEXT,
    locked_until TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nextral_jobs_ready
    ON nextral_jobs (tenant_id, kind, status, locked_until, created_at);

CREATE TABLE IF NOT EXISTS nextral_outbox_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nextral_outbox_ready
    ON nextral_outbox_events (tenant_id, status, next_attempt_at, created_at);

CREATE TABLE IF NOT EXISTS nextral_archive_objects (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT,
    memory_id TEXT,
    bucket TEXT NOT NULL,
    object_key TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    object_kind TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nextral_archive_lookup
    ON nextral_archive_objects (tenant_id, user_id, session_id, memory_id);
