-- Nextral 0.4: Scoped descending keyset and GIN tag/entity indexes for bounded memory browsing and filtering.

CREATE INDEX IF NOT EXISTS idx_nextral_memories_keyset
    ON nextral_memories (tenant_id, user_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_nextral_memories_tags
    ON nextral_memories USING GIN (tags);

CREATE INDEX IF NOT EXISTS idx_nextral_memories_entities
    ON nextral_memories USING GIN (entities);

CREATE INDEX IF NOT EXISTS idx_nextral_memories_session_time
    ON nextral_memories (tenant_id, user_id, session_id, created_at DESC)
    WHERE session_id IS NOT NULL;
