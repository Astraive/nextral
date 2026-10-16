-- Nextral 0.2: Fenced database leases, indexed lifecycle jobs, and scoped outbox.

ALTER TABLE nextral_runtime_state ADD COLUMN IF NOT EXISTS lease_fence BIGINT NOT NULL DEFAULT 0;
ALTER TABLE nextral_outbox_events ADD COLUMN IF NOT EXISTS lease_fence BIGINT NOT NULL DEFAULT 0;
ALTER TABLE nextral_outbox_events ADD COLUMN IF NOT EXISTS user_id TEXT;

-- Backfill user_id on nextral_outbox_events
DO $$
DECLARE
    r RECORD;
    extracted_user TEXT;
BEGIN
    FOR r IN SELECT id, tenant_id, event_type, payload FROM nextral_outbox_events WHERE user_id IS NULL LOOP
        IF r.event_type IN ('memory.sync', 'graph.replay') THEN
            extracted_user := r.payload->'record'->>'user_id';
            IF extracted_user IS NULL OR trim(extracted_user) = '' THEN
                extracted_user := r.payload->>'user_id';
            END IF;
        ELSIF r.event_type IN ('policy.invalidate', 'index.metadata.repair') THEN
            extracted_user := r.payload->>'user_id';
        ELSE
            RAISE EXCEPTION 'unsupported event type % for outbox event %', r.event_type, r.id;
        END IF;

        IF extracted_user IS NULL OR trim(extracted_user) = '' THEN
            RAISE EXCEPTION 'invalid or unattributable user_id for outbox event %', r.id;
        END IF;

        IF r.payload->'record'->>'tenant_id' IS NOT NULL AND r.payload->'record'->>'tenant_id' != r.tenant_id THEN
            RAISE EXCEPTION 'tenant mismatch in outbox event %', r.id;
        END IF;

        IF r.payload->>'tenant_id' IS NOT NULL AND r.payload->>'tenant_id' != r.tenant_id THEN
            RAISE EXCEPTION 'tenant mismatch in outbox event %', r.id;
        END IF;

        UPDATE nextral_outbox_events SET user_id = extracted_user WHERE id = r.id;
    END LOOP;
END $$;

CREATE INDEX IF NOT EXISTS idx_nextral_outbox_due
    ON nextral_outbox_events (tenant_id, user_id, status, locked_until, next_attempt_at, created_at);

-- Dedicated table for lifecycle jobs, preserving generic nextral_jobs table untouched.
CREATE TABLE IF NOT EXISTS nextral_lifecycle_jobs (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    id TEXT NOT NULL,
    request JSONB NOT NULL,
    progress JSONB NOT NULL DEFAULT '{}',
    status TEXT NOT NULL,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by TEXT,
    locked_until TIMESTAMPTZ,
    lease_fence BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, user_id, id)
);

CREATE INDEX IF NOT EXISTS idx_nextral_lifecycle_jobs_due
    ON nextral_lifecycle_jobs (tenant_id, user_id, status, next_attempt_at, id);

-- Migrate validated scheduled-job/... runtime-state records
DO $$
DECLARE
    r RECORD;
    parts TEXT[];
    parsed_user TEXT;
    parsed_id TEXT;
    job_id TEXT;
    job_status TEXT;
    job_request JSONB;
    job_attempts INT;
    job_next_attempt TIMESTAMPTZ;
    job_updated TIMESTAMPTZ;
BEGIN
    FOR r IN SELECT tenant_id, key, value, locked_by, locked_until, lease_fence FROM nextral_runtime_state WHERE key LIKE 'scheduled-job/%' LOOP
        parts := string_to_array(r.key, '/');
        IF array_length(parts, 1) != 3 THEN
            RAISE EXCEPTION 'corrupt scheduled-job key % for tenant %', r.key, r.tenant_id;
        END IF;
        parsed_user := parts[2];
        parsed_id := parts[3];

        IF parsed_user IS NULL OR trim(parsed_user) = '' OR parsed_id IS NULL OR trim(parsed_id) = '' THEN
            RAISE EXCEPTION 'corrupt scheduled-job user or id in key %', r.key;
        END IF;

        job_id := r.value->>'id';
        IF job_id IS NULL OR job_id != parsed_id THEN
            RAISE EXCEPTION 'corrupt scheduled-job id mismatch in key %: expected % got %', r.key, parsed_id, job_id;
        END IF;

        IF r.value->'request'->>'user_id' IS NOT NULL AND r.value->'request'->>'user_id' != parsed_user THEN
            RAISE EXCEPTION 'corrupt scheduled-job request user mismatch in key %: % vs %', r.key, parsed_user, r.value->'request'->>'user_id';
        END IF;

        IF r.value->'request'->>'tenant_id' IS NOT NULL AND r.value->'request'->>'tenant_id' != r.tenant_id THEN
            RAISE EXCEPTION 'corrupt scheduled-job request tenant mismatch in key %', r.key;
        END IF;

        job_status := r.value->>'status';
        IF job_status NOT IN ('queued', 'running', 'retry_pending', 'terminal_failed', 'completed', 'cancelled') THEN
            RAISE EXCEPTION 'corrupt scheduled-job status % in key %', job_status, r.key;
        END IF;

        job_request := r.value->'request';
        IF job_request IS NULL THEN
            RAISE EXCEPTION 'corrupt scheduled-job missing request in key %', r.key;
        END IF;

        job_attempts := COALESCE((r.value->>'attempts')::int, 0);
        BEGIN
            job_next_attempt := (r.value->>'next_attempt_at')::timestamptz;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'corrupt scheduled-job invalid next_attempt_at in key %: %', r.key, r.value->>'next_attempt_at';
        END;

        IF job_next_attempt IS NULL THEN
            RAISE EXCEPTION 'corrupt scheduled-job null next_attempt_at in key %', r.key;
        END IF;

        BEGIN
            job_updated := (r.value->>'updated_at')::timestamptz;
        EXCEPTION WHEN OTHERS THEN
            job_updated := now();
        END;
        IF job_updated IS NULL THEN
            job_updated := now();
        END IF;

        INSERT INTO nextral_lifecycle_jobs (
            tenant_id, user_id, id, request, progress, status, next_attempt_at,
            attempts, updated_at, locked_by, locked_until, lease_fence
        ) VALUES (
            r.tenant_id, parsed_user, parsed_id, job_request,
            COALESCE(r.value->'progress', '{}'::jsonb),
            job_status, job_next_attempt, job_attempts, job_updated,
            r.locked_by, r.locked_until, r.lease_fence
        );

        DELETE FROM nextral_runtime_state WHERE tenant_id = r.tenant_id AND key = r.key;
    END LOOP;
END $$;

-- Enable and force Row Level Security
ALTER TABLE nextral_lifecycle_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE nextral_lifecycle_jobs FORCE ROW LEVEL SECURITY;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename='nextral_lifecycle_jobs' AND policyname='nextral_tenant_scope') THEN
        CREATE POLICY nextral_tenant_scope ON nextral_lifecycle_jobs
            USING (tenant_id = current_setting('nextral.tenant_id', true))
            WITH CHECK (tenant_id = current_setting('nextral.tenant_id', true));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename='nextral_lifecycle_jobs' AND policyname='nextral_user_scope') THEN
        CREATE POLICY nextral_user_scope ON nextral_lifecycle_jobs AS RESTRICTIVE
            USING (user_id = current_setting('nextral.user_id', true))
            WITH CHECK (user_id = current_setting('nextral.user_id', true));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename='nextral_outbox_events' AND policyname='nextral_user_scope') THEN
        CREATE POLICY nextral_user_scope ON nextral_outbox_events AS RESTRICTIVE
            USING (user_id IS NULL OR user_id = current_setting('nextral.user_id', true) OR current_setting('nextral.user_id', true) = '')
            WITH CHECK (user_id IS NULL OR user_id = current_setting('nextral.user_id', true) OR current_setting('nextral.user_id', true) = '');
    END IF;
END $$;
