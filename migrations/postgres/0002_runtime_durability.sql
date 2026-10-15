ALTER TABLE nextral_memories ADD COLUMN IF NOT EXISTS record_json JSONB;
ALTER TABLE nextral_session_messages ADD COLUMN IF NOT EXISTS record_json JSONB;
ALTER TABLE nextral_session_messages ADD COLUMN IF NOT EXISTS sequence BIGSERIAL;
ALTER TABLE nextral_session_messages ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
UPDATE nextral_session_messages SET idempotency_key=id WHERE idempotency_key IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_nextral_message_idempotency ON nextral_session_messages(tenant_id,user_id,session_id,idempotency_key);
CREATE INDEX IF NOT EXISTS idx_nextral_message_sequence ON nextral_session_messages(tenant_id,user_id,session_id,sequence DESC);
ALTER TABLE nextral_reminders ADD COLUMN IF NOT EXISTS record_json JSONB;
ALTER TABLE nextral_outbox_events ADD COLUMN IF NOT EXISTS locked_by TEXT;
ALTER TABLE nextral_outbox_events ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ;
ALTER TABLE nextral_outbox_events ADD COLUMN IF NOT EXISTS last_error TEXT;
CREATE TABLE IF NOT EXISTS nextral_runtime_state (
 tenant_id TEXT NOT NULL, key TEXT NOT NULL, value JSONB NOT NULL,
 locked_by TEXT, locked_until TIMESTAMPTZ, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY (tenant_id,key)
);
UPDATE nextral_memories m SET record_json =
 (to_jsonb(m) - 'record_json' - 'vector_point_id') ||
 jsonb_build_object('created_at',floor(extract(epoch from created_at))::bigint::text,
 'updated_at',floor(extract(epoch from updated_at))::bigint::text,
 'last_accessed_at',CASE WHEN last_accessed_at IS NULL THEN NULL ELSE floor(extract(epoch from last_accessed_at))::bigint::text END,
 'embedding',NULL)
 WHERE record_json IS NULL;
UPDATE nextral_session_messages m SET record_json =
 (to_jsonb(m) - 'record_json') ||
 jsonb_build_object('created_at',floor(extract(epoch from created_at))::bigint::text,
 'idempotency_key',id,'privacy_level','private')
 WHERE record_json IS NULL;
UPDATE nextral_reminders r SET record_json =
 (to_jsonb(r) - 'record_json') ||
 jsonb_build_object('due_at',floor(extract(epoch from due_at))::bigint::text,
 'created_at',floor(extract(epoch from created_at))::bigint::text,
 'updated_at',floor(extract(epoch from updated_at))::bigint::text,
 'last_attempt_at',CASE WHEN last_attempt_at IS NULL THEN NULL ELSE floor(extract(epoch from last_attempt_at))::bigint::text END,
 'next_attempt_at',CASE WHEN next_attempt_at IS NULL THEN NULL ELSE floor(extract(epoch from next_attempt_at))::bigint::text END)
 WHERE record_json IS NULL;
DO $$ DECLARE name TEXT; BEGIN
 FOREACH name IN ARRAY ARRAY['nextral_memories','nextral_session_messages','nextral_session_summaries','nextral_reminders','nextral_audit_events','nextral_idempotency_keys','nextral_jobs','nextral_outbox_events','nextral_archive_objects','nextral_runtime_state'] LOOP
  EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',name);
  EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',name);
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename=name AND policyname='nextral_tenant_scope') THEN
   EXECUTE format('CREATE POLICY nextral_tenant_scope ON %I USING (tenant_id = current_setting(''nextral.tenant_id'', true)) WITH CHECK (tenant_id = current_setting(''nextral.tenant_id'', true))',name);
  END IF;
 END LOOP;
END $$;
DO $$ DECLARE name TEXT; BEGIN
 FOREACH name IN ARRAY ARRAY['nextral_memories','nextral_session_messages','nextral_session_summaries','nextral_reminders'] LOOP
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename=name AND policyname='nextral_user_scope') THEN
   EXECUTE format('CREATE POLICY nextral_user_scope ON %I AS RESTRICTIVE USING (user_id = current_setting(''nextral.user_id'', true)) WITH CHECK (user_id = current_setting(''nextral.user_id'', true))',name);
  END IF;
 END LOOP;
END $$;
