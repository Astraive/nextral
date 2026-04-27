use crate::{
    adapters::AdapterHealth,
    contracts::{CoreError, CoreResult},
    memory::{
        deterministic_id, ContentType, MemoryRecord, MemoryStatus, MemoryType, PrivacyLevel,
        SourceType,
    },
    ports::{AuditWrite, PostgresPort, TenantUserScope},
    prospective::ReminderRecord,
};
use postgres::{Client, NoTls, Row};
use serde::de::DeserializeOwned;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresAdapter {
    pub url: String,
}

impl PostgresAdapter {
    pub fn new(url: impl Into<String>) -> CoreResult<Self> {
        let url = url.into();
        if url.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "postgres_url is required".to_string(),
            ));
        }
        Ok(Self { url })
    }

    pub fn migration_sql(&self) -> &'static str {
        include_str!("../../migrations/postgres/0001_core_schema.sql")
    }

    pub fn health(&self) -> AdapterHealth {
        AdapterHealth::configured("postgres")
    }

    pub fn migrate(&self) -> CoreResult<()> {
        let mut client = self.client()?;
        client
            .batch_execute(self.migration_sql())
            .map_err(|error| CoreError::Io(error.to_string()))?;
        Ok(())
    }

    fn client(&self) -> CoreResult<Client> {
        Client::connect(&self.url, NoTls).map_err(|error| CoreError::Io(error.to_string()))
    }
}

impl PostgresPort for PostgresAdapter {
    fn upsert_memory(&self, record: &MemoryRecord) -> CoreResult<()> {
        record.validate()?;
        let mut client = self.client()?;
        let embedding_provider = record.embedding_provider.as_ref().ok_or_else(|| {
            CoreError::InvalidInput(
                "embedding_provider is required for postgres writes".to_string(),
            )
        })?;
        let embedding_model = record.embedding_model.as_ref().ok_or_else(|| {
            CoreError::InvalidInput("embedding_model is required for postgres writes".to_string())
        })?;
        let embedding_dim = record.embedding_dim.ok_or_else(|| {
            CoreError::InvalidInput("embedding_dim is required for postgres writes".to_string())
        })?;
        let vector_point_id: Option<String> = None;
        let source_message_ids = json_value(&record.source_message_ids)?;
        let entities = json_value(&record.entities)?;
        let tags = json_value(&record.tags)?;
        let created_at = epoch_seconds(&record.created_at, "created_at")?;
        let updated_at = epoch_seconds(&record.updated_at, "updated_at")?;
        let last_accessed_at =
            optional_epoch_seconds(record.last_accessed_at.as_deref(), "last_accessed_at")?;

        client
            .execute(
                r#"
                INSERT INTO nextral_memories (
                    id, tenant_id, user_id, session_id, content, content_type, memory_type,
                    source_type, source_message_ids, importance_score, confidence_score,
                    embedding_provider, embedding_model, embedding_dim, vector_point_id,
                    entities, tags, privacy_level, created_at, updated_at, last_accessed_at,
                    access_count, status, schema_version
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                    $12, $13, $14, $15, $16, $17, $18,
                    to_timestamp($19), to_timestamp($20), to_timestamp($21),
                    $22, $23, $24
                )
                ON CONFLICT (id) DO UPDATE SET
                    tenant_id = EXCLUDED.tenant_id,
                    user_id = EXCLUDED.user_id,
                    session_id = EXCLUDED.session_id,
                    content = EXCLUDED.content,
                    content_type = EXCLUDED.content_type,
                    memory_type = EXCLUDED.memory_type,
                    source_type = EXCLUDED.source_type,
                    source_message_ids = EXCLUDED.source_message_ids,
                    importance_score = EXCLUDED.importance_score,
                    confidence_score = EXCLUDED.confidence_score,
                    embedding_provider = EXCLUDED.embedding_provider,
                    embedding_model = EXCLUDED.embedding_model,
                    embedding_dim = EXCLUDED.embedding_dim,
                    vector_point_id = EXCLUDED.vector_point_id,
                    entities = EXCLUDED.entities,
                    tags = EXCLUDED.tags,
                    privacy_level = EXCLUDED.privacy_level,
                    updated_at = EXCLUDED.updated_at,
                    last_accessed_at = EXCLUDED.last_accessed_at,
                    access_count = EXCLUDED.access_count,
                    status = EXCLUDED.status,
                    schema_version = EXCLUDED.schema_version
                "#,
                &[
                    &record.id,
                    &record.tenant_id,
                    &record.user_id,
                    &record.session_id,
                    &record.content,
                    &enum_text(&record.content_type)?,
                    &enum_text(&record.memory_type)?,
                    &enum_text(&record.source_type)?,
                    &source_message_ids,
                    &record.importance_score,
                    &record.confidence_score,
                    embedding_provider,
                    embedding_model,
                    &(embedding_dim as i32),
                    &vector_point_id,
                    &entities,
                    &tags,
                    &enum_text(&record.privacy_level)?,
                    &created_at,
                    &updated_at,
                    &last_accessed_at,
                    &(record.access_count as i64),
                    &enum_text(&record.status)?,
                    &record.schema_version,
                ],
            )
            .map_err(|error| CoreError::Io(error.to_string()))?;
        Ok(())
    }

    fn get_memory(
        &self,
        scope: &TenantUserScope,
        memory_id: &str,
    ) -> CoreResult<Option<MemoryRecord>> {
        let mut client = self.client()?;
        let row = client
            .query_opt(
                r#"
                SELECT id, tenant_id, user_id, session_id, content, content_type, memory_type,
                    source_type, source_message_ids, importance_score, confidence_score,
                    embedding_provider, embedding_model, embedding_dim, entities, tags,
                    privacy_level, extract(epoch from created_at)::text,
                    extract(epoch from updated_at)::text,
                    COALESCE(extract(epoch from last_accessed_at)::text, ''),
                    access_count, status, schema_version
                FROM nextral_memories
                WHERE tenant_id = $1 AND user_id = $2 AND id = $3
                "#,
                &[&scope.tenant_id, &scope.user_id, &memory_id],
            )
            .map_err(|error| CoreError::Io(error.to_string()))?;
        row.map(row_to_memory).transpose()
    }

    fn append_session_message(
        &self,
        scope: &TenantUserScope,
        session_id: &str,
        role: &str,
        content: &str,
    ) -> CoreResult<String> {
        let id = deterministic_id(&[&scope.tenant_id, &scope.user_id, session_id, role, content]);
        let mut client = self.client()?;
        client
            .execute(
                r#"
                INSERT INTO nextral_session_messages
                    (id, tenant_id, user_id, session_id, role, content, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, now())
                ON CONFLICT (id) DO NOTHING
                "#,
                &[
                    &id,
                    &scope.tenant_id,
                    &scope.user_id,
                    &session_id,
                    &role,
                    &content,
                ],
            )
            .map_err(|error| CoreError::Io(error.to_string()))?;
        Ok(id)
    }

    fn upsert_reminder(&self, reminder: &ReminderRecord) -> CoreResult<()> {
        let due_at = epoch_seconds(&reminder.due_at, "due_at")?;
        let last_attempt_at =
            optional_epoch_seconds(reminder.last_attempt_at.as_deref(), "last_attempt_at")?;
        let next_attempt_at =
            optional_epoch_seconds(reminder.next_attempt_at.as_deref(), "next_attempt_at")?;
        let created_at = epoch_seconds(&reminder.created_at, "created_at")?;
        let updated_at = epoch_seconds(&reminder.updated_at, "updated_at")?;
        let mut client = self.client()?;
        client
            .execute(
                r#"
                INSERT INTO nextral_reminders (
                    id, tenant_id, user_id, source_memory_id, kind, title, details, due_at,
                    timezone, priority, status, attempt_count, last_attempt_at, next_attempt_at,
                    dedupe_key, created_at, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, to_timestamp($8),
                    $9, $10, $11, $12,
                    to_timestamp($13),
                    to_timestamp($14),
                    $15, to_timestamp($16), to_timestamp($17)
                )
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    details = EXCLUDED.details,
                    due_at = EXCLUDED.due_at,
                    timezone = EXCLUDED.timezone,
                    priority = EXCLUDED.priority,
                    status = EXCLUDED.status,
                    attempt_count = EXCLUDED.attempt_count,
                    last_attempt_at = EXCLUDED.last_attempt_at,
                    next_attempt_at = EXCLUDED.next_attempt_at,
                    updated_at = EXCLUDED.updated_at
                "#,
                &[
                    &reminder.id,
                    &reminder.tenant_id,
                    &reminder.user_id,
                    &reminder.source_memory_id,
                    &enum_text(&reminder.kind)?,
                    &reminder.title,
                    &reminder.details,
                    &due_at,
                    &reminder.timezone,
                    &enum_text(&reminder.priority)?,
                    &enum_text(&reminder.status)?,
                    &(reminder.attempt_count as i32),
                    &last_attempt_at,
                    &next_attempt_at,
                    &reminder.dedupe_key,
                    &created_at,
                    &updated_at,
                ],
            )
            .map_err(|error| CoreError::Io(error.to_string()))?;
        Ok(())
    }

    fn write_audit(&self, event: &AuditWrite) -> CoreResult<()> {
        let metadata = serde_json::from_str::<Value>(&event.metadata_json)?;
        let id = deterministic_id(&[
            &event.tenant_id,
            event.target_id.as_deref().unwrap_or(""),
            &event.action,
            &event.reason,
        ]);
        let mut client = self.client()?;
        client
            .execute(
                r#"
                INSERT INTO nextral_audit_events
                    (id, tenant_id, user_id, actor_id, action, target_type, target_id, reason, request_id, trace_id, metadata, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now())
                ON CONFLICT (id) DO NOTHING
                "#,
                &[
                    &id,
                    &event.tenant_id,
                    &event.user_id,
                    &event.actor_id,
                    &event.action,
                    &event.target_type,
                    &event.target_id,
                    &event.reason,
                    &event.request_id,
                    &event.trace_id,
                    &metadata,
                ],
            )
            .map_err(|error| CoreError::Io(error.to_string()))?;
        Ok(())
    }

    fn enqueue_outbox(
        &self,
        tenant_id: &str,
        event_type: &str,
        aggregate_id: &str,
        payload_json: &str,
    ) -> CoreResult<String> {
        let payload = serde_json::from_str::<Value>(payload_json)?;
        let id = deterministic_id(&[tenant_id, event_type, aggregate_id, payload_json]);
        let mut client = self.client()?;
        client
            .execute(
                r#"
                INSERT INTO nextral_outbox_events
                    (id, tenant_id, event_type, aggregate_type, aggregate_id, payload, status, created_at, updated_at)
                VALUES ($1, $2, $3, 'memory', $4, $5, 'pending', now(), now())
                ON CONFLICT (id) DO NOTHING
                "#,
                &[&id, &tenant_id, &event_type, &aggregate_id, &payload],
            )
            .map_err(|error| CoreError::Io(error.to_string()))?;
        Ok(id)
    }
}

fn enum_text<T: serde::Serialize>(value: &T) -> CoreResult<String> {
    let value = serde_json::to_value(value)?;
    match value {
        Value::String(text) => Ok(text),
        other => Err(CoreError::Serialization(format!(
            "expected enum to serialize as string, got {other}"
        ))),
    }
}

fn json_value<T: serde::Serialize>(value: &T) -> CoreResult<Value> {
    Ok(serde_json::to_value(value)?)
}

fn from_text<T: DeserializeOwned>(value: String) -> CoreResult<T> {
    Ok(serde_json::from_value(Value::String(value))?)
}

fn epoch_seconds(value: &str, field: &str) -> CoreResult<f64> {
    value
        .parse::<f64>()
        .map_err(|error| CoreError::InvalidInput(format!("{field} must be epoch seconds: {error}")))
}

fn optional_epoch_seconds(value: Option<&str>, field: &str) -> CoreResult<Option<f64>> {
    value
        .map(|timestamp| epoch_seconds(timestamp, field))
        .transpose()
}

fn row_to_memory(row: Row) -> CoreResult<MemoryRecord> {
    Ok(MemoryRecord {
        id: row.get(0),
        tenant_id: row.get(1),
        user_id: row.get(2),
        session_id: row.get(3),
        content: row.get(4),
        content_type: from_text::<ContentType>(row.get(5))?,
        memory_type: from_text::<MemoryType>(row.get(6))?,
        source_type: from_text::<SourceType>(row.get(7))?,
        source_message_ids: serde_json::from_value(row.get::<_, Value>(8))?,
        importance_score: row.get::<_, f32>(9),
        confidence_score: row.get(10),
        embedding_provider: row.get(11),
        embedding_model: row.get(12),
        embedding_dim: Some(row.get::<_, i32>(13) as u32),
        extraction_provider: None,
        extraction_model: None,
        entities: serde_json::from_value(row.get::<_, Value>(14))?,
        tags: serde_json::from_value(row.get::<_, Value>(15))?,
        privacy_level: from_text::<PrivacyLevel>(row.get(16))?,
        created_at: row.get(17),
        updated_at: row.get(18),
        last_accessed_at: {
            let value: String = row.get(19);
            if value.is_empty() {
                None
            } else {
                Some(value)
            }
        },
        access_count: row.get::<_, i64>(20) as u64,
        status: from_text::<MemoryStatus>(row.get(21))?,
        schema_version: row.get(22),
    })
}
