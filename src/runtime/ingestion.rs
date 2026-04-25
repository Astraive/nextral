use crate::{
    config::IngestionPolicy,
    contracts::CoreResult,
    domain::{AuditAction, AuditEvent},
    graph::{graphify_record, merge_graph, GraphHint},
    memory::{
        deterministic_id, now_timestamp, ContentType, MemoryRecord, MemoryType, PrivacyLevel,
        SourceType,
    },
    store::{AuditSink, GraphStore, MemoryIndexStore},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestMemoryRequest {
    pub id: Option<String>,
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: Option<String>,
    pub content: String,
    pub content_type: ContentType,
    pub memory_type: MemoryType,
    pub source_type: SourceType,
    pub source_message_ids: Vec<String>,
    pub importance_score: f32,
    pub confidence_score: Option<f32>,
    pub entities: Vec<String>,
    pub tags: Vec<String>,
    pub privacy_level: PrivacyLevel,
    pub graph_hints: Vec<GraphHint>,
    pub policy: IngestionPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IngestStatus {
    Accepted,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestMemoryResponse {
    pub status: IngestStatus,
    pub record_id: Option<String>,
    pub validation_errors: Vec<String>,
    pub write_receipts: Vec<String>,
}

impl IngestMemoryRequest {
    pub fn new(
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
        content: impl Into<String>,
        content_type: ContentType,
        memory_type: MemoryType,
        source_type: SourceType,
        policy: IngestionPolicy,
    ) -> Self {
        Self {
            id: None,
            tenant_id: tenant_id.into(),
            user_id: user_id.into(),
            session_id: None,
            content: content.into(),
            content_type,
            memory_type,
            source_type,
            source_message_ids: Vec::new(),
            importance_score: 0.0,
            confidence_score: None,
            entities: Vec::new(),
            tags: Vec::new(),
            privacy_level: PrivacyLevel::Private,
            graph_hints: Vec::new(),
            policy,
        }
    }
}

pub fn ingest_memory<T>(
    store: &mut T,
    request: IngestMemoryRequest,
) -> CoreResult<IngestMemoryResponse>
where
    T: MemoryIndexStore + AuditSink + GraphStore,
{
    let attempt_id = deterministic_id(&[&request.user_id, &request.content, "write_attempt"]);
    store.emit_audit(AuditEvent {
        id: attempt_id,
        tenant_id: request.tenant_id.clone(),
        user_id: Some(request.user_id.clone()),
        actor: request.user_id.clone(),
        action: AuditAction::WriteAttempt,
        target_type: "memory".to_string(),
        target_id: request.id.clone(),
        reason: "ingest requested".to_string(),
        request_id: None,
        trace_id: None,
        metadata_json: "{}".to_string(),
        created_at: now_timestamp(),
    })?;

    let mut errors = Vec::new();
    if request.content.trim().is_empty() {
        errors.push("content cannot be empty".to_string());
    }
    if request.importance_score < request.policy.min_importance_score {
        errors.push("importance_score is below write threshold".to_string());
    }
    if request.confidence_score.unwrap_or(0.0) < request.policy.min_confidence_score {
        errors.push("confidence_score is below write threshold".to_string());
    }
    if !errors.is_empty() {
        store.emit_audit(AuditEvent {
            id: deterministic_id(&[&request.user_id, &request.content, "write_rejected"]),
            tenant_id: request.tenant_id,
            user_id: Some(request.user_id.clone()),
            actor: request.user_id,
            action: AuditAction::WriteRejected,
            target_type: "memory".to_string(),
            target_id: None,
            reason: errors.join("; "),
            request_id: None,
            trace_id: None,
            metadata_json: "{}".to_string(),
            created_at: now_timestamp(),
        })?;
        return Ok(IngestMemoryResponse {
            status: IngestStatus::Rejected,
            record_id: None,
            validation_errors: errors,
            write_receipts: Vec::new(),
        });
    }

    let id = request.id.unwrap_or_else(|| {
        deterministic_id(&[
            &request.tenant_id,
            &request.user_id,
            request.session_id.as_deref().unwrap_or(""),
            &request.content,
        ])
    });
    let mut record = MemoryRecord::new(
        id.clone(),
        request.tenant_id.clone(),
        request.user_id.clone(),
        request.content,
        request.content_type,
        request.memory_type,
        request.source_type,
    );
    record.session_id = request.session_id;
    record.source_message_ids = request.source_message_ids;
    record.importance_score = request.importance_score;
    record.confidence_score = request.confidence_score;
    record.entities = request.entities;
    record.tags = request.tags;
    record.privacy_level = request.privacy_level;
    record.validate()?;

    let graph_output = graphify_record(&record, &request.graph_hints)?;
    store.upsert_memory(record)?;
    merge_graph(store, graph_output)?;
    store.emit_audit(AuditEvent {
        id: deterministic_id(&[&request.user_id, &id, "write_accepted"]),
        tenant_id: request.tenant_id,
        user_id: Some(request.user_id.clone()),
        actor: request.user_id,
        action: AuditAction::WriteAccepted,
        target_type: "memory".to_string(),
        target_id: Some(id.clone()),
        reason: "memory accepted".to_string(),
        request_id: None,
        trace_id: None,
        metadata_json: "{}".to_string(),
        created_at: now_timestamp(),
    })?;

    Ok(IngestMemoryResponse {
        status: IngestStatus::Accepted,
        record_id: Some(id),
        validation_errors: Vec::new(),
        write_receipts: vec![
            "memory_index".to_string(),
            "audit".to_string(),
            "graph".to_string(),
        ],
    })
}
