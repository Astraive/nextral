use crate::{
    config::IngestionPolicy,
    contracts::CoreResult,
    domain::{AuditAction, AuditEvent},
    graph::{graphify_record, GraphHint},
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
    #[serde(default)]
    pub source_type: SourceType,
    #[serde(default)]
    pub source_message_ids: Vec<String>,
    pub importance_score: f32,
    pub confidence_score: Option<f32>,
    #[serde(default)]
    pub entities: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub privacy_level: PrivacyLevel,
    #[serde(default)]
    pub graph_hints: Vec<GraphHint>,
    #[serde(default)]
    pub policy: IngestionPolicy,
    pub trace_id: Option<String>,
    #[serde(default)]
    pub embedding: Option<Vec<f32>>,
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
    pub trace_id: String,
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
            trace_id: None,
            embedding: None,
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
    let mut span = crate::runtime::observability::start_span("nextral.ingest");
    let result = ingest_memory_impl(store, request);
    let accepted = result
        .as_ref()
        .is_ok_and(|response| response.status == IngestStatus::Accepted);
    crate::runtime::observability::record_ingest(accepted);
    if !accepted {
        span.fail();
    }
    result
}

fn ingest_memory_impl<T>(
    store: &mut T,
    request: IngestMemoryRequest,
) -> CoreResult<IngestMemoryResponse>
where
    T: MemoryIndexStore + AuditSink + GraphStore,
{
    let trace_id = request.trace_id.clone().unwrap_or_else(|| {
        deterministic_id(&[
            &request.tenant_id,
            &request.user_id,
            &request.content,
            "ingest_trace",
        ])
    });
    let attempt_id = crate::runtime::lifecycle::worker_owner();
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
        trace_id: Some(trace_id.clone()),
        metadata_json: "{}".to_string(),
        created_at: now_timestamp(),
    })?;

    let policy = store.ingestion_policy(&request.policy);
    let mut errors = Vec::new();
    if request.content.trim().is_empty() {
        errors.push("content cannot be empty".to_string());
    }
    if !request.importance_score.is_finite()
        || request.importance_score < policy.min_importance_score
    {
        errors.push("importance_score is below write threshold".to_string());
    }
    if !request.confidence_score.unwrap_or(0.0).is_finite()
        || request.confidence_score.unwrap_or(0.0) < policy.min_confidence_score
    {
        errors.push("confidence_score is below write threshold".to_string());
    }
    if !errors.is_empty() {
        store.emit_audit(AuditEvent {
            id: crate::runtime::lifecycle::worker_owner(),
            tenant_id: request.tenant_id,
            user_id: Some(request.user_id.clone()),
            actor: request.user_id,
            action: AuditAction::WriteRejected,
            target_type: "memory".to_string(),
            target_id: None,
            reason: errors.join("; "),
            request_id: None,
            trace_id: Some(trace_id.clone()),
            metadata_json: "{}".to_string(),
            created_at: now_timestamp(),
        })?;
        return Ok(IngestMemoryResponse {
            status: IngestStatus::Rejected,
            trace_id,
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
    store.prepare_embedding(&mut record, request.embedding.as_deref())?;
    record.validate()?;

    let graph_output = graphify_record(&record, &request.graph_hints)?;
    let audit = AuditEvent {
        id: crate::runtime::lifecycle::worker_owner(),
        tenant_id: request.tenant_id,
        user_id: Some(request.user_id.clone()),
        actor: request.user_id,
        action: AuditAction::WriteAccepted,
        target_type: "memory".to_string(),
        target_id: Some(id.clone()),
        reason: "memory accepted".to_string(),
        request_id: None,
        trace_id: Some(trace_id.clone()),
        metadata_json: "{}".to_string(),
        created_at: now_timestamp(),
    };
    let receipts = store.commit_ingestion(
        record,
        graph_output.nodes,
        graph_output.relationships,
        audit,
    )?;

    Ok(IngestMemoryResponse {
        status: IngestStatus::Accepted,
        trace_id,
        record_id: Some(id),
        validation_errors: Vec::new(),
        write_receipts: receipts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted_mutations_have_distinct_audit_events() {
        let mut store = crate::testkit::TestMemoryStore::new();
        for (content, trace) in [
            ("First decision", "attempt-one"),
            ("Changed decision", "attempt-two"),
        ] {
            let mut request = IngestMemoryRequest::new(
                "tenant",
                "owner",
                content,
                ContentType::Decision,
                MemoryType::Semantic,
                SourceType::Manual,
                IngestionPolicy {
                    min_importance_score: 0.0,
                    min_confidence_score: 0.0,
                },
            );
            request.id = Some("same-memory".into());
            request.trace_id = Some(trace.into());
            assert_eq!(
                ingest_memory(&mut store, request).unwrap().status,
                IngestStatus::Accepted
            );
        }
        let accepted: Vec<_> = store
            .audit_events
            .iter()
            .filter(|event| event.action == AuditAction::WriteAccepted)
            .collect();
        assert_eq!(
            accepted.len(),
            2,
            "each accepted mutation must persist its own acceptance event"
        );
        assert_ne!(accepted[0].id, accepted[1].id);
        assert_eq!(accepted[0].trace_id.as_deref(), Some("attempt-one"));
        assert_eq!(accepted[1].trace_id.as_deref(), Some("attempt-two"));
        assert_eq!(
            store
                .get_memory("tenant", "owner", "same-memory")
                .unwrap()
                .unwrap()
                .content,
            "Changed decision"
        );
    }

    #[test]
    fn repeated_trace_and_content_do_not_deduplicate_accepted_mutations() {
        let mut store = crate::testkit::TestMemoryStore::new();
        let mut request = IngestMemoryRequest::new(
            "tenant",
            "owner",
            "Same decision",
            ContentType::Decision,
            MemoryType::Semantic,
            SourceType::Manual,
            IngestionPolicy {
                min_importance_score: 0.0,
                min_confidence_score: 0.0,
            },
        );
        request.id = Some("same-memory".into());
        request.trace_id = Some("shared-trace".into());
        for _ in 0..3 {
            assert_eq!(
                ingest_memory(&mut store, request.clone()).unwrap().status,
                IngestStatus::Accepted
            );
        }
        for action in [AuditAction::WriteAttempt, AuditAction::WriteAccepted] {
            let events: Vec<_> = store
                .audit_events
                .iter()
                .filter(|event| event.action == action)
                .collect();
            assert_eq!(
                events.len(),
                3,
                "correlation traces must not deduplicate distinct ingestion calls"
            );
            let ids: std::collections::HashSet<_> = events.iter().map(|event| &event.id).collect();
            assert_eq!(ids.len(), 3);
            assert!(events
                .iter()
                .all(|event| event.trace_id.as_deref() == Some("shared-trace")
                    && event.tenant_id == "tenant"
                    && event.user_id.as_deref() == Some("owner")));
        }
    }
}
