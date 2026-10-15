use crate::{
    config::IngestionPolicy,
    contracts::{CoreError, CoreResult},
    domain::{AuditAction, AuditEvent, RuntimeJob, StoreReceipt},
    ingestion::{ingest_memory, IngestMemoryRequest, IngestMemoryResponse},
    memory::{deterministic_id, now_timestamp, ContentType, MemoryType, SourceType},
    store::{AuditSink, GraphStore, MemoryIndexStore, SessionStore},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsolidationLane {
    Fast,
    Deep,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConsolidationRequest {
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: String,
    pub lane: ConsolidationLane,
    pub policy: IngestionPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConsolidationResponse {
    pub job: RuntimeJob,
    pub accepted: Vec<IngestMemoryResponse>,
    pub receipts: Vec<StoreReceipt>,
}

pub fn consolidate_session<T>(
    store: &mut T,
    request: ConsolidationRequest,
) -> CoreResult<ConsolidationResponse>
where
    T: SessionStore + MemoryIndexStore + AuditSink + GraphStore,
{
    const MAX_SESSION_MESSAGES: usize = 10_000;
    let messages = store.session_tail(
        &request.tenant_id,
        &request.user_id,
        &request.session_id,
        MAX_SESSION_MESSAGES,
    )?;
    if messages.is_empty() {
        return Err(CoreError::NotFound(
            "session has no messages to consolidate".to_string(),
        ));
    }
    let transcript = messages
        .iter()
        .map(|message| format!("{}: {}", message.role, message.content))
        .collect::<Vec<_>>()
        .join("\n");
    let memory_type = match request.lane {
        ConsolidationLane::Fast => MemoryType::Semantic,
        ConsolidationLane::Deep => MemoryType::Episodic,
    };
    let source_type = match request.lane {
        ConsolidationLane::Fast => SourceType::FastLane,
        ConsolidationLane::Deep => SourceType::DeepLane,
    };
    let mut ingest = IngestMemoryRequest::new(
        request.tenant_id.clone(),
        request.user_id.clone(),
        transcript,
        ContentType::Event,
        memory_type,
        source_type,
        request.policy,
    );
    ingest.session_id = Some(request.session_id.clone());
    ingest.source_message_ids = messages.iter().map(|message| message.id.clone()).collect();
    ingest.importance_score = 1.0;
    ingest.confidence_score = Some(1.0);

    let memory_id = deterministic_id(&[
        &request.tenant_id,
        &request.user_id,
        &request.session_id,
        &ingest.content,
    ]);
    if let Some(existing) = store.get_memory(&request.tenant_id, &request.user_id, &memory_id)? {
        if existing.status != crate::memory::MemoryStatus::Active {
            return Err(CoreError::InvalidInput(
                "cannot consolidate into a non-active forgotten memory".to_string(),
            ));
        }
    }

    store.emit_audit(AuditEvent::new(
        deterministic_id(&[
            &request.tenant_id,
            &request.session_id,
            "consolidation_queued",
        ]),
        &request.tenant_id,
        "system",
        AuditAction::ConsolidationQueued,
        "session",
        Some(request.session_id.clone()),
        format!("{:?} lane consolidation queued", request.lane),
    ))?;
    let accepted = vec![ingest_memory(store, ingest)?];
    let now = now_timestamp();
    let tenant_id = request.tenant_id.clone();
    let session_id = request.session_id.clone();
    let job = RuntimeJob {
        id: deterministic_id(&[&tenant_id, &session_id, "consolidation"]),
        tenant_id,
        job_type: format!("{:?}", request.lane).to_lowercase(),
        target_id: session_id,
        status: "completed".to_string(),
        attempts: 1,
        last_error: None,
        created_at: now.clone(),
        updated_at: now,
    };
    // Use the IDs already moved into the job for the audit event
    store.emit_audit(AuditEvent::new(
        deterministic_id(&[&job.tenant_id, &job.target_id, "consolidation_completed"]),
        &job.tenant_id,
        "system",
        AuditAction::ConsolidationCompleted,
        "session",
        Some(job.target_id.clone()),
        "consolidation completed".to_string(),
    ))?;
    Ok(ConsolidationResponse {
        job,
        accepted,
        receipts: vec![StoreReceipt::ok("runtime", "consolidate_session", None)],
    })
}
