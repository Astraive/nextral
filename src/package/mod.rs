use crate::contracts::CoreError;
use crate::{
    config::IngestionPolicy,
    ingestion::IngestMemoryRequest,
    memory::{ContentType, MemoryType, SourceType},
    retrieval::RetrievalRequest,
    runtime::{
        consolidation::{consolidate_session, ConsolidationLane, ConsolidationRequest},
        governance::{forget_memory, ForgetMemoryRequest},
        reembed::plan_reembed,
        reminders::{schedule_reminder, ScheduleReminderRequest},
        session::{append_session_message, assemble_working_context, AppendSessionMessageRequest},
    },
    store::TestMemoryStore,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PackageError {
    pub code: String,
    pub message: String,
}

pub fn e2e_smoke_json() -> Result<String, PackageError> {
    let mut store = TestMemoryStore::new();
    let appended = append_session_message(
        &mut store,
        AppendSessionMessageRequest {
            tenant_id: "test_tenant".to_string(),
            user_id: "test_user".to_string(),
            session_id: "test_session".to_string(),
            role: "user".to_string(),
            content: "Use PostgreSQL for Atlas and remind me Friday".to_string(),
            idempotency_key: Some("testkit-message".to_string()),
        },
        20,
    )?;
    let consolidated = consolidate_session(
        &mut store,
        ConsolidationRequest {
            tenant_id: "test_tenant".to_string(),
            user_id: "test_user".to_string(),
            session_id: "test_session".to_string(),
            lane: ConsolidationLane::Fast,
            policy: IngestionPolicy {
                min_importance_score: 0.1,
                min_confidence_score: 0.1,
            },
        },
    )?;
    let memory_id = consolidated.accepted[0]
        .record_id
        .clone()
        .unwrap_or_default();
    let scheduled = schedule_reminder(
        &mut store,
        ScheduleReminderRequest {
            user_id: "test_user".to_string(),
            source_memory_id: memory_id.clone(),
            kind: crate::prospective::ReminderKind::FollowUp,
            title: "Check Atlas migration".to_string(),
            due_at: "9999999999".to_string(),
            timezone: "configured-by-user".to_string(),
        },
    )?;
    let mut retrieval = RetrievalRequest::test("test_tenant", "test_user", "PostgreSQL");
    retrieval.session_id = Some("test_session".to_string());
    let context = assemble_working_context(&mut store, retrieval, 20)?;
    let forgotten = forget_memory(
        &mut store,
        ForgetMemoryRequest {
            tenant_id: "test_tenant".to_string(),
            user_id: "test_user".to_string(),
            memory_id,
            actor: "test_user".to_string(),
            reason: "testkit redaction".to_string(),
            redact: true,
        },
    )?;
    let payload = serde_json::json!({
        "status": "ok",
        "backend": "testkit",
        "message_id": appended.message_id,
        "consolidation_job": consolidated.job.id,
        "reminder_id": scheduled.reminder.id,
        "working_context_items": context.retrieved_memory_ids.len(),
        "redacted": forgotten.redaction_transition.is_some()
    });
    Ok(payload.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReembedPlanRequest {
    pub tenant_id: String,
    pub source_collection: String,
    pub shadow_collection: String,
    pub target_embedding_provider: String,
    pub target_embedding_model: String,
}

pub fn reembed_plan_json(request_json: &str) -> Result<String, PackageError> {
    let request: ReembedPlanRequest =
        serde_json::from_str(request_json).map_err(CoreError::from)?;
    let plan = plan_reembed(
        &request.tenant_id,
        &request.source_collection,
        &request.shadow_collection,
        &request.target_embedding_provider,
        &request.target_embedding_model,
    )?;
    Ok(serde_json::to_string(&plan).map_err(CoreError::from)?)
}

pub fn ingest_request_schema_json() -> String {
    let request = IngestMemoryRequest::new(
        "tenant_id",
        "user_id",
        "content",
        ContentType::Fact,
        MemoryType::Semantic,
        SourceType::Manual,
        IngestionPolicy {
            min_importance_score: 0.0,
            min_confidence_score: 0.0,
        },
    );
    serde_json::to_string(&request).unwrap_or_else(|_| "{}".to_string())
}

impl From<CoreError> for PackageError {
    fn from(error: CoreError) -> Self {
        let code = match &error {
            CoreError::InvalidInput(_) => "invalid_input",
            CoreError::NotFound(_) => "not_found",
            CoreError::Conflict(_) => "conflict",
            CoreError::Io(_) => "io",
            CoreError::Serialization(_) => "serialization",
        };
        Self {
            code: code.to_string(),
            message: error.to_string(),
        }
    }
}
