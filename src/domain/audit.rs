use crate::memory::now_timestamp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    WriteAttempt,
    WriteAccepted,
    WriteRejected,
    ValidationFailed,
    SessionAppend,
    SessionClose,
    ConsolidationQueued,
    ConsolidationCompleted,
    GraphifyMerged,
    Retrieval,
    Transition,
    ReminderScheduled,
    ReminderTransition,
    ForgetRequested,
    ForgetCompleted,
    RedactionCompleted,
    ArchiveWritten,
    ReembedStarted,
    ReembedCompleted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditEvent {
    pub id: String,
    pub tenant_id: String,
    pub user_id: Option<String>,
    pub actor: String,
    pub action: AuditAction,
    pub target_type: String,
    pub target_id: Option<String>,
    pub reason: String,
    pub request_id: Option<String>,
    pub trace_id: Option<String>,
    pub metadata_json: String,
    pub created_at: String,
}

impl AuditEvent {
    pub fn new(
        id: impl Into<String>,
        tenant_id: impl Into<String>,
        actor: impl Into<String>,
        action: AuditAction,
        target_type: impl Into<String>,
        target_id: Option<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            tenant_id: tenant_id.into(),
            user_id: None,
            actor: actor.into(),
            action,
            target_type: target_type.into(),
            target_id,
            reason: reason.into(),
            request_id: None,
            trace_id: None,
            metadata_json: "{}".to_string(),
            created_at: now_timestamp(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoreReceipt {
    pub backend: String,
    pub operation: String,
    pub target_id: Option<String>,
    pub status: String,
}

impl StoreReceipt {
    pub fn ok(
        backend: impl Into<String>,
        operation: impl Into<String>,
        target_id: Option<String>,
    ) -> Self {
        Self {
            backend: backend.into(),
            operation: operation.into(),
            target_id,
            status: "ok".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeJob {
    pub id: String,
    pub tenant_id: String,
    pub job_type: String,
    pub target_id: String,
    pub status: String,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}
