use crate::memory::{deterministic_id, now_timestamp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionMessage {
    pub id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: String,
    pub role: String,
    pub content: String,
    pub idempotency_key: String,
    pub created_at: String,
}

impl SessionMessage {
    pub fn new(
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
        session_id: impl Into<String>,
        role: impl Into<String>,
        content: impl Into<String>,
        idempotency_key: impl Into<String>,
    ) -> Self {
        let tenant_id = tenant_id.into();
        let user_id = user_id.into();
        let session_id = session_id.into();
        let role = role.into();
        let content = content.into();
        let idempotency_key = idempotency_key.into();
        let id = deterministic_id(&[
            &tenant_id,
            &user_id,
            &session_id,
            &role,
            &content,
            &idempotency_key,
        ]);
        Self {
            id,
            tenant_id,
            user_id,
            session_id,
            role,
            content,
            idempotency_key,
            created_at: now_timestamp(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionSummary {
    pub id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: String,
    pub summary: String,
    pub source_message_ids: Vec<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkingContext {
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: Option<String>,
    pub session_tail: Vec<SessionMessage>,
    pub retrieved_memory_ids: Vec<String>,
    pub procedural_policy_ids: Vec<String>,
}
