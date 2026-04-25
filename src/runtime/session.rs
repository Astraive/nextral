use crate::{
    contracts::{CoreError, CoreResult},
    domain::{SessionMessage, WorkingContext},
    memory::deterministic_id,
    retrieval::{retrieve, RetrievalRequest},
    store::{GraphStore, MemoryIndexStore, SessionStore},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendSessionMessageRequest {
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: String,
    pub role: String,
    pub content: String,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendSessionMessageResponse {
    pub message_id: String,
    pub hot_tail_count: usize,
}

pub fn append_session_message<T>(
    store: &mut T,
    request: AppendSessionMessageRequest,
    hot_tail_limit: usize,
) -> CoreResult<AppendSessionMessageResponse>
where
    T: SessionStore,
{
    if request.tenant_id.trim().is_empty()
        || request.user_id.trim().is_empty()
        || request.session_id.trim().is_empty()
        || request.role.trim().is_empty()
        || request.content.trim().is_empty()
    {
        return Err(CoreError::InvalidInput(
            "tenant_id, user_id, session_id, role, and content are required".to_string(),
        ));
    }
    if hot_tail_limit == 0 {
        return Err(CoreError::InvalidInput(
            "hot_tail_limit must be non-zero".to_string(),
        ));
    }
    let idempotency_key = request.idempotency_key.unwrap_or_else(|| {
        deterministic_id(&[
            &request.tenant_id,
            &request.user_id,
            &request.session_id,
            &request.role,
            &request.content,
        ])
    });
    let message = SessionMessage::new(
        request.tenant_id.clone(),
        request.user_id.clone(),
        request.session_id.clone(),
        request.role,
        request.content,
        idempotency_key,
    );
    let message_id = message.id.clone();
    store.append_session_message(message)?;
    let tail = store.session_tail(
        &request.tenant_id,
        &request.user_id,
        &request.session_id,
        hot_tail_limit,
    )?;
    Ok(AppendSessionMessageResponse {
        message_id,
        hot_tail_count: tail.len(),
    })
}

pub fn assemble_working_context<T>(
    store: &mut T,
    request: RetrievalRequest,
    hot_tail_limit: usize,
) -> CoreResult<WorkingContext>
where
    T: SessionStore + MemoryIndexStore + GraphStore,
{
    let session_tail = if let Some(session_id) = &request.session_id {
        store.session_tail(
            &request.tenant_id,
            &request.user_id,
            session_id,
            hot_tail_limit,
        )?
    } else {
        Vec::new()
    };
    let retrieval = retrieve(store, request.clone())?;
    Ok(WorkingContext {
        tenant_id: request.tenant_id,
        user_id: request.user_id,
        session_id: request.session_id,
        session_tail,
        retrieved_memory_ids: retrieval
            .items
            .iter()
            .map(|item| item.memory_id.clone())
            .collect(),
        procedural_policy_ids: Vec::new(),
    })
}
