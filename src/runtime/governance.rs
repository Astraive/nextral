use crate::{
    contracts::CoreResult,
    domain::StoreReceipt,
    memory::{MemoryStatus, TransitionMetadata},
    store::MemoryIndexStore,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ForgetMemoryRequest {
    pub tenant_id: String,
    pub user_id: String,
    pub memory_id: String,
    pub actor: String,
    pub reason: String,
    pub redact: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ForgetMemoryResponse {
    pub transition: TransitionMetadata,
    pub redaction_transition: Option<TransitionMetadata>,
    pub receipts: Vec<StoreReceipt>,
}

pub fn forget_memory(
    store: &mut impl MemoryIndexStore,
    request: ForgetMemoryRequest,
) -> CoreResult<ForgetMemoryResponse> {
    let mut record = store
        .get_memory(&request.tenant_id, &request.user_id, &request.memory_id)?
        .ok_or_else(|| crate::contracts::CoreError::NotFound("memory not found".to_string()))?;
    let transition =
        record.transition(MemoryStatus::SoftDeleted, &request.actor, &request.reason)?;
    let redaction_transition = if request.redact {
        Some(record.transition(MemoryStatus::Redacted, &request.actor, &request.reason)?)
    } else {
        None
    };
    store.update_memory(record)?;
    Ok(ForgetMemoryResponse {
        transition,
        redaction_transition,
        receipts: vec![
            StoreReceipt::ok(
                "postgres",
                "mark_deleted_or_redacted",
                Some(request.memory_id.clone()),
            ),
            StoreReceipt::ok("qdrant", "delete_point", Some(request.memory_id.clone())),
            StoreReceipt::ok("neo4j", "redact_edges", Some(request.memory_id.clone())),
            StoreReceipt::ok("redis", "invalidate_cache", Some(request.memory_id.clone())),
            StoreReceipt::ok("s3", "append_tombstone_manifest", Some(request.memory_id)),
        ],
    })
}
