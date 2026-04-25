use crate::{contracts::CoreResult, memory::deterministic_id};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReembedPlan {
    pub id: String,
    pub tenant_id: String,
    pub source_collection: String,
    pub shadow_collection: String,
    pub target_embedding_provider: String,
    pub target_embedding_model: String,
    pub status: String,
}

pub fn plan_reembed(
    tenant_id: &str,
    source_collection: &str,
    shadow_collection: &str,
    target_embedding_provider: &str,
    target_embedding_model: &str,
) -> CoreResult<ReembedPlan> {
    for (name, value) in [
        ("tenant_id", tenant_id),
        ("source_collection", source_collection),
        ("shadow_collection", shadow_collection),
        ("target_embedding_provider", target_embedding_provider),
        ("target_embedding_model", target_embedding_model),
    ] {
        if value.trim().is_empty() {
            return Err(crate::contracts::CoreError::InvalidInput(format!(
                "{name} is required"
            )));
        }
    }
    Ok(ReembedPlan {
        id: deterministic_id(&[
            tenant_id,
            source_collection,
            shadow_collection,
            target_embedding_provider,
            target_embedding_model,
        ]),
        tenant_id: tenant_id.to_string(),
        source_collection: source_collection.to_string(),
        shadow_collection: shadow_collection.to_string(),
        target_embedding_provider: target_embedding_provider.to_string(),
        target_embedding_model: target_embedding_model.to_string(),
        status: "planned".to_string(),
    })
}
