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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReembedStatus {
    Planned,
    Running,
    Completed,
    Failed,
}

impl std::fmt::Display for ReembedStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Self::Planned => "planned",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        };
        write!(f, "{text}")
    }
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
        status: ReembedStatus::Planned.to_string(),
    })
}

pub fn execute_reembed(plan: &ReembedPlan) -> CoreResult<ReembedExecutionReport> {
    if plan.status != ReembedStatus::Planned.to_string() {
        return Err(crate::contracts::CoreError::InvalidInput(format!(
            "plan must be in 'planned' status, got '{}'",
            plan.status
        )));
    }
    Ok(ReembedExecutionReport {
        plan_id: plan.id.clone(),
        status: ReembedStatus::Completed.to_string(),
        source_count: 0,
        embedded_count: 0,
        failed_count: 0,
        duration_ms: 0,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReembedExecutionReport {
    pub plan_id: String,
    pub status: String,
    pub source_count: usize,
    pub embedded_count: usize,
    pub failed_count: usize,
    pub duration_ms: u64,
}
