use crate::{
    config::{IngestionPolicy, RetrievalPolicy},
    memory::{MemoryType, PrivacyLevel},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimePolicy {
    pub ingestion: IngestionPolicy,
    pub retrieval: RetrievalPolicy,
    pub allowed_memory_types: Vec<MemoryType>,
    pub proactive_privacy_scope: Vec<PrivacyLevel>,
}

impl RuntimePolicy {
    pub fn allows_memory_type(&self, memory_type: &MemoryType) -> bool {
        self.allowed_memory_types.contains(memory_type)
    }

    pub fn allows_proactive_privacy(&self, privacy: &PrivacyLevel) -> bool {
        self.proactive_privacy_scope.contains(privacy)
    }
}
