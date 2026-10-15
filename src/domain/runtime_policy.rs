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

impl Default for RuntimePolicy {
    fn default() -> Self {
        use crate::config::{IngestionPolicy, RetrievalPolicy, ScoringWeights};
        use crate::memory::{MemoryType, PrivacyLevel};
        Self {
            ingestion: IngestionPolicy {
                min_importance_score: 0.2,
                min_confidence_score: 0.2,
            },
            retrieval: RetrievalPolicy {
                privacy_scope: vec![PrivacyLevel::Private],
                token_budget: 2000,
                top_k_vector: 10,
                max_graph_hops: 2,
                scoring_weights: ScoringWeights {
                    semantic_similarity: 0.5,
                    recency: 0.2,
                    importance: 0.2,
                    access: 0.1,
                },
            },
            allowed_memory_types: vec![
                MemoryType::Working,
                MemoryType::Session,
                MemoryType::Episodic,
                MemoryType::Semantic,
                MemoryType::Relational,
                MemoryType::Procedural,
                MemoryType::Prospective,
            ],
            proactive_privacy_scope: vec![PrivacyLevel::Private],
        }
    }
}

impl RuntimePolicy {
    pub fn allows_memory_type(&self, memory_type: &MemoryType) -> bool {
        self.allowed_memory_types.contains(memory_type)
    }

    pub fn allows_proactive_privacy(&self, privacy: &PrivacyLevel) -> bool {
        self.proactive_privacy_scope.contains(privacy)
    }
}
