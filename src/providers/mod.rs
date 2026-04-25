use crate::{
    contracts::CoreResult,
    graph::GraphHint,
    memory::{deterministic_id, estimate_tokens, now_timestamp},
    retrieval::{RetrievalRequest, RetrievedItem},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmbeddingRequest {
    pub model: String,
    pub provider: String,
    pub input: Vec<String>,
    pub tenant_id: String,
    pub user_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmbeddingBatch {
    pub provider: String,
    pub model: String,
    pub dimension: u32,
    pub vectors: Vec<Vec<f32>>,
}

pub trait EmbeddingProvider {
    fn embed(&self, request: EmbeddingRequest) -> CoreResult<EmbeddingBatch>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExtractionRequest {
    pub provider: String,
    pub model: String,
    pub tenant_id: String,
    pub user_id: String,
    pub content: String,
    pub entities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExtractedRelationship {
    pub from_label: String,
    pub from_name: String,
    pub relationship_type: String,
    pub to_label: String,
    pub to_name: String,
    pub confidence: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExtractionOutput {
    pub entities: Vec<String>,
    pub relationships: Vec<ExtractedRelationship>,
    pub confidence: f32,
}

pub trait ExtractionProvider {
    fn extract(&self, request: ExtractionRequest) -> CoreResult<ExtractionOutput>;
}

impl ExtractedRelationship {
    pub fn into_graph_hint(self) -> GraphHint {
        GraphHint {
            from_label: self.from_label,
            from_name: self.from_name,
            relationship_type: self.relationship_type,
            to_label: self.to_label,
            to_name: self.to_name,
            confidence: self.confidence,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RerankRequest {
    pub provider: String,
    pub model: String,
    pub retrieval: RetrievalRequest,
    pub items: Vec<RetrievedItem>,
}

pub trait RerankerProvider {
    fn rerank(&self, request: RerankRequest) -> CoreResult<Vec<RetrievedItem>>;
}

pub trait TokenEstimator {
    fn estimate_tokens(&self, text: &str) -> CoreResult<u32>;
}

pub trait IdGenerator {
    fn memory_id(&self, tenant_id: &str, user_id: &str, content: &str) -> CoreResult<String>;
    fn job_id(&self, tenant_id: &str, job_type: &str, target_id: &str) -> CoreResult<String>;
}

pub trait Clock {
    fn now_timestamp(&self) -> CoreResult<String>;
}

#[derive(Debug, Clone, Default)]
pub struct DeterministicIds;

impl IdGenerator for DeterministicIds {
    fn memory_id(&self, tenant_id: &str, user_id: &str, content: &str) -> CoreResult<String> {
        Ok(deterministic_id(&[tenant_id, user_id, content]))
    }

    fn job_id(&self, tenant_id: &str, job_type: &str, target_id: &str) -> CoreResult<String> {
        Ok(deterministic_id(&[tenant_id, job_type, target_id]))
    }
}

#[derive(Debug, Clone, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_timestamp(&self) -> CoreResult<String> {
        Ok(now_timestamp())
    }
}

#[derive(Debug, Clone, Default)]
pub struct WhitespaceTokenEstimator;

impl TokenEstimator for WhitespaceTokenEstimator {
    fn estimate_tokens(&self, text: &str) -> CoreResult<u32> {
        Ok(estimate_tokens(text))
    }
}
