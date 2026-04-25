use crate::{
    contracts::{CoreError, CoreResult},
    memory::PrivacyLevel,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeBackend {
    ProductionStores,
    TestMemory,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingProviderKind {
    OpenAiCompatible,
    Http,
    ExternalCallback,
    Test,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmbeddingProviderConfig {
    pub kind: EmbeddingProviderKind,
    pub model: String,
    pub dimension: u32,
    pub endpoint: Option<String>,
    pub api_key_env: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExtractionProviderKind {
    OpenAiCompatible,
    Http,
    ExternalCallback,
    Test,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtractionProviderConfig {
    pub kind: ExtractionProviderKind,
    pub model: String,
    pub endpoint: Option<String>,
    pub api_key_env: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RerankerProviderKind {
    None,
    OpenAiCompatible,
    Http,
    ExternalCallback,
    Test,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RerankerProviderConfig {
    pub kind: RerankerProviderKind,
    pub model: Option<String>,
    pub endpoint: Option<String>,
    pub api_key_env: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestionPolicy {
    pub min_importance_score: f32,
    pub min_confidence_score: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScoringWeights {
    pub semantic_similarity: f32,
    pub recency: f32,
    pub importance: f32,
    pub access: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetrievalPolicy {
    pub privacy_scope: Vec<PrivacyLevel>,
    pub token_budget: u32,
    pub top_k_vector: usize,
    pub max_graph_hops: u8,
    pub scoring_weights: ScoringWeights,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoreConfig {
    pub postgres_url: String,
    pub redis_url: String,
    pub qdrant_url: String,
    pub neo4j_url: String,
    pub s3_endpoint: String,
    pub s3_bucket: String,
    pub s3_region: String,
    pub s3_access_key_env: String,
    pub s3_secret_key_env: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheConfig {
    pub key_prefix: String,
    pub session_ttl_seconds: u64,
    pub retrieval_ttl_seconds: u64,
    pub policy_ttl_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceConfig {
    pub http_bind: Option<String>,
    pub grpc_bind: Option<String>,
    pub graphql_bind: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthConfig {
    pub issuer: Option<String>,
    pub audience: Option<String>,
    pub jwks_url: Option<String>,
    pub service_token_env: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObservabilityConfig {
    pub enabled: bool,
    pub otlp_endpoint: Option<String>,
    pub prometheus_bind: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NextralConfig {
    pub backend: RuntimeBackend,
    pub stores: Option<StoreConfig>,
    pub embedding: EmbeddingProviderConfig,
    pub extraction: ExtractionProviderConfig,
    pub reranker: Option<RerankerProviderConfig>,
    pub ingestion_policy: IngestionPolicy,
    pub retrieval_policy: RetrievalPolicy,
    pub cache: CacheConfig,
    pub service: ServiceConfig,
    pub auth: AuthConfig,
    pub observability: ObservabilityConfig,
}

impl NextralConfig {
    pub fn validate(&self) -> CoreResult<()> {
        validate_score(
            "ingestion_policy.min_importance_score",
            self.ingestion_policy.min_importance_score,
        )?;
        validate_score(
            "ingestion_policy.min_confidence_score",
            self.ingestion_policy.min_confidence_score,
        )?;
        validate_retrieval_policy(&self.retrieval_policy)?;
        validate_embedding(&self.embedding)?;
        validate_extraction(&self.extraction)?;
        if let Some(reranker) = &self.reranker {
            validate_reranker(reranker)?;
        }
        validate_cache(&self.cache)?;

        if self.backend == RuntimeBackend::ProductionStores {
            let stores = self.stores.as_ref().ok_or_else(|| {
                CoreError::InvalidInput(
                    "stores config is required for production_stores backend".to_string(),
                )
            })?;
            validate_stores(stores)?;
        }

        if self.backend == RuntimeBackend::TestMemory
            && (self.embedding.kind != EmbeddingProviderKind::Test
                || self.extraction.kind != ExtractionProviderKind::Test)
        {
            return Err(CoreError::InvalidInput(
                "test_memory backend requires test embedding and extraction providers".to_string(),
            ));
        }

        Ok(())
    }
}

pub fn validate_config_json(config_json: &str) -> CoreResult<String> {
    let config: NextralConfig = serde_json::from_str(config_json)?;
    config.validate()?;
    Ok("{\"status\":\"ok\"}".to_string())
}

fn validate_score(name: &str, value: f32) -> CoreResult<()> {
    if !(0.0..=1.0).contains(&value) || value.is_nan() {
        return Err(CoreError::InvalidInput(format!(
            "{name} must be within 0..=1"
        )));
    }
    Ok(())
}

fn validate_retrieval_policy(policy: &RetrievalPolicy) -> CoreResult<()> {
    if policy.privacy_scope.is_empty() {
        return Err(CoreError::InvalidInput(
            "retrieval_policy.privacy_scope is required".to_string(),
        ));
    }
    if policy.token_budget == 0 || policy.top_k_vector == 0 || policy.max_graph_hops == 0 {
        return Err(CoreError::InvalidInput(
            "retrieval token budget, vector top-k, and graph hops must be non-zero".to_string(),
        ));
    }
    let total = policy.scoring_weights.semantic_similarity
        + policy.scoring_weights.recency
        + policy.scoring_weights.importance
        + policy.scoring_weights.access;
    if (total - 1.0).abs() > 0.001 {
        return Err(CoreError::InvalidInput(
            "retrieval scoring weights must sum to 1.0".to_string(),
        ));
    }
    Ok(())
}

fn validate_embedding(config: &EmbeddingProviderConfig) -> CoreResult<()> {
    require("embedding.model", &config.model)?;
    if config.dimension == 0 {
        return Err(CoreError::InvalidInput(
            "embedding.dimension must be non-zero".to_string(),
        ));
    }
    match config.kind {
        EmbeddingProviderKind::OpenAiCompatible | EmbeddingProviderKind::Http => {
            require_option("embedding.endpoint", &config.endpoint)?;
            require_option("embedding.api_key_env", &config.api_key_env)?;
        }
        EmbeddingProviderKind::ExternalCallback | EmbeddingProviderKind::Test => {}
    }
    Ok(())
}

fn validate_extraction(config: &ExtractionProviderConfig) -> CoreResult<()> {
    require("extraction.model", &config.model)?;
    match config.kind {
        ExtractionProviderKind::OpenAiCompatible | ExtractionProviderKind::Http => {
            require_option("extraction.endpoint", &config.endpoint)?;
            require_option("extraction.api_key_env", &config.api_key_env)?;
        }
        ExtractionProviderKind::ExternalCallback | ExtractionProviderKind::Test => {}
    }
    Ok(())
}

fn validate_reranker(config: &RerankerProviderConfig) -> CoreResult<()> {
    match config.kind {
        RerankerProviderKind::None
        | RerankerProviderKind::ExternalCallback
        | RerankerProviderKind::Test => {}
        RerankerProviderKind::OpenAiCompatible | RerankerProviderKind::Http => {
            require_option("reranker.model", &config.model)?;
            require_option("reranker.endpoint", &config.endpoint)?;
            require_option("reranker.api_key_env", &config.api_key_env)?;
        }
    }
    Ok(())
}

fn validate_cache(config: &CacheConfig) -> CoreResult<()> {
    require("cache.key_prefix", &config.key_prefix)?;
    if config.session_ttl_seconds == 0
        || config.retrieval_ttl_seconds == 0
        || config.policy_ttl_seconds == 0
    {
        return Err(CoreError::InvalidInput(
            "cache TTLs must be non-zero".to_string(),
        ));
    }
    Ok(())
}

fn validate_stores(config: &StoreConfig) -> CoreResult<()> {
    require("stores.postgres_url", &config.postgres_url)?;
    require("stores.redis_url", &config.redis_url)?;
    require("stores.qdrant_url", &config.qdrant_url)?;
    require("stores.neo4j_url", &config.neo4j_url)?;
    require("stores.s3_endpoint", &config.s3_endpoint)?;
    require("stores.s3_bucket", &config.s3_bucket)?;
    require("stores.s3_region", &config.s3_region)?;
    require("stores.s3_access_key_env", &config.s3_access_key_env)?;
    require("stores.s3_secret_key_env", &config.s3_secret_key_env)?;
    Ok(())
}

fn require(name: &str, value: &str) -> CoreResult<()> {
    if value.trim().is_empty() {
        return Err(CoreError::InvalidInput(format!("{name} is required")));
    }
    Ok(())
}

fn require_option(name: &str, value: &Option<String>) -> CoreResult<()> {
    require(name, value.as_deref().unwrap_or(""))
}
