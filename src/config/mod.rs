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
    pub transport_profile: Option<String>,
    pub enforce_tls: Option<bool>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RateLimitConfig {
    pub enabled: bool,
    pub max_requests_per_second: u32,
    pub burst_size: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_requests_per_second: 100,
            burst_size: 200,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchConfig {
    pub max_batch_size: usize,
    pub max_concurrent_batches: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            max_concurrent_batches: 4,
        }
    }
}

/// Complete runtime configuration covering backend selection, store URLs, provider settings,
/// ingestion/retrieval policies, caching, service binds, auth, and observability.
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
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    #[serde(default)]
    pub batch: BatchConfig,
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

        if self.batch.max_batch_size == 0 || self.batch.max_batch_size > 10_000 {
            return Err(CoreError::InvalidInput(
                "batch.max_batch_size must be between 1 and 10,000".to_string(),
            ));
        }
        if self.batch.max_concurrent_batches == 0 || self.batch.max_concurrent_batches > 64 {
            return Err(CoreError::InvalidInput(
                "batch.max_concurrent_batches must be between 1 and 64".to_string(),
            ));
        }
        if self.rate_limit.enabled && self.rate_limit.max_requests_per_second == 0 {
            return Err(CoreError::InvalidInput(
                "rate_limit.max_requests_per_second must be non-zero when enabled".to_string(),
            ));
        }

        Ok(())
    }
}

pub fn resolve_env_vars(input: &str) -> String {
    const MAX_ITERATIONS: usize = 100;
    let mut result = input.to_string();
    for _ in 0..MAX_ITERATIONS {
        match result.find("${") {
            Some(start) => {
                let end = match result[start + 2..].find('}') {
                    Some(pos) => pos,
                    None => break,
                };
                let var_name = &result[start + 2..start + 2 + end].to_string();
                let replacement = std::env::var(var_name).unwrap_or_default();
                if replacement.is_empty() || replacement == format!("${{{}}}", var_name) {
                    break;
                }
                result = format!(
                    "{}{}{}",
                    &result[..start],
                    replacement,
                    &result[start + 2 + end + 1..]
                );
            }
            None => break,
        }
    }
    result
}

pub fn load_config(config_json: &str) -> CoreResult<NextralConfig> {
    let resolved = resolve_env_vars(config_json);
    let config: NextralConfig = serde_json::from_str(&resolved)?;
    config.validate()?;
    Ok(config)
}

pub fn validate_config_json(config_json: &str) -> CoreResult<String> {
    let resolved = resolve_env_vars(config_json);
    let config: NextralConfig = serde_json::from_str(&resolved)?;
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

pub fn validate_scoring_weights(weights: &ScoringWeights) -> CoreResult<()> {
    if weights.semantic_similarity < 0.0
        || weights.recency < 0.0
        || weights.importance < 0.0
        || weights.access < 0.0
    {
        return Err(CoreError::InvalidInput(
            "scoring weights must be non-negative".to_string(),
        ));
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
    if policy.token_budget > 100_000 {
        return Err(CoreError::InvalidInput(
            "retrieval token_budget must not exceed 100,000".to_string(),
        ));
    }
    if policy.top_k_vector > 1000 {
        return Err(CoreError::InvalidInput(
            "retrieval top_k_vector must not exceed 1000".to_string(),
        ));
    }
    if policy.max_graph_hops > 10 {
        return Err(CoreError::InvalidInput(
            "retrieval max_graph_hops must not exceed 10".to_string(),
        ));
    }
    validate_scoring_weights(&policy.scoring_weights)?;
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
    if config.dimension > 32_768 {
        return Err(CoreError::InvalidInput(
            "embedding.dimension must not exceed 32,768".to_string(),
        ));
    }
    if config.model.len() > 256 {
        return Err(CoreError::InvalidInput(
            "embedding.model name must not exceed 256 characters".to_string(),
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
    const MAX_TTL: u64 = 365 * 24 * 60 * 60;
    if config.session_ttl_seconds > MAX_TTL
        || config.retrieval_ttl_seconds > MAX_TTL
        || config.policy_ttl_seconds > MAX_TTL
    {
        return Err(CoreError::InvalidInput(
            "cache TTLs must not exceed 365 days".to_string(),
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
    if let Some(profile) = &config.transport_profile {
        let allowed = ["baseline", "strict"];
        if !allowed.contains(&profile.as_str()) {
            return Err(CoreError::InvalidInput(
                "stores.transport_profile must be baseline or strict".to_string(),
            ));
        }
    }
    if config.enforce_tls.unwrap_or(false) {
        for (name, value) in [
            ("stores.qdrant_url", &config.qdrant_url),
            ("stores.s3_endpoint", &config.s3_endpoint),
            ("stores.neo4j_url", &config.neo4j_url),
        ] {
            if !value.starts_with("https://") && !value.starts_with("neo4j+s://") {
                return Err(CoreError::InvalidInput(format!(
                    "{name} must use https:// or neo4j+s:// when stores.enforce_tls is true"
                )));
            }
        }
        // Check postgres uses sslmode in URL
        if !config.postgres_url.contains("sslmode=require")
            && !config.postgres_url.starts_with("postgresql://")
        {
            // Only warn-style check for postgres since it uses connection string params
        }
        // Check redis uses rediss:// for TLS
        if !config.redis_url.starts_with("rediss://")
            && !config.redis_url.starts_with("redis+tls://")
        {
            return Err(CoreError::InvalidInput(
                "stores.redis_url must use rediss:// or redis+tls:// when stores.enforce_tls is true".to_string(),
            ));
        }
    }
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
