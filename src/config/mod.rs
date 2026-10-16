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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
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
impl Default for ScoringWeights {
    fn default() -> Self {
        Self {
            semantic_similarity: 0.7,
            recency: 0.1,
            importance: 0.1,
            access: 0.1,
        }
    }
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
    #[serde(default)]
    pub qdrant_collection: Option<String>,
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
impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            key_prefix: "nextral".into(),
            session_ttl_seconds: 7200,
            retrieval_ttl_seconds: 300,
            policy_ttl_seconds: 3600,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceConfig {
    pub http_bind: Option<String>,
    pub grpc_bind: Option<String>,
    pub graphql_bind: Option<String>,
}
impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            http_bind: None,
            grpc_bind: None,
            graphql_bind: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthConfig {
    pub issuer: Option<String>,
    pub audience: Option<String>,
    pub jwks_url: Option<String>,
    /// Environment variable containing an additional PEM CA for the HTTPS JWKS endpoint.
    #[serde(default)]
    pub jwks_ca_pem_env: Option<String>,
    pub service_token_env: Option<String>,
}
impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            issuer: None,
            audience: None,
            jwks_url: None,
            jwks_ca_pem_env: None,
            service_token_env: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObservabilityConfig {
    pub enabled: bool,
    pub otlp_endpoint: Option<String>,
    pub prometheus_bind: Option<String>,
}
impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            otlp_endpoint: None,
            prometheus_bind: None,
        }
    }
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionConfig {
    #[serde(default = "default_max_in_flight")]
    pub max_in_flight: usize,
    #[serde(default = "default_max_pending")]
    pub max_pending: usize,
    #[serde(default = "default_max_pending_bytes")]
    pub max_pending_bytes: usize,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_shutdown_timeout_ms")]
    pub shutdown_timeout_ms: u64,
    #[serde(default = "default_lease_seconds")]
    pub lease_seconds: u32,
}

fn default_max_in_flight() -> usize {
    8
}
fn default_max_pending() -> usize {
    64
}
fn default_max_pending_bytes() -> usize {
    16777216
}
fn default_request_timeout_ms() -> u64 {
    30000
}
fn default_shutdown_timeout_ms() -> u64 {
    30000
}
fn default_lease_seconds() -> u32 {
    30
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            max_in_flight: default_max_in_flight(),
            max_pending: default_max_pending(),
            max_pending_bytes: default_max_pending_bytes(),
            request_timeout_ms: default_request_timeout_ms(),
            shutdown_timeout_ms: default_shutdown_timeout_ms(),
            lease_seconds: default_lease_seconds(),
        }
    }
}

fn default_control_namespace() -> String {
    "default".to_string()
}

fn default_base_policy_version() -> String {
    "configured-base".to_string()
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
    #[serde(default)]
    pub execution: ExecutionConfig,
    #[serde(default = "default_control_namespace")]
    pub control_namespace: String,
    #[serde(default = "default_base_policy_version")]
    pub base_policy_version: String,
}
impl Default for NextralConfig {
    fn default() -> Self {
        Self {
            backend: RuntimeBackend::TestMemory,
            stores: None,
            embedding: EmbeddingProviderConfig {
                kind: EmbeddingProviderKind::Test,
                model: "test-model".to_string(),
                dimension: 128,
                endpoint: None,
                api_key_env: None,
            },
            extraction: ExtractionProviderConfig {
                kind: ExtractionProviderKind::Test,
                model: "test-model".to_string(),
                endpoint: None,
                api_key_env: None,
            },
            reranker: Some(RerankerProviderConfig {
                kind: RerankerProviderKind::None,
                model: None,
                endpoint: None,
                api_key_env: None,
            }),
            ingestion_policy: IngestionPolicy {
                min_importance_score: 0.0,
                min_confidence_score: 0.0,
            },
            retrieval_policy: RetrievalPolicy {
                privacy_scope: vec![crate::memory::PrivacyLevel::Private],
                token_budget: 1024,
                top_k_vector: 10,
                max_graph_hops: 2,
                scoring_weights: ScoringWeights::default(),
            },
            cache: CacheConfig::default(),
            service: ServiceConfig::default(),
            auth: AuthConfig::default(),
            observability: ObservabilityConfig::default(),
            rate_limit: RateLimitConfig::default(),
            batch: BatchConfig::default(),
            execution: ExecutionConfig::default(),
            control_namespace: default_control_namespace(),
            base_policy_version: default_base_policy_version(),
        }
    }
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
        validate_auth_config(&self.auth)?;

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
        if self.execution.max_in_flight == 0 || self.execution.max_in_flight > 64 {
            return Err(CoreError::InvalidInput(
                "execution.max_in_flight must be between 1 and 64".to_string(),
            ));
        }
        if self.execution.max_pending < self.execution.max_in_flight
            || self.execution.max_pending > 1024
        {
            return Err(CoreError::InvalidInput(
                "execution.max_pending must be between max_in_flight and 1024".to_string(),
            ));
        }
        if self.execution.max_pending_bytes < 10485760
            || self.execution.max_pending_bytes > 268435456
        {
            return Err(CoreError::InvalidInput(
                "execution.max_pending_bytes must be between 10485760 and 268435456".to_string(),
            ));
        }
        if self.execution.request_timeout_ms == 0 || self.execution.request_timeout_ms > 300000 {
            return Err(CoreError::InvalidInput(
                "execution.request_timeout_ms must be between 1 and 300000".to_string(),
            ));
        }
        if self.execution.shutdown_timeout_ms == 0 || self.execution.shutdown_timeout_ms > 300000 {
            return Err(CoreError::InvalidInput(
                "execution.shutdown_timeout_ms must be between 1 and 300000".to_string(),
            ));
        }
        if self.execution.lease_seconds < 15 || self.execution.lease_seconds > 3600 {
            return Err(CoreError::InvalidInput(
                "execution.lease_seconds must be between 15 and 3600".to_string(),
            ));
        }
        if self.control_namespace.is_empty()
            || self.control_namespace.len() > 64
            || !self
                .control_namespace
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(CoreError::InvalidInput(
                "control_namespace must be 1..=64 characters of ASCII letters, digits, '_' or '-'"
                    .to_string(),
            ));
        }
        if self.base_policy_version.is_empty() || self.base_policy_version.len() > 128 {
            return Err(CoreError::InvalidInput(
                "base_policy_version must be nonempty and at most 128 bytes".to_string(),
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
    let config: NextralConfig = serde_json::from_str(&resolved)
        .map_err(|_| CoreError::InvalidInput("invalid input format".into()))?;
    config.validate()?;
    Ok(config)
}

pub fn validate_config_json(config_json: &str) -> CoreResult<String> {
    load_config(config_json)?;
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
    if [
        weights.semantic_similarity,
        weights.recency,
        weights.importance,
        weights.access,
    ]
    .iter()
    .any(|weight| !weight.is_finite() || *weight < 0.0)
    {
        return Err(CoreError::InvalidInput(
            "scoring weights must be finite and non-negative".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_retrieval_weights(weights: &ScoringWeights) -> CoreResult<()> {
    validate_scoring_weights(weights)?;
    let total = weights.semantic_similarity + weights.recency + weights.importance + weights.access;
    if (total - 1.0).abs() > 0.001 {
        return Err(CoreError::InvalidInput(
            "retrieval scoring weights must sum to 1.0".to_string(),
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
    validate_retrieval_weights(&policy.scoring_weights)?;
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

fn validate_auth_config(config: &AuthConfig) -> CoreResult<()> {
    validate_jwks_ca(config)?;
    match (&config.issuer, &config.audience, &config.jwks_url) {
        (None, None, None) => Ok(()),
        (Some(_), Some(_), Some(url)) => {
            if !url.starts_with("https://") {
                return Err(CoreError::InvalidInput(
                    "auth.jwks_url must use HTTPS".into(),
                ));
            }
            Ok(())
        }
        _ => Err(CoreError::InvalidInput(
            "issuer, audience and jwks_url must be configured together".into(),
        )),
    }
}

fn validate_jwks_ca(config: &AuthConfig) -> CoreResult<()> {
    if let Some(name) = &config.jwks_ca_pem_env {
        require("auth.jwks_ca_pem_env", name)?;
        if name.bytes().any(|byte| matches!(byte, b'=' | 0)) {
            return Err(CoreError::InvalidInput(
                "auth.jwks_ca_pem_env must be a valid environment variable name".into(),
            ));
        }
        require_option("auth.issuer", &config.issuer)?;
        require_option("auth.audience", &config.audience)?;
        require_option("auth.jwks_url", &config.jwks_url)?;
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
    let collection = config.qdrant_collection.as_deref().ok_or_else(|| {
        CoreError::InvalidInput("stores.qdrant_collection is required for production".into())
    })?;
    if collection.is_empty()
        || collection.len() > 240
        || !collection
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        || matches!(collection, "." | "..")
    {
        return Err(CoreError::InvalidInput(
            "stores.qdrant_collection must be a valid nonempty collection alias".into(),
        ));
    }
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
    let require_tls = config.enforce_tls.unwrap_or(false)
        || config.transport_profile.as_deref() == Some("strict");
    crate::adapters::transport::validate_transport_url(&config.qdrant_url, require_tls)
        .map_err(CoreError::InvalidInput)?;
    crate::adapters::transport::validate_transport_url(&config.s3_endpoint, require_tls)
        .map_err(CoreError::InvalidInput)?;
    let neo4j = config
        .neo4j_url
        .replacen("neo4j+s://", "https://", 1)
        .replacen("bolt+s://", "https://", 1)
        .replacen("neo4j://", "http://", 1)
        .replacen("bolt://", "http://", 1);
    crate::adapters::transport::validate_transport_url(&neo4j, require_tls)
        .map_err(CoreError::InvalidInput)?;
    let redis = reqwest::Url::parse(&config.redis_url)
        .map_err(|_| CoreError::InvalidInput("invalid stores.redis_url".into()))?;
    if !matches!(redis.scheme(), "redis" | "rediss")
        || redis.host_str().is_none()
        || redis.fragment().is_some()
    {
        return Err(CoreError::InvalidInput(
            "stores.redis_url must use redis:// or verified rediss:// without fragments".into(),
        ));
    }
    if require_tls {
        if redis.scheme() != "rediss" {
            return Err(CoreError::InvalidInput(
                "strict Redis transport requires rediss://".into(),
            ));
        }
        validate_postgres_tls(&config.postgres_url)?;
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

fn validate_postgres_tls(value: &str) -> CoreResult<()> {
    let mode = if value.starts_with("postgres://") || value.starts_with("postgresql://") {
        let url = reqwest::Url::parse(value)
            .map_err(|_| CoreError::InvalidInput("invalid stores.postgres_url".into()))?;
        let modes: Vec<String> = url
            .query_pairs()
            .filter(|(key, _)| key == "sslmode")
            .map(|(_, value)| value.into_owned())
            .collect();
        if modes.len() != 1 {
            return Err(CoreError::InvalidInput(
                "strict Postgres transport requires exactly one sslmode=require or verify-full"
                    .into(),
            ));
        }
        modes.into_iter().next()
    } else {
        let modes: Vec<&str> = value
            .split_whitespace()
            .filter_map(|part| part.strip_prefix("sslmode="))
            .collect();
        if modes.len() != 1 {
            return Err(CoreError::InvalidInput(
                "strict Postgres transport requires exactly one sslmode=require or verify-full"
                    .into(),
            ));
        }
        modes
            .first()
            .map(|value| value.trim_matches('\'').to_string())
    };
    if !matches!(
        mode.as_deref(),
        Some("require" | "verify-ca" | "verify-full")
    ) {
        return Err(CoreError::InvalidInput("strict Postgres transport requires sslmode=require, verify-ca, or verify-full with certificate verification".into()));
    }
    Ok(())
}

#[cfg(test)]
mod hardening_tests {
    use super::*;

    fn jwt_auth() -> AuthConfig {
        AuthConfig {
            issuer: Some("https://issuer.example".into()),
            audience: Some("nextral".into()),
            jwks_url: Some("https://issuer.example/jwks".into()),
            jwks_ca_pem_env: Some("NEXTRAL_JWKS_CA".into()),
            service_token_env: None,
        }
    }

    #[test]
    fn jwks_ca_environment_name_rejects_empty_and_invalid_names() {
        for name in ["", " ", "BAD=NAME", "BAD\0NAME"] {
            let mut auth = jwt_auth();
            auth.jwks_ca_pem_env = Some(name.into());
            assert!(validate_jwks_ca(&auth).is_err());
        }
        assert!(validate_jwks_ca(&jwt_auth()).is_ok());
    }

    #[test]
    fn jwks_ca_requires_complete_jwt_configuration() {
        for field in 0..3 {
            let mut auth = jwt_auth();
            match field {
                0 => auth.issuer = None,
                1 => auth.audience = None,
                _ => auth.jwks_url = None,
            }
            assert!(validate_jwks_ca(&auth).is_err());
        }
    }

    #[test]
    fn jwks_ca_is_optional_when_deserializing_existing_configuration() {
        let auth: AuthConfig = serde_json::from_value(serde_json::json!({
            "issuer": null, "audience": null, "jwks_url": null, "service_token_env": null
        }))
        .unwrap();
        assert!(auth.jwks_ca_pem_env.is_none());
        assert!(validate_jwks_ca(&auth).is_ok());
    }

    #[test]
    fn postgres_tls_requires_explicit_unambiguous_mode() {
        assert!(validate_postgres_tls("postgresql://localhost/db").is_err());
        assert!(
            validate_postgres_tls("postgresql://localhost/db?sslmode=disable&sslmode=require")
                .is_err()
        );
        assert!(validate_postgres_tls("postgresql://localhost/db?sslmode=require").is_ok());
        assert!(validate_postgres_tls("postgresql://localhost/db?sslmode=verify-ca").is_ok());
        assert!(validate_postgres_tls("postgresql://localhost/db?sslmode=disable").is_err());
        assert!(validate_postgres_tls("host=localhost sslmode='verify-full'").is_ok());
        assert!(validate_postgres_tls("host=localhost sslmode=prefer").is_err());
    }

    #[test]
    fn scoring_weights_reject_each_nonfinite_component() {
        for invalid in [f32::NAN, f32::INFINITY] {
            for index in 0..4 {
                let mut values = [0.25; 4];
                values[index] = invalid;
                let weights = ScoringWeights {
                    semantic_similarity: values[0],
                    recency: values[1],
                    importance: values[2],
                    access: values[3],
                };
                assert!(validate_scoring_weights(&weights).is_err());
            }
        }
    }

    #[test]
    fn retrieval_weights_reject_zero_and_overflowing_totals() {
        for weight in [0.0, 2.0, f32::MAX] {
            let weights = ScoringWeights {
                semantic_similarity: weight,
                recency: weight,
                importance: weight,
                access: weight,
            };
            assert!(validate_retrieval_weights(&weights).is_err());
        }
    }
}
