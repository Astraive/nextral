pub mod neo4j;
pub mod postgres;
pub mod qdrant;
pub mod redis;
pub mod s3;
pub mod transport;

use crate::{
    config::StoreConfig,
    contracts::{CoreError, CoreResult},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionStoreEndpoints {
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

impl ProductionStoreEndpoints {
    pub fn from_config(config: &StoreConfig) -> CoreResult<Self> {
        let endpoints = Self {
            postgres_url: config.postgres_url.clone(),
            redis_url: config.redis_url.clone(),
            qdrant_url: config.qdrant_url.clone(),
            neo4j_url: config.neo4j_url.clone(),
            s3_endpoint: config.s3_endpoint.clone(),
            s3_bucket: config.s3_bucket.clone(),
            s3_region: config.s3_region.clone(),
            s3_access_key_env: config.s3_access_key_env.clone(),
            s3_secret_key_env: config.s3_secret_key_env.clone(),
        };
        endpoints.validate()?;
        Ok(endpoints)
    }

    pub fn validate(&self) -> CoreResult<()> {
        for (name, value) in [
            ("postgres_url", &self.postgres_url),
            ("redis_url", &self.redis_url),
            ("qdrant_url", &self.qdrant_url),
            ("neo4j_url", &self.neo4j_url),
            ("s3_endpoint", &self.s3_endpoint),
            ("s3_bucket", &self.s3_bucket),
            ("s3_region", &self.s3_region),
            ("s3_access_key_env", &self.s3_access_key_env),
            ("s3_secret_key_env", &self.s3_secret_key_env),
        ] {
            if value.trim().is_empty() {
                return Err(CoreError::InvalidInput(format!("{name} is required")));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterHealth {
    pub backend: String,
    pub configured: bool,
    pub detail: String,
}

impl AdapterHealth {
    pub fn configured(backend: impl Into<String>) -> Self {
        Self {
            backend: backend.into(),
            configured: true,
            detail: "configuration validated; network check not executed".to_string(),
        }
    }
}
