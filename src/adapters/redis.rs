use crate::{
    adapters::AdapterHealth,
    contracts::{CoreError, CoreResult},
    ports::{CacheEntry, RedisPort},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisAdapter {
    pub url: String,
    pub key_prefix: String,
}

impl RedisAdapter {
    pub fn new(url: impl Into<String>, key_prefix: impl Into<String>) -> CoreResult<Self> {
        let url = url.into();
        let key_prefix = key_prefix.into();
        if url.trim().is_empty() || key_prefix.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "redis_url and cache key prefix are required".to_string(),
            ));
        }
        Ok(Self { url, key_prefix })
    }

    pub fn namespaced_key(&self, key: &str) -> String {
        format!("{}:{}", self.key_prefix, key)
    }

    pub fn health(&self) -> AdapterHealth {
        AdapterHealth::configured("redis")
    }
}

impl RedisPort for RedisAdapter {
    fn put_cache(&self, _entry: &CacheEntry) -> CoreResult<()> {
        Err(CoreError::InvalidInput(
            "redis driver execution is not enabled in this build".to_string(),
        ))
    }

    fn get_cache(&self, _key: &str) -> CoreResult<Option<String>> {
        Err(CoreError::InvalidInput(
            "redis driver execution is not enabled in this build".to_string(),
        ))
    }

    fn invalidate_prefix(&self, _prefix: &str) -> CoreResult<()> {
        Err(CoreError::InvalidInput(
            "redis driver execution is not enabled in this build".to_string(),
        ))
    }

    fn acquire_lease(&self, _key: &str, _owner: &str, _ttl_seconds: u64) -> CoreResult<bool> {
        Err(CoreError::InvalidInput(
            "redis driver execution is not enabled in this build".to_string(),
        ))
    }

    fn release_lease(&self, _key: &str, _owner: &str) -> CoreResult<()> {
        Err(CoreError::InvalidInput(
            "redis driver execution is not enabled in this build".to_string(),
        ))
    }
}
