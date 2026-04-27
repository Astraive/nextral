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

    fn connection(&self) -> CoreResult<redis::Connection> {
        redis::Client::open(self.url.as_str())
            .map_err(|error| CoreError::Io(error.to_string()))?
            .get_connection()
            .map_err(|error| CoreError::Io(error.to_string()))
    }
}

impl RedisPort for RedisAdapter {
    fn put_cache(&self, entry: &CacheEntry) -> CoreResult<()> {
        let mut connection = self.connection()?;
        let key = self.namespaced_key(&entry.key);
        let _: () = redis::cmd("SETEX")
            .arg(key)
            .arg(entry.ttl_seconds)
            .arg(&entry.value_json)
            .query(&mut connection)
            .map_err(|error| CoreError::Io(error.to_string()))?;
        Ok(())
    }

    fn get_cache(&self, key: &str) -> CoreResult<Option<String>> {
        let mut connection = self.connection()?;
        let value: Option<String> = redis::cmd("GET")
            .arg(self.namespaced_key(key))
            .query(&mut connection)
            .map_err(|error| CoreError::Io(error.to_string()))?;
        Ok(value)
    }

    fn invalidate_prefix(&self, prefix: &str) -> CoreResult<()> {
        let mut connection = self.connection()?;
        let pattern = self.namespaced_key(&format!("{prefix}*"));
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(pattern)
            .query(&mut connection)
            .map_err(|error| CoreError::Io(error.to_string()))?;
        if !keys.is_empty() {
            let _: () = redis::cmd("DEL")
                .arg(keys)
                .query(&mut connection)
                .map_err(|error| CoreError::Io(error.to_string()))?;
        }
        Ok(())
    }

    fn acquire_lease(&self, key: &str, owner: &str, ttl_seconds: u64) -> CoreResult<bool> {
        let mut connection = self.connection()?;
        let result: Option<String> = redis::cmd("SET")
            .arg(self.namespaced_key(key))
            .arg(owner)
            .arg("NX")
            .arg("EX")
            .arg(ttl_seconds)
            .query(&mut connection)
            .map_err(|error| CoreError::Io(error.to_string()))?;
        Ok(result.as_deref() == Some("OK"))
    }

    fn release_lease(&self, key: &str, owner: &str) -> CoreResult<()> {
        let mut connection = self.connection()?;
        let namespaced = self.namespaced_key(key);
        let current: Option<String> = redis::cmd("GET")
            .arg(&namespaced)
            .query(&mut connection)
            .map_err(|error| CoreError::Io(error.to_string()))?;
        if current.as_deref() == Some(owner) {
            let _: () = redis::cmd("DEL")
                .arg(namespaced)
                .query(&mut connection)
                .map_err(|error| CoreError::Io(error.to_string()))?;
        }
        Ok(())
    }
}
