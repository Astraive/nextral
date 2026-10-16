use crate::{
    adapters::{
        transport::{key_component, TransportHardeningProfile},
        AdapterHealth,
    },
    contracts::{CoreError, CoreResult},
    ports::{CacheEntry, RedisPort, TenantUserScope},
};
#[cfg(feature = "redis")]
use std::sync::Mutex;

pub struct RedisAdapter {
    pub url: String,
    pub key_prefix: String,
    pub hardening: TransportHardeningProfile,
    #[cfg(feature = "redis")]
    client: redis::Client,
    #[cfg(feature = "redis")]
    pool: Mutex<Vec<redis::Connection>>,
}
impl PartialEq for RedisAdapter {
    fn eq(&self, other: &Self) -> bool {
        self.url == other.url
            && self.key_prefix == other.key_prefix
            && self.hardening == other.hardening
    }
}
impl Eq for RedisAdapter {}
macro_rules! redis_operation {
    ($this:ident,$conn:ident,$body:block) => {{
        #[cfg(feature = "redis")]
        {
            $this.with_connection(|$conn| $body)
        }
        #[cfg(not(feature = "redis"))]
        {
            Err(CoreError::InvalidInput(
                "Redis support is disabled; enable the redis feature".into(),
            ))
        }
    }};
}
impl RedisAdapter {
    pub fn new(url: impl Into<String>, key_prefix: impl Into<String>) -> CoreResult<Self> {
        let url = url.into();
        let key_prefix = key_prefix.into();
        if key_prefix.is_empty() {
            return Err(CoreError::InvalidInput("Redis key prefix required".into()));
        }
        let parsed = reqwest::Url::parse(&url)
            .map_err(|_| CoreError::InvalidInput("invalid Redis URL".into()))?;
        if !matches!(parsed.scheme(), "redis" | "rediss") || parsed.host_str().is_none() {
            return Err(CoreError::InvalidInput(
                "Redis URL must use redis:// or rediss://".into(),
            ));
        }
        if parsed.fragment().is_some() {
            return Err(CoreError::InvalidInput(
                "Redis URL fragments (including insecure TLS) are forbidden".into(),
            ));
        }
        Ok(Self {
            #[cfg(feature = "redis")]
            client: redis::Client::open(url.as_str())
                .map_err(|e| CoreError::InvalidInput(e.to_string()))?,
            url,
            key_prefix,
            hardening: TransportHardeningProfile::baseline(None),
            #[cfg(feature = "redis")]
            pool: Mutex::new(Vec::new()),
        })
    }
    #[cfg(feature = "redis")]
    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&mut redis::Connection) -> redis::RedisResult<T>,
    ) -> CoreResult<T> {
        use std::time::Duration;
        if self.hardening.require_tls && !self.url.starts_with("rediss://") {
            return Err(CoreError::InvalidInput(
                "strict Redis transport requires rediss://".into(),
            ));
        }
        if self.hardening.connect_timeout_ms == 0 || self.hardening.request_timeout_ms == 0 {
            return Err(CoreError::InvalidInput(
                "Redis timeouts must be positive".into(),
            ));
        }
        let mut conn = self
            .pool
            .lock()
            .map_err(|e| CoreError::Io(e.to_string()))?
            .pop();
        if conn.is_none() {
            conn = Some(
                self.client
                    .get_connection_with_timeout(Duration::from_millis(
                        self.hardening.connect_timeout_ms,
                    ))
                    .map_err(|e| CoreError::Io(e.to_string()))?,
            );
        }
        let mut conn = conn.ok_or_else(|| CoreError::Io("Redis connection unavailable".into()))?;
        conn.set_read_timeout(Some(Duration::from_millis(
            self.hardening.request_timeout_ms,
        )))
        .map_err(|e| CoreError::Io(e.to_string()))?;
        conn.set_write_timeout(Some(Duration::from_millis(
            self.hardening.request_timeout_ms,
        )))
        .map_err(|e| CoreError::Io(e.to_string()))?;
        let result = operation(&mut conn).map_err(|e| CoreError::Io(e.to_string()));
        // Discard failed connections rather than returning broken sockets to callers.
        if result.is_ok() {
            let mut pool = self.pool.lock().map_err(|e| CoreError::Io(e.to_string()))?;
            if pool.len() < 10 {
                pool.push(conn);
            }
        }
        result
    }
    pub fn namespaced_key(&self, key: &str) -> String {
        format!("{}:{}", key_component(&self.key_prefix), key_component(key))
    }
    pub fn scoped_key(&self, scope: &TenantUserScope, key: &str) -> CoreResult<String> {
        if scope.tenant_id.is_empty() || scope.user_id.is_empty() {
            return Err(CoreError::InvalidInput(
                "Redis tenant and user required".into(),
            ));
        }
        Ok(format!(
            "tenant:{}:user:{}:{}",
            key_component(&scope.tenant_id),
            key_component(&scope.user_id),
            key
        ))
    }
    pub fn health(&self) -> AdapterHealth {
        AdapterHealth::configured("redis")
    }
    pub fn readiness(&self) -> CoreResult<String> {
        redis_operation!(self, conn, { redis::cmd("PING").query(conn) })
    }
    pub fn put_scoped_cache(
        &self,
        scope: &TenantUserScope,
        key: &str,
        value_json: &str,
        ttl_seconds: u64,
    ) -> CoreResult<()> {
        self.put_cache(&CacheEntry {
            key: self.scoped_key(scope, &format!("cache:{key}"))?,
            value_json: value_json.into(),
            ttl_seconds,
        })
    }
    pub fn get_scoped_cache(
        &self,
        scope: &TenantUserScope,
        key: &str,
    ) -> CoreResult<Option<String>> {
        self.get_cache(&self.scoped_key(scope, &format!("cache:{key}"))?)
    }
    pub fn invalidate_scope(&self, scope: &TenantUserScope) -> CoreResult<()> {
        self.invalidate_prefix(&self.scoped_key(scope, "")?)
    }
    pub fn hot_tail_append(
        &self,
        scope: &TenantUserScope,
        session_id: &str,
        value_json: &str,
        max_entries: usize,
        ttl_seconds: u64,
    ) -> CoreResult<()> {
        validate_ttl(ttl_seconds)?;
        if session_id.is_empty() || max_entries == 0 || max_entries > 100000 {
            return Err(CoreError::InvalidInput(
                "hot tail requires session and max_entries 1..100000".into(),
            ));
        }
        serde_json::from_str::<serde_json::Value>(value_json)?;
        let key = self.namespaced_key(
            &self.scoped_key(scope, &format!("tail:{}", key_component(session_id)))?,
        );
        redis_operation!(self, conn, {
            redis::Script::new("redis.call('LPUSH',KEYS[1],ARGV[1]); redis.call('LTRIM',KEYS[1],0,tonumber(ARGV[2])-1); redis.call('EXPIRE',KEYS[1],ARGV[3]); return 1")
                .key(&key).arg(value_json).arg(max_entries).arg(ttl_seconds).invoke::<i32>(conn).map(|_|())
        })
    }
    pub fn hot_tail_replace(
        &self,
        scope: &TenantUserScope,
        session_id: &str,
        values_json: &[String],
        ttl_seconds: u64,
    ) -> CoreResult<()> {
        validate_ttl(ttl_seconds)?;
        if session_id.is_empty() {
            return Err(CoreError::InvalidInput("hot tail session required".into()));
        }
        let values = &values_json[values_json.len().saturating_sub(20)..];
        for value in values {
            serde_json::from_str::<serde_json::Value>(value)?;
        }
        let key = self.namespaced_key(
            &self.scoped_key(scope, &format!("tail:{}", key_component(session_id)))?,
        );
        redis_operation!(self, conn, {
            let script=redis::Script::new("redis.call('DEL',KEYS[1]); for i=2,#ARGV do redis.call('LPUSH',KEYS[1],ARGV[i]); end; if #ARGV > 1 then redis.call('EXPIRE',KEYS[1],ARGV[1]); end; return #ARGV-1");
            script
                .key(&key)
                .arg(ttl_seconds)
                .arg(values)
                .invoke::<usize>(conn)
                .map(|_| ())
        })
    }
    pub fn hot_tail_read(
        &self,
        scope: &TenantUserScope,
        session_id: &str,
        limit: usize,
    ) -> CoreResult<Vec<String>> {
        if session_id.is_empty() || limit == 0 || limit > 100000 {
            return Err(CoreError::InvalidInput(
                "hot tail requires session and limit 1..100000".into(),
            ));
        }
        let key = self.namespaced_key(
            &self.scoped_key(scope, &format!("tail:{}", key_component(session_id)))?,
        );
        let mut result: Vec<String> = redis_operation!(self, conn, {
            redis::cmd("LRANGE")
                .arg(&key)
                .arg(0)
                .arg(limit - 1)
                .query(conn)
        })?;
        result.reverse();
        Ok(result)
    }
    pub fn renew_lease(&self, key: &str, owner: &str, ttl_seconds: u64) -> CoreResult<bool> {
        validate_lease(key, owner, ttl_seconds)?;
        let key = self.namespaced_key(key);
        let result: i32 = redis_operation!(self, conn, {
            redis::Script::new("if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('EXPIRE',KEYS[1],ARGV[2]) else return 0 end").key(&key).arg(owner).arg(ttl_seconds).invoke(conn)
        })?;
        Ok(result == 1)
    }
}
impl RedisPort for RedisAdapter {
    fn put_cache(&self, entry: &CacheEntry) -> CoreResult<()> {
        validate_ttl(entry.ttl_seconds)?;
        serde_json::from_str::<serde_json::Value>(&entry.value_json)?;
        redis_operation!(self, conn, {
            redis::cmd("SETEX")
                .arg(self.namespaced_key(&entry.key))
                .arg(entry.ttl_seconds)
                .arg(&entry.value_json)
                .query(conn)
        })
    }
    fn get_cache(&self, key: &str) -> CoreResult<Option<String>> {
        redis_operation!(self, conn, {
            redis::cmd("GET").arg(self.namespaced_key(key)).query(conn)
        })
    }
    fn invalidate_prefix(&self, prefix: &str) -> CoreResult<()> {
        // Append the wildcard after encoding; caller bytes can never become glob syntax.
        let pattern = format!("{}*", self.namespaced_key(prefix));
        redis_operation!(self, conn, {
            let mut cursor = 0u64;
            loop {
                let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(&pattern)
                    .arg("COUNT")
                    .arg(100)
                    .query(conn)?;
                if !keys.is_empty() {
                    redis::cmd("UNLINK").arg(keys).query::<usize>(conn)?;
                }
                cursor = next;
                if cursor == 0 {
                    break;
                }
            }
            Ok(())
        })
    }
    fn acquire_lease(&self, key: &str, owner: &str, ttl_seconds: u64) -> CoreResult<bool> {
        validate_lease(key, owner, ttl_seconds)?;
        let value: Option<String> = redis_operation!(self, conn, {
            redis::cmd("SET")
                .arg(self.namespaced_key(key))
                .arg(owner)
                .arg("NX")
                .arg("EX")
                .arg(ttl_seconds)
                .query(conn)
        })?;
        Ok(value.as_deref() == Some("OK"))
    }
    fn release_lease(&self, key: &str, owner: &str) -> CoreResult<()> {
        if key.is_empty() || owner.is_empty() {
            return Err(CoreError::InvalidInput(
                "lease key and owner required".into(),
            ));
        }
        let deleted: i32 = redis_operation!(self, conn, {
            redis::Script::new("if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end").key(self.namespaced_key(key)).arg(owner).invoke(conn)
        })?;
        let _ = deleted;
        Ok(())
    }
}
fn validate_ttl(ttl: u64) -> CoreResult<()> {
    if ttl == 0 || ttl > 365 * 24 * 60 * 60 {
        return Err(CoreError::InvalidInput(
            "Redis TTL must be 1..31536000 seconds".into(),
        ));
    }
    Ok(())
}
fn validate_lease(key: &str, owner: &str, ttl: u64) -> CoreResult<()> {
    validate_ttl(ttl)?;
    if key.is_empty() || owner.is_empty() {
        return Err(CoreError::InvalidInput(
            "lease key and owner required".into(),
        ));
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cache_keys_do_not_alias_sanitized_or_truncated_inputs() {
        let adapter = RedisAdapter::new("redis://localhost:6379", "nextral").unwrap();
        assert_ne!(adapter.namespaced_key("a:b"), adapter.namespaced_key("ab"));
        assert_ne!(
            adapter.namespaced_key(&"a".repeat(300)),
            adapter.namespaced_key(&"a".repeat(301))
        );
        let a = TenantUserScope {
            tenant_id: "a:b".into(),
            user_id: "c".into(),
        };
        let b = TenantUserScope {
            tenant_id: "a".into(),
            user_id: "b:c".into(),
        };
        assert_ne!(
            adapter.scoped_key(&a, "cache").unwrap(),
            adapter.scoped_key(&b, "cache").unwrap()
        );
    }
    #[test]
    fn zero_ttl_is_rejected_before_network() {
        let adapter = RedisAdapter::new("redis://localhost:1", "nextral").unwrap();
        assert!(adapter.acquire_lease("lease", "owner", 0).is_err());
        assert!(adapter
            .put_cache(&CacheEntry {
                key: "k".into(),
                value_json: "{}".into(),
                ttl_seconds: 0
            })
            .is_err());
    }
    #[test]
    #[cfg(feature = "redis")]
    #[ignore = "requires Redis at NEXTRAL_REDIS_URL"]
    fn real_redis_scopes_hot_tails_and_checks_lease_owner_atomically() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let adapter = RedisAdapter::new(
            std::env::var("NEXTRAL_REDIS_URL").unwrap(),
            format!("adapter-test-{unique}"),
        )
        .unwrap();
        let a = TenantUserScope {
            tenant_id: "tenant-a".into(),
            user_id: "user".into(),
        };
        let b = TenantUserScope {
            tenant_id: "tenant-b".into(),
            user_id: "user".into(),
        };
        adapter
            .put_scoped_cache(&a, "query", "{\"ok\":true}", 60)
            .unwrap();
        adapter
            .put_scoped_cache(&b, "query", "{\"ok\":false}", 60)
            .unwrap();
        adapter
            .hot_tail_append(&a, "session", "{\"n\":1}", 2, 60)
            .unwrap();
        adapter
            .hot_tail_append(&a, "session", "{\"n\":2}", 2, 60)
            .unwrap();
        adapter
            .hot_tail_append(&a, "session", "{\"n\":3}", 2, 60)
            .unwrap();
        assert_eq!(
            adapter.hot_tail_read(&a, "session", 10).unwrap(),
            vec!["{\"n\":2}", "{\"n\":3}"]
        );
        assert!(adapter.hot_tail_read(&b, "session", 10).unwrap().is_empty());
        let canonical: Vec<String> = (0..25).map(|n| format!("{{\"n\":{n}}}")).collect();
        adapter
            .hot_tail_replace(&a, "session", &canonical, 60)
            .unwrap();
        adapter
            .hot_tail_replace(&a, "session", &canonical, 60)
            .unwrap();
        assert_eq!(
            adapter.hot_tail_read(&a, "session", 100).unwrap(),
            canonical[5..]
        );
        adapter.hot_tail_replace(&a, "session", &[], 60).unwrap();
        assert!(adapter
            .hot_tail_read(&a, "session", 100)
            .unwrap()
            .is_empty());
        let lease = adapter.scoped_key(&a, "lease").unwrap();
        assert!(adapter.acquire_lease(&lease, "owner-a", 60).unwrap());
        assert!(!adapter.acquire_lease(&lease, "owner-b", 60).unwrap());
        assert!(!adapter.renew_lease(&lease, "owner-b", 60).unwrap());
        adapter.release_lease(&lease, "owner-b").unwrap();
        assert!(!adapter.acquire_lease(&lease, "owner-b", 60).unwrap());
        adapter.release_lease(&lease, "owner-a").unwrap();
        assert!(adapter.acquire_lease(&lease, "owner-b", 60).unwrap());
        adapter.invalidate_scope(&a).unwrap();
        assert!(adapter.get_scoped_cache(&a, "query").unwrap().is_none());
        assert_eq!(
            adapter.get_scoped_cache(&b, "query").unwrap().as_deref(),
            Some("{\"ok\":false}")
        );
        adapter.invalidate_scope(&b).unwrap();
    }
}
