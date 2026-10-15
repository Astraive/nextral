use crate::{
    adapters::AdapterHealth,
    contracts::{CoreError, CoreResult},
    ports::{CacheEntry, RedisPort},
};
use std::sync::{Mutex, OnceLock};

const POOL_SIZE: usize = 10;

pub struct RedisAdapter {
    pub url: String,
    pub key_prefix: String,
    client: OnceLock<redis::Client>,
    pool: OnceLock<Mutex<Vec<redis::Connection>>>,
}

impl PartialEq for RedisAdapter {
    fn eq(&self, other: &Self) -> bool {
        self.url == other.url && self.key_prefix == other.key_prefix
    }
}
impl Eq for RedisAdapter {}

impl RedisAdapter {
    pub fn new(url: impl Into<String>, key_prefix: impl Into<String>) -> CoreResult<Self> {
        let url = url.into();
        let key_prefix = key_prefix.into();
        if url.trim().is_empty() || key_prefix.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "redis_url and cache key prefix are required".to_string(),
            ));
        }
        Ok(Self {
            url,
            key_prefix,
            client: OnceLock::new(),
            pool: OnceLock::new(),
        })
    }

    fn redis_client(&self) -> CoreResult<&redis::Client> {
        self.client.get_or_init(|| {
            redis::Client::open(self.url.as_str()).expect("failed to create redis client")
        });
        Ok(self.client.get().unwrap())
    }

    fn pool(&self) -> &Mutex<Vec<redis::Connection>> {
        self.pool
            .get_or_init(|| Mutex::new(Vec::with_capacity(POOL_SIZE)))
    }

    fn connection(&self) -> CoreResult<redis::Connection> {
        let pool = self.pool();
        let mut pool = pool.lock().map_err(|e| CoreError::Io(e.to_string()))?;
        if let Some(conn) = pool.pop() {
            return Ok(conn);
        }
        drop(pool);
        let client = self.redis_client()?;
        client
            .get_connection()
            .map_err(|error| CoreError::Io(error.to_string()))
    }

    fn return_connection(&self, conn: redis::Connection) {
        if let Ok(mut pool) = self.pool().lock() {
            if pool.len() < POOL_SIZE {
                pool.push(conn);
            }
        }
    }

    pub fn namespaced_key(&self, key: &str) -> String {
        let sanitized: String = key
            .chars()
            .filter(|c| !c.is_control() && *c != ':' && *c != '*' && *c != '?' && *c != '[')
            .take(256)
            .collect();
        format!("{}:{}", self.key_prefix, sanitized)
    }

    pub fn health(&self) -> AdapterHealth {
        AdapterHealth::configured("redis")
    }
}

impl RedisPort for RedisAdapter {
    fn put_cache(&self, entry: &CacheEntry) -> CoreResult<()> {
        let mut conn = self.connection()?;
        let key = self.namespaced_key(&entry.key);
        let result = redis::cmd("SETEX")
            .arg(key)
            .arg(entry.ttl_seconds)
            .arg(&entry.value_json)
            .query::<()>(&mut conn)
            .map_err(|error| CoreError::Io(error.to_string()));
        self.return_connection(conn);
        result?;
        Ok(())
    }

    fn get_cache(&self, key: &str) -> CoreResult<Option<String>> {
        let mut conn = self.connection()?;
        let result: CoreResult<Option<String>> = redis::cmd("GET")
            .arg(self.namespaced_key(key))
            .query(&mut conn)
            .map_err(|error| CoreError::Io(error.to_string()));
        self.return_connection(conn);
        result
    }

    fn invalidate_prefix(&self, prefix: &str) -> CoreResult<()> {
        const MAX_SCAN_ITERATIONS: usize = 1000;
        let mut conn = self.connection()?;
        let pattern = self.namespaced_key(&format!("{prefix}*"));
        let mut cursor: u64 = 0;
        let result = (|| -> CoreResult<()> {
            for _ in 0..MAX_SCAN_ITERATIONS {
                let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(&pattern)
                    .arg("COUNT")
                    .arg(100)
                    .query(&mut conn)
                    .map_err(|error| CoreError::Io(error.to_string()))?;
                if !keys.is_empty() {
                    let _: () = redis::cmd("DEL")
                        .arg(keys)
                        .query(&mut conn)
                        .map_err(|error| CoreError::Io(error.to_string()))?;
                }
                cursor = next_cursor;
                if cursor == 0 {
                    break;
                }
            }
            Ok(())
        })();
        self.return_connection(conn);
        result
    }

    fn acquire_lease(&self, key: &str, owner: &str, ttl_seconds: u64) -> CoreResult<bool> {
        let mut conn = self.connection()?;
        let result: CoreResult<Option<String>> = redis::cmd("SET")
            .arg(self.namespaced_key(key))
            .arg(owner)
            .arg("NX")
            .arg("EX")
            .arg(ttl_seconds)
            .query(&mut conn)
            .map_err(|error| CoreError::Io(error.to_string()));
        self.return_connection(conn);
        Ok(result?.as_deref() == Some("OK"))
    }

    fn release_lease(&self, key: &str, owner: &str) -> CoreResult<()> {
        let mut conn = self.connection()?;
        let namespaced = self.namespaced_key(key);
        let script = redis::Script::new(
            r#"if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            else
                return 0
            end"#,
        );
        let result: CoreResult<i32> = script
            .key(&namespaced)
            .arg(owner)
            .invoke(&mut conn)
            .map_err(|error| CoreError::Io(error.to_string()));
        self.return_connection(conn);
        result?;
        Ok(())
    }
}
