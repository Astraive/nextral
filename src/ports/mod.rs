use crate::{
    contracts::CoreResult,
    graph::{GraphEdge, GraphNode},
    memory::{MemoryRecord, MemoryStatus, PrivacyLevel},
    prospective::ReminderRecord,
    retrieval::{RetrievalRequest, RetrievedItem},
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantUserScope {
    pub tenant_id: String,
    pub user_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VectorPoint {
    pub memory_id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub vector: Vec<f32>,
    pub privacy_level: PrivacyLevel,
    pub status: MemoryStatus,
    pub content_type: String,
    pub memory_type: String,
    pub schema_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VectorSearchRequest {
    pub scope: TenantUserScope,
    pub query_vector: Vec<f32>,
    pub privacy_scope: Vec<PrivacyLevel>,
    pub top_k: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VectorSearchHit {
    pub memory_id: String,
    pub score: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheEntry {
    pub key: String,
    pub value_json: String,
    pub ttl_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArchiveObject {
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: Option<String>,
    pub memory_id: Option<String>,
    pub object_kind: String,
    pub content_sha256: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArchiveReceipt {
    pub bucket: String,
    pub object_key: String,
    pub content_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditWrite {
    pub tenant_id: String,
    pub user_id: Option<String>,
    pub actor_id: String,
    pub action: String,
    pub target_type: String,
    pub target_id: Option<String>,
    pub reason: String,
    pub request_id: Option<String>,
    pub trace_id: Option<String>,
    pub metadata_json: String,
}

pub trait PostgresPort {
    fn upsert_memory(&self, record: &MemoryRecord) -> CoreResult<()>;
    fn get_memory(
        &self,
        scope: &TenantUserScope,
        memory_id: &str,
    ) -> CoreResult<Option<MemoryRecord>>;
    fn append_session_message(
        &self,
        scope: &TenantUserScope,
        session_id: &str,
        role: &str,
        content: &str,
    ) -> CoreResult<String>;
    fn upsert_reminder(&self, reminder: &ReminderRecord) -> CoreResult<()>;
    fn write_audit(&self, event: &AuditWrite) -> CoreResult<()>;
    fn enqueue_outbox(
        &self,
        tenant_id: &str,
        event_type: &str,
        aggregate_id: &str,
        payload_json: &str,
    ) -> CoreResult<String>;
}

pub trait RedisPort {
    fn put_cache(&self, entry: &CacheEntry) -> CoreResult<()>;
    fn get_cache(&self, key: &str) -> CoreResult<Option<String>>;
    fn invalidate_prefix(&self, prefix: &str) -> CoreResult<()>;
    fn acquire_lease(&self, key: &str, owner: &str, ttl_seconds: u64) -> CoreResult<bool>;
    fn release_lease(&self, key: &str, owner: &str) -> CoreResult<()>;
}

pub trait QdrantPort {
    fn ensure_collection(&self, collection: &str, dimension: u32, distance: &str)
        -> CoreResult<()>;
    fn upsert_point(&self, collection: &str, point: &VectorPoint) -> CoreResult<()>;
    fn search(
        &self,
        collection: &str,
        request: &VectorSearchRequest,
    ) -> CoreResult<Vec<VectorSearchHit>>;
    fn delete_point(&self, collection: &str, tenant_id: &str, memory_id: &str) -> CoreResult<()>;
}

pub trait Neo4jPort {
    fn merge_node(&self, node: &GraphNode) -> CoreResult<()>;
    fn merge_edge(&self, edge: &GraphEdge) -> CoreResult<()>;
    fn related_memory_ids(
        &self,
        scope: &TenantUserScope,
        query_entities: &[String],
        max_hops: u8,
    ) -> CoreResult<Vec<String>>;
    fn redact_memory_edges(&self, scope: &TenantUserScope, memory_id: &str) -> CoreResult<()>;
}

pub trait ObjectArchivePort {
    fn put_object(&self, object: &ArchiveObject) -> CoreResult<ArchiveReceipt>;
    fn tombstone_object(&self, tenant_id: &str, object_key: &str, reason: &str) -> CoreResult<()>;
}

pub trait RetrievalCacheKey {
    fn retrieval_cache_key(
        &self,
        request: &RetrievalRequest,
        embedding_model: &str,
        schema_version: &str,
    ) -> CoreResult<String>;
}

pub trait RerankerPort {
    fn rerank(
        &self,
        request: &RetrievalRequest,
        items: Vec<RetrievedItem>,
    ) -> CoreResult<Vec<RetrievedItem>>;
}

#[async_trait]
pub trait AsyncPostgresPort {
    async fn upsert_memory(&self, record: &MemoryRecord) -> CoreResult<()>;
    async fn get_memory(
        &self,
        scope: &TenantUserScope,
        memory_id: &str,
    ) -> CoreResult<Option<MemoryRecord>>;
    async fn append_session_message(
        &self,
        scope: &TenantUserScope,
        session_id: &str,
        role: &str,
        content: &str,
    ) -> CoreResult<String>;
    async fn upsert_reminder(&self, reminder: &ReminderRecord) -> CoreResult<()>;
    async fn write_audit(&self, event: &AuditWrite) -> CoreResult<()>;
    async fn enqueue_outbox(
        &self,
        tenant_id: &str,
        event_type: &str,
        aggregate_id: &str,
        payload_json: &str,
    ) -> CoreResult<String>;
}

#[async_trait]
impl<T> AsyncPostgresPort for T
where
    T: PostgresPort + Sync,
{
    async fn upsert_memory(&self, record: &MemoryRecord) -> CoreResult<()> {
        PostgresPort::upsert_memory(self, record)
    }
    async fn get_memory(
        &self,
        scope: &TenantUserScope,
        memory_id: &str,
    ) -> CoreResult<Option<MemoryRecord>> {
        PostgresPort::get_memory(self, scope, memory_id)
    }
    async fn append_session_message(
        &self,
        scope: &TenantUserScope,
        session_id: &str,
        role: &str,
        content: &str,
    ) -> CoreResult<String> {
        PostgresPort::append_session_message(self, scope, session_id, role, content)
    }
    async fn upsert_reminder(&self, reminder: &ReminderRecord) -> CoreResult<()> {
        PostgresPort::upsert_reminder(self, reminder)
    }
    async fn write_audit(&self, event: &AuditWrite) -> CoreResult<()> {
        PostgresPort::write_audit(self, event)
    }
    async fn enqueue_outbox(
        &self,
        tenant_id: &str,
        event_type: &str,
        aggregate_id: &str,
        payload_json: &str,
    ) -> CoreResult<String> {
        PostgresPort::enqueue_outbox(self, tenant_id, event_type, aggregate_id, payload_json)
    }
}

#[async_trait]
pub trait AsyncRedisPort {
    async fn put_cache(&self, entry: &CacheEntry) -> CoreResult<()>;
    async fn get_cache(&self, key: &str) -> CoreResult<Option<String>>;
    async fn invalidate_prefix(&self, prefix: &str) -> CoreResult<()>;
    async fn acquire_lease(&self, key: &str, owner: &str, ttl_seconds: u64) -> CoreResult<bool>;
    async fn release_lease(&self, key: &str, owner: &str) -> CoreResult<()>;
}

#[async_trait]
impl<T> AsyncRedisPort for T
where
    T: RedisPort + Sync,
{
    async fn put_cache(&self, entry: &CacheEntry) -> CoreResult<()> {
        RedisPort::put_cache(self, entry)
    }
    async fn get_cache(&self, key: &str) -> CoreResult<Option<String>> {
        RedisPort::get_cache(self, key)
    }
    async fn invalidate_prefix(&self, prefix: &str) -> CoreResult<()> {
        RedisPort::invalidate_prefix(self, prefix)
    }
    async fn acquire_lease(&self, key: &str, owner: &str, ttl_seconds: u64) -> CoreResult<bool> {
        RedisPort::acquire_lease(self, key, owner, ttl_seconds)
    }
    async fn release_lease(&self, key: &str, owner: &str) -> CoreResult<()> {
        RedisPort::release_lease(self, key, owner)
    }
}
