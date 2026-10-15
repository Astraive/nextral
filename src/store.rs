pub use crate::testkit::TestMemoryStore;
use crate::{
    config::IngestionPolicy,
    contracts::CoreResult,
    domain::{AuditEvent, GraphEdge, GraphNode, SessionMessage},
    memory::{MemoryRecord, PrivacyLevel},
    prospective::ReminderRecord,
};

pub fn validate_scope_component(value: &str, name: &str) -> CoreResult<()> {
    if value.trim().is_empty()
        || value
            .chars()
            .any(|c| c == '/' || c == '\\' || c.is_control())
    {
        return Err(crate::contracts::CoreError::InvalidInput(format!(
            "{name} must be nonempty without path separators or control characters"
        )));
    }
    Ok(())
}

pub trait MemoryIndexStore: Sync {
    fn prepare_retrieval(&self, request: &mut crate::retrieval::RetrievalRequest)
        -> CoreResult<()>;
    fn prepare_embedding(
        &self,
        record: &mut MemoryRecord,
        embedding: Option<&[f32]>,
    ) -> CoreResult<()>;
    fn ingestion_policy(&self, requested: &IngestionPolicy) -> IngestionPolicy;
    fn vector_candidates(
        &self,
        tenant_id: &str,
        user_id: &str,
        query: &str,
        query_vector: Option<&[f32]>,
        top_k: usize,
        privacy_scope: &[PrivacyLevel],
    ) -> CoreResult<Vec<(String, f32)>>;
    fn commit_ingestion(
        &mut self,
        record: MemoryRecord,
        nodes: Vec<GraphNode>,
        edges: Vec<GraphEdge>,
        audit: AuditEvent,
    ) -> CoreResult<Vec<String>>;
    /// Commit graph replay, audit and recovery work without rewriting canonical memory.
    fn commit_graph_replay(
        &mut self,
        record: &MemoryRecord,
        nodes: &[GraphNode],
        edges: &[GraphEdge],
        audit: &AuditEvent,
    ) -> CoreResult<Vec<String>>;
    fn upsert_memory(&mut self, record: MemoryRecord) -> CoreResult<()>;
    fn get_memory(
        &self,
        tenant_id: &str,
        user_id: &str,
        id: &str,
    ) -> CoreResult<Option<MemoryRecord>>;
    fn get_active_memories(
        &self,
        tenant_id: &str,
        user_id: &str,
        ids: &[&str],
    ) -> CoreResult<Vec<MemoryRecord>>;
    fn list_memories(
        &self,
        tenant_id: &str,
        user_id: &str,
        privacy_scope: &[PrivacyLevel],
        include_inactive: bool,
    ) -> CoreResult<Vec<MemoryRecord>>;
    fn update_memory(&mut self, record: MemoryRecord) -> CoreResult<()>;
    /// Update access metadata without rewriting source memory.
    fn record_accesses(&mut self, tenant_id: &str, user_id: &str, ids: &[&str]) -> CoreResult<()>;
    fn count_memories(
        &self,
        tenant_id: &str,
        user_id: &str,
        include_inactive: bool,
    ) -> CoreResult<usize>;
    fn list_entities(&self, tenant_id: &str, user_id: &str) -> CoreResult<Vec<String>>;
    fn list_tags(&self, tenant_id: &str, user_id: &str) -> CoreResult<Vec<String>>;
}

pub trait GraphStore: Sync {
    fn merge_node(&mut self, node: GraphNode) -> CoreResult<()>;
    fn merge_edge(&mut self, edge: GraphEdge) -> CoreResult<()>;
    fn graph_memory_ids(
        &self,
        tenant_id: &str,
        user_id: &str,
        query: &str,
        max_hops: u8,
        privacy_scope: &[PrivacyLevel],
    ) -> CoreResult<Vec<String>>;
}

pub trait AuditSink {
    fn emit_audit(&mut self, event: AuditEvent) -> CoreResult<()>;
}

pub trait ReminderStore {
    fn upsert_reminder(&mut self, reminder: ReminderRecord) -> CoreResult<()>;
    fn list_due_reminders(
        &self,
        tenant_id: &str,
        user_id: &str,
        due_at_or_before: &str,
    ) -> CoreResult<Vec<ReminderRecord>>;
}

pub trait SessionStore {
    fn hot_tail_status(
        &self,
        tenant_id: &str,
        user_id: &str,
        session_id: &str,
    ) -> CoreResult<(usize, Vec<String>)>;
    fn append_session_message(&mut self, message: SessionMessage) -> CoreResult<()>;
    fn session_tail(
        &self,
        tenant_id: &str,
        user_id: &str,
        session_id: &str,
        limit: usize,
    ) -> CoreResult<Vec<SessionMessage>>;
}
