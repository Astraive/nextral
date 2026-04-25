use crate::{
    contracts::{CoreError, CoreResult},
    domain::{AuditEvent, GraphEdge, GraphNode, SessionMessage},
    memory::{deterministic_id, MemoryRecord, MemoryStatus, PrivacyLevel},
    prospective::ReminderRecord,
};
use serde::{Deserialize, Serialize};

pub trait MemoryIndexStore {
    fn upsert_memory(&mut self, record: MemoryRecord) -> CoreResult<()>;
    fn get_memory(
        &self,
        tenant_id: &str,
        user_id: &str,
        id: &str,
    ) -> CoreResult<Option<MemoryRecord>>;
    fn list_memories(
        &self,
        tenant_id: &str,
        user_id: &str,
        privacy_scope: &[PrivacyLevel],
        include_inactive: bool,
    ) -> CoreResult<Vec<MemoryRecord>>;
    fn update_memory(&mut self, record: MemoryRecord) -> CoreResult<()>;
}

pub trait GraphStore {
    fn merge_node(&mut self, node: GraphNode) -> CoreResult<()>;
    fn merge_edge(&mut self, edge: GraphEdge) -> CoreResult<()>;
    fn graph_memory_ids(&self, user_id: &str, query: &str, max_hops: u8)
        -> CoreResult<Vec<String>>;
}

pub trait AuditSink {
    fn emit_audit(&mut self, event: AuditEvent) -> CoreResult<()>;
}

pub trait ReminderStore {
    fn upsert_reminder(&mut self, reminder: ReminderRecord) -> CoreResult<()>;
    fn list_due_reminders(
        &self,
        user_id: &str,
        due_at_or_before: &str,
    ) -> CoreResult<Vec<ReminderRecord>>;
}

pub trait SessionStore {
    fn append_session_message(&mut self, message: SessionMessage) -> CoreResult<()>;
    fn session_tail(
        &self,
        tenant_id: &str,
        user_id: &str,
        session_id: &str,
        limit: usize,
    ) -> CoreResult<Vec<SessionMessage>>;
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TestMemoryStore {
    pub memories: Vec<MemoryRecord>,
    pub session_messages: Vec<SessionMessage>,
    pub graph_nodes: Vec<GraphNode>,
    pub graph_edges: Vec<GraphEdge>,
    pub reminders: Vec<ReminderRecord>,
    pub audit_events: Vec<AuditEvent>,
}

impl TestMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl MemoryIndexStore for TestMemoryStore {
    fn upsert_memory(&mut self, record: MemoryRecord) -> CoreResult<()> {
        record.validate()?;
        if let Some(existing) = self
            .memories
            .iter_mut()
            .find(|memory| memory.id == record.id)
        {
            *existing = record;
            return Ok(());
        }
        self.memories.push(record);
        Ok(())
    }

    fn get_memory(
        &self,
        tenant_id: &str,
        user_id: &str,
        id: &str,
    ) -> CoreResult<Option<MemoryRecord>> {
        Ok(self
            .memories
            .iter()
            .find(|memory| {
                memory.tenant_id == tenant_id && memory.user_id == user_id && memory.id == id
            })
            .cloned())
    }

    fn list_memories(
        &self,
        tenant_id: &str,
        user_id: &str,
        privacy_scope: &[PrivacyLevel],
        include_inactive: bool,
    ) -> CoreResult<Vec<MemoryRecord>> {
        Ok(self
            .memories
            .iter()
            .filter(|memory| memory.tenant_id == tenant_id)
            .filter(|memory| memory.user_id == user_id)
            .filter(|memory| privacy_scope.contains(&memory.privacy_level))
            .filter(|memory| include_inactive || memory.status == MemoryStatus::Active)
            .cloned()
            .collect())
    }

    fn update_memory(&mut self, record: MemoryRecord) -> CoreResult<()> {
        if let Some(existing) = self.memories.iter_mut().find(|memory| {
            memory.tenant_id == record.tenant_id
                && memory.user_id == record.user_id
                && memory.id == record.id
        }) {
            *existing = record;
            return Ok(());
        }
        Err(CoreError::NotFound("memory record not found".to_string()))
    }
}

impl GraphStore for TestMemoryStore {
    fn merge_node(&mut self, node: GraphNode) -> CoreResult<()> {
        if !self.graph_nodes.iter().any(|existing| {
            existing.user_id == node.user_id
                && existing.label == node.label
                && existing.canonical_name == node.canonical_name
        }) {
            self.graph_nodes.push(node);
        }
        Ok(())
    }

    fn merge_edge(&mut self, edge: GraphEdge) -> CoreResult<()> {
        if let Some(existing) = self.graph_edges.iter_mut().find(|existing| {
            existing.user_id == edge.user_id
                && existing.from_key == edge.from_key
                && existing.relationship_type == edge.relationship_type
                && existing.to_key == edge.to_key
        }) {
            existing.last_confirmed_at = edge.last_confirmed_at;
            existing.confidence = existing.confidence.max(edge.confidence);
            if !existing
                .source_memory_ids
                .contains(&edge.source_memory_ids[0])
            {
                existing
                    .source_memory_ids
                    .extend(edge.source_memory_ids.iter().cloned());
            }
            return Ok(());
        }
        self.graph_edges.push(edge);
        Ok(())
    }

    fn graph_memory_ids(
        &self,
        user_id: &str,
        query: &str,
        _max_hops: u8,
    ) -> CoreResult<Vec<String>> {
        let normalized = query.trim().to_lowercase();
        if normalized.is_empty() {
            return Ok(Vec::new());
        }
        let node_keys: Vec<String> = self
            .graph_nodes
            .iter()
            .filter(|node| {
                node.user_id == user_id
                    && (node.name.to_lowercase().contains(&normalized)
                        || normalized.contains(&node.name.to_lowercase()))
            })
            .map(|node| node.key.clone())
            .collect();
        let mut ids = Vec::new();
        for edge in self
            .graph_edges
            .iter()
            .filter(|edge| edge.user_id == user_id)
        {
            if node_keys.contains(&edge.from_key) || node_keys.contains(&edge.to_key) {
                ids.extend(edge.source_memory_ids.iter().cloned());
            }
        }
        ids.sort();
        ids.dedup();
        Ok(ids)
    }
}

impl AuditSink for TestMemoryStore {
    fn emit_audit(&mut self, event: AuditEvent) -> CoreResult<()> {
        self.audit_events.push(event);
        Ok(())
    }
}

impl SessionStore for TestMemoryStore {
    fn append_session_message(&mut self, message: SessionMessage) -> CoreResult<()> {
        if self
            .session_messages
            .iter()
            .any(|existing| existing.idempotency_key == message.idempotency_key)
        {
            return Ok(());
        }
        self.session_messages.push(message);
        Ok(())
    }

    fn session_tail(
        &self,
        tenant_id: &str,
        user_id: &str,
        session_id: &str,
        limit: usize,
    ) -> CoreResult<Vec<SessionMessage>> {
        let mut messages: Vec<SessionMessage> = self
            .session_messages
            .iter()
            .filter(|message| message.tenant_id == tenant_id)
            .filter(|message| message.user_id == user_id)
            .filter(|message| message.session_id == session_id)
            .cloned()
            .collect();
        messages.sort_by(|left, right| left.created_at.cmp(&right.created_at));
        if messages.len() > limit {
            messages = messages[messages.len() - limit..].to_vec();
        }
        Ok(messages)
    }
}

pub fn test_id(parts: &[&str]) -> String {
    deterministic_id(parts)
}

impl ReminderStore for TestMemoryStore {
    fn upsert_reminder(&mut self, reminder: ReminderRecord) -> CoreResult<()> {
        if self.reminders.iter().any(|existing| {
            existing.id != reminder.id && existing.dedupe_key == reminder.dedupe_key
        }) {
            return Err(CoreError::Conflict(
                "duplicate reminder dedupe key".to_string(),
            ));
        }
        if let Some(existing) = self
            .reminders
            .iter_mut()
            .find(|existing| existing.id == reminder.id)
        {
            *existing = reminder;
            return Ok(());
        }
        self.reminders.push(reminder);
        Ok(())
    }

    fn list_due_reminders(
        &self,
        user_id: &str,
        due_at_or_before: &str,
    ) -> CoreResult<Vec<ReminderRecord>> {
        Ok(self
            .reminders
            .iter()
            .filter(|reminder| reminder.user_id == user_id)
            .filter(|reminder| {
                reminder
                    .next_attempt_at
                    .as_deref()
                    .unwrap_or(&reminder.due_at)
                    <= due_at_or_before
            })
            .filter(|reminder| reminder.is_due_visible())
            .cloned()
            .collect())
    }
}
