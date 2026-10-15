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
    fn count_memories(
        &self,
        tenant_id: &str,
        user_id: &str,
        include_inactive: bool,
    ) -> CoreResult<usize>;
    fn list_entities(&self, tenant_id: &str, user_id: &str) -> CoreResult<Vec<String>>;
    fn list_tags(&self, tenant_id: &str, user_id: &str) -> CoreResult<Vec<String>>;
}

pub trait GraphStore {
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

    pub fn vector_search(
        &self,
        tenant_id: &str,
        user_id: &str,
        query: &str,
        top_k: usize,
        privacy_scope: &[PrivacyLevel],
    ) -> Vec<(String, f32)> {
        use crate::scoring::lexical_score;
        let mut scored: Vec<(String, f32)> = self
            .memories
            .iter()
            .filter(|m| m.tenant_id == tenant_id && m.user_id == user_id)
            .filter(|m| m.status == crate::memory::MemoryStatus::Active)
            .filter(|m| privacy_scope.contains(&m.privacy_level))
            .map(|m| (m.id.clone(), lexical_score(&m.content, query)))
            .filter(|(_, score)| *score > 0.0)
            .collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1));
        scored.truncate(top_k);
        scored
    }
}

impl MemoryIndexStore for TestMemoryStore {
    fn upsert_memory(&mut self, record: MemoryRecord) -> CoreResult<()> {
        record.validate()?;
        if self.memories.iter().any(|memory| {
            memory.id == record.id
                && (memory.tenant_id != record.tenant_id || memory.user_id != record.user_id)
        }) {
            return Err(CoreError::Conflict(
                "memory id is already in use by a different tenant/user scope".to_string(),
            ));
        }
        if let Some(existing) = self.memories.iter_mut().find(|memory| {
            memory.id == record.id
                && memory.tenant_id == record.tenant_id
                && memory.user_id == record.user_id
        }) {
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

    fn count_memories(
        &self,
        tenant_id: &str,
        user_id: &str,
        include_inactive: bool,
    ) -> CoreResult<usize> {
        Ok(self
            .memories
            .iter()
            .filter(|m| m.tenant_id == tenant_id && m.user_id == user_id)
            .filter(|m| include_inactive || m.status == MemoryStatus::Active)
            .count())
    }

    fn list_entities(&self, tenant_id: &str, user_id: &str) -> CoreResult<Vec<String>> {
        let mut entities: std::collections::HashSet<String> = std::collections::HashSet::new();
        for memory in self.memories.iter().filter(|m| {
            m.tenant_id == tenant_id && m.user_id == user_id && m.status == MemoryStatus::Active
        }) {
            for entity in &memory.entities {
                entities.insert(entity.clone());
            }
        }
        let mut result: Vec<String> = entities.into_iter().collect();
        result.sort();
        Ok(result)
    }

    fn list_tags(&self, tenant_id: &str, user_id: &str) -> CoreResult<Vec<String>> {
        let mut tags: std::collections::HashSet<String> = std::collections::HashSet::new();
        for memory in self.memories.iter().filter(|m| {
            m.tenant_id == tenant_id && m.user_id == user_id && m.status == MemoryStatus::Active
        }) {
            for tag in &memory.tags {
                tags.insert(tag.clone());
            }
        }
        let mut result: Vec<String> = tags.into_iter().collect();
        result.sort();
        Ok(result)
    }
}

impl GraphStore for TestMemoryStore {
    fn merge_node(&mut self, node: GraphNode) -> CoreResult<()> {
        if !self.graph_nodes.iter().any(|existing| {
            existing.tenant_id == node.tenant_id
                && existing.user_id == node.user_id
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
            for id in &edge.source_memory_ids {
                if !existing.source_memory_ids.contains(id) {
                    existing.source_memory_ids.push(id.clone());
                }
            }
            return Ok(());
        }
        self.graph_edges.push(edge);
        Ok(())
    }

    fn graph_memory_ids(
        &self,
        tenant_id: &str,
        user_id: &str,
        query: &str,
        max_hops: u8,
        privacy_scope: &[PrivacyLevel],
    ) -> CoreResult<Vec<String>> {
        let normalized = query.trim().to_lowercase();
        if normalized.is_empty() {
            return Ok(Vec::new());
        }
        let allowed_memory_ids: std::collections::HashSet<String> = self
            .memories
            .iter()
            .filter(|m| m.tenant_id == tenant_id && m.user_id == user_id)
            .filter(|m| privacy_scope.contains(&m.privacy_level))
            .map(|m| m.id.clone())
            .collect();

        let seed_keys: std::collections::HashSet<String> = self
            .graph_nodes
            .iter()
            .filter(|node| {
                node.tenant_id == tenant_id
                    && node.user_id == user_id
                    && (node.name.to_lowercase().contains(&normalized)
                        || normalized.contains(&node.name.to_lowercase())
                        || node.key.to_lowercase().contains(&normalized)
                        || normalized.contains(&node.key.to_lowercase()))
            })
            .map(|node| node.key.clone())
            .collect();

        let mut visited_keys = seed_keys.clone();
        let mut current_keys = seed_keys;
        let mut ids = Vec::new();

        for _ in 0..max_hops {
            let mut next_keys = std::collections::HashSet::new();
            for edge in self
                .graph_edges
                .iter()
                .filter(|edge| edge.tenant_id == tenant_id && edge.user_id == user_id)
            {
                let from_match = current_keys.contains(&edge.from_key);
                let to_match = current_keys.contains(&edge.to_key);
                if from_match || to_match {
                    ids.extend(
                        edge.source_memory_ids
                            .iter()
                            .filter(|id| allowed_memory_ids.contains(*id))
                            .cloned(),
                    );
                    if from_match && !visited_keys.contains(&edge.to_key) {
                        next_keys.insert(edge.to_key.clone());
                    }
                    if to_match && !visited_keys.contains(&edge.from_key) {
                        next_keys.insert(edge.from_key.clone());
                    }
                }
            }
            if next_keys.is_empty() {
                break;
            }
            visited_keys.extend(next_keys.iter().cloned());
            current_keys = next_keys;
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
        tenant_id: &str,
        user_id: &str,
        due_at_or_before: &str,
    ) -> CoreResult<Vec<ReminderRecord>> {
        Ok(self
            .reminders
            .iter()
            .filter(|reminder| reminder.tenant_id == tenant_id)
            .filter(|reminder| reminder.user_id == user_id)
            .filter(|reminder| {
                let effective = reminder
                    .next_attempt_at
                    .as_deref()
                    .unwrap_or(&reminder.due_at);
                let effective_ts = effective.parse::<u64>().unwrap_or(0);
                let cutoff = due_at_or_before.parse::<u64>().unwrap_or(u64::MAX);
                effective_ts <= cutoff
            })
            .filter(|reminder| reminder.is_due_visible())
            .cloned()
            .collect())
    }
}
