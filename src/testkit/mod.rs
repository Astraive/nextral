pub use crate::store::{AuditSink, GraphStore, MemoryIndexStore, ReminderStore, SessionStore};
use crate::{
    contracts::{CoreError, CoreResult},
    domain::{AuditEvent, GraphEdge, GraphNode, SessionMessage},
    memory::{deterministic_id, MemoryRecord, MemoryStatus, PrivacyLevel},
    prospective::ReminderRecord,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TestMemoryStore {
    pub memories: Vec<MemoryRecord>,
    pub session_messages: Vec<SessionMessage>,
    pub graph_nodes: Vec<GraphNode>,
    pub graph_edges: Vec<GraphEdge>,
    pub reminders: Vec<ReminderRecord>,
    pub audit_events: Vec<AuditEvent>,
    #[serde(skip)]
    pub lifecycle_state: std::collections::HashMap<(String, String), serde_json::Value>,
    #[serde(skip)]
    pub lifecycle_leases: std::collections::HashMap<(String, String), (String, u64)>,
    #[serde(skip)]
    pub lifecycle_vectors: std::collections::HashMap<(String, String, String, String), Vec<f32>>,
    #[serde(skip)]
    pub lifecycle_fences: std::collections::HashMap<(String, String), (String, u64, u64)>,
    #[serde(skip)]
    pub lifecycle_jobs: std::collections::HashMap<
        (String, String, String),
        (
            crate::runtime::prospective::LifecycleScheduledJob,
            Option<String>,
            Option<u64>,
            u64,
        ),
    >,
}

impl TestMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
    fn audit_replayed(&self, event: &AuditEvent) -> CoreResult<bool> {
        if let Some(existing) = self
            .audit_events
            .iter()
            .find(|existing| existing.tenant_id == event.tenant_id && existing.id == event.id)
        {
            if !existing.same_payload(event)? {
                return Err(CoreError::Conflict(
                    "audit identity reused for a different event".into(),
                ));
            }
            return Ok(true);
        }
        serde_json::from_str::<serde_json::Value>(&event.metadata_json)?;
        Ok(false)
    }
    fn protect_record(&self, mut record: MemoryRecord) -> MemoryRecord {
        for source in &record.source_message_ids {
            if let Some(tombstone) = self.lifecycle_state.get(&(
                record.tenant_id.clone(),
                format!("forgotten-source/{}/{}", record.user_id, source),
            )) {
                if record.status == MemoryStatus::Active {
                    record.status = MemoryStatus::SoftDeleted;
                }
                if tombstone["redact"].as_bool() == Some(true) || tombstone["status"].as_str() == Some("redacted") {
                    record.content = "[redacted]".into();
                    record.entities.clear();
                    record.tags.clear();
                    record.embedding = None;
                }
            }
        }
        record
    }
    fn remove_graph_source(&mut self, record: &MemoryRecord) {
        for edge in self
            .graph_edges
            .iter_mut()
            .filter(|edge| edge.tenant_id == record.tenant_id && edge.user_id == record.user_id)
        {
            edge.source_memory_ids.retain(|id| id != &record.id);
        }
        self.graph_edges
            .retain(|edge| !edge.source_memory_ids.is_empty());
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
    fn prepare_retrieval(
        &self,
        _request: &mut crate::retrieval::RetrievalRequest,
    ) -> CoreResult<()> {
        Ok(())
    }
    fn prepare_embedding(
        &self,
        record: &mut MemoryRecord,
        embedding: Option<&[f32]>,
    ) -> CoreResult<()> {
        if let Some(vector) = embedding {
            if vector.is_empty() || vector.iter().any(|v| !v.is_finite()) {
                return Err(CoreError::InvalidInput(
                    "embedding must be nonempty and finite".into(),
                ));
            }
            record.embedding = Some(vector.to_vec());
            record.embedding_dim = Some(vector.len() as u32);
        }
        Ok(())
    }
    fn ingestion_policy(
        &self,
        requested: &crate::config::IngestionPolicy,
    ) -> crate::config::IngestionPolicy {
        requested.clone()
    }
    fn vector_candidates(
        &self,
        tenant_id: &str,
        user_id: &str,
        query: &str,
        _query_vector: Option<&[f32]>,
        top_k: usize,
        privacy_scope: &[PrivacyLevel],
        filters: Option<&crate::memory::MemoryFilters>,
    ) -> CoreResult<Vec<(String, f32)>> {
        let hits = self.vector_search(tenant_id, user_id, query, top_k * 2, privacy_scope);
        if let Some(filters) = filters {
            let mut filtered = Vec::new();
            for (id, score) in hits {
                if let Some(record) = self.memories.iter().find(|m| m.tenant_id == tenant_id && m.user_id == user_id && m.id == id) {
                    let protected = self.protect_record(record.clone());
                    if filters.matches(&protected) {
                        filtered.push((id, score));
                    }
                }
            }
            filtered.truncate(top_k);
            Ok(filtered)
        } else {
            Ok(hits.into_iter().take(top_k).collect())
        }
    }
    fn commit_ingestion(
        &mut self,
        record: MemoryRecord,
        nodes: Vec<GraphNode>,
        edges: Vec<GraphEdge>,
        audit: AuditEvent,
    ) -> CoreResult<Vec<String>> {
        let replayed = self.audit_replayed(&audit)?;
        crate::graph::validate_source_graph(
            &record.tenant_id,
            &record.user_id,
            &record.id,
            &nodes,
            &edges,
        )?;
        self.upsert_memory(record)?;
        for node in nodes {
            self.merge_node(node)?;
        }
        for edge in edges {
            self.merge_edge(edge)?;
        }
        if !replayed {
            self.audit_events.push(audit);
        }
        Ok(vec![
            "test_memory".into(),
            "test_graph".into(),
            "test_audit".into(),
        ])
    }
    fn commit_graph_replay(
        &mut self,
        record: &MemoryRecord,
        nodes: &[GraphNode],
        edges: &[GraphEdge],
        audit: &AuditEvent,
    ) -> CoreResult<Vec<String>> {
        let replayed = self.audit_replayed(audit)?;
        crate::graph::validate_source_graph(
            &record.tenant_id,
            &record.user_id,
            &record.id,
            nodes,
            edges,
        )?;
        let current = self
            .get_memory(&record.tenant_id, &record.user_id, &record.id)?
            .ok_or_else(|| CoreError::NotFound("canonical graph source missing".into()))?;
        if current.status != MemoryStatus::Active
            || crate::runtime::graphify::source_fingerprint(&current)?
                != crate::runtime::graphify::source_fingerprint(record)?
        {
            return Err(CoreError::Conflict("canonical graph source changed".into()));
        }
        let now = crate::runtime::lifecycle::timestamp_epoch(&crate::memory::now_timestamp())?;
        if self
            .lifecycle_leases
            .get(&(
                record.tenant_id.clone(),
                format!("privacy/{}", record.user_id),
            ))
            .is_some_and(|(_, until)| *until > now)
        {
            return Err(CoreError::Conflict("source lifecycle is leased".into()));
        }
        self.remove_graph_source(record);
        for node in nodes {
            self.merge_node(node.clone())?;
        }
        for edge in edges {
            self.merge_edge(edge.clone())?;
        }
        if !replayed {
            self.audit_events.push(audit.clone());
        }
        Ok(vec![
            "test_graph:source:replaced".into(),
            "test_audit".into(),
        ])
    }
    fn upsert_memory(&mut self, record: MemoryRecord) -> CoreResult<()> {
        record.validate()?;
        if record.status == MemoryStatus::Active
            && (self.memories.iter().any(|r| {
                r.tenant_id == record.tenant_id
                    && r.user_id == record.user_id
                    && r.id == record.id
                    && r.status != MemoryStatus::Active
            }) || record.source_message_ids.iter().any(|source| {
                self.lifecycle_state.contains_key(&(
                    record.tenant_id.clone(),
                    format!("forgotten-source/{}/{}", record.user_id, source),
                ))
            }))
        {
            return Err(CoreError::Conflict(
                "forgotten memory/source cannot be resurrected".into(),
            ));
        }
        if self.memories.iter().any(|memory| {
            memory.id == record.id
                && memory.tenant_id == record.tenant_id
                && memory.user_id != record.user_id
        }) {
            return Err(CoreError::Conflict(
                "memory id is already in use by a different tenant/user scope".to_string(),
            ));
        }
        self.remove_graph_source(&record);
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
            .map(|record| self.protect_record(record.clone())))
    }

    fn get_active_memories(
        &self,
        tenant_id: &str,
        user_id: &str,
        ids: &[&str],
    ) -> CoreResult<Vec<MemoryRecord>> {
        Ok(self
            .memories
            .iter()
            .filter(|memory| {
                memory.tenant_id == tenant_id
                    && memory.user_id == user_id
                    && ids.contains(&memory.id.as_str())
            })
            .map(|record| self.protect_record(record.clone()))
            .filter(|record| record.status == MemoryStatus::Active)
            .collect())
    }
    fn query_memories(
        &self,
        request: &crate::memory::MemoryQuery,
    ) -> CoreResult<crate::memory::MemoryPage> {
        let mut req = request.clone();
        req.validate()?;

        let effective_privacy = if req.privacy_scope.is_empty() {
            vec![
                crate::memory::PrivacyLevel::Private,
                crate::memory::PrivacyLevel::Shared,
                crate::memory::PrivacyLevel::Sensitive,
            ]
        } else {
            req.privacy_scope.clone()
        };

        let fingerprint = crate::memory::compute_query_fingerprint(
            &req.tenant_id,
            &req.user_id,
            &effective_privacy,
            &req.filters,
        );

        let after = if let Some(ref c) = req.cursor {
            Some(crate::memory::decode_cursor(c, &fingerprint)?)
        } else {
            None
        };

        let mut candidates = Vec::new();
        for record in self
            .memories
            .iter()
            .filter(|r| r.tenant_id == req.tenant_id && r.user_id == req.user_id)
        {
            let protected = self.protect_record(record.clone());
            if !effective_privacy.contains(&protected.privacy_level) {
                continue;
            }
            if !req.filters.matches(&protected) {
                continue;
            }
            candidates.push(protected);
        }

        candidates.sort_by(|a, b| {
            let a_time = crate::memory::normalize_rfc3339_micros(&a.created_at).unwrap_or_else(|_| a.created_at.clone());
            let b_time = crate::memory::normalize_rfc3339_micros(&b.created_at).unwrap_or_else(|_| b.created_at.clone());
            b_time.cmp(&a_time).then(b.id.cmp(&a.id))
        });

        if let Some((after_created_at, after_id)) = after {
            candidates.retain(|r| {
                let r_time = crate::memory::normalize_rfc3339_micros(&r.created_at).unwrap_or_else(|_| r.created_at.clone());
                r_time < after_created_at || (r_time == after_created_at && r.id < after_id)
            });
        }

        let has_next = candidates.len() > req.limit;
        let paged_records: Vec<_> = candidates.into_iter().take(req.limit).collect();
        let next_cursor = if has_next {
            if let Some(last) = paged_records.last() {
                let last_time = crate::memory::normalize_rfc3339_micros(&last.created_at).unwrap_or_else(|_| last.created_at.clone());
                Some(crate::memory::encode_cursor(&last_time, &last.id, &fingerprint))
            } else {
                None
            }
        } else {
            None
        };

        let items = paged_records
            .iter()
            .map(|r| crate::memory::MemorySummary::from_record(r))
            .collect();

        Ok(crate::memory::MemoryPage { items, next_cursor })
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
            .map(|record| self.protect_record(record.clone()))
            .filter(|memory| include_inactive || memory.status == MemoryStatus::Active)
            .collect())
    }

    fn update_memory(&mut self, record: MemoryRecord) -> CoreResult<()> {
        if self
            .get_memory(&record.tenant_id, &record.user_id, &record.id)?
            .is_none()
        {
            return Err(CoreError::NotFound("memory record not found".to_string()));
        }
        self.upsert_memory(record)
    }

    fn record_accesses(&mut self, tenant_id: &str, user_id: &str, ids: &[&str]) -> CoreResult<()> {
        for record in self.memories.iter_mut().filter(|record| {
            record.tenant_id == tenant_id
                && record.user_id == user_id
                && record.status == MemoryStatus::Active
                && ids.contains(&record.id.as_str())
        }) {
            record.mark_accessed();
        }
        Ok(())
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
        crate::graph::validate_node(&node)?;
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
        crate::graph::validate_edge(&edge)?;
        if let Some(existing) = self.graph_edges.iter_mut().find(|existing| {
            existing.tenant_id == edge.tenant_id
                && existing.user_id == edge.user_id
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
        if max_hops > 10 {
            return Err(CoreError::InvalidInput(
                "graph traversal exceeds 10 hops".into(),
            ));
        }
        let normalized = query.trim().to_lowercase();
        if normalized.is_empty() {
            return Ok(Vec::new());
        }
        let allowed_memory_ids: std::collections::HashSet<String> = self
            .memories
            .iter()
            .filter(|m| m.tenant_id == tenant_id && m.user_id == user_id)
            .filter(|m| privacy_scope.contains(&m.privacy_level))
            .filter(|m| self.protect_record((*m).clone()).status == MemoryStatus::Active)
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
        if !self.audit_replayed(&event)? {
            self.audit_events.push(event);
        }
        Ok(())
    }
}

impl SessionStore for TestMemoryStore {
    fn eligible_sessions(
        &self,
        tenant_id: &str,
        user_id: &str,
        inactive_at_or_before: &str,
        limit: usize,
    ) -> CoreResult<Vec<crate::domain::EligibleSession>> {
        crate::runtime::lifecycle::validate_identity(tenant_id, user_id, None)?;
        if !(1..=1000).contains(&limit) {
            return Err(CoreError::InvalidInput(
                "eligible session limit must be 1..=1000".into(),
            ));
        }
        let cutoff = crate::runtime::lifecycle::timestamp_nanos(inactive_at_or_before)?;
        let mut sessions = std::collections::HashMap::<&str, (u128, u64)>::new();
        for message in self
            .session_messages
            .iter()
            .filter(|message| message.tenant_id == tenant_id && message.user_id == user_id)
        {
            let time = crate::runtime::lifecycle::timestamp_nanos(&message.created_at)?;
            let entry = sessions.entry(&message.session_id).or_insert((time, 0));
            entry.0 = entry.0.max(time);
            entry.1 += 1;
        }
        let mut sessions = sessions
            .into_iter()
            .filter(|(_, (time, _))| *time <= cutoff)
            .collect::<Vec<_>>();
        sessions.sort_unstable_by(|(a_id, (a_time, _)), (b_id, (b_time, _))| {
            a_time.cmp(b_time).then(a_id.cmp(b_id))
        });
        sessions
            .into_iter()
            .take(limit)
            .map(|(session_id, (time, message_count))| {
                let seconds = i64::try_from(time / 1_000_000_000).map_err(|_| {
                    CoreError::InvalidInput("timestamp exceeds RFC3339 range".into())
                })?;
                let timestamp =
                    chrono::DateTime::from_timestamp(seconds, (time % 1_000_000_000) as u32)
                        .ok_or_else(|| {
                            CoreError::InvalidInput("timestamp exceeds RFC3339 range".into())
                        })?;
                Ok(crate::domain::EligibleSession {
                    session_id: session_id.into(),
                    last_message_at: timestamp.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
                    message_count,
                })
            })
            .collect()
    }
    fn hot_tail_status(&self, t: &str, u: &str, s: &str) -> CoreResult<(usize, Vec<String>)> {
        Ok((self.session_tail(t, u, s, 20)?.len(), Vec::new()))
    }
    fn session_tail_with_status(
        &self,
        tenant: &str,
        user: &str,
        session: &str,
        limit: usize,
    ) -> CoreResult<(Vec<SessionMessage>, Vec<String>)> {
        Ok((self.session_tail(tenant, user, session, limit)?, Vec::new()))
    }
    fn append_session_message(&mut self, message: SessionMessage) -> CoreResult<()> {
        if let Some(existing) = self.session_messages.iter().find(|existing| {
            existing.tenant_id == message.tenant_id
                && existing.user_id == message.user_id
                && existing.session_id == message.session_id
                && existing.idempotency_key == message.idempotency_key
        }) {
            if existing.content != message.content || existing.role != message.role {
                return Err(CoreError::Conflict(
                    "session idempotency key reused for different message".into(),
                ));
            }
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
        let cutoff = crate::runtime::lifecycle::timestamp_nanos(due_at_or_before)?;
        let mut due = Vec::new();
        for reminder in self
            .reminders
            .iter()
            .filter(|r| r.tenant_id == tenant_id && r.user_id == user_id && r.is_due_visible())
        {
            let effective = reminder
                .next_attempt_at
                .as_deref()
                .unwrap_or(&reminder.due_at);
            if crate::runtime::lifecycle::timestamp_nanos(effective)? <= cutoff {
                due.push(reminder.clone());
            }
        }
        Ok(due)
    }
    fn commit_reminder_delivery(
        &mut self,
        claim: &crate::runtime::lifecycle::LeaseClaim,
        state_key: &str,
        state: &serde_json::Value,
        reminder: &ReminderRecord,
        audits: &[AuditEvent],
    ) -> CoreResult<()> {
        let now = crate::runtime::lifecycle::timestamp_epoch(&crate::memory::now_timestamp())?;
        let lease_key = (claim.tenant_id.clone(), claim.key.clone());
        let Some((owner, expiry, fence)) = self.lifecycle_fences.get(&lease_key).cloned() else {
            return Err(CoreError::LeaseLost("user lease missing".into()));
        };
        if owner != claim.owner || fence != claim.fence || expiry <= now {
            return Err(CoreError::LeaseLost("user lease expired or lost".into()));
        }
        if claim.tenant_id != reminder.tenant_id || claim.user_id.as_deref() != Some(&reminder.user_id) {
            return Err(CoreError::InvalidInput("scope mismatch for reminder delivery".into()));
        }

        self.lifecycle_state
            .insert((reminder.tenant_id.clone(), state_key.to_string()), state.clone());
        if let Some(pos) = self
            .reminders
            .iter()
            .position(|r| r.tenant_id == reminder.tenant_id && r.id == reminder.id)
        {
            self.reminders[pos] = reminder.clone();
        } else {
            self.reminders.push(reminder.clone());
        }
        for audit in audits {
            self.audit_events.push(audit.clone());
        }
        Ok(())
    }
}
