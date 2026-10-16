use crate::{
    contracts::{CoreError, CoreResult},
    domain::{AuditAction, AuditEvent},
    graph::{graphify_record, GraphHint, GraphifyOutput},
    memory::{MemoryRecord, MemoryStatus},
    store::MemoryIndexStore,
};
use sha2::{Digest, Sha256};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct GraphReplayOutput {
    #[serde(flatten)]
    pub graph: GraphifyOutput,
    pub status: String,
    pub write_receipts: Vec<String>,
}

/// Replay graph evidence from an active canonical memory without rewriting it.
pub fn graphify_memory(
    store: &mut impl MemoryIndexStore,
    tenant_id: &str,
    user_id: &str,
    memory_id: &str,
    hints: &[GraphHint],
) -> CoreResult<GraphReplayOutput> {
    crate::store::validate_scope_component(tenant_id, "tenant_id")?;
    crate::store::validate_scope_component(user_id, "user_id")?;
    crate::store::validate_scope_component(memory_id, "memory_id")?;
    if hints.len() > 256 {
        return Err(CoreError::InvalidInput(
            "graph replay accepts at most 256 hints".into(),
        ));
    }
    let record = store
        .get_memory(tenant_id, user_id, memory_id)?
        .filter(|record| record.status == MemoryStatus::Active)
        .ok_or_else(|| CoreError::NotFound("active canonical memory not found".into()))?;
    if record.entities.len() > 256 {
        return Err(CoreError::InvalidInput(
            "graph replay accepts at most 256 canonical entities".into(),
        ));
    }
    let mut output = graphify_record(&record, hints)?;
    // Stable evidence times make an unchanged replay deterministic.
    for node in &mut output.nodes {
        node.created_at.clone_from(&record.updated_at);
    }
    for edge in &mut output.relationships {
        edge.created_at.clone_from(&record.updated_at);
        edge.last_confirmed_at.clone_from(&record.updated_at);
    }
    let mut audit = AuditEvent::new(
        crate::runtime::lifecycle::worker_owner(),
        tenant_id,
        user_id,
        AuditAction::GraphifyMerged,
        "memory",
        Some(memory_id.into()),
        "canonical graph replay",
    );
    audit.user_id = Some(user_id.into());
    audit.trace_id = Some(output.trace_id.clone());
    let write_receipts =
        store.commit_graph_replay(&record, &output.nodes, &output.relationships, &audit)?;
    Ok(GraphReplayOutput {
        graph: output,
        status: "accepted".into(),
        write_receipts,
    })
}

pub(crate) fn source_fingerprint(record: &MemoryRecord) -> CoreResult<String> {
    let mut value = serde_json::to_value(record)?;
    // Reads do not change the version of source evidence.
    if let Some(fields) = value.as_object_mut() {
        fields.remove("access_count");
        fields.remove("last_accessed_at");
        fields.remove("embedding");
        fields.remove("embedding_dim");
        fields.remove("embedding_model");
        fields.remove("embedding_provider");
    }
    Ok(format!(
        "{:x}",
        Sha256::digest(value.to_string().as_bytes())
    ))
}

pub(crate) fn graph_state_key(user_id: &str, memory_id: &str) -> String {
    format!("graph/{user_id}/{memory_id}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        memory::{ContentType, MemoryType, PrivacyLevel, SourceType},
        store::{GraphStore, TestMemoryStore},
    };

    fn source(id: &str, user: &str) -> MemoryRecord {
        MemoryRecord::new(
            id,
            "tenant",
            user,
            "Aster uses PostgreSQL.",
            ContentType::Fact,
            MemoryType::Semantic,
            SourceType::Manual,
        )
    }

    fn hint() -> GraphHint {
        GraphHint {
            from_label: "Project".into(),
            from_name: "Aster".into(),
            relationship_type: "USES".into(),
            to_label: "Technology".into(),
            to_name: "PostgreSQL".into(),
            confidence: 0.95,
        }
    }

    #[test]
    fn canonical_change_with_empty_hints_removes_only_that_source() {
        let mut store = TestMemoryStore::new();
        for id in ["source", "peer"] {
            store.upsert_memory(source(id, "owner")).unwrap();
            graphify_memory(&mut store, "tenant", "owner", id, &[hint()]).unwrap();
        }
        let mut replacement = source("source", "owner");
        replacement.content = "Replacement without graph facts.".into();
        store.update_memory(replacement).unwrap();
        assert_eq!(
            store
                .graph_memory_ids("tenant", "owner", "Aster", 2, &[PrivacyLevel::Private])
                .unwrap(),
            vec!["peer"]
        );
    }

    #[test]
    fn replay_is_scoped_idempotent_and_does_not_rewrite_canonical_content() {
        let mut store = TestMemoryStore::new();
        let canonical = source("source", "owner");
        store.upsert_memory(canonical.clone()).unwrap();
        let mut duplicate = hint();
        duplicate.from_name = "  ASTER  ".into();
        let hints = [hint(), duplicate];
        let first = graphify_memory(&mut store, "tenant", "owner", "source", &hints).unwrap();
        let edges = store.graph_edges.clone();
        graphify_memory(&mut store, "tenant", "owner", "source", &hints).unwrap();
        assert_eq!(store.graph_edges, edges);
        assert_eq!(first.graph.relationships.len(), 1);
        assert_eq!(
            store.get_memory("tenant", "owner", "source").unwrap(),
            Some(canonical)
        );
        assert!(graphify_memory(&mut store, "tenant", "other", "source", &hints).is_err());
        assert!(graphify_memory(&mut store, "other", "owner", "source", &hints).is_err());
    }

    #[test]
    fn replay_commit_rejects_a_stale_or_forged_snapshot() {
        let mut store = TestMemoryStore::new();
        let stale = source("source", "owner");
        store.upsert_memory(stale.clone()).unwrap();
        let mut changed = stale.clone();
        changed.content = "Changed canonical fact.".into();
        store.update_memory(changed.clone()).unwrap();
        let output = graphify_record(&stale, &[hint()]).unwrap();
        let audit = AuditEvent::new(
            "audit",
            "tenant",
            "owner",
            AuditAction::GraphifyMerged,
            "memory",
            Some("source".into()),
            "test",
        );
        assert!(store
            .commit_graph_replay(&stale, &output.nodes, &output.relationships, &audit)
            .is_err());
        assert_eq!(
            store.get_memory("tenant", "owner", "source").unwrap(),
            Some(changed)
        );
        assert!(store.graph_edges.is_empty());
    }

    #[test]
    fn replay_after_forget_cannot_resurrect_shared_evidence() {
        let mut store = TestMemoryStore::new();
        for id in ["source", "peer"] {
            store.upsert_memory(source(id, "owner")).unwrap();
            graphify_memory(&mut store, "tenant", "owner", id, &[hint()]).unwrap();
        }
        let before_forget = store
            .get_memory("tenant", "owner", "source")
            .unwrap()
            .unwrap();
        let output = graphify_record(&before_forget, &[hint()]).unwrap();
        crate::runtime::governance::forget_memory(
            &mut store,
            crate::runtime::governance::ForgetMemoryRequest {
                tenant_id: "tenant".into(),
                user_id: "owner".into(),
                memory_id: "source".into(),
                actor: "owner".into(),
                reason: "forget graph source".into(),
                redact: true,
            },
        )
        .unwrap();
        let audit = AuditEvent::new(
            "late",
            "tenant",
            "owner",
            AuditAction::GraphifyMerged,
            "memory",
            Some("source".into()),
            "late replay",
        );
        assert!(store
            .commit_graph_replay(&before_forget, &output.nodes, &output.relationships, &audit)
            .is_err());
        assert!(graphify_memory(&mut store, "tenant", "owner", "source", &[hint()]).is_err());
        assert_eq!(
            store
                .graph_memory_ids("tenant", "owner", "Aster", 2, &[PrivacyLevel::Private])
                .unwrap(),
            vec!["peer"]
        );
    }

    #[test]
    fn forgotten_causal_sources_and_leased_lifecycles_reject_replay() {
        let mut store = TestMemoryStore::new();
        let mut record = source("derived", "owner");
        record.source_message_ids = vec!["message".into()];
        store.upsert_memory(record).unwrap();
        store.lifecycle_state.insert(
            ("tenant".into(), "forgotten-source/owner/message".into()),
            serde_json::json!({"redact":true}),
        );
        assert!(graphify_memory(&mut store, "tenant", "owner", "derived", &[hint()]).is_err());
        store.upsert_memory(source("leased", "owner")).unwrap();
        store.lifecycle_leases.insert(
            ("tenant".into(), "privacy/owner".into()),
            ("forget-worker".into(), u64::MAX),
        );
        assert!(graphify_memory(&mut store, "tenant", "owner", "leased", &[hint()]).is_err());
    }

    #[test]
    fn canonical_privacy_updates_remove_old_hint_evidence() {
        let mut store = TestMemoryStore::new();
        store.upsert_memory(source("source", "owner")).unwrap();
        graphify_memory(&mut store, "tenant", "owner", "source", &[hint()]).unwrap();
        let mut changed = source("source", "owner");
        changed.privacy_level = PrivacyLevel::Sensitive;
        store.update_memory(changed).unwrap();
        assert!(store
            .graph_memory_ids(
                "tenant",
                "owner",
                "Aster",
                2,
                &[PrivacyLevel::Private, PrivacyLevel::Sensitive]
            )
            .unwrap()
            .is_empty());
    }
}

#[cfg(test)]
mod version_tests {
    use super::*;

    #[test]
    fn access_and_vector_routing_do_not_change_graph_source_version() {
        let record = MemoryRecord::new(
            "source",
            "tenant",
            "owner",
            "Canonical fact",
            crate::memory::ContentType::Fact,
            crate::memory::MemoryType::Semantic,
            crate::memory::SourceType::Manual,
        );
        let version = source_fingerprint(&record).unwrap();
        let mut accessed = record.clone();
        accessed.mark_accessed();
        accessed.embedding = Some(vec![0.1, 0.2]);
        accessed.embedding_dim = Some(2);
        accessed.embedding_model = Some("new-model".into());
        accessed.embedding_provider = Some("external_callback".into());
        assert_eq!(source_fingerprint(&accessed).unwrap(), version);
        accessed.source_message_ids.push("new-source".into());
        assert_ne!(source_fingerprint(&accessed).unwrap(), version);
    }
}
