use crate::{
    contracts::{CoreError, CoreResult},
    memory::{estimate_tokens, MemoryRecord, PrivacyLevel},
    scoring::{lexical_score, retrieval_score},
    store::{GraphStore, MemoryIndexStore},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetrievalRequest {
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: Option<String>,
    pub query_text: String,
    pub entities: Vec<String>,
    pub intent_topic: Option<String>,
    pub token_budget: u32,
    pub privacy_scope: Vec<PrivacyLevel>,
    pub top_k_vector: usize,
    pub max_graph_hops: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RetrievalStatus {
    Ok,
    Degraded,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourcePath {
    Vector,
    Graph,
    Both,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScoreBreakdown {
    pub semantic_similarity: f32,
    pub recency: f32,
    pub importance: f32,
    pub access: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetrievedItem {
    pub memory_id: String,
    pub content: String,
    pub source_path: SourcePath,
    pub retrieval_score: f32,
    pub score_breakdown: ScoreBreakdown,
    pub token_estimate: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RetrievalTelemetry {
    pub vector_candidates: usize,
    pub graph_candidates: usize,
    pub merged_candidates: usize,
    pub selected_candidates: usize,
    pub token_estimate: u32,
    pub degraded_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetrievalResponse {
    pub retrieval_id: String,
    pub status: RetrievalStatus,
    pub telemetry: RetrievalTelemetry,
    pub items: Vec<RetrievedItem>,
}

impl RetrievalRequest {
    pub fn test(
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
        query_text: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            user_id: user_id.into(),
            session_id: None,
            query_text: query_text.into(),
            entities: Vec::new(),
            intent_topic: None,
            token_budget: 1800,
            privacy_scope: vec![
                PrivacyLevel::Private,
                PrivacyLevel::Sensitive,
                PrivacyLevel::Shared,
            ],
            top_k_vector: 12,
            max_graph_hops: 2,
        }
    }
}

pub fn keyword_search(records: &[MemoryRecord], query: &str) -> CoreResult<Vec<MemoryRecord>> {
    let normalized = query.trim().to_lowercase();
    if normalized.is_empty() {
        return Err(CoreError::InvalidInput("query cannot be empty".to_string()));
    }

    Ok(records
        .iter()
        .filter(|record| record.content.to_lowercase().contains(&normalized))
        .cloned()
        .collect())
}

pub fn retrieve<T>(store: &mut T, request: RetrievalRequest) -> CoreResult<RetrievalResponse>
where
    T: MemoryIndexStore + GraphStore,
{
    if request.query_text.trim().is_empty() {
        return Err(CoreError::InvalidInput("query cannot be empty".to_string()));
    }
    let privacy_scope = if request.privacy_scope.is_empty() {
        vec![PrivacyLevel::Private]
    } else {
        request.privacy_scope.clone()
    };
    let records =
        store.list_memories(&request.tenant_id, &request.user_id, &privacy_scope, false)?;

    let mut vector_items: Vec<(MemoryRecord, f32)> = records
        .iter()
        .map(|record| {
            (
                record.clone(),
                lexical_score(&record.content, &request.query_text),
            )
        })
        .filter(|(_, score)| *score > 0.0)
        .collect();
    vector_items.sort_by(|left, right| right.1.total_cmp(&left.1));
    vector_items.truncate(request.top_k_vector);

    let graph_ids = store.graph_memory_ids(
        &request.user_id,
        &request.query_text,
        request.max_graph_hops,
    )?;
    let mut merged: HashMap<String, RetrievedItem> = HashMap::new();

    for (record, semantic_similarity) in &vector_items {
        merged.insert(
            record.id.clone(),
            item_from_record(record, SourcePath::Vector, *semantic_similarity),
        );
    }
    for memory_id in &graph_ids {
        if let Some(record) = records.iter().find(|record| &record.id == memory_id) {
            merged
                .entry(memory_id.clone())
                .and_modify(|item| item.source_path = SourcePath::Both)
                .or_insert_with(|| {
                    item_from_record(
                        record,
                        SourcePath::Graph,
                        lexical_score(&record.content, &request.query_text),
                    )
                });
        }
    }

    let mut items: Vec<RetrievedItem> = merged.into_values().collect();
    items.sort_by(|left, right| right.retrieval_score.total_cmp(&left.retrieval_score));

    let mut token_total = 0;
    let mut selected = Vec::new();
    for item in items {
        if token_total + item.token_estimate <= request.token_budget {
            token_total += item.token_estimate;
            selected.push(item);
        }
    }

    for item in &selected {
        if let Some(mut record) =
            store.get_memory(&request.tenant_id, &request.user_id, &item.memory_id)?
        {
            record.mark_accessed();
            store.update_memory(record)?;
        }
    }

    let telemetry = RetrievalTelemetry {
        vector_candidates: vector_items.len(),
        graph_candidates: graph_ids.len(),
        merged_candidates: selected.len(),
        selected_candidates: selected.len(),
        token_estimate: token_total,
        degraded_reasons: Vec::new(),
    };
    Ok(RetrievalResponse {
        retrieval_id: crate::memory::deterministic_id(&[
            &request.user_id,
            &request.query_text,
            &crate::memory::now_timestamp(),
        ]),
        status: RetrievalStatus::Ok,
        telemetry,
        items: selected,
    })
}

fn item_from_record(
    record: &MemoryRecord,
    source_path: SourcePath,
    semantic_similarity: f32,
) -> RetrievedItem {
    let access = (record.access_count as f32 / 10.0).min(1.0);
    let recency = 1.0;
    let score = retrieval_score(
        semantic_similarity,
        recency,
        record.importance_score,
        access,
    );
    RetrievedItem {
        memory_id: record.id.clone(),
        content: record.content.clone(),
        source_path,
        retrieval_score: score,
        score_breakdown: ScoreBreakdown {
            semantic_similarity,
            recency,
            importance: record.importance_score,
            access,
        },
        token_estimate: estimate_tokens(&record.content),
    }
}
