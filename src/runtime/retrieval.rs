use crate::{
    contracts::{CoreError, CoreResult},
    memory::{estimate_tokens, MemoryRecord, PrivacyLevel},
    runtime::intelligence::{
        classify_severity, decision_for, DecisionAction, MemoryQualityController, RuntimeLane,
        SafetyPolicy, Severity,
    },
    scoring::{lexical_score, retrieval_score},
    store::{GraphStore, MemoryIndexStore},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::Instant};

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
    pub lane: Option<RuntimeLane>,
    pub policy_version: Option<String>,
    pub trace_id: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct RetrievalTelemetry {
    pub vector_candidates: usize,
    pub graph_candidates: usize,
    pub merged_candidates: usize,
    pub selected_candidates: usize,
    pub token_estimate: u32,
    pub token_utilization: f32,
    pub vector_ms: u64,
    pub graph_ms: u64,
    pub merge_ms: u64,
    pub dedupe_ratio: f32,
    pub degraded_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetrievalResponse {
    pub retrieval_id: String,
    pub trace_id: String,
    pub status: RetrievalStatus,
    pub lane: RuntimeLane,
    pub policy_version: String,
    pub quality_score: f32,
    pub severity: Severity,
    pub decision_action: DecisionAction,
    pub rollback_id: Option<String>,
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
            lane: None,
            policy_version: None,
            trace_id: None,
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
    let trace_id = request.trace_id.clone().unwrap_or_else(|| {
        crate::memory::deterministic_id(&[
            &request.tenant_id,
            &request.user_id,
            &request.query_text,
            "retrieval_trace",
        ])
    });
    let lane = request.lane.clone().unwrap_or(RuntimeLane::Stable);
    let policy_version = request
        .policy_version
        .clone()
        .unwrap_or_else(|| "policy-v1".to_string());
    let privacy_scope = if request.privacy_scope.is_empty() {
        vec![PrivacyLevel::Private]
    } else {
        request.privacy_scope.clone()
    };
    let records =
        store.list_memories(&request.tenant_id, &request.user_id, &privacy_scope, false)?;
    let vector_start = Instant::now();
    let vector_result: CoreResult<Vec<(MemoryRecord, f32)>> = (|| {
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
        Ok(vector_items)
    })();
    let vector_ms = vector_start.elapsed().as_millis() as u64;

    let graph_start = Instant::now();
    let graph_result =
        store.graph_memory_ids(&request.user_id, &request.query_text, request.max_graph_hops);
    let graph_ms = graph_start.elapsed().as_millis() as u64;

    let mut degraded_reasons = Vec::new();
    let vector_items = match vector_result {
        Ok(items) => items,
        Err(error) => {
            degraded_reasons.push(format!("vector_path_failed:{error}"));
            Vec::new()
        }
    };
    let graph_ids = match graph_result {
        Ok(ids) => ids,
        Err(error) => {
            degraded_reasons.push(format!("graph_path_failed:{error}"));
            Vec::new()
        }
    };
    if vector_items.is_empty() && graph_ids.is_empty() && !degraded_reasons.is_empty() {
        return Ok(RetrievalResponse {
            retrieval_id: crate::memory::deterministic_id(&[
                &request.user_id,
                &request.query_text,
                &crate::memory::now_timestamp(),
            ]),
            trace_id,
            status: RetrievalStatus::Error,
            lane,
            policy_version,
            quality_score: 0.0,
            severity: Severity::Destructive,
            decision_action: DecisionAction::AutoRollbackAndQuarantine,
            rollback_id: Some(crate::memory::deterministic_id(&[
                &request.user_id,
                &request.query_text,
                "retrieval_rollback",
            ])),
            telemetry: RetrievalTelemetry {
                vector_candidates: 0,
                graph_candidates: 0,
                merged_candidates: 0,
                selected_candidates: 0,
                token_estimate: 0,
                token_utilization: 0.0,
                vector_ms,
                graph_ms,
                merge_ms: 0,
                dedupe_ratio: 0.0,
                degraded_reasons,
            },
            items: Vec::new(),
        });
    }

    let merge_start = Instant::now();
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

    let merged_candidates = items.len();
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
    let merge_ms = merge_start.elapsed().as_millis() as u64;
    let utilization = if request.token_budget == 0 {
        0.0
    } else {
        (token_total as f32 / request.token_budget as f32).clamp(0.0, 1.0)
    };
    let union_candidates = vector_items.len() + graph_ids.len();
    let dedupe_ratio = if union_candidates == 0 {
        0.0
    } else {
        1.0 - (merged_candidates as f32 / union_candidates as f32)
    };

    let telemetry = RetrievalTelemetry {
        vector_candidates: vector_items.len(),
        graph_candidates: graph_ids.len(),
        merged_candidates,
        selected_candidates: selected.len(),
        token_estimate: token_total,
        token_utilization: utilization,
        vector_ms,
        graph_ms,
        merge_ms,
        dedupe_ratio,
        degraded_reasons: degraded_reasons.clone(),
    };
    let contradiction_rate = if selected.is_empty() { 1.0 } else { 0.0 };
    let quality = MemoryQualityController::score(
        if vector_items.is_empty() { 0.4 } else { 0.9 },
        if merged_candidates == 0 {
            0.0
        } else {
            selected.len() as f32 / merged_candidates as f32
        },
        contradiction_rate,
        if degraded_reasons.is_empty() { 0.9 } else { 0.5 },
    );
    let severity = classify_severity(
        quality.overall,
        quality.contradiction_rate,
        !degraded_reasons.is_empty(),
    );
    let decision_action = decision_for(&SafetyPolicy::default(), &severity);
    let rollback_id = if severity == Severity::Destructive {
        Some(crate::memory::deterministic_id(&[
            &request.tenant_id,
            &request.user_id,
            &request.query_text,
            "rollback",
        ]))
    } else {
        None
    };
    let status = if degraded_reasons.is_empty() {
        RetrievalStatus::Ok
    } else {
        RetrievalStatus::Degraded
    };
    Ok(RetrievalResponse {
        retrieval_id: crate::memory::deterministic_id(&[
            &request.user_id,
            &request.query_text,
            &crate::memory::now_timestamp(),
        ]),
        trace_id,
        status,
        lane,
        policy_version,
        quality_score: quality.overall,
        severity,
        decision_action,
        rollback_id,
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
