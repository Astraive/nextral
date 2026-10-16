use crate::{
    config::ScoringWeights,
    contracts::{CoreError, CoreResult},
    memory::{estimate_tokens, MemoryRecord, PrivacyLevel},
    runtime::intelligence::{decision_for, DecisionAction, RuntimeLane, SafetyPolicy, Severity},
    scoring::{lexical_score, retrieval_score},
    store::{GraphStore, MemoryIndexStore},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::Instant};

/// Request to retrieve memories matching a query, with token budget, privacy scope, and scoring weights.
fn default_token_budget() -> u32 { 1800 }
fn default_top_k_vector() -> usize { 12 }
fn default_max_graph_hops() -> u8 { 2 }
fn default_privacy_scope() -> Vec<PrivacyLevel> { vec![PrivacyLevel::Private] }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetrievalRequest {
    pub tenant_id: String,
    pub user_id: String,
    #[serde(default)]
    pub session_id: Option<String>,
    pub query_text: String,
    #[serde(default)]
    pub query_vector: Option<Vec<f32>>,
    #[serde(default)]
    pub entities: Vec<String>,
    #[serde(default)]
    pub intent_topic: Option<String>,
    #[serde(default = "default_token_budget")]
    pub token_budget: u32,
    #[serde(default = "default_privacy_scope")]
    pub privacy_scope: Vec<PrivacyLevel>,
    #[serde(default = "default_top_k_vector")]
    pub top_k_vector: usize,
    #[serde(default = "default_max_graph_hops")]
    pub max_graph_hops: u8,
    #[serde(default)]
    pub lane: Option<RuntimeLane>,
    #[serde(default)]
    pub policy_version: Option<String>,
    #[serde(default)]
    pub trace_id: Option<String>,
    #[serde(default)]
    pub scoring_weights: Option<ScoringWeights>,
    #[serde(default)]
    pub filters: Option<crate::memory::MemoryFilters>,
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

/// Response containing ranked retrieved items, telemetry, quality score, and safety decision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetrievalResponse {
    pub retrieval_id: String,
    pub tenant_id: String,
    pub user_id: String,
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
            query_vector: None,
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
            scoring_weights: Some(ScoringWeights {
                semantic_similarity: 0.5,
                recency: 0.2,
                importance: 0.2,
                access: 0.1,
            }),
            filters: None,
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
    let mut span = crate::runtime::observability::start_span("nextral.retrieve");
    let result = retrieve_impl(store, request);
    match &result {
        Ok(response) => {
            crate::runtime::observability::record_retrieval(&response.telemetry);
            if response.status == RetrievalStatus::Error {
                span.fail();
            }
        }
        Err(_) => span.fail(),
    }
    result
}

fn retrieve_impl<T>(store: &mut T, mut request: RetrievalRequest) -> CoreResult<RetrievalResponse>
where
    T: MemoryIndexStore + GraphStore,
{
    if request.query_text.trim().is_empty() {
        return Err(CoreError::InvalidInput("query cannot be empty".to_string()));
    }
    if request.privacy_scope.is_empty() {
        request.privacy_scope = vec![PrivacyLevel::Private];
    }
    store.prepare_retrieval(&mut request)?;
    request.top_k_vector = request.top_k_vector.min(1000);
    const MAX_GRAPH_HOPS: u8 = 10;
    let max_graph_hops = request.max_graph_hops.min(MAX_GRAPH_HOPS);
    let scoring_weights = request.scoring_weights.clone().unwrap_or(ScoringWeights {
        semantic_similarity: 0.5,
        recency: 0.2,
        importance: 0.2,
        access: 0.1,
    });
    crate::config::validate_retrieval_weights(&scoring_weights)?;
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
    let privacy_scope = request.privacy_scope.clone();
    let shared = &*store;
    let (vector_result, graph_result, vector_ms, graph_ms) = std::thread::scope(|threads| {
        let vector_path = threads.spawn(|| {
            let start = Instant::now();
            let result = shared.vector_candidates(
                &request.tenant_id,
                &request.user_id,
                &request.query_text,
                request.query_vector.as_deref(),
                request.top_k_vector,
                &privacy_scope,
                request.filters.as_ref(),
            );
            (result, start.elapsed().as_millis() as u64)
        });
        let graph_path = threads.spawn(|| {
            let start = Instant::now();
            let graph_query = if request.entities.is_empty() {
                request.query_text.clone()
            } else {
                request.entities.join(" ")
            };
            let result = shared.graph_memory_ids(
                &request.tenant_id,
                &request.user_id,
                &graph_query,
                max_graph_hops,
                &privacy_scope,
            );
            (result, start.elapsed().as_millis() as u64)
        });
        let (vectors, vector_ms) = vector_path
            .join()
            .unwrap_or_else(|_| (Err(CoreError::Io("vector worker panicked".into())), 0));
        let (graph, graph_ms) = graph_path
            .join()
            .unwrap_or_else(|_| (Err(CoreError::Io("graph worker panicked".into())), 0));
        (vectors, graph, vector_ms, graph_ms)
    });

    let all_paths_failed = vector_result.is_err() && (max_graph_hops == 0 || graph_result.is_err());
    let mut degraded_reasons = Vec::new();
    let vector_hits = match vector_result {
        Ok(mut items) => {
            items.truncate(request.top_k_vector);
            items
        }
        Err(error) => {
            degraded_reasons.push(format!("vector_path_failed:{error}"));
            Vec::new()
        }
    };
    let graph_ids = match graph_result {
        Ok(mut ids) => {
            ids.truncate(1000);
            ids
        }
        Err(error) => {
            degraded_reasons.push(format!("graph_path_failed:{error}"));
            Vec::new()
        }
    };
    if all_paths_failed {
        return Ok(RetrievalResponse {
            retrieval_id: crate::memory::deterministic_id(&[
                &request.user_id,
                &request.query_text,
                &crate::memory::now_timestamp(),
            ]),
            tenant_id: request.tenant_id,
            user_id: request.user_id,
            trace_id,
            status: RetrievalStatus::Error,
            lane,
            policy_version,
            quality_score: 0.0,
            severity: Severity::Destructive,
            decision_action: DecisionAction::AutoRollbackAndQuarantine,
            rollback_id: None,
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
    let now_nanos = crate::runtime::lifecycle::timestamp_nanos(&crate::memory::now_timestamp())?;
    let graph_candidates = graph_ids.len();
    let mut hits: HashMap<String, (Option<f32>, SourcePath)> = vector_hits
        .into_iter()
        .map(|(id, score)| {
            (
                id,
                (
                    Some(crate::scoring::normalize_score(score)),
                    SourcePath::Vector,
                ),
            )
        })
        .collect();
    for id in graph_ids {
        match hits.entry(id) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if entry.get().0.is_some() {
                    entry.get_mut().1 = SourcePath::Both;
                }
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert((None, SourcePath::Graph));
            }
        }
    }
    let records = {
        let ids: Vec<_> = hits.keys().map(String::as_str).collect();
        store.get_active_memories(&request.tenant_id, &request.user_id, &ids)?
    };
    let mut vector_candidates = 0;
    let mut items = Vec::with_capacity(records.len());
    for record in records {
        if record.tenant_id != request.tenant_id
            || record.user_id != request.user_id
            || record.status != crate::memory::MemoryStatus::Active
            || !privacy_scope.contains(&record.privacy_level)
            || request.filters.as_ref().map_or(false, |f| !f.matches(&record))
        {
            continue;
        }
        let Some((score, source_path)) = hits.remove(record.id.as_str()) else {
            continue;
        };
        vector_candidates += usize::from(score.is_some());
        let similarity =
            score.unwrap_or_else(|| lexical_score(&record.content, &request.query_text));
        items.push(item_from_record(
            record,
            source_path,
            similarity,
            &scoring_weights,
            now_nanos,
        )?);
    }
    items.sort_by(|left, right| {
        right
            .retrieval_score
            .total_cmp(&left.retrieval_score)
            .then_with(|| left.memory_id.cmp(&right.memory_id))
    });

    let merged_candidates = items.len();
    let mut token_total = 0;
    let mut selected = Vec::new();
    for item in items {
        if token_total + item.token_estimate <= request.token_budget {
            token_total += item.token_estimate;
            selected.push(item);
        }
    }

    let access_ids: Vec<_> = selected
        .iter()
        .map(|item| item.memory_id.as_str())
        .collect();
    store.record_accesses(&request.tenant_id, &request.user_id, &access_ids)?;
    let merge_ms = merge_start.elapsed().as_millis() as u64;
    let utilization = if request.token_budget == 0 {
        0.0
    } else {
        (token_total as f32 / request.token_budget as f32).clamp(0.0, 1.0)
    };
    let union_candidates = vector_candidates + graph_candidates;
    let dedupe_ratio = if union_candidates == 0 {
        0.0
    } else {
        1.0 - (merged_candidates as f32 / union_candidates as f32)
    };

    let telemetry = RetrievalTelemetry {
        vector_candidates,
        graph_candidates,
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
    let quality_score = if selected.is_empty() {
        0.0
    } else {
        selected
            .iter()
            .map(|item| item.retrieval_score)
            .sum::<f32>()
            / selected.len() as f32
    };
    let severity = if degraded_reasons.is_empty() {
        Severity::Success
    } else {
        Severity::Warning
    };
    let decision_action = decision_for(&SafetyPolicy::default(), &severity);
    let rollback_id = None;
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
        tenant_id: request.tenant_id,
        user_id: request.user_id,
        trace_id,
        status,
        lane,
        policy_version,
        quality_score,
        severity,
        decision_action,
        rollback_id,
        telemetry,
        items: selected,
    })
}

fn item_from_record(
    record: MemoryRecord,
    source_path: SourcePath,
    semantic_similarity: f32,
    weights: &ScoringWeights,
    now_nanos: u128,
) -> CoreResult<RetrievedItem> {
    let access = (record.access_count as f32 / 10.0).min(1.0);
    let created = crate::runtime::lifecycle::timestamp_nanos(&record.created_at)?;
    let recency = crate::scoring::recency_score(
        now_nanos.saturating_sub(created) as f32 / 86_400_000_000_000.0,
    );
    let score = retrieval_score(
        semantic_similarity,
        recency,
        record.importance_score,
        access,
        weights,
    );
    let token_estimate = estimate_tokens(&record.content);
    Ok(RetrievedItem {
        memory_id: record.id,
        content: record.content,
        source_path,
        retrieval_score: score,
        score_breakdown: ScoreBreakdown {
            semantic_similarity,
            recency,
            importance: record.importance_score,
            access,
        },
        token_estimate,
    })
}
