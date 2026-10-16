use crate::{
    config::ScoringWeights,
    contracts::{CoreError, CoreResult},
    memory::MemoryRecord,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScoredRecord {
    pub id: String,
    pub content: String,
    pub score: f32,
}

pub fn lexical_score(text: &str, query: &str) -> f32 {
    let query_tokens: Vec<&str> = query
        .split_whitespace()
        .filter(|token| !token.is_empty())
        .collect();
    if query_tokens.is_empty() {
        return 0.0;
    }
    if text.trim().is_empty() {
        return 0.0;
    }

    let text_lower = text.to_lowercase();
    let overlap = query_tokens
        .iter()
        .filter(|token| text_lower.contains(&token.to_lowercase()))
        .count();
    overlap as f32 / query_tokens.len() as f32
}

pub fn try_lexical_score(text: &str, query: &str) -> CoreResult<f32> {
    if query.trim().is_empty() {
        return Err(CoreError::InvalidInput("query cannot be empty".to_string()));
    }

    Ok(lexical_score(text, query))
}

pub fn rank_records(records: &[MemoryRecord], query: &str) -> CoreResult<Vec<ScoredRecord>> {
    if query.trim().is_empty() {
        return Err(CoreError::InvalidInput("query cannot be empty".to_string()));
    }

    let mut ranked: Vec<ScoredRecord> = records
        .iter()
        .map(|record| ScoredRecord {
            id: record.id.clone(),
            content: record.content.clone(),
            score: lexical_score(&record.content, query),
        })
        .collect();
    ranked.sort_by(|left, right| right.score.total_cmp(&left.score));
    Ok(ranked)
}

/// Canonical unit-interval normalization. Invalid components contribute no score.
pub fn normalize_score(value: f32) -> f32 {
    if value.is_finite() {
        value.clamp(0.0, 1.0)
    } else {
        0.0
    }
}

/// Canonical recency decay, exp(-0.05 * age in days). Future records have age zero.
pub fn recency_score(age_days: f32) -> f32 {
    if !age_days.is_finite() {
        return 0.0;
    }
    (-0.05 * age_days.max(0.0)).exp()
}

pub fn retrieval_score(
    semantic_similarity: f32,
    recency: f32,
    importance: f32,
    access: f32,
    weights: &ScoringWeights,
) -> f32 {
    (weights.semantic_similarity * normalize_score(semantic_similarity))
        + (weights.recency * normalize_score(recency))
        + (weights.importance * normalize_score(importance))
        + (weights.access * normalize_score(access))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recency_uses_canonical_exponential_decay() {
        assert!((recency_score(20.0) - (-1.0_f32).exp()).abs() < 1e-6);
    }

    #[test]
    fn recency_does_not_reward_future_records() {
        assert_eq!(recency_score(-10.0), 1.0);
    }

    #[test]
    fn normalization_rejects_nonfinite_components() {
        assert_eq!(
            [f32::NAN, f32::INFINITY, f32::NEG_INFINITY].map(normalize_score),
            [0.0; 3]
        );
    }

    #[test]
    fn normalization_bounds_finite_components() {
        assert_eq!([-1.0, 0.4, 2.0].map(normalize_score), [0.0, 0.4, 1.0]);
    }
}
