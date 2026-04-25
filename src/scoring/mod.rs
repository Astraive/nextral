use crate::{
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

pub fn retrieval_score(
    semantic_similarity: f32,
    recency: f32,
    importance: f32,
    access: f32,
) -> f32 {
    (0.5 * semantic_similarity) + (0.2 * recency) + (0.2 * importance) + (0.1 * access)
}
