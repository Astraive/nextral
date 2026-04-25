use crate::{
    contracts::{CoreError, CoreResult},
    topology,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    time::{SystemTime, UNIX_EPOCH},
};

pub const CURRENT_SCHEMA_VERSION: &str = "1.0.0";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ContentType {
    Decision,
    Preference,
    Goal,
    Fact,
    Task,
    Event,
    Note,
    Commitment,
    Pattern,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MemoryType {
    Working,
    Session,
    Episodic,
    Semantic,
    Relational,
    Procedural,
    Prospective,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    Realtime,
    FastLane,
    DeepLane,
    Import,
    Manual,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PrivacyLevel {
    Private,
    Sensitive,
    Shared,
    Restricted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MemoryStatus {
    Active,
    SoftDeleted,
    Redacted,
    Archived,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryRecord {
    pub id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: Option<String>,
    pub content: String,
    pub content_type: ContentType,
    pub memory_type: MemoryType,
    pub source_type: SourceType,
    pub source_message_ids: Vec<String>,
    pub importance_score: f32,
    pub confidence_score: Option<f32>,
    pub embedding_provider: Option<String>,
    pub embedding_model: Option<String>,
    pub embedding_dim: Option<u32>,
    pub extraction_provider: Option<String>,
    pub extraction_model: Option<String>,
    pub entities: Vec<String>,
    pub tags: Vec<String>,
    pub privacy_level: PrivacyLevel,
    pub created_at: String,
    pub updated_at: String,
    pub last_accessed_at: Option<String>,
    pub access_count: u64,
    pub status: MemoryStatus,
    pub schema_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransitionMetadata {
    pub memory_id: String,
    pub from: MemoryStatus,
    pub to: MemoryStatus,
    pub actor: String,
    pub reason: String,
    pub changed_at: String,
}

impl MemoryRecord {
    pub fn new(
        id: impl Into<String>,
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
        content: impl Into<String>,
        content_type: ContentType,
        memory_type: MemoryType,
        source_type: SourceType,
    ) -> Self {
        let now = now_timestamp();
        Self {
            id: id.into(),
            tenant_id: tenant_id.into(),
            user_id: user_id.into(),
            session_id: None,
            content: content.into(),
            content_type,
            memory_type,
            source_type,
            source_message_ids: Vec::new(),
            importance_score: 0.5,
            confidence_score: Some(0.8),
            embedding_provider: None,
            embedding_model: None,
            embedding_dim: None,
            extraction_provider: None,
            extraction_model: None,
            entities: Vec::new(),
            tags: Vec::new(),
            privacy_level: PrivacyLevel::Private,
            created_at: now.clone(),
            updated_at: now,
            last_accessed_at: None,
            access_count: 0,
            status: MemoryStatus::Active,
            schema_version: CURRENT_SCHEMA_VERSION.to_string(),
        }
    }

    pub fn validate(&self) -> CoreResult<()> {
        if self.id.trim().is_empty() {
            return Err(CoreError::InvalidInput("memory id is required".to_string()));
        }
        if self.tenant_id.trim().is_empty() {
            return Err(CoreError::InvalidInput("tenant_id is required".to_string()));
        }
        if self.user_id.trim().is_empty() {
            return Err(CoreError::InvalidInput("user_id is required".to_string()));
        }
        if self.content.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "content cannot be empty".to_string(),
            ));
        }
        validate_score("importance_score", self.importance_score)?;
        if let Some(score) = self.confidence_score {
            validate_score("confidence_score", score)?;
        }
        if self.schema_version != CURRENT_SCHEMA_VERSION {
            return Err(CoreError::InvalidInput(format!(
                "unsupported schema_version {}; expected {}",
                self.schema_version, CURRENT_SCHEMA_VERSION
            )));
        }
        let type_profile = topology::profile(&self.memory_type);
        if self.status == MemoryStatus::Active
            && !type_profile.durable
            && self.source_type != SourceType::Realtime
        {
            return Err(CoreError::InvalidInput(
                "working memory must remain ephemeral realtime state".to_string(),
            ));
        }
        Ok(())
    }

    pub fn transition(
        &mut self,
        to: MemoryStatus,
        actor: impl Into<String>,
        reason: impl Into<String>,
    ) -> CoreResult<TransitionMetadata> {
        let actor = actor.into();
        let reason = reason.into();
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "transition actor and reason are required".to_string(),
            ));
        }
        let from = self.status.clone();
        let allowed = matches!(
            (&from, &to),
            (MemoryStatus::Active, MemoryStatus::SoftDeleted)
                | (MemoryStatus::SoftDeleted, MemoryStatus::Redacted)
                | (MemoryStatus::Active, MemoryStatus::Archived)
        );
        if !allowed {
            return Err(CoreError::InvalidInput(format!(
                "invalid lifecycle transition from {:?} to {:?}",
                from, to
            )));
        }
        let changed_at = now_timestamp();
        self.status = to.clone();
        self.updated_at = changed_at.clone();
        if to == MemoryStatus::Redacted {
            self.content = "[redacted]".to_string();
            self.entities.clear();
            self.tags.clear();
        }
        Ok(TransitionMetadata {
            memory_id: self.id.clone(),
            from,
            to,
            actor,
            reason,
            changed_at,
        })
    }

    pub fn mark_accessed(&mut self) {
        self.access_count = self.access_count.saturating_add(1);
        self.last_accessed_at = Some(now_timestamp());
    }
}

pub fn upsert(records: &mut Vec<MemoryRecord>, record: MemoryRecord) {
    if let Some(existing) = records
        .iter_mut()
        .find(|candidate| candidate.id == record.id)
    {
        *existing = record;
        return;
    }
    records.push(record);
}

pub fn validate_score(name: &str, score: f32) -> CoreResult<()> {
    if !(0.0..=1.0).contains(&score) || score.is_nan() {
        return Err(CoreError::InvalidInput(format!(
            "{name} must be within 0..=1"
        )));
    }
    Ok(())
}

pub fn now_timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

pub fn deterministic_id(parts: &[&str]) -> String {
    let mut hasher = DefaultHasher::new();
    parts.hash(&mut hasher);
    format!("mem_{:016x}", hasher.finish())
}

pub fn estimate_tokens(text: &str) -> u32 {
    text.split_whitespace().count().max(1) as u32
}
