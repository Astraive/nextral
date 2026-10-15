use crate::{
    contracts::{CoreError, CoreResult},
    topology,
};
use serde::{Deserialize, Serialize};
use std::{
    hash::Hash,
    time::{SystemTime, UNIX_EPOCH},
};

/// Current schema version for memory records. Must be bumped on breaking changes.
pub const CURRENT_SCHEMA_VERSION: &str = "1.0.0";

/// Classifies the semantic nature of a memory's content.
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

/// The seven-type memory architecture: Working, Session, Episodic, Semantic,
/// Relational, Procedural, and Prospective.
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

/// Privacy classification controlling visibility across tenants and retrieval scopes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PrivacyLevel {
    Private,
    Sensitive,
    Shared,
    Restricted,
}

/// Lifecycle state of a memory record. Transitions follow a strict state machine:
/// Active → SoftDeleted → Redacted, or Active → Archived.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MemoryStatus {
    Active,
    SoftDeleted,
    Redacted,
    Archived,
}

/// Core memory record containing content, metadata, embedding info, and lifecycle state.
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
    #[serde(default)]
    pub embedding: Option<Vec<f32>>,
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
            embedding: None,
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
        crate::store::validate_scope_component(&self.id, "memory id")?;
        crate::store::validate_scope_component(&self.tenant_id, "tenant_id")?;
        crate::store::validate_scope_component(&self.user_id, "user_id")?;
        if let Some(session) = &self.session_id {
            crate::store::validate_scope_component(session, "session_id")?;
        }
        crate::runtime::lifecycle::timestamp_nanos(&self.created_at)?;
        crate::runtime::lifecycle::timestamp_nanos(&self.updated_at)?;
        if let Some(accessed) = &self.last_accessed_at {
            crate::runtime::lifecycle::timestamp_nanos(accessed)?;
        }
        if let Some(vector) = &self.embedding {
            if vector.is_empty()
                || vector.iter().any(|v| !v.is_finite())
                || self.embedding_dim != Some(vector.len() as u32)
            {
                return Err(CoreError::InvalidInput(
                    "embedding must be finite and match embedding_dim".into(),
                ));
            }
        }
        const MAX_CONTENT_LENGTH: usize = 1_000_000;
        const MAX_ID_LENGTH: usize = 256;
        const MAX_ENTITIES: usize = 1000;
        const MAX_TAGS: usize = 1000;
        if self.id.trim().is_empty() {
            return Err(CoreError::InvalidInput("memory id is required".to_string()));
        }
        if self.id.len() > MAX_ID_LENGTH {
            return Err(CoreError::InvalidInput(format!(
                "memory id must not exceed {MAX_ID_LENGTH} characters"
            )));
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
        if self.content.len() > MAX_CONTENT_LENGTH {
            return Err(CoreError::InvalidInput(format!(
                "content must not exceed {MAX_CONTENT_LENGTH} characters"
            )));
        }
        if self.entities.len() > MAX_ENTITIES {
            return Err(CoreError::InvalidInput(format!(
                "entities must not contain more than {MAX_ENTITIES} items"
            )));
        }
        if self.tags.len() > MAX_TAGS {
            return Err(CoreError::InvalidInput(format!(
                "tags must not contain more than {MAX_TAGS} items"
            )));
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
        let policy = crate::domain::RuntimePolicy::default();
        if !policy.allows_memory_type(&self.memory_type) {
            return Err(CoreError::InvalidInput(format!(
                "memory_type {:?} is not allowed by runtime policy",
                self.memory_type
            )));
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
            (
                MemoryStatus::Active | MemoryStatus::Archived,
                MemoryStatus::SoftDeleted
            ) | (MemoryStatus::SoftDeleted, MemoryStatus::Redacted)
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
            self.embedding = None;
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

    pub fn add_tag(&mut self, tag: impl Into<String>) {
        let tag = tag.into();
        if !self.tags.contains(&tag) {
            self.tags.push(tag);
            self.updated_at = now_timestamp();
        }
    }

    pub fn remove_tag(&mut self, tag: &str) -> bool {
        let len = self.tags.len();
        self.tags.retain(|t| t != tag);
        if self.tags.len() < len {
            self.updated_at = now_timestamp();
            true
        } else {
            false
        }
    }

    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|t| t == tag)
    }

    pub fn add_entity(&mut self, entity: impl Into<String>) {
        let entity = entity.into();
        if !self.entities.contains(&entity) {
            self.entities.push(entity);
            self.updated_at = now_timestamp();
        }
    }

    pub fn remove_entity(&mut self, entity: &str) -> bool {
        let len = self.entities.len();
        self.entities.retain(|e| e != entity);
        if self.entities.len() < len {
            self.updated_at = now_timestamp();
            true
        } else {
            false
        }
    }

    pub fn has_entity(&self, entity: &str) -> bool {
        self.entities.iter().any(|e| e == entity)
    }

    pub fn patch(
        &mut self,
        content: Option<String>,
        content_type: Option<ContentType>,
        importance_score: Option<f32>,
        confidence_score: Option<Option<f32>>,
        privacy_level: Option<PrivacyLevel>,
    ) -> CoreResult<()> {
        if let Some(c) = content {
            if c.trim().is_empty() {
                return Err(CoreError::InvalidInput(
                    "content cannot be empty".to_string(),
                ));
            }
            if c.len() > 1_000_000 {
                return Err(CoreError::InvalidInput(
                    "content must not exceed 1,000,000 characters".to_string(),
                ));
            }
            self.content = c;
        }
        if let Some(ct) = content_type {
            self.content_type = ct;
        }
        if let Some(score) = importance_score {
            crate::memory::validate_score("importance_score", score)?;
            self.importance_score = score;
        }
        if let Some(cs) = confidence_score {
            if let Some(s) = cs {
                crate::memory::validate_score("confidence_score", s)?;
            }
            self.confidence_score = cs;
        }
        if let Some(pl) = privacy_level {
            self.privacy_level = pl;
        }
        self.updated_at = now_timestamp();
        Ok(())
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
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for (i, part) in parts.iter().enumerate() {
        if i > 0 {
            hasher.update([0xFF]);
        }
        hasher.update(part.as_bytes());
    }
    let result = hasher.finalize();
    format!(
        "mem_{:016x}",
        u64::from_be_bytes(result[..8].try_into().unwrap_or([0; 8]))
    )
}

pub fn estimate_tokens(text: &str) -> u32 {
    text.split_whitespace().count().max(1) as u32
}
