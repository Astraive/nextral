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

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    Realtime,
    FastLane,
    DeepLane,
    Import,
    #[default]
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

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryFilters {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_types: Option<Vec<MemoryType>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_types: Option<Vec<ContentType>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_types: Option<Vec<SourceType>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags_any: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags_all: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entities_any: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entities_all: Option<Vec<String>>,
    #[serde(default, alias = "created_after")]
    pub created_at_gte: Option<String>,
    #[serde(default, alias = "created_before")]
    pub created_at_lt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statuses: Option<Vec<MemoryStatus>>,
}

pub fn normalize_rfc3339_micros(input: &str) -> CoreResult<String> {
    let parsed = chrono::DateTime::parse_from_rfc3339(input)
        .map_err(|e| CoreError::InvalidInput(format!("invalid RFC3339 timestamp '{input}': {e}")))?
        .with_timezone(&chrono::Utc);
    Ok(parsed.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
}

impl MemoryFilters {
    pub fn validate(&mut self) -> CoreResult<()> {
        fn validate_terms(terms: &mut Option<Vec<String>>) -> CoreResult<()> {
            if let Some(list) = terms {
                if list.len() > 32 {
                    return Err(CoreError::InvalidInput(
                        "filter array exceeds 32 entries".into(),
                    ));
                }
                for item in list.iter() {
                    if item.is_empty() || item.chars().count() > 256 {
                        return Err(CoreError::InvalidInput(
                            "filter term must be 1..=256 Unicode characters".into(),
                        ));
                    }
                }
                list.sort();
                list.dedup();
            }
            Ok(())
        }
        validate_terms(&mut self.tags_any)?;
        validate_terms(&mut self.tags_all)?;
        validate_terms(&mut self.entities_any)?;
        validate_terms(&mut self.entities_all)?;

        if let Some(ref session) = self.session_id {
            if session.is_empty() || session.len() > 256 {
                return Err(CoreError::InvalidInput(
                    "session_id must be 1..=256 bytes".into(),
                ));
            }
        }
        if let Some(ref mut gte) = self.created_at_gte {
            *gte = normalize_rfc3339_micros(gte)?;
        }
        if let Some(ref mut lt) = self.created_at_lt {
            *lt = normalize_rfc3339_micros(lt)?;
        }
        if let (Some(gte), Some(lt)) = (&self.created_at_gte, &self.created_at_lt) {
            if gte > lt {
                return Err(CoreError::InvalidInput("reversed date interval".into()));
            }
        }
        if let Some(ref mut statuses) = self.statuses {
            statuses.sort_by_key(|s| serde_json::to_string(s).unwrap_or_default());
            statuses.dedup();
        }
        Ok(())
    }

    pub fn normalized(&self) -> Self {
        let mut copy = self.clone();
        let _ = copy.validate();
        if copy.statuses.as_ref().map_or(true, |s| s.is_empty()) {
            copy.statuses = Some(vec![MemoryStatus::Active]);
        }
        copy
    }

    pub fn matches(&self, record: &MemoryRecord) -> bool {
        if let Some(ref types) = self.memory_types {
            if !types.is_empty() && !types.contains(&record.memory_type) {
                return false;
            }
        }
        if let Some(ref types) = self.content_types {
            if !types.is_empty() && !types.contains(&record.content_type) {
                return false;
            }
        }
        if let Some(ref types) = self.source_types {
            if !types.is_empty() && !types.contains(&record.source_type) {
                return false;
            }
        }
        if let Some(ref session_id) = self.session_id {
            if record.session_id.as_deref() != Some(session_id.as_str()) {
                return false;
            }
        }
        if let Some(ref tags_any) = self.tags_any {
            if !tags_any.is_empty() && !tags_any.iter().any(|t| record.has_tag(t)) {
                return false;
            }
        }
        if let Some(ref tags_all) = self.tags_all {
            if !tags_all.iter().all(|t| record.has_tag(t)) {
                return false;
            }
        }
        if let Some(ref entities_any) = self.entities_any {
            if !entities_any.is_empty() && !entities_any.iter().any(|e| record.has_entity(e)) {
                return false;
            }
        }
        if let Some(ref entities_all) = self.entities_all {
            if !entities_all.iter().all(|e| record.has_entity(e)) {
                return false;
            }
        }
        if let Some(ref gte) = self.created_at_gte {
            let record_created = normalize_rfc3339_micros(&record.created_at)
                .unwrap_or_else(|_| record.created_at.clone());
            if record_created < *gte {
                return false;
            }
        }
        if let Some(ref lt) = self.created_at_lt {
            let record_created = normalize_rfc3339_micros(&record.created_at)
                .unwrap_or_else(|_| record.created_at.clone());
            if record_created >= *lt {
                return false;
            }
        }
        let effective_statuses = self.statuses.as_ref().filter(|s| !s.is_empty());
        match effective_statuses {
            Some(statuses) => statuses.contains(&record.status),
            None => record.status == MemoryStatus::Active,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemorySummary {
    pub id: String,
    #[serde(default)]
    pub tenant_id: String,
    #[serde(default)]
    pub user_id: String,
    pub session_id: Option<String>,
    pub memory_type: MemoryType,
    pub content_type: ContentType,
    pub source_type: SourceType,
    pub privacy_level: PrivacyLevel,
    pub status: MemoryStatus,
    #[serde(default)]
    pub importance_score: f32,
    #[serde(default)]
    pub confidence_score: Option<f32>,
    pub created_at: String,
    pub updated_at: String,
    pub preview: String,
    pub content_preview: String,
    #[serde(default)]
    pub content: String,
    #[serde(default)]
    pub source_message_ids: Vec<String>,
    #[serde(default)]
    pub entities: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub embedding: Option<Vec<f32>>,
    #[serde(default)]
    pub embedding_dim: Option<u32>,
    #[serde(default)]
    pub embedding_model: Option<String>,
    #[serde(default)]
    pub access_count: u64,
    #[serde(default)]
    pub last_accessed_at: Option<String>,
}

impl MemorySummary {
    pub fn from_record(record: &MemoryRecord) -> Self {
        let preview = if record.status == MemoryStatus::Redacted {
            "[redacted]".to_string()
        } else {
            record.content.chars().take(256).collect::<String>()
        };
        Self {
            id: record.id.clone(),
            tenant_id: record.tenant_id.clone(),
            user_id: record.user_id.clone(),
            session_id: record.session_id.clone(),
            content: record.content.clone(),
            memory_type: record.memory_type.clone(),
            content_type: record.content_type.clone(),
            source_type: record.source_type.clone(),
            source_message_ids: record.source_message_ids.clone(),
            entities: record.entities.clone(),
            tags: record.tags.clone(),
            embedding: record.embedding.clone(),
            embedding_dim: record.embedding_dim,
            embedding_model: record.embedding_model.clone(),
            access_count: record.access_count,
            last_accessed_at: record.last_accessed_at.clone(),
            privacy_level: record.privacy_level.clone(),
            status: record.status.clone(),
            importance_score: record.importance_score,
            confidence_score: record.confidence_score,
            created_at: record.created_at.clone(),
            updated_at: record.updated_at.clone(),
            preview: preview.clone(),
            content_preview: preview,
        }
    }
}

fn default_query_limit() -> usize {
    50
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + serde::Deserialize<'de>,
{
    let opt = Option::<T>::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

fn deserialize_limit_null_default<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<usize>::deserialize(deserializer)?;
    Ok(opt.unwrap_or(50))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryQuery {
    pub tenant_id: String,
    pub user_id: String,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub privacy_scope: Vec<PrivacyLevel>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub filters: MemoryFilters,
    #[serde(
        default = "default_query_limit",
        deserialize_with = "deserialize_limit_null_default"
    )]
    pub limit: usize,
    #[serde(default)]
    pub cursor: Option<String>,
}

impl MemoryQuery {
    pub fn validate(&mut self) -> CoreResult<()> {
        if self.tenant_id.is_empty() || self.tenant_id.len() > 256 {
            return Err(CoreError::InvalidInput(
                "tenant_id must be 1..=256 bytes".into(),
            ));
        }
        if self.user_id.is_empty() || self.user_id.len() > 256 {
            return Err(CoreError::InvalidInput(
                "user_id must be 1..=256 bytes".into(),
            ));
        }
        if self.limit == 0 || self.limit > 100 {
            return Err(CoreError::InvalidInput(
                "limit must be between 1 and 100".into(),
            ));
        }
        if let Some(ref c) = self.cursor {
            if c.is_empty() {
                return Err(CoreError::InvalidInput(
                    "cursor cannot be empty string".into(),
                ));
            }
            if c.len() > 4096 {
                return Err(CoreError::InvalidInput(
                    "cursor exceeds maximum size of 4096 bytes".into(),
                ));
            }
        }
        self.filters.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryPage {
    pub items: Vec<MemorySummary>,
    pub next_cursor: Option<String>,
}

pub fn compute_query_fingerprint(
    tenant_id: &str,
    user_id: &str,
    effective_privacy: &[PrivacyLevel],
    filters: &MemoryFilters,
) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(tenant_id.as_bytes());
    hasher.update(b":");
    hasher.update(user_id.as_bytes());
    hasher.update(b":");
    let mut sorted_privacy = effective_privacy.to_vec();
    sorted_privacy.sort_by_key(|p| serde_json::to_string(p).unwrap_or_default());
    sorted_privacy.dedup();
    hasher.update(
        serde_json::to_string(&sorted_privacy)
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.update(b":");
    let normalized_filters = filters.normalized();
    hasher.update(
        serde_json::to_string(&normalized_filters)
            .unwrap_or_default()
            .as_bytes(),
    );
    format!("{:x}", hasher.finalize())
}

pub fn encode_cursor(after_created_at: &str, after_id: &str, fingerprint: &str) -> String {
    let payload = serde_json::json!({
        "v": 1,
        "after_created_at": after_created_at,
        "after_id": after_id,
        "scope_filter_sha256": fingerprint,
    });
    URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes())
}

pub fn decode_cursor(cursor: &str, expected_fingerprint: &str) -> CoreResult<(String, String)> {
    if cursor.is_empty() {
        return Err(CoreError::InvalidInput(
            "cursor cannot be empty string".into(),
        ));
    }
    if cursor.len() > 4096 {
        return Err(CoreError::InvalidInput("cursor exceeds 4096 bytes".into()));
    }
    let decoded_bytes = URL_SAFE_NO_PAD
        .decode(cursor.as_bytes())
        .map_err(|_| CoreError::InvalidInput("malformed base64url cursor".into()))?;
    let val: serde_json::Value = serde_json::from_slice(&decoded_bytes)
        .map_err(|_| CoreError::InvalidInput("malformed JSON in cursor".into()))?;
    if val["v"].as_u64() != Some(1) {
        return Err(CoreError::InvalidInput("unsupported cursor version".into()));
    }
    let after_created = val["after_created_at"]
        .as_str()
        .ok_or_else(|| CoreError::InvalidInput("cursor missing after_created_at".into()))?;
    let after_id = val["after_id"]
        .as_str()
        .ok_or_else(|| CoreError::InvalidInput("cursor missing after_id".into()))?;
    let fingerprint = val["scope_filter_sha256"]
        .as_str()
        .ok_or_else(|| CoreError::InvalidInput("cursor missing scope_filter_sha256".into()))?;
    if fingerprint != expected_fingerprint {
        return Err(CoreError::InvalidInput(
            "cursor filter or scope mismatch".into(),
        ));
    }
    Ok((after_created.to_string(), after_id.to_string()))
}
