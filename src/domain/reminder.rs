use crate::{
    contracts::{CoreError, CoreResult},
    memory::{deterministic_id, now_timestamp},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuietHours {
    pub start_hour: u8,
    pub end_hour: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReminderTerminalReason {
    Delivered,
    RetryExhausted,
    SourceUnavailable,
    PrivacyDenied,
    ChannelDenied,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReminderDeliveryAttempt {
    pub attempt: u32,
    pub attempted_at: String,
    pub outcome_code: String,
    pub http_status: Option<u16>,
    pub receipt: crate::domain::StoreReceipt,
}

pub fn default_channel() -> String {
    "webhook".into()
}
pub fn default_channels() -> Vec<String> {
    vec![default_channel()]
}
fn default_privacy() -> crate::memory::PrivacyLevel {
    crate::memory::PrivacyLevel::Private
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ReminderKind {
    FollowUp,
    Commitment,
    Task,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ReminderPriority {
    Low,
    Normal,
    High,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ReminderStatus {
    Draft,
    Scheduled,
    Due,
    Dispatched,
    Completed,
    Failed,
    RetryScheduled,
    Cancelled,
    Expired,
}

impl std::fmt::Display for ReminderStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Self::Draft => "draft",
            Self::Scheduled => "scheduled",
            Self::Due => "due",
            Self::Dispatched => "dispatched",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::RetryScheduled => "retry_scheduled",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
        };
        write!(f, "{text}")
    }
}

/// A scheduled reminder linked to a source memory, with retry and expiry lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReminderRecord {
    pub id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub source_memory_id: String,
    pub kind: ReminderKind,
    pub title: String,
    pub details: String,
    pub due_at: String,
    pub timezone: String,
    pub priority: ReminderPriority,
    pub status: ReminderStatus,
    pub attempt_count: u32,
    pub last_attempt_at: Option<String>,
    pub next_attempt_at: Option<String>,
    pub dedupe_key: String,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default = "default_channel")]
    pub channel: String,
    #[serde(default = "default_channels")]
    pub allowed_channels: Vec<String>,
    #[serde(default)]
    pub quiet_hours: Option<QuietHours>,
    #[serde(default)]
    pub expires_at: Option<String>,
    #[serde(default = "default_privacy")]
    pub privacy_level: crate::memory::PrivacyLevel,
    #[serde(default)]
    pub terminal_reason: Option<ReminderTerminalReason>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub delivery_attempts: Vec<ReminderDeliveryAttempt>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReminderTransition {
    pub reminder_id: String,
    pub from: ReminderStatus,
    pub to: ReminderStatus,
    pub actor: String,
    pub reason: String,
    pub changed_at: String,
}

impl ReminderRecord {
    pub fn new(
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
        source_memory_id: impl Into<String>,
        kind: ReminderKind,
        title: impl Into<String>,
        due_at: impl Into<String>,
        timezone: impl Into<String>,
    ) -> CoreResult<Self> {
        let tenant_id = tenant_id.into();
        let user_id = user_id.into();
        let source_memory_id = source_memory_id.into();
        crate::store::validate_scope_component(&tenant_id, "tenant_id")?;
        crate::store::validate_scope_component(&user_id, "user_id")?;
        crate::store::validate_scope_component(&source_memory_id, "source_memory_id")?;
        let title = title.into();
        let due_at = due_at.into();
        let timezone = timezone.into();
        if title.trim().is_empty() {
            return Err(CoreError::InvalidInput("title is required".to_string()));
        }
        let due_at = crate::runtime::lifecycle::canonical_timestamp(&due_at)?;
        if timezone.trim().is_empty() {
            return Err(CoreError::InvalidInput("timezone is required".to_string()));
        }
        if timezone.parse::<chrono_tz::Tz>().is_err() {
            return Err(CoreError::InvalidInput(
                "timezone must be an IANA timezone".into(),
            ));
        }
        let kind_text = format!("{:?}", kind);
        let dedupe_key =
            deterministic_id(&[&tenant_id, &user_id, &source_memory_id, &kind_text, &due_at]);
        let now = now_timestamp();
        Ok(Self {
            id: deterministic_id(&[&dedupe_key, "reminder"]),
            tenant_id,
            user_id,
            source_memory_id,
            kind,
            title,
            details: String::new(),
            due_at: due_at.clone(),
            timezone,
            priority: ReminderPriority::Normal,
            status: ReminderStatus::Scheduled,
            attempt_count: 0,
            last_attempt_at: None,
            next_attempt_at: Some(due_at),
            dedupe_key,
            created_at: now.clone(),
            updated_at: now,
            channel: default_channel(),
            allowed_channels: default_channels(),
            quiet_hours: None,
            expires_at: None,
            privacy_level: default_privacy(),
            terminal_reason: None,
            last_error: None,
            delivery_attempts: Vec::new(),
        })
    }

    pub fn transition(
        &mut self,
        to: ReminderStatus,
        actor: impl Into<String>,
        reason: impl Into<String>,
    ) -> CoreResult<ReminderTransition> {
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
            (ReminderStatus::Draft, ReminderStatus::Scheduled)
                | (ReminderStatus::Scheduled, ReminderStatus::Due)
                | (ReminderStatus::Due, ReminderStatus::Dispatched)
                | (ReminderStatus::Dispatched, ReminderStatus::Completed)
                | (ReminderStatus::Dispatched, ReminderStatus::Failed)
                | (ReminderStatus::Failed, ReminderStatus::RetryScheduled)
                | (ReminderStatus::RetryScheduled, ReminderStatus::Dispatched)
                | (ReminderStatus::Scheduled, ReminderStatus::Cancelled)
                | (ReminderStatus::RetryScheduled, ReminderStatus::Due)
                | (ReminderStatus::RetryScheduled, ReminderStatus::Cancelled)
                | (ReminderStatus::Due, ReminderStatus::Cancelled)
                | (ReminderStatus::Dispatched, ReminderStatus::Cancelled)
                | (ReminderStatus::Failed, ReminderStatus::Cancelled)
                | (ReminderStatus::Failed, ReminderStatus::Expired)
                | (ReminderStatus::Scheduled, ReminderStatus::Expired)
                | (ReminderStatus::RetryScheduled, ReminderStatus::Expired)
                | (ReminderStatus::Dispatched, ReminderStatus::Expired)
                | (ReminderStatus::Due, ReminderStatus::Expired)
        );
        if !allowed {
            return Err(CoreError::InvalidInput(format!(
                "invalid reminder transition from {:?} to {:?}",
                from, to
            )));
        }
        let changed_at = now_timestamp();
        self.status = to.clone();
        self.updated_at = changed_at.clone();
        if to == ReminderStatus::Dispatched {
            self.attempt_count = self.attempt_count.saturating_add(1);
            self.last_attempt_at = Some(changed_at.clone());
        }
        Ok(ReminderTransition {
            reminder_id: self.id.clone(),
            from,
            to,
            actor,
            reason,
            changed_at,
        })
    }

    pub fn is_due_visible(&self) -> bool {
        matches!(
            self.status,
            ReminderStatus::Scheduled | ReminderStatus::Due | ReminderStatus::RetryScheduled
        )
    }
}
