use crate::{
    contracts::{CoreError, CoreResult},
    memory::{deterministic_id, now_timestamp},
};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReminderRecord {
    pub id: String,
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
        user_id: impl Into<String>,
        source_memory_id: impl Into<String>,
        kind: ReminderKind,
        title: impl Into<String>,
        due_at: impl Into<String>,
        timezone: impl Into<String>,
    ) -> CoreResult<Self> {
        let user_id = user_id.into();
        let source_memory_id = source_memory_id.into();
        let title = title.into();
        let due_at = due_at.into();
        if user_id.trim().is_empty()
            || source_memory_id.trim().is_empty()
            || title.trim().is_empty()
        {
            return Err(CoreError::InvalidInput(
                "user_id, source_memory_id, and title are required".to_string(),
            ));
        }
        let kind_text = format!("{:?}", kind);
        let dedupe_key = deterministic_id(&[&user_id, &source_memory_id, &kind_text, &due_at]);
        let now = now_timestamp();
        Ok(Self {
            id: deterministic_id(&[&dedupe_key, "reminder"]),
            user_id,
            source_memory_id,
            kind,
            title,
            details: String::new(),
            due_at: due_at.clone(),
            timezone: timezone.into(),
            priority: ReminderPriority::Normal,
            status: ReminderStatus::Scheduled,
            attempt_count: 0,
            last_attempt_at: None,
            next_attempt_at: Some(due_at),
            dedupe_key,
            created_at: now.clone(),
            updated_at: now,
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
