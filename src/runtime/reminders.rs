use crate::{
    contracts::CoreResult,
    domain::StoreReceipt,
    prospective::{ReminderKind, ReminderRecord, ReminderStatus, ReminderTransition},
    store::ReminderStore,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduleReminderRequest {
    pub user_id: String,
    pub source_memory_id: String,
    pub kind: ReminderKind,
    pub title: String,
    pub due_at: String,
    pub timezone: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduleReminderResponse {
    pub reminder: ReminderRecord,
    pub receipts: Vec<StoreReceipt>,
}

pub fn schedule_reminder(
    store: &mut impl ReminderStore,
    request: ScheduleReminderRequest,
) -> CoreResult<ScheduleReminderResponse> {
    let reminder = ReminderRecord::new(
        request.user_id,
        request.source_memory_id,
        request.kind,
        request.title,
        request.due_at,
        request.timezone,
    )?;
    store.upsert_reminder(reminder.clone())?;
    Ok(ScheduleReminderResponse {
        reminder: reminder.clone(),
        receipts: vec![StoreReceipt::ok(
            "postgres",
            "upsert_reminder",
            Some(reminder.id),
        )],
    })
}

pub fn transition_reminder(
    store: &mut impl ReminderStore,
    mut reminder: ReminderRecord,
    to: ReminderStatus,
    actor: &str,
    reason: &str,
) -> CoreResult<ReminderTransition> {
    let transition = reminder.transition(to, actor, reason)?;
    store.upsert_reminder(reminder)?;
    Ok(transition)
}
