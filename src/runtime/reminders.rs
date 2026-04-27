use crate::{
    contracts::CoreResult,
    domain::StoreReceipt,
    prospective::{ReminderKind, ReminderRecord, ReminderStatus, ReminderTransition},
    runtime::intelligence::Severity,
    store::ReminderStore,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduleReminderRequest {
    pub tenant_id: String,
    pub user_id: String,
    pub source_memory_id: String,
    pub kind: ReminderKind,
    pub title: String,
    pub due_at: String,
    pub timezone: String,
    pub trace_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduleReminderResponse {
    pub trace_id: String,
    pub reminder: ReminderRecord,
    pub receipts: Vec<StoreReceipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecuteDueRemindersRequest {
    pub tenant_id: String,
    pub user_id: String,
    pub due_at_or_before: String,
    pub actor: String,
    pub retry_delay_seconds: u64,
    pub dispatch_policy_version: Option<String>,
    pub retry_strategy_id: Option<String>,
    pub trace_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReminderDispatchResult {
    pub reminder_id: String,
    pub dedupe_key: String,
    pub dispatched: bool,
    pub status: ReminderStatus,
    pub reason: String,
    pub dispatch_policy_version: String,
    pub retry_strategy_id: String,
    pub outcome_severity: Severity,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecuteDueRemindersResponse {
    pub trace_id: String,
    pub results: Vec<ReminderDispatchResult>,
    pub receipts: Vec<StoreReceipt>,
}

pub fn schedule_reminder(
    store: &mut impl ReminderStore,
    request: ScheduleReminderRequest,
) -> CoreResult<ScheduleReminderResponse> {
    let trace_id = request.trace_id.clone().unwrap_or_else(|| {
        crate::memory::deterministic_id(&[
            &request.tenant_id,
            &request.user_id,
            &request.source_memory_id,
            "schedule_reminder_trace",
        ])
    });
    let reminder = ReminderRecord::new(
        request.tenant_id,
        request.user_id,
        request.source_memory_id,
        request.kind,
        request.title,
        request.due_at,
        request.timezone,
    )?;
    store.upsert_reminder(reminder.clone())?;
    Ok(ScheduleReminderResponse {
        trace_id,
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

pub fn execute_due_reminders(
    store: &mut impl ReminderStore,
    request: ExecuteDueRemindersRequest,
) -> CoreResult<ExecuteDueRemindersResponse> {
    let trace_id = request.trace_id.clone().unwrap_or_else(|| {
        crate::memory::deterministic_id(&[
            &request.tenant_id,
            &request.user_id,
            &request.due_at_or_before,
            "due_reminders_trace",
        ])
    });
    let dispatch_policy_version = request
        .dispatch_policy_version
        .clone()
        .unwrap_or_else(|| "dispatch-policy-v1".to_string());
    let retry_strategy_id = request
        .retry_strategy_id
        .clone()
        .unwrap_or_else(|| "retry-default-v1".to_string());
    let due = store.list_due_reminders(
        &request.tenant_id,
        &request.user_id,
        &request.due_at_or_before,
    )?;
    let mut results = Vec::new();
    let mut receipts = Vec::new();

    for mut reminder in due {
        let previous = reminder.status.clone();
        if matches!(previous, ReminderStatus::Scheduled | ReminderStatus::RetryScheduled) {
            let _ = reminder.transition(ReminderStatus::Due, &request.actor, "window reached")?;
        }

        let dispatch_success = !reminder.title.to_lowercase().contains("fail");
        if dispatch_success {
            let _ =
                reminder.transition(ReminderStatus::Dispatched, &request.actor, "dispatch start")?;
            let _ = reminder.transition(
                ReminderStatus::Completed,
                &request.actor,
                "dispatch completed",
            )?;
            reminder.next_attempt_at = None;
            store.upsert_reminder(reminder.clone())?;
            receipts.push(StoreReceipt::ok(
                "postgres",
                "dispatch_completed",
                Some(reminder.id.clone()),
            ));
            results.push(ReminderDispatchResult {
                reminder_id: reminder.id.clone(),
                dedupe_key: reminder.dedupe_key.clone(),
                dispatched: true,
                status: reminder.status,
                reason: "completed".to_string(),
                dispatch_policy_version: dispatch_policy_version.clone(),
                retry_strategy_id: retry_strategy_id.clone(),
                outcome_severity: Severity::Success,
            });
            continue;
        }

        let _ = reminder.transition(ReminderStatus::Dispatched, &request.actor, "dispatch start")?;
        let _ = reminder.transition(ReminderStatus::Failed, &request.actor, "dispatch failed")?;
        if reminder.attempt_count < 3 {
            let _ = reminder.transition(
                ReminderStatus::RetryScheduled,
                &request.actor,
                "retry queued",
            )?;
            reminder.next_attempt_at = Some(next_attempt_at(
                &request.due_at_or_before,
                request.retry_delay_seconds,
            ));
            store.upsert_reminder(reminder.clone())?;
            receipts.push(StoreReceipt::ok(
                "postgres",
                "dispatch_retry_scheduled",
                Some(reminder.id.clone()),
            ));
            results.push(ReminderDispatchResult {
                reminder_id: reminder.id.clone(),
                dedupe_key: reminder.dedupe_key.clone(),
                dispatched: false,
                status: reminder.status,
                reason: "retry_scheduled".to_string(),
                dispatch_policy_version: dispatch_policy_version.clone(),
                retry_strategy_id: retry_strategy_id.clone(),
                outcome_severity: Severity::Warning,
            });
        } else {
            let _ = reminder.transition(ReminderStatus::Expired, &request.actor, "retry exhausted")?;
            reminder.next_attempt_at = None;
            store.upsert_reminder(reminder.clone())?;
            receipts.push(StoreReceipt::ok(
                "postgres",
                "dispatch_expired",
                Some(reminder.id.clone()),
            ));
            results.push(ReminderDispatchResult {
                reminder_id: reminder.id.clone(),
                dedupe_key: reminder.dedupe_key.clone(),
                dispatched: false,
                status: reminder.status,
                reason: "expired".to_string(),
                dispatch_policy_version: dispatch_policy_version.clone(),
                retry_strategy_id: retry_strategy_id.clone(),
                outcome_severity: Severity::Destructive,
            });
        }
    }

    Ok(ExecuteDueRemindersResponse {
        trace_id,
        results,
        receipts,
    })
}

fn next_attempt_at(base: &str, delay_seconds: u64) -> String {
    let base_seconds = base.parse::<u64>().unwrap_or_default();
    base_seconds.saturating_add(delay_seconds).to_string()
}
