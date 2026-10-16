use crate::{
    contracts::{CoreError, CoreResult},
    domain::{
        AuditAction, AuditEvent, QuietHours, ReminderKind, ReminderPriority, ReminderRecord,
        ReminderStatus, ReminderTerminalReason, ReminderTransition, StoreReceipt,
    },
    memory::{deterministic_id, now_timestamp, MemoryStatus, PrivacyLevel},
    runtime::{
        intelligence::Severity,
        lifecycle::{acknowledged, durable_save, timestamp_epoch, worker_owner, LifecycleStore},
    },
    store::{AuditSink, MemoryIndexStore, ReminderStore},
};
use chrono::Timelike;
use serde::{Deserialize, Serialize};

fn default_privacy_scope() -> Vec<PrivacyLevel> {
    vec![PrivacyLevel::Private]
}
fn default_lease() -> u64 {
    300
}

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
    #[serde(default = "crate::domain::default_channel")]
    pub channel: String,
    #[serde(default = "crate::domain::default_channels")]
    pub allowed_channels: Vec<String>,
    #[serde(default)]
    pub quiet_hours: Option<QuietHours>,
    #[serde(default)]
    pub expires_at: Option<String>,
    #[serde(default)]
    pub priority: Option<ReminderPriority>,
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
    pub max_retries: Option<u32>,
    pub dispatch_policy_version: Option<String>,
    pub retry_strategy_id: Option<String>,
    pub trace_id: Option<String>,
    #[serde(default = "default_privacy_scope")]
    pub privacy_scope: Vec<PrivacyLevel>,
    #[serde(default = "default_lease")]
    pub lease_seconds: u64,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeliveryState {
    reminder: ReminderRecord,
    transitions: Vec<ReminderTransition>,
    receipt: Option<StoreReceipt>,
}

pub fn schedule_reminder<T>(
    store: &mut T,
    request: ScheduleReminderRequest,
) -> CoreResult<ScheduleReminderResponse>
where
    T: ReminderStore + MemoryIndexStore + LifecycleStore + AuditSink,
{
    let tenant = request.tenant_id.clone();
    let user = request.user_id.clone();
    crate::runtime::lifecycle::with_user_lease(store, &tenant, &user, |store| {
        schedule_claimed(store, request)
    })
}

fn schedule_claimed<T>(
    store: &mut T,
    request: ScheduleReminderRequest,
) -> CoreResult<ScheduleReminderResponse>
where
    T: ReminderStore + MemoryIndexStore + LifecycleStore + AuditSink,
{
    crate::store::validate_scope_component(&request.source_memory_id, "source_memory_id")?;
    let source = store
        .get_memory(
            &request.tenant_id,
            &request.user_id,
            &request.source_memory_id,
        )?
        .ok_or_else(|| CoreError::NotFound("reminder source not found".into()))?;
    if source.status != MemoryStatus::Active {
        return Err(CoreError::InvalidInput(
            "reminder source must be active".into(),
        ));
    }
    if let Some(hours) = &request.quiet_hours {
        if hours.start_hour > 23 || hours.end_hour > 23 || hours.start_hour == hours.end_hour {
            return Err(CoreError::InvalidInput(
                "quiet hours must be distinct hours in 0..=23".into(),
            ));
        }
    }
    if request.channel.trim().is_empty() || !request.allowed_channels.contains(&request.channel) {
        return Err(CoreError::InvalidInput(
            "reminder channel is not allowed".into(),
        ));
    }
    let trace_id = request.trace_id.unwrap_or_else(|| {
        deterministic_id(&[
            &request.tenant_id,
            &request.user_id,
            &request.source_memory_id,
            "schedule_reminder_trace",
        ])
    });
    let mut reminder = ReminderRecord::new(
        request.tenant_id,
        request.user_id,
        request.source_memory_id,
        request.kind,
        request.title,
        request.due_at,
        request.timezone,
    )?;
    reminder.channel = request.channel;
    reminder.allowed_channels = request.allowed_channels;
    reminder.quiet_hours = request.quiet_hours;
    reminder.expires_at = request
        .expires_at
        .map(|value| crate::runtime::lifecycle::canonical_timestamp(&value))
        .transpose()?;
    reminder.privacy_level = source.privacy_level;
    if let Some(priority) = request.priority {
        reminder.priority = priority;
    }
    if let Some(expiry) = &reminder.expires_at {
        if crate::runtime::lifecycle::timestamp_nanos(expiry)?
            <= crate::runtime::lifecycle::timestamp_nanos(&reminder.due_at)?
        {
            return Err(CoreError::InvalidInput(
                "expiry must be later than due time".into(),
            ));
        }
    }
    let key = delivery_key(&reminder);
    if let Some(value) = store.lifecycle_load(&reminder.tenant_id, &key)? {
        let existing: DeliveryState = serde_json::from_value(value)?;
        // Scheduling the same dedupe key must never reset a completed or retrying reminder.
        return Ok(ScheduleReminderResponse {
            trace_id,
            reminder: existing.reminder,
            receipts: Vec::new(),
        });
    }
    let state = DeliveryState {
        reminder: reminder.clone(),
        transitions: Vec::new(),
        receipt: None,
    };
    let receipt = durable_save(
        store,
        &reminder.tenant_id,
        &key,
        &serde_json::to_value(&state)?,
    )?;
    store.upsert_reminder(reminder.clone())?;
    store.emit_audit(AuditEvent::new(
        deterministic_id(&[&reminder.id, "scheduled"]),
        &reminder.tenant_id,
        &reminder.user_id,
        AuditAction::ReminderScheduled,
        "reminder",
        Some(reminder.id.clone()),
        "reminder scheduled",
    ))?;
    Ok(ScheduleReminderResponse {
        trace_id,
        reminder,
        receipts: vec![receipt],
    })
}

pub fn transition_reminder<T>(
    store: &mut T,
    reminder: ReminderRecord,
    to: ReminderStatus,
    actor: &str,
    reason: &str,
) -> CoreResult<ReminderTransition>
where
    T: ReminderStore + LifecycleStore + AuditSink,
{
    let tenant = reminder.tenant_id.clone();
    let user = reminder.user_id.clone();
    crate::store::validate_scope_component(&reminder.id, "reminder_id")?;
    crate::runtime::lifecycle::with_user_lease(store, &tenant, &user, |store| {
        transition_claimed(store, reminder, to, actor, reason)
    })
}

fn transition_claimed<T>(
    store: &mut T,
    reminder: ReminderRecord,
    to: ReminderStatus,
    actor: &str,
    reason: &str,
) -> CoreResult<ReminderTransition>
where
    T: ReminderStore + LifecycleStore + AuditSink,
{
    let mut state =
        if let Some(value) = store.lifecycle_load(&reminder.tenant_id, &delivery_key(&reminder))? {
            serde_json::from_value(value)?
        } else {
            DeliveryState {
                reminder,
                transitions: Vec::new(),
                receipt: None,
            }
        };
    let transition = state.reminder.transition(to, actor, reason)?;
    state.transitions.push(transition.clone());
    persist_delivery(store, &state)?;
    Ok(transition)
}

pub fn execute_due_reminders<T>(
    store: &mut T,
    request: ExecuteDueRemindersRequest,
) -> CoreResult<ExecuteDueRemindersResponse>
where
    T: ReminderStore + MemoryIndexStore + LifecycleStore + AuditSink,
{
    crate::runtime::lifecycle::validate_identity(&request.tenant_id, &request.user_id, None)?;
    let now_nanos = crate::runtime::lifecycle::timestamp_nanos(&request.due_at_or_before)?;
    let now = (now_nanos / 1_000_000_000) as u64;
    if request.actor.trim().is_empty()
        || request.lease_seconds == 0
        || request.retry_delay_seconds == 0
    {
        return Err(CoreError::InvalidInput(
            "actor, positive lease and retry delay are required".into(),
        ));
    }
    let owner = worker_owner();
    let lock = format!("privacy/{}", request.user_id);
    if !store.lifecycle_claim(
        &request.tenant_id,
        &lock,
        &owner,
        timestamp_epoch(&now_timestamp())?,
        request.lease_seconds,
    )? {
        return Err(CoreError::InvalidInput(
            "user lifecycle is leased by another worker".into(),
        ));
    }
    let result = execute_claimed(store, &request, now, now_nanos, &owner, &lock);
    let release = store.lifecycle_release(&request.tenant_id, &lock, &owner);
    match result {
        Ok(value) => {
            release?;
            Ok(value)
        }
        Err(error) => {
            let _ = release;
            Err(error)
        }
    }
}

fn execute_claimed<T>(
    store: &mut T,
    request: &ExecuteDueRemindersRequest,
    now: u64,
    now_nanos: u128,
    owner: &str,
    lock: &str,
) -> CoreResult<ExecuteDueRemindersResponse>
where
    T: ReminderStore + MemoryIndexStore + LifecycleStore + AuditSink,
{
    let mut response = ExecuteDueRemindersResponse {
        trace_id: request.trace_id.clone().unwrap_or_else(|| {
            deterministic_id(&[
                &request.tenant_id,
                &request.user_id,
                &request.due_at_or_before,
                "due_reminders_trace",
            ])
        }),
        results: Vec::new(),
        receipts: Vec::new(),
    };
    let due = store.list_due_reminders(
        &request.tenant_id,
        &request.user_id,
        &request.due_at_or_before,
    )?;
    for reminder in due {
        if reminder.tenant_id != request.tenant_id || reminder.user_id != request.user_id {
            return Err(CoreError::InvalidInput(
                "reminder store returned an out-of-scope reminder".into(),
            ));
        }
        if !store.lifecycle_claim(
            &request.tenant_id,
            lock,
            owner,
            timestamp_epoch(&now_timestamp())?,
            request.lease_seconds,
        )? {
            return Err(CoreError::InvalidInput(
                "worker lost lifecycle lease".into(),
            ));
        }
        let mut state: DeliveryState = if let Some(value) =
            store.lifecycle_load(&request.tenant_id, &delivery_key(&reminder))?
        {
            serde_json::from_value(value)?
        } else {
            DeliveryState {
                reminder,
                transitions: Vec::new(),
                receipt: None,
            }
        };
        if matches!(
            state.reminder.status,
            ReminderStatus::Completed | ReminderStatus::Cancelled | ReminderStatus::Expired
        ) {
            store.upsert_reminder(state.reminder)?;
            continue;
        }
        let effective = state
            .reminder
            .next_attempt_at
            .as_deref()
            .unwrap_or(&state.reminder.due_at);
        if crate::runtime::lifecycle::timestamp_nanos(effective)? > now_nanos
            || crate::runtime::lifecycle::timestamp_nanos(&state.reminder.due_at)? > now_nanos
        {
            continue;
        }
        if state.receipt.as_ref().is_some_and(acknowledged) {
            if state.reminder.status == ReminderStatus::Dispatched {
                apply_transition(
                    &mut state,
                    ReminderStatus::Completed,
                    &request.actor,
                    "delivered",
                )?;
            }
            state.reminder.terminal_reason = Some(ReminderTerminalReason::Delivered);
            state.reminder.next_attempt_at = None;
            persist_delivery(store, &state)?;
            response
                .results
                .push(dispatch_result(&state, request, true, "delivered"));
            continue;
        }
        let source = store.get_memory(
            &request.tenant_id,
            &request.user_id,
            &state.reminder.source_memory_id,
        )?;
        let source = match source {
            Some(source) if source.status == MemoryStatus::Active => source,
            _ => {
                cancel(
                    &mut state,
                    &request.actor,
                    ReminderTerminalReason::SourceUnavailable,
                    "source_unavailable",
                )?;
                persist_delivery(store, &state)?;
                response.results.push(dispatch_result(
                    &state,
                    request,
                    false,
                    "source_unavailable",
                ));
                continue;
            }
        };
        let mut forgotten_source = false;
        for source_id in &source.source_message_ids {
            forgotten_source |= store
                .lifecycle_load(
                    &request.tenant_id,
                    &format!("forgotten-source/{}/{}", request.user_id, source_id),
                )?
                .is_some();
        }
        if forgotten_source {
            cancel(
                &mut state,
                &request.actor,
                ReminderTerminalReason::SourceUnavailable,
                "source_unavailable",
            )?;
            persist_delivery(store, &state)?;
            response.results.push(dispatch_result(
                &state,
                request,
                false,
                "source_unavailable",
            ));
            continue;
        }
        if !request.privacy_scope.contains(&source.privacy_level)
            || !request
                .privacy_scope
                .contains(&state.reminder.privacy_level)
        {
            cancel(
                &mut state,
                &request.actor,
                ReminderTerminalReason::PrivacyDenied,
                "privacy_denied",
            )?;
            persist_delivery(store, &state)?;
            response
                .results
                .push(dispatch_result(&state, request, false, "privacy_denied"));
            continue;
        }
        state.reminder.privacy_level = source.privacy_level;
        if !state
            .reminder
            .allowed_channels
            .contains(&state.reminder.channel)
        {
            cancel(
                &mut state,
                &request.actor,
                ReminderTerminalReason::ChannelDenied,
                "channel_denied",
            )?;
            persist_delivery(store, &state)?;
            response
                .results
                .push(dispatch_result(&state, request, false, "channel_denied"));
            continue;
        }
        if state
            .reminder
            .expires_at
            .as_deref()
            .map(crate::runtime::lifecycle::timestamp_nanos)
            .transpose()?
            .is_some_and(|expiry| expiry <= now_nanos)
        {
            cancel(
                &mut state,
                &request.actor,
                ReminderTerminalReason::Expired,
                "expired",
            )?;
            persist_delivery(store, &state)?;
            response
                .results
                .push(dispatch_result(&state, request, false, "expired"));
            continue;
        }
        if in_quiet_hours(&state.reminder, now)? {
            // Scan actual timezone instants to handle DST jumps and overnight quiet windows.
            let mut next = now;
            for _ in 0..(26 * 60) {
                next = next.saturating_add(60);
                if !in_quiet_hours(&state.reminder, next)? {
                    break;
                }
            }
            state.reminder.next_attempt_at =
                Some(crate::runtime::lifecycle::timestamp_add_seconds(
                    &request.due_at_or_before,
                    next.saturating_sub(now),
                )?);
            persist_delivery(store, &state)?;
            response
                .results
                .push(dispatch_result(&state, request, false, "quiet_hours"));
            continue;
        }
        if matches!(
            state.reminder.status,
            ReminderStatus::Scheduled | ReminderStatus::RetryScheduled
        ) {
            apply_transition(
                &mut state,
                ReminderStatus::Due,
                &request.actor,
                "window_reached",
            )?;
        }
        if state.reminder.status == ReminderStatus::Failed {
            apply_transition(
                &mut state,
                ReminderStatus::RetryScheduled,
                &request.actor,
                "retry_recovered",
            )?;
        }
        if state.reminder.status != ReminderStatus::Dispatched {
            apply_transition(
                &mut state,
                ReminderStatus::Dispatched,
                &request.actor,
                "dispatch_started",
            )?;
        }
        persist_delivery(store, &state)?;
        let outcome = store.lifecycle_dispatch(&state.reminder);
        crate::runtime::lifecycle::renew_lease(
            store,
            &request.tenant_id,
            lock,
            owner,
            request.lease_seconds,
        )?;
        match outcome {
            Ok(receipt) if acknowledged(&receipt) => {
                record_delivery_attempt(&mut state.reminder, receipt.clone(), "delivered", None);
                response.receipts.push(receipt.clone());
                state.receipt = Some(receipt);
                // Persist acknowledgement before completion; replay will not call the host again.
                persist_delivery(store, &state)?;
                apply_transition(
                    &mut state,
                    ReminderStatus::Completed,
                    &request.actor,
                    "delivered",
                )?;
            }
            outcome => {
                let (receipt, code, http_status) =
                    dispatch_failure_receipt(&state.reminder, outcome);
                record_delivery_attempt(&mut state.reminder, receipt.clone(), &code, http_status);
                response.receipts.push(receipt);
                apply_transition(&mut state, ReminderStatus::Failed, &request.actor, &code)?;
                if state.reminder.attempt_count <= request.max_retries.unwrap_or(3) {
                    apply_transition(
                        &mut state,
                        ReminderStatus::RetryScheduled,
                        &request.actor,
                        "retry_scheduled",
                    )?;
                    state.reminder.next_attempt_at =
                        Some(crate::runtime::lifecycle::timestamp_add_seconds(
                            &request.due_at_or_before,
                            request.retry_delay_seconds,
                        )?);
                    persist_delivery(store, &state)?;
                    response.results.push(dispatch_result(
                        &state,
                        request,
                        false,
                        "retry_scheduled",
                    ));
                    continue;
                }
                apply_transition(
                    &mut state,
                    ReminderStatus::Expired,
                    &request.actor,
                    "retry_exhausted",
                )?;
                state.reminder.terminal_reason = Some(ReminderTerminalReason::RetryExhausted);
                state.reminder.next_attempt_at = None;
                persist_delivery(store, &state)?;
                response
                    .results
                    .push(dispatch_result(&state, request, false, "retry_exhausted"));
                continue;
            }
        }
        state.reminder.terminal_reason = Some(ReminderTerminalReason::Delivered);
        state.reminder.next_attempt_at = None;
        persist_delivery(store, &state)?;
        response
            .results
            .push(dispatch_result(&state, request, true, "delivered"));
    }
    Ok(response)
}

fn delivery_key(reminder: &ReminderRecord) -> String {
    format!("reminder/{}/{}", reminder.user_id, reminder.id)
}
fn apply_transition(
    state: &mut DeliveryState,
    to: ReminderStatus,
    actor: &str,
    reason: &str,
) -> CoreResult<()> {
    state
        .transitions
        .push(state.reminder.transition(to, actor, reason)?);
    Ok(())
}
fn cancel(
    state: &mut DeliveryState,
    actor: &str,
    reason: ReminderTerminalReason,
    code: &str,
) -> CoreResult<()> {
    let to = if reason == ReminderTerminalReason::Expired {
        ReminderStatus::Expired
    } else {
        ReminderStatus::Cancelled
    };
    apply_transition(state, to, actor, code)?;
    state.reminder.terminal_reason = Some(reason);
    state.reminder.next_attempt_at = None;
    Ok(())
}

fn record_delivery_attempt(
    reminder: &mut ReminderRecord,
    receipt: StoreReceipt,
    code: &str,
    http_status: Option<u16>,
) {
    let attempt = crate::domain::ReminderDeliveryAttempt {
        attempt: reminder.attempt_count,
        attempted_at: reminder
            .last_attempt_at
            .clone()
            .unwrap_or_else(now_timestamp),
        outcome_code: code.into(),
        http_status,
        receipt,
    };
    reminder.last_error = if code == "delivered" {
        None
    } else {
        Some(code.into())
    };
    if let Some(previous) = reminder
        .delivery_attempts
        .iter_mut()
        .find(|previous| previous.attempt == attempt.attempt)
    {
        *previous = attempt;
    } else {
        reminder.delivery_attempts.push(attempt);
    }
}

fn dispatch_failure_receipt(
    reminder: &ReminderRecord,
    outcome: CoreResult<StoreReceipt>,
) -> (StoreReceipt, String, Option<u16>) {
    let (backend, code, http_status) = match outcome {
        Ok(receipt) => {
            let backend = match receipt.backend.as_str() {
                "host_webhook" => "host_webhook",
                "test_host" => "test_host",
                _ => "host_dispatch",
            };
            let http = receipt
                .status
                .strip_prefix("http_")
                .and_then(|status| status.parse::<u16>().ok())
                .filter(|status| (100..=599).contains(status));
            let code = if let Some(status) = http {
                format!("http_{status}")
            } else {
                match receipt.status.as_str() {
                    "configuration_missing" => "configuration_missing",
                    "configuration_invalid" => "configuration_invalid",
                    "transport_failed" => "transport_failed",
                    "protocol_failed" => "protocol_failed",
                    _ => "host_rejected",
                }
                .into()
            };
            (backend, code, http)
        }
        Err(error) => {
            let code = match error {
                CoreError::InvalidInput(_) => "configuration_invalid",
                CoreError::NotFound(_) => "dispatch_not_found",
                CoreError::Conflict(_) => "dispatch_conflict",
                CoreError::ResourceExhausted(_) => "resource_exhausted",
                CoreError::DeadlineExceeded(_) => "deadline_exceeded",
                CoreError::Cancelled(_) => "cancelled",
                CoreError::LeaseLost(_) => "lease_lost",
                CoreError::IndexNotReady(_) => "index_not_ready",
                CoreError::Io(_) => "transport_failed",
                CoreError::Serialization(_) => "protocol_failed",
            };
            ("host_dispatch", code.into(), None)
        }
    };
    let receipt = StoreReceipt {
        backend: backend.into(),
        operation: "dispatch".into(),
        target_id: Some(reminder.id.clone()),
        status: code.clone(),
    };
    (receipt, code, http_status)
}
fn persist_delivery<T>(store: &mut T, state: &DeliveryState) -> CoreResult<()>
where
    T: LifecycleStore + ReminderStore + AuditSink,
{
    durable_save(
        store,
        &state.reminder.tenant_id,
        &delivery_key(&state.reminder),
        &serde_json::to_value(state)?,
    )?;
    store.upsert_reminder(state.reminder.clone())?;
    for (index, transition) in state.transitions.iter().enumerate() {
        store.emit_audit(AuditEvent::new(
            deterministic_id(&[&state.reminder.id, &index.to_string(), "transition"]),
            &state.reminder.tenant_id,
            &transition.actor,
            AuditAction::ReminderTransition,
            "reminder",
            Some(state.reminder.id.clone()),
            &transition.reason,
        ))?;
    }
    Ok(())
}
fn dispatch_result(
    state: &DeliveryState,
    request: &ExecuteDueRemindersRequest,
    dispatched: bool,
    reason: &str,
) -> ReminderDispatchResult {
    ReminderDispatchResult {
        reminder_id: state.reminder.id.clone(),
        dedupe_key: state.reminder.dedupe_key.clone(),
        dispatched,
        status: state.reminder.status.clone(),
        reason: reason.into(),
        dispatch_policy_version: request
            .dispatch_policy_version
            .clone()
            .unwrap_or_else(|| "dispatch-policy-v1".into()),
        retry_strategy_id: request
            .retry_strategy_id
            .clone()
            .unwrap_or_else(|| "retry-default-v1".into()),
        outcome_severity: if dispatched {
            Severity::Success
        } else if state.reminder.status == ReminderStatus::RetryScheduled || reason == "quiet_hours"
        {
            Severity::Warning
        } else {
            Severity::Destructive
        },
    }
}
fn in_quiet_hours(reminder: &ReminderRecord, now: u64) -> CoreResult<bool> {
    let Some(hours) = &reminder.quiet_hours else {
        return Ok(false);
    };
    let timezone: chrono_tz::Tz = reminder
        .timezone
        .parse()
        .map_err(|_| CoreError::InvalidInput("invalid reminder timezone".into()))?;
    let epoch = i64::try_from(now)
        .map_err(|_| CoreError::InvalidInput("timestamp exceeds supported range".into()))?;
    let instant = chrono::DateTime::from_timestamp(epoch, 0)
        .ok_or_else(|| CoreError::InvalidInput("timestamp exceeds supported range".into()))?;
    let hour = instant.with_timezone(&timezone).hour() as u8;
    Ok(if hours.start_hour < hours.end_hour {
        hour >= hours.start_hour && hour < hours.end_hour
    } else {
        hour >= hours.start_hour || hour < hours.end_hour
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn overnight_quiet_hours_use_named_timezone() {
        let mut reminder = ReminderRecord::new(
            "t",
            "u",
            "m",
            ReminderKind::Task,
            "a title containing fail",
            "2026-01-01T00:00:00Z",
            "America/New_York",
        )
        .unwrap();
        reminder.quiet_hours = Some(QuietHours {
            start_hour: 22,
            end_hour: 7,
        });
        assert!(
            in_quiet_hours(&reminder, timestamp_epoch("2026-01-01T04:00:00Z").unwrap()).unwrap()
        );
    }
    #[test]
    fn equivalent_due_inputs_produce_same_dedupe_key() {
        let a = ReminderRecord::new(
            "t",
            "u",
            "m",
            ReminderKind::Task,
            "x",
            "2026-01-01T00:00:00Z",
            "UTC",
        )
        .unwrap();
        let b = ReminderRecord::new("t", "u", "m", ReminderKind::Task, "x", "1767225600", "UTC")
            .unwrap();
        assert_eq!(a.dedupe_key, b.dedupe_key);
    }

    fn delivery_store() -> crate::store::TestMemoryStore {
        let mut store = crate::store::TestMemoryStore::new();
        let record = crate::memory::MemoryRecord::new(
            "m",
            "t",
            "u",
            "source",
            crate::memory::ContentType::Task,
            crate::memory::MemoryType::Prospective,
            crate::memory::SourceType::Manual,
        );
        store.upsert_memory(record).unwrap();
        store
    }
    fn due_request(cutoff: &str) -> ExecuteDueRemindersRequest {
        serde_json::from_value(serde_json::json!({"tenant_id":"t","user_id":"u","due_at_or_before":cutoff,"actor":"worker","retry_delay_seconds":60})).unwrap()
    }
    fn schedule_request(title: &str) -> ScheduleReminderRequest {
        serde_json::from_value(serde_json::json!({"tenant_id":"t","user_id":"u","source_memory_id":"m","kind":"task","title":title,"due_at":"2","timezone":"UTC"})).unwrap()
    }

    #[test]
    fn title_does_not_control_real_host_delivery_outcome() {
        let mut store = delivery_store();
        schedule_reminder(&mut store, schedule_request("This title says fail")).unwrap();
        let response = execute_due_reminders(&mut store, due_request("3")).unwrap();
        assert_eq!(response.results[0].status, ReminderStatus::Completed);
        assert!(response.results[0].dispatched);
        assert!(execute_due_reminders(&mut store, due_request("3"))
            .unwrap()
            .results
            .is_empty());
    }

    #[test]
    fn retry_schedule_is_not_dispatched_at_original_due_time() {
        let mut store = delivery_store();
        let mut reminder =
            ReminderRecord::new("t", "u", "m", ReminderKind::Task, "retry", "2", "UTC").unwrap();
        for status in [
            ReminderStatus::Due,
            ReminderStatus::Dispatched,
            ReminderStatus::Failed,
            ReminderStatus::RetryScheduled,
        ] {
            reminder
                .transition(status, "worker", "retry lifecycle")
                .unwrap();
        }
        reminder.next_attempt_at = Some("100".into());
        store.upsert_reminder(reminder).unwrap();
        assert!(execute_due_reminders(&mut store, due_request("50"))
            .unwrap()
            .results
            .is_empty());
        assert!(
            execute_due_reminders(&mut store, due_request("100"))
                .unwrap()
                .results[0]
                .dispatched
        );
    }

    #[test]
    fn source_privacy_denies_delivery_without_explicit_scope() {
        let mut store = delivery_store();
        let mut record = store.get_memory("t", "u", "m").unwrap().unwrap();
        record.privacy_level = PrivacyLevel::Sensitive;
        store.update_memory(record).unwrap();
        schedule_reminder(&mut store, schedule_request("private task")).unwrap();
        let response = execute_due_reminders(&mut store, due_request("3")).unwrap();
        assert_eq!(response.results[0].reason, "privacy_denied");
        assert!(!response.results[0].dispatched);
    }

    #[test]
    fn fractional_due_time_never_dispatches_earlier_in_the_same_second() {
        let mut store = delivery_store();
        let mut request = schedule_request("fractional");
        request.due_at = "2026-01-01T00:00:00.900Z".into();
        schedule_reminder(&mut store, request).unwrap();
        assert!(
            execute_due_reminders(&mut store, due_request("2026-01-01T00:00:00.100Z"))
                .unwrap()
                .results
                .is_empty()
        );
        assert!(
            execute_due_reminders(&mut store, due_request("2026-01-01T00:00:00.900Z"))
                .unwrap()
                .results[0]
                .dispatched
        );
    }
}
