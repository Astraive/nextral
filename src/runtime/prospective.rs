use crate::{
    contracts::{CoreError, CoreResult},
    runtime::{
        consolidation::{consolidate_session, ConsolidationLane, ConsolidationRequest},
        governance::{retain_memory, ForgetMemoryRequest},
        lifecycle::{durable_save, timestamp_epoch, worker_owner, LifecycleStore},
        observability::{record_job, JobOutcome},
        reminders::{
            execute_due_reminders, ExecuteDueRemindersRequest, ExecuteDueRemindersResponse,
        },
    },
    store::{AuditSink, GraphStore, MemoryIndexStore, ReminderStore, SessionStore},
};
use crate::domain::ReminderStatus;
use crate::runtime::storage::RuntimeStore;
use serde::{Deserialize, Serialize};

fn default_limit() -> usize {
    100
}
fn default_lease() -> u64 {
    300
}
fn default_attempts() -> u32 {
    5
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleTrigger {
    Idle,
    Close,
    Deep,
    Retention,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LifecycleJobRequest {
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: String,
    pub trigger: LifecycleTrigger,
    pub run_at: String,
    #[serde(default)]
    pub idle_seconds: Option<u64>,
    #[serde(default)]
    pub consolidation: Option<ConsolidationRequest>,
    #[serde(default)]
    pub memory_ids: Vec<String>,
    #[serde(default)]
    pub redact: bool,
    #[serde(default = "default_attempts")]
    pub max_attempts: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunLifecycleJobsRequest {
    pub tenant_id: String,
    pub user_id: String,
    pub now: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default = "default_lease")]
    pub lease_seconds: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LifecycleScheduledJob {
    pub id: String,
    pub request: LifecycleJobRequest,
    pub status: String,
    pub attempts: u32,
    pub next_attempt_at: String,
    pub created_at: String,
    pub updated_at: String,
    pub completed_memory_ids: Vec<String>,
    pub last_error: Option<String>,
}

pub fn enqueue_lifecycle_job<T>(
    store: &mut T,
    mut request: LifecycleJobRequest,
) -> CoreResult<LifecycleScheduledJob>
where
    T: LifecycleStore,
{
    crate::runtime::lifecycle::validate_identity(
        &request.tenant_id,
        &request.user_id,
        Some(&request.session_id),
    )?;
    for id in &request.memory_ids {
        crate::store::validate_scope_component(id, "memory_id")?;
    }
    if request.max_attempts == 0 {
        return Err(CoreError::InvalidInput(
            "max_attempts must be positive".into(),
        ));
    }
    request.run_at = crate::runtime::lifecycle::canonical_timestamp(&request.run_at)?;
    if request.trigger == LifecycleTrigger::Idle && request.idle_seconds.unwrap_or(0) == 0 {
        return Err(CoreError::InvalidInput(
            "idle jobs require positive idle_seconds".into(),
        ));
    }
    if request.trigger == LifecycleTrigger::Retention {
        if request.memory_ids.is_empty() || request.consolidation.is_some() {
            return Err(CoreError::InvalidInput(
                "retention requires explicit memory_ids and no consolidation output".into(),
            ));
        }
    } else {
        let consolidation = request.consolidation.as_ref().ok_or_else(|| {
            CoreError::InvalidInput("consolidation jobs require explicit host outputs".into())
        })?;
        let expected = if request.trigger == LifecycleTrigger::Deep {
            ConsolidationLane::Deep
        } else {
            ConsolidationLane::Fast
        };
        if consolidation.tenant_id != request.tenant_id
            || consolidation.user_id != request.user_id
            || consolidation.session_id != request.session_id
            || consolidation.lane != expected
        {
            return Err(CoreError::InvalidInput(
                "queued host consolidation must match identity and trigger lane".into(),
            ));
        }
    }
    let id = crate::memory::deterministic_id(&[&serde_json::to_string(&request)?]);
    if let Some(existing) = store.lifecycle_job_get(&request.tenant_id, &request.user_id, &id)? {
        return Ok(existing);
    }
    let now = crate::memory::now_timestamp();
    if request.trigger == LifecycleTrigger::Close {
        durable_save(
            store,
            &request.tenant_id,
            &format!(
                "session-activity/{}/{}",
                request.user_id, request.session_id
            ),
            &serde_json::json!({"last_activity_at":now,"state":"closed"}),
        )?;
    }
    let job = LifecycleScheduledJob {
        id,
        next_attempt_at: request.run_at.clone(),
        request,
        status: "queued".into(),
        attempts: 0,
        created_at: now.clone(),
        updated_at: now,
        completed_memory_ids: Vec::new(),
        last_error: None,
    };
    store.lifecycle_job_enqueue(&job)?;
    Ok(job)
}

pub fn execute_lifecycle_jobs<T>(
    store: &mut T,
    request: RunLifecycleJobsRequest,
) -> CoreResult<Vec<LifecycleScheduledJob>>
where
    T: LifecycleStore + SessionStore + MemoryIndexStore + AuditSink + GraphStore,
{
    crate::runtime::lifecycle::validate_identity(&request.tenant_id, &request.user_id, None)?;
    let now_nanos = crate::runtime::lifecycle::timestamp_nanos(&request.now)?;
    let now = (now_nanos / 1_000_000_000) as u64;
    if request.limit == 0 || request.limit > 1000 || request.lease_seconds == 0 {
        return Err(CoreError::InvalidInput(
            "job limit must be 1..=1000 and lease positive".into(),
        ));
    }
    let owner = worker_owner();
    let claimed_jobs = store.lifecycle_job_claim_due(
        &request.tenant_id,
        &request.user_id,
        &owner,
        request.limit,
        request.lease_seconds,
    )?;

    let mut results = Vec::new();
    for (mut job, claim) in claimed_jobs {
        let attempts_before = job.attempts;
        let mut postponed = false;
        if job.request.trigger == LifecycleTrigger::Idle {
            let activity_key = format!(
                "session-activity/{}/{}",
                request.user_id, job.request.session_id
            );
            if let Some(activity) = store.lifecycle_load(&request.tenant_id, &activity_key)? {
                if let Some(last) = activity["last_activity_at"].as_str() {
                    let idle_due = crate::runtime::lifecycle::timestamp_add_seconds(
                        last,
                        job.request.idle_seconds.unwrap_or(0),
                    )?;
                    if crate::runtime::lifecycle::timestamp_nanos(&idle_due)? > now_nanos {
                        job.next_attempt_at = idle_due;
                        job.status = "queued".into();
                        job.updated_at = crate::memory::now_timestamp();
                        store.lifecycle_job_save_guarded(&claim, &job)?;
                        postponed = true;
                    }
                }
            }
        }
        if postponed {
            results.push(job);
            continue;
        }

        job.attempts = job.attempts.saturating_add(1);
        job.status = "running".into();
        job.updated_at = crate::memory::now_timestamp();
        store.lifecycle_job_save_guarded(&claim, &job)?;

        let work: CoreResult<()> = (|| {
            if job.request.trigger == LifecycleTrigger::Retention {
                for id in job.request.memory_ids.clone() {
                    if job.completed_memory_ids.contains(&id) {
                        continue;
                    }
                    let response = retain_memory(
                        store,
                        ForgetMemoryRequest {
                            tenant_id: request.tenant_id.clone(),
                            user_id: request.user_id.clone(),
                            memory_id: id.clone(),
                            actor: "retention_worker".into(),
                            reason: "scheduled retention".into(),
                            redact: job.request.redact,
                        },
                        |store| {
                            if !store.lifecycle_renew_fenced(&claim, request.lease_seconds)? {
                                return Err(CoreError::LeaseLost("job lease lost during retention".into()));
                            }
                            Ok(())
                        },
                    )?;
                    if response.status != "completed" {
                        return Err(CoreError::InvalidInput(
                            "retention propagation remains incomplete".into(),
                        ));
                    }
                    job.completed_memory_ids.push(id);
                    job.updated_at = crate::memory::now_timestamp();
                    store.lifecycle_job_save_guarded(&claim, &job)?;
                }
            } else {
                let consolidation = job.request.consolidation.clone().ok_or_else(|| {
                    CoreError::InvalidInput("queued host outputs are unavailable".into())
                })?;
                consolidate_session(store, consolidation)?;
            }
            job.status = "completed".into();
            job.last_error = None;
            job.updated_at = crate::memory::now_timestamp();
            store.lifecycle_job_save_guarded(&claim, &job)?;
            Ok(())
        })();

        if let Err(error) = work {
            if job.attempts == attempts_before {
                job.attempts = job.attempts.saturating_add(1);
            }
            job.last_error = Some(error.to_string());
            if job.attempts >= job.request.max_attempts {
                job.status = "terminal_failed".into();
            } else {
                job.status = "retry_pending".into();
                job.next_attempt_at =
                    crate::runtime::lifecycle::timestamp_add_seconds(&request.now, 60)?;
            }
            let _ = store.lifecycle_job_save_guarded(&claim, &job);
        }

        let outcome = match job.status.as_str() {
            "completed" => Some(JobOutcome::Completed),
            "retry_pending" => Some(JobOutcome::Retried),
            "terminal_failed" => Some(JobOutcome::Failed),
            _ => None,
        };
        if let Some(outcome) = outcome {
            record_job(
                outcome,
                now.saturating_sub(timestamp_epoch(&job.created_at)?),
            );
        }
        results.push(job);
    }
    Ok(results)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerScope {
    pub tenant_id: String,
    pub user_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerConfig {
    pub scopes: Vec<WorkerScope>,
    pub actor: String,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_worker_limit")]
    pub job_limit: usize,
    #[serde(default = "default_worker_limit")]
    pub reminder_limit: usize,
    #[serde(default = "default_worker_limit")]
    pub outbox_limit: usize,
    #[serde(default = "default_worker_lease_seconds")]
    pub lease_seconds: u64,
}

fn default_poll_interval_ms() -> u64 {
    1000
}
fn default_worker_limit() -> usize {
    100
}
fn default_worker_lease_seconds() -> u64 {
    30
}

impl WorkerConfig {
    pub fn validate(&self) -> CoreResult<()> {
        if self.scopes.is_empty() || self.scopes.len() > 64 {
            return Err(CoreError::InvalidInput(
                "worker scopes must be between 1 and 64".into(),
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for scope in &self.scopes {
            crate::store::validate_scope_component(&scope.tenant_id, "tenant_id")?;
            crate::store::validate_scope_component(&scope.user_id, "user_id")?;
            if !seen.insert((&scope.tenant_id, &scope.user_id)) {
                return Err(CoreError::InvalidInput("duplicate worker scope".into()));
            }
        }
        if self.actor.trim().is_empty() {
            return Err(CoreError::InvalidInput("worker actor cannot be empty".into()));
        }
        if self.poll_interval_ms < 100 || self.poll_interval_ms > 60000 {
            return Err(CoreError::InvalidInput(
                "poll_interval_ms must be between 100 and 60000".into(),
            ));
        }
        if self.job_limit == 0 || self.job_limit > 1000 {
            return Err(CoreError::InvalidInput(
                "job_limit must be between 1 and 1000".into(),
            ));
        }
        if self.reminder_limit == 0 || self.reminder_limit > 1000 {
            return Err(CoreError::InvalidInput(
                "reminder_limit must be between 1 and 1000".into(),
            ));
        }
        if self.outbox_limit == 0 || self.outbox_limit > 1000 {
            return Err(CoreError::InvalidInput(
                "outbox_limit must be between 1 and 1000".into(),
            ));
        }
        if self.lease_seconds < 15 || self.lease_seconds > 3600 {
            return Err(CoreError::InvalidInput(
                "lease_seconds must be between 15 and 3600".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct KindStats {
    pub processed: usize,
    pub succeeded: usize,
    pub retried: usize,
    pub failed: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerTickReport {
    pub jobs: KindStats,
    pub reminders: KindStats,
    pub outbox: KindStats,
    pub lease_lost: usize,
    pub errors: usize,
}

pub fn run_worker_tick(
    store: &RuntimeStore,
    config: &WorkerConfig,
) -> CoreResult<WorkerTickReport> {
    let mut report = WorkerTickReport::default();
    let mut store = store.clone();

    for scope in &config.scopes {
        // 1. Process Lifecycle Jobs
        let jobs_res = execute_lifecycle_jobs(
            &mut store,
            RunLifecycleJobsRequest {
                tenant_id: scope.tenant_id.clone(),
                user_id: scope.user_id.clone(),
                now: crate::memory::now_timestamp(),
                limit: config.job_limit,
                lease_seconds: config.lease_seconds,
            },
        );
        match jobs_res {
            Ok(jobs) => {
                for job in jobs {
                    report.jobs.processed += 1;
                    match job.status.as_str() {
                        "completed" => report.jobs.succeeded += 1,
                        "retry_pending" => report.jobs.retried += 1,
                        "terminal_failed" => report.jobs.failed += 1,
                        _ => {}
                    }
                }
            }
            Err(CoreError::LeaseLost(_)) => {
                report.lease_lost += 1;
            }
            Err(_) => {
                report.errors += 1;
            }
        }

        // 2. Process Reminders
        let reminders_res = crate::runtime::reminders::execute_due_reminders(
            &mut store,
            crate::runtime::reminders::ExecuteDueRemindersRequest {
                tenant_id: scope.tenant_id.clone(),
                user_id: scope.user_id.clone(),
                due_at_or_before: crate::memory::now_timestamp(),
                actor: config.actor.clone(),
                retry_delay_seconds: 60,
                max_retries: Some(3),
                dispatch_policy_version: None,
                retry_strategy_id: None,
                trace_id: None,
                privacy_scope: vec![crate::memory::PrivacyLevel::Private],
                lease_seconds: config.lease_seconds,
            },
        );
        match reminders_res {
            Ok(rem_resp) => {
                for res in rem_resp.results {
                    report.reminders.processed += 1;
                    match res.status {
                        ReminderStatus::Completed | ReminderStatus::Dispatched => {
                            report.reminders.succeeded += 1
                        }
                        ReminderStatus::RetryScheduled => report.reminders.retried += 1,
                        ReminderStatus::Failed
                        | ReminderStatus::Cancelled
                        | ReminderStatus::Expired => report.reminders.failed += 1,
                        _ => {}
                    }
                }
            }
            Err(CoreError::LeaseLost(_)) => {
                report.lease_lost += 1;
            }
            Err(_) => {
                report.errors += 1;
            }
        }

        // 3. Process Outbox
        let outbox_res = store.drain_outbox(&scope.tenant_id);
        match outbox_res {
            Ok(val) => {
                let comp = val["completed"].as_u64().unwrap_or(0) as usize;
                let fail = val["failed"].as_u64().unwrap_or(0) as usize;
                report.outbox.processed += comp + fail;
                report.outbox.succeeded += comp;
                report.outbox.failed += fail;
            }
            Err(CoreError::LeaseLost(_)) => {
                report.lease_lost += 1;
            }
            Err(_) => {
                report.errors += 1;
            }
        }
    }

    crate::runtime::observability::record_worker_tick(report.errors == 0 && report.lease_lost == 0);
    Ok(report)
}
fn reminder_request(
    tenant: &str,
    user: &str,
    actor: &str,
    retry_delay_seconds: u64,
    now: &str,
) -> ExecuteDueRemindersRequest {
    ExecuteDueRemindersRequest {
        tenant_id: tenant.into(),
        user_id: user.into(),
        due_at_or_before: now.into(),
        actor: actor.into(),
        retry_delay_seconds,
        max_retries: None,
        dispatch_policy_version: None,
        retry_strategy_id: None,
        trace_id: None,
        privacy_scope: vec![crate::memory::PrivacyLevel::Private],
        lease_seconds: 300,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scheduled_jobs_accept_documented_rfc3339_inputs() {
        let request: RunLifecycleJobsRequest = serde_json::from_value(
            serde_json::json!({"tenant_id":"t","user_id":"u","now":"2026-01-01T00:00:00Z"}),
        )
        .unwrap();
        assert_eq!(timestamp_epoch(&request.now).unwrap(), 1767225600);
    }

    fn retention_request() -> LifecycleJobRequest {
        LifecycleJobRequest {
            tenant_id: "t".into(),
            user_id: "u".into(),
            session_id: "s".into(),
            trigger: LifecycleTrigger::Retention,
            run_at: "100".into(),
            idle_seconds: None,
            consolidation: None,
            memory_ids: vec!["m".into()],
            redact: true,
            max_attempts: 2,
        }
    }
    fn runner(now: &str) -> RunLifecycleJobsRequest {
        RunLifecycleJobsRequest {
            tenant_id: "t".into(),
            user_id: "u".into(),
            now: now.into(),
            limit: 100,
            lease_seconds: 300,
        }
    }

    #[test]
    fn retention_waits_for_due_time_then_persists_acknowledged_completion() {
        let mut store = crate::store::TestMemoryStore::new();
        store
            .upsert_memory(crate::memory::MemoryRecord::new(
                "m",
                "t",
                "u",
                "expired data",
                crate::memory::ContentType::Fact,
                crate::memory::MemoryType::Semantic,
                crate::memory::SourceType::Manual,
            ))
            .unwrap();
        enqueue_lifecycle_job(&mut store, retention_request()).unwrap();
        assert!(execute_lifecycle_jobs(&mut store, runner("99"))
            .unwrap()
            .is_empty());
        let jobs = execute_lifecycle_jobs(&mut store, runner("100")).unwrap();
        assert_eq!(jobs[0].status, "completed");
        assert_eq!(
            store.get_memory("t", "u", "m").unwrap().unwrap().status,
            crate::memory::MemoryStatus::Redacted
        );
        assert!(execute_lifecycle_jobs(&mut store, runner("101"))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn failed_retention_retries_then_has_a_durable_terminal_state() {
        let mut store = crate::store::TestMemoryStore::new();
        let job = enqueue_lifecycle_job(&mut store, retention_request()).unwrap();
        assert_eq!(
            execute_lifecycle_jobs(&mut store, runner("100")).unwrap()[0].status,
            "retry_pending"
        );
        assert!(execute_lifecycle_jobs(&mut store, runner("159"))
            .unwrap()
            .is_empty());
        assert_eq!(
            execute_lifecycle_jobs(&mut store, runner("160")).unwrap()[0].status,
            "terminal_failed"
        );
        let value = store
            .lifecycle_load("t", &job_key("u", &job.id))
            .unwrap()
            .unwrap();
        assert_eq!(value["attempts"], 2);
    }

    fn fast_job(trigger: LifecycleTrigger) -> (crate::store::TestMemoryStore, LifecycleJobRequest) {
        let mut store = crate::store::TestMemoryStore::new();
        let message =
            crate::domain::SessionMessage::new("t", "u", "s", "user", "Keep this fact", "m1");
        store.append_session_message(message.clone()).unwrap();
        let policy = crate::config::IngestionPolicy {
            min_importance_score: 0.2,
            min_confidence_score: 0.2,
        };
        let mut candidate = crate::ingestion::IngestMemoryRequest::new(
            "t",
            "u",
            "An explicit host fact",
            crate::memory::ContentType::Fact,
            crate::memory::MemoryType::Semantic,
            crate::memory::SourceType::Manual,
            policy.clone(),
        );
        candidate.source_message_ids = vec![message.id];
        candidate.importance_score = 0.6;
        candidate.confidence_score = Some(0.7);
        let consolidation = ConsolidationRequest {
            tenant_id: "t".into(),
            user_id: "u".into(),
            session_id: "s".into(),
            lane: ConsolidationLane::Fast,
            policy,
            version: "scheduled-v1".into(),
            summary: Some("Host summary".into()),
            candidates: vec![candidate],
            chunk_messages: 100,
        };
        let request = LifecycleJobRequest {
            tenant_id: "t".into(),
            user_id: "u".into(),
            session_id: "s".into(),
            trigger,
            run_at: "10".into(),
            idle_seconds: Some(20),
            consolidation: Some(consolidation),
            memory_ids: Vec::new(),
            redact: false,
            max_attempts: 2,
        };
        (store, request)
    }

    #[test]
    fn planned_archive_manifest_cannot_authorize_retention() {
        let (mut store, host) = fast_job(LifecycleTrigger::Close);
        let fast = consolidate_session(&mut store, host.consolidation.unwrap()).unwrap();
        let memory_id = fast.accepted[0].record_id.as_ref().unwrap();
        let memory = store.get_memory("t", "u", memory_id).unwrap().unwrap();
        store
            .lifecycle_save(
                "t",
                "archive/planned",
                &serde_json::json!({
                    "tenant_id":"t", "user_id":"u", "session_id":"s",
                    "source_message_ids":memory.source_message_ids,
                    "upload_status":"pending",
                }),
            )
            .unwrap();
        let mut request = retention_request();
        request.memory_ids = vec![memory_id.clone()];
        enqueue_lifecycle_job(&mut store, request).unwrap();
        assert_eq!(
            execute_lifecycle_jobs(&mut store, runner("100")).unwrap()[0].status,
            "retry_pending"
        );
        assert_eq!(
            store
                .get_memory("t", "u", memory_id)
                .unwrap()
                .unwrap()
                .status,
            crate::memory::MemoryStatus::Active
        );
        assert_eq!(store.session_tail("t", "u", "s", 100).unwrap().len(), 1);
    }

    #[test]
    fn retention_preserves_sources_until_archiving_then_recovers_and_purges() {
        let (mut store, host) = fast_job(LifecycleTrigger::Close);
        let mut deep = host.consolidation.unwrap();
        let fast = consolidate_session(&mut store, deep.clone()).unwrap();
        let memory_id = fast.accepted[0].record_id.clone().unwrap();
        let before = store.get_memory("t", "u", &memory_id).unwrap().unwrap();
        let mut request = retention_request();
        request.memory_ids = vec![memory_id.clone()];
        enqueue_lifecycle_job(&mut store, request).unwrap();
        let waiting = execute_lifecycle_jobs(&mut store, runner("100")).unwrap();
        assert_eq!(waiting[0].status, "retry_pending");
        assert_eq!(
            store.get_memory("t", "u", &memory_id).unwrap().unwrap(),
            before
        );
        assert_eq!(store.session_tail("t", "u", "s", 100).unwrap().len(), 1);
        assert!(store
            .lifecycle_load("t", &format!("forget/u/{memory_id}"))
            .unwrap()
            .is_none());

        let chunks = crate::runtime::consolidation::transcript_chunks(
            &store.session_tail("t", "u", "s", 100).unwrap(),
            100,
        )
        .unwrap();
        deep.lane = ConsolidationLane::Deep;
        deep.summary = None;
        deep.version = "retention-archive".into();
        deep.candidates[0].content = chunks[0].0.clone();
        deep.candidates[0].source_message_ids = chunks[0].1.clone();
        let archived = consolidate_session(&mut store, deep).unwrap();
        assert!(archived.receipts.iter().any(|receipt| {
            receipt.operation == "archive_transcript"
                && crate::runtime::lifecycle::acknowledged(receipt)
        }));
        assert_eq!(
            execute_lifecycle_jobs(&mut store, runner("160")).unwrap()[0].status,
            "completed"
        );
        assert_eq!(
            store
                .get_memory("t", "u", &memory_id)
                .unwrap()
                .unwrap()
                .status,
            crate::memory::MemoryStatus::Redacted
        );
        assert!(store.session_tail("t", "u", "s", 100).unwrap().is_empty());
        assert!(store.lifecycle_list("t", "archive/").unwrap().is_empty());
    }

    #[test]
    fn retention_rejects_metadata_claims_for_unstored_transcript_messages() {
        let (mut store, host) = fast_job(LifecycleTrigger::Close);
        let fast = consolidate_session(&mut store, host.consolidation.unwrap()).unwrap();
        let memory_id = fast.accepted[0].record_id.clone().unwrap();
        let message = store.session_tail("t", "u", "s", 100).unwrap().remove(0);
        let extra =
            crate::domain::SessionMessage::new("t", "u", "s", "user", "Unarchived source", "m2");
        store.append_session_message(extra.clone()).unwrap();
        let mut related = crate::memory::MemoryRecord::new(
            "related",
            "t",
            "u",
            "Combined sources",
            crate::memory::ContentType::Fact,
            crate::memory::MemoryType::Semantic,
            crate::memory::SourceType::Manual,
        );
        related.session_id = Some("s".into());
        related.source_message_ids = vec![message.id.clone(), extra.id.clone()];
        store.upsert_memory(related).unwrap();
        let before = store.session_tail("t", "u", "s", 100).unwrap();
        store
            .lifecycle_archive(
                "t",
                "claimed-coverage",
                &serde_json::json!({
                    "tenant_id":"t", "user_id":"u", "session_id":"s",
                    "source_message_ids":[message.id, extra.id], "messages":[message],
                }),
            )
            .unwrap();
        let mut request = retention_request();
        request.memory_ids = vec![memory_id.clone()];
        enqueue_lifecycle_job(&mut store, request).unwrap();
        assert_eq!(
            execute_lifecycle_jobs(&mut store, runner("100")).unwrap()[0].status,
            "retry_pending"
        );
        assert_eq!(
            store
                .get_memory("t", "u", &memory_id)
                .unwrap()
                .unwrap()
                .status,
            crate::memory::MemoryStatus::Active
        );
        assert_eq!(store.session_tail("t", "u", "s", 100).unwrap(), before);
    }

    #[test]
    fn retention_rejects_archive_identifiers_without_transcript_payloads() {
        let (mut store, host) = fast_job(LifecycleTrigger::Close);
        let fast = consolidate_session(&mut store, host.consolidation.unwrap()).unwrap();
        let memory_id = fast.accepted[0].record_id.clone().unwrap();
        let before = store.session_tail("t", "u", "s", 100).unwrap();
        store.lifecycle_archive("t", "metadata-only", &serde_json::json!({
            "tenant_id":"t", "user_id":"u", "session_id":"s",
            "messages":[{"id":before[0].id, "tenant_id":"t", "user_id":"u", "session_id":"s"}],
        })).unwrap();
        let mut request = retention_request();
        request.memory_ids = vec![memory_id.clone()];
        enqueue_lifecycle_job(&mut store, request).unwrap();
        assert_eq!(
            execute_lifecycle_jobs(&mut store, runner("100")).unwrap()[0].status,
            "retry_pending"
        );
        assert_eq!(
            store
                .get_memory("t", "u", &memory_id)
                .unwrap()
                .unwrap()
                .status,
            crate::memory::MemoryStatus::Active
        );
        assert_eq!(store.session_tail("t", "u", "s", 100).unwrap(), before);
    }

    #[test]
    fn idle_job_postpones_until_activity_window_has_elapsed() {
        let (mut store, request) = fast_job(LifecycleTrigger::Idle);
        enqueue_lifecycle_job(&mut store, request).unwrap();
        store
            .lifecycle_save(
                "t",
                "session-activity/u/s",
                &serde_json::json!({"last_activity_at":"100","state":"open"}),
            )
            .unwrap();
        let postponed = execute_lifecycle_jobs(&mut store, runner("110")).unwrap();
        assert_eq!(
            (postponed[0].next_attempt_at.as_str(), postponed[0].attempts),
            ("120", 0)
        );
        assert_eq!(
            execute_lifecycle_jobs(&mut store, runner("120")).unwrap()[0].status,
            "completed"
        );
    }

    #[test]
    fn close_job_executes_host_summary_once_after_restartable_queueing() {
        let (mut store, request) = fast_job(LifecycleTrigger::Close);
        enqueue_lifecycle_job(&mut store, request).unwrap();
        assert_eq!(
            execute_lifecycle_jobs(&mut store, runner("10")).unwrap()[0].status,
            "completed"
        );
        assert!(execute_lifecycle_jobs(&mut store, runner("11"))
            .unwrap()
            .is_empty());
        assert_eq!(store.memories.len(), 1);
    }

    #[test]
    fn fractional_job_due_time_is_not_rounded_down() {
        let (mut store, mut request) = fast_job(LifecycleTrigger::Close);
        request.run_at = "2026-01-01T00:00:00.900Z".into();
        enqueue_lifecycle_job(&mut store, request).unwrap();
        assert!(
            execute_lifecycle_jobs(&mut store, runner("2026-01-01T00:00:00.100Z"))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            execute_lifecycle_jobs(&mut store, runner("2026-01-01T00:00:00.900Z")).unwrap()[0]
                .status,
            "completed"
        );
    }
}
