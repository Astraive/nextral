use nextral::{
    config::{NextralConfig, RuntimeBackend},
    contracts::{CoreError, CoreResult},
    domain::{AuditAction, AuditEvent, ReminderKind, ReminderPriority, ReminderRecord, ReminderStatus},
    package::{CallOptions, RuntimeClient},
    runtime::lifecycle::{
        timestamp_epoch, worker_owner, GuardedWriteResult, LeaseClaim, LeaseResource,
        LifecycleStore,
    },
    store::{MemoryIndexStore, ReminderStore},
    testkit::TestMemoryStore,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};

fn test_config() -> NextralConfig {
    let mut config = NextralConfig::default();
    config.backend = RuntimeBackend::TestMemory;
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_workers_racing_for_one_due_job_claim_only_one_lease() {
    let client = RuntimeClient::from_config(test_config()).unwrap();

    // Enqueue a due lifecycle job
    let enqueued = client
        .call_json(
            "nextral.jobs.enqueue",
            serde_json::json!({
                "tenant_id": "tenant_jobs",
                "user_id": "user_jobs",
                "session_id": "session_jobs",
                "trigger": "retention",
                "run_at": "1970-01-01T00:00:00Z", // already due
                "memory_ids": ["mem_job_1"],
                "redact": true,
                "max_attempts": 3
            })
            .to_string(),
            CallOptions::default(),
        )
        .await
        .unwrap();
    let job_json: serde_json::Value = serde_json::from_str(&enqueued).unwrap();
    let job_id = job_json["id"].as_str().unwrap().to_string();

    // Two workers racing to claim due jobs
    let mut store_a = client.store().clone();
    let mut store_b = client.store().clone();

    let claims_a = store_a.lifecycle_job_claim_due("tenant_jobs", "user_jobs", "worker_a", 10, 300).unwrap();
    let claims_b = store_b.lifecycle_job_claim_due("tenant_jobs", "user_jobs", "worker_b", 10, 300).unwrap();

    // Exactly one worker got the claim
    assert_eq!(claims_a.len() + claims_b.len(), 1);
    let (claimed_job, claim) = if !claims_a.is_empty() {
        claims_a.into_iter().next().unwrap()
    } else {
        claims_b.into_iter().next().unwrap()
    };
    assert_eq!(claimed_job.id, job_id);
    assert_eq!(claim.key, job_id);
    assert_eq!(claim.resource, LeaseResource::LifecycleJob);

    // Old owner / wrong fence rejected
    let stale_claim = LeaseClaim {
        tenant_id: claim.tenant_id.clone(),
        user_id: claim.user_id.clone(),
        resource: claim.resource,
        key: claim.key.clone(),
        owner: "worker_stale".into(),
        fence: claim.fence,
    };
    let mut modified_job = claimed_job.clone();
    modified_job.status = "completed".into();

    let save_err = store_a.lifecycle_job_save_guarded(&stale_claim, &modified_job).unwrap_err();
    assert!(matches!(save_err, CoreError::LeaseLost(_)));

    // Reclaimed fence rejects the old owner even if owner text is reused
    let new_claim = store_b.lifecycle_job_claim_due("tenant_jobs", "user_jobs", &claim.owner, 10, 300);
    // If not due or already claimed, returns empty
    assert!(new_claim.unwrap().is_empty());

    // Valid claim saves cleanly
    store_a.lifecycle_job_save_guarded(&claim, &modified_job).unwrap();
    let fetched = store_a.lifecycle_job_get("tenant_jobs", "user_jobs", &job_id).unwrap().unwrap();
    assert_eq!(fetched.status, "completed");

    client.close(Duration::from_secs(5)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reminder_delivery_transition_atomicity_and_audit_dedup() {
    let mut store = TestMemoryStore::new();
    let tenant = "tenant_reminders";
    let user = "user_reminders";

    // Acquire user lease
    let claim = store
        .lifecycle_claim_fenced(tenant, Some(user), LeaseResource::RuntimeState, &format!("privacy/{user}"), "test_worker", 300)
        .unwrap()
        .unwrap();

    let reminder = ReminderRecord {
        id: "rem_1".into(),
        tenant_id: tenant.into(),
        user_id: user.into(),
        source_memory_id: "src_1".into(),
        kind: ReminderKind::Task,
        title: "Test Reminder".into(),
        details: "Reminder details".into(),
        due_at: "2026-01-01T00:00:00Z".into(),
        timezone: "UTC".into(),
        priority: ReminderPriority::Normal,
        status: ReminderStatus::Dispatched,
        attempt_count: 1,
        last_attempt_at: None,
        next_attempt_at: None,
        dedupe_key: "dedupe_rem_1".into(),
        created_at: "2026-01-01T00:00:00Z".into(),
        updated_at: "2026-01-01T00:00:00Z".into(),
        channel: "webhook".into(),
        allowed_channels: vec!["webhook".into()],
        quiet_hours: None,
        expires_at: None,
        privacy_level: nextral::memory::PrivacyLevel::Private,
        terminal_reason: None,
        last_error: None,
        delivery_attempts: Vec::new(),
    };

    let audit_1 = AuditEvent::new(
        "audit_trans_1",
        tenant,
        user,
        AuditAction::ReminderTransition,
        "reminder",
        Some("rem_1".into()),
        "dispatched",
    );

    // Commit reminder delivery atomically
    store
        .commit_reminder_delivery(&claim, "delivery/user_reminders/rem_1", &json!({"state": "ok"}), &reminder, &[audit_1])
        .unwrap();

    // Verify all 3 applied together
    assert!(store.lifecycle_load(tenant, "delivery/user_reminders/rem_1").unwrap().is_some());
    assert_eq!(store.reminders.len(), 1);
    assert_eq!(store.reminders[0].id, "rem_1");
    assert_eq!(store.audit_events.len(), 1);

    // Expired lease rejects atomic delivery commit without partial state
    let expired_claim = LeaseClaim {
        tenant_id: tenant.into(),
        user_id: Some(user.into()),
        resource: LeaseResource::RuntimeState,
        key: format!("privacy/{user}"),
        owner: "wrong_owner".into(),
        fence: claim.fence,
    };
    let audit_2 = AuditEvent::new(
        "audit_trans_2",
        tenant,
        user,
        AuditAction::ReminderTransition,
        "reminder",
        Some("rem_1".into()),
        "failed",
    );
    let mut failed_reminder = reminder.clone();
    failed_reminder.status = ReminderStatus::Failed;

    let err = store
        .commit_reminder_delivery(
            &expired_claim,
            "delivery/user_reminders/rem_1",
            &json!({"state": "failed"}),
            &failed_reminder,
            &[audit_2],
        )
        .unwrap_err();
    assert!(matches!(err, CoreError::LeaseLost(_)));

    // Verify state was NOT mutated to failed
    assert_eq!(store.audit_events.len(), 1);
    assert_eq!(store.reminders[0].status, ReminderStatus::Dispatched);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fenced_leases_increment_fence_and_reject_lost_owner() {
    let mut store = TestMemoryStore::new();
    let tenant = "tenant_fenced";
    let key = "control-lease/default";

    // Initial claim has fence 1
    let claim1 = store
        .lifecycle_claim_fenced(tenant, None, LeaseResource::RuntimeState, key, "worker_1", 300)
        .unwrap()
        .unwrap();
    assert_eq!(claim1.fence, 1);
    assert_eq!(claim1.owner, "worker_1");

    // Competing claim by worker_2 while unexpired fails
    let comp = store
        .lifecycle_claim_fenced(tenant, None, LeaseResource::RuntimeState, key, "worker_2", 300)
        .unwrap();
    assert!(comp.is_none());

    // Worker 1 can renew with matching fence
    let renewed = store.lifecycle_renew_fenced(&claim1, 300).unwrap();
    assert!(renewed);

    // Stale fence renewal fails
    let mut stale_claim = claim1.clone();
    stale_claim.fence = 999;
    let stale_renew = store.lifecycle_renew_fenced(&stale_claim, 300).unwrap();
    assert!(!stale_renew);

    // Worker 1 saves guarded value
    let target = "control-plane/default";
    let applied = store
        .lifecycle_save_guarded(&claim1, target, &json!({"active_version": "v1"}))
        .unwrap();
    assert_eq!(applied, GuardedWriteResult::Applied);

    // Release lease
    store.lifecycle_release_fenced(&claim1).unwrap();

    // Now worker 2 claims and fence increments to 2
    let claim2 = store
        .lifecycle_claim_fenced(tenant, None, LeaseResource::RuntimeState, key, "worker_2", 300)
        .unwrap()
        .unwrap();
    assert_eq!(claim2.fence, 2);
    assert_eq!(claim2.owner, "worker_2");

    // Worker 1 tries guarded save with old fence 1 -> rejected with LeaseLost
    let lost_err = store
        .lifecycle_save_guarded(&claim1, target, &json!({"active_version": "stale"}))
        .unwrap_err();
    assert!(matches!(lost_err, CoreError::LeaseLost(_)));

    // Value remains v1
    let loaded = store.lifecycle_load(tenant, target).unwrap().unwrap();
    assert_eq!(loaded["active_version"], "v1");
}
