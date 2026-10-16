use nextral::{
    config::{NextralConfig, RuntimeBackend},
    package::{CallOptions, RuntimeClient},
    runtime::{
        evaluation::{
            promotion_replay_gate, CaseObservation, EvaluationReport, LatencySummary,
            OutcomeObservation, PromotionEvidence, ReminderOutcome, ReportMetadata, RunMetadata,
        },
        intelligence::RuntimeLane,
    },
};
use serde_json::json;

fn test_config() -> NextralConfig {
    let mut config = NextralConfig::default();
    config.backend = RuntimeBackend::TestMemory;
    config
}

fn make_report(
    run_id: &str,
    policy_version: &str,
    matched: u64,
    stage: Option<&str>,
    config_digest: Option<&str>,
) -> EvaluationReport {
    let obs = vec![
        CaseObservation {
            case_id: "c1".into(),
            elapsed_ns: 100,
            destructive_events: 0,
            outcome: OutcomeObservation::Retrieval {
                expected: 4,
                matched,
                returned: 4,
                forbidden: 0,
            },
        },
        CaseObservation {
            case_id: "c2".into(),
            elapsed_ns: 200,
            destructive_events: 0,
            outcome: OutcomeObservation::Contradiction {
                expected: true,
                detected: true,
            },
        },
        CaseObservation {
            case_id: "c3".into(),
            elapsed_ns: 300,
            destructive_events: 0,
            outcome: OutcomeObservation::Reminder {
                expected: ReminderOutcome::Completed,
                observed: ReminderOutcome::Completed,
            },
        },
    ];

    let metadata = ReportMetadata {
        schema_version: 1,
        suite_id: "suite-1".into(),
        suite_revision: "rev-1".into(),
        suite_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
        run: RunMetadata {
            run_id: run_id.into(),
            implementation_revision: "rev-1".into(),
            policy_version: policy_version.into(),
            host_id: "host-1".into(),
            latency_scope: "client".into(),
        },
        started_at_unix_ms: 1700000000000,
        build_profile: "release".into(),
        target_os: "windows".into(),
        target_arch: "x86_64".into(),
        configuration_digest: config_digest.map(Into::into),
        stage: stage.map(Into::into),
    };

    nextral::runtime::evaluation::summarize(metadata, obs, 10_000).unwrap()
}

#[tokio::test]
async fn versioned_retrieval_execution_policy_applies_dynamically_without_restart() {
    let client = RuntimeClient::from_config(test_config()).unwrap();
    let tenant = "tenant_policy";
    let user = "user_policy";

    // Ingest a test memory
    let ingest_res = client
        .call_json(
            "nextral.memory.ingest",
            json!({
                "tenant_id": tenant,
                "user_id": user,
                "content": "policy dynamic configuration fact",
                "content_type": "fact",
                "memory_type": "semantic",
                "privacy_level": "private",
                "importance_score": 0.8,
                "confidence_score": 0.8
            })
            .to_string(),
            CallOptions::default(),
        )
        .await
        .unwrap();
    assert!(ingest_res.contains("record_id"));

    // Baseline retrieval uses default policy-v1 and stable lane
    let ret1 = client
        .call_json(
            "nextral.memory.retrieve",
            json!({
                "tenant_id": tenant,
                "user_id": user,
                "query_text": "policy dynamic",
                "privacy_scope": ["private"]
            })
            .to_string(),
            CallOptions::default(),
        )
        .await
        .unwrap();
    let ret1_val: serde_json::Value = serde_json::from_str(&ret1).unwrap();
    assert_eq!(ret1_val["policy_version"], "policy-v1");
    assert_eq!(ret1_val["lane"], "stable");

    // Create a new candidate experiment with custom scoring weights and canary lane
    let exp = client
        .call_json(
            "experiments.create",
            json!({
                "lane": "canary",
                "policy_version": "policy-v2-canary",
                "description": "dynamic weighting experiment",
                "scoring_weights": {
                    "semantic_similarity": 0.1,
                    "recency": 0.1,
                    "importance": 0.7,
                    "access": 0.1
                },
                "token_budget": 800
            })
            .to_string(),
            CallOptions::default(),
        )
        .await
        .unwrap();
    let exp_val: serde_json::Value = serde_json::from_str(&exp).unwrap();
    let exp_id = exp_val["id"].as_str().unwrap();

    // Promote the experiment with qualification evidence
    let baseline_rep = make_report(
        "run_base",
        "policy-v1",
        3,
        Some("qualification"),
        Some("digest123"),
    );
    let cand_rep = make_report(
        "run_cand",
        "policy-v2-canary",
        4,
        Some("qualification"),
        Some("digest123"),
    );
    let promote_res = client
        .call_json(
            "experiments.promote",
            json!({
                "experiment_id": exp_id,
                "evidence": {
                    "baseline": baseline_rep,
                    "candidate": cand_rep
                }
            })
            .to_string(),
            CallOptions::default(),
        )
        .await
        .unwrap();
    let promote_val: serde_json::Value = serde_json::from_str(&promote_res).unwrap();
    assert_eq!(promote_val["status"], "promoted");

    // Next retrieval on the SAME client immediately reflects promoted policy and lane
    let ret2 = client
        .call_json(
            "nextral.memory.retrieve",
            json!({
                "tenant_id": tenant,
                "user_id": user,
                "query_text": "policy dynamic",
                "privacy_scope": ["private"]
            })
            .to_string(),
            CallOptions::default(),
        )
        .await
        .unwrap();
    let ret2_val: serde_json::Value = serde_json::from_str(&ret2).unwrap();
    assert_eq!(ret2_val["policy_version"], "policy-v2-canary");
    assert_eq!(ret2_val["lane"], "canary");

    // Rollback experiment
    let rb_res = client
        .call_json(
            "experiments.rollback",
            json!({
                "experiment_id": exp_id,
                "reason": "canary evaluation complete"
            })
            .to_string(),
            CallOptions::default(),
        )
        .await
        .unwrap();
    let rb_val: serde_json::Value = serde_json::from_str(&rb_res).unwrap();
    assert_eq!(rb_val["status"], "rolled_back");

    // After rollback, retrieval immediately restores previous baseline
    let ret3 = client
        .call_json(
            "nextral.memory.retrieve",
            json!({
                "tenant_id": tenant,
                "user_id": user,
                "query_text": "policy dynamic",
                "privacy_scope": ["private"]
            })
            .to_string(),
            CallOptions::default(),
        )
        .await
        .unwrap();
    let ret3_val: serde_json::Value = serde_json::from_str(&ret3).unwrap();
    assert_eq!(ret3_val["policy_version"], "policy-v1");
    assert_eq!(ret3_val["lane"], "stable");
}

#[tokio::test]
async fn research_evidence_binds_to_measured_config_and_rejects_calibration_stage() {
    // Calibration stage evidence must be rejected by promotion gate
    let cal_baseline = make_report(
        "run1",
        "policy-v1",
        3,
        Some("calibration"),
        Some("digest123"),
    );
    let cal_cand = make_report(
        "run2",
        "policy-v2",
        4,
        Some("calibration"),
        Some("digest123"),
    );
    let cal_ev = PromotionEvidence {
        baseline: cal_baseline,
        candidate: cal_cand,
    };
    assert!(
        promotion_replay_gate(&cal_ev).is_err(),
        "calibration evidence must be rejected"
    );

    // Mismatched configuration digests must be rejected
    let diff_baseline = make_report(
        "run1",
        "policy-v1",
        3,
        Some("qualification"),
        Some("digestA"),
    );
    let diff_cand = make_report(
        "run2",
        "policy-v2",
        4,
        Some("qualification"),
        Some("digestB"),
    );
    let diff_ev = PromotionEvidence {
        baseline: diff_baseline,
        candidate: diff_cand,
    };
    assert!(
        promotion_replay_gate(&diff_ev).is_err(),
        "mismatched configuration digest must be rejected"
    );

    // Matching qualification evidence passes
    let qual_baseline = make_report(
        "run1",
        "policy-v1",
        3,
        Some("qualification"),
        Some("digestA"),
    );
    let qual_cand = make_report(
        "run2",
        "policy-v2",
        4,
        Some("qualification"),
        Some("digestA"),
    );
    let qual_ev = PromotionEvidence {
        baseline: qual_baseline,
        candidate: qual_cand,
    };
    assert!(
        promotion_replay_gate(&qual_ev).is_ok(),
        "qualification evidence with matching config must pass"
    );
}

#[tokio::test]
async fn fenced_policy_rollout_rejects_conflicting_lease() {
    use nextral::runtime::lifecycle::{LeaseResource, LifecycleStore};
    let client = RuntimeClient::from_config(test_config()).unwrap();

    // Acquire control lease fence externally
    let mut store = client.store().clone();
    let claim = store
        .lifecycle_claim_fenced(
            "__nextral_control",
            None,
            LeaseResource::RuntimeState,
            "control-lease",
            "competing_worker",
            300,
        )
        .unwrap();
    assert!(claim.is_some(), "Expected initial lease claim to succeed");

    // Attempting to modify experiment via client must fail with Conflict (409) because lease is held by competing worker
    let res = client
        .call_json(
            "experiments.create",
            json!({
                "lane": "canary",
                "policy_version": "policy-v2-canary",
                "description": "conflict test"
            })
            .to_string(),
            CallOptions::default(),
        )
        .await;

    assert!(
        res.is_err(),
        "Expected client operation to fail due to active lease fence"
    );
    let err = res.unwrap_err();
    assert_eq!(err.code, "conflict");

    // Release lease
    store.lifecycle_release_fenced(&claim.unwrap()).unwrap();

    // Now operation succeeds
    let res_after = client
        .call_json(
            "experiments.create",
            json!({
                "lane": "canary",
                "policy_version": "policy-v2-canary",
                "description": "after release test"
            })
            .to_string(),
            CallOptions::default(),
        )
        .await;
    assert!(
        res_after.is_ok(),
        "Operation must succeed after lease is released"
    );
}
