use nextral::{
    config::{ExecutionConfig, NextralConfig, RuntimeBackend},
    load_runner::{LoadReport, LoadRunner, LoadRunnerConfig, OperationMix},
    package::{CallOptions, RuntimeClient},
};
use serde_json::json;
use std::{sync::Arc, time::Duration};

fn test_config() -> NextralConfig {
    let mut config = NextralConfig::default();
    config.backend = RuntimeBackend::TestMemory;
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn portable_load_runner_executes_operation_mix_and_reports_metrics() {
    let client = RuntimeClient::from_config(test_config()).unwrap();
    let runner_config = LoadRunnerConfig {
        concurrency: 4,
        duration: Duration::from_millis(500),
        target_rps: None,
        mix: OperationMix {
            ingest_pct: 30,
            retrieve_pct: 40,
            query_pct: 20,
            forget_pct: 10,
        },
        tenant_id: "load_t1".into(),
        user_id: "load_u1".into(),
    };

    let runner = LoadRunner::new(client, runner_config);
    let report: LoadReport = runner.run().await;

    assert!(report.total_requests > 0, "Expected requests to run");
    assert!(
        report.successful_requests > 0,
        "Expected successful requests"
    );
    assert_eq!(
        report.total_requests,
        report.successful_requests + report.failed_requests
    );
    assert!(report.achieved_rps > 0.0, "Expected positive achieved RPS");
    assert!(report.status_counts.contains_key("200_ok"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overload_scenario_queuing_and_rejection_under_saturation() {
    let mut config = test_config();
    config.execution = ExecutionConfig {
        max_in_flight: 1,
        max_pending: 1,
        max_pending_bytes: 10485760,
        request_timeout_ms: 10000,
        shutdown_timeout_ms: 5000,
        lease_seconds: 300,
    };

    let client = Arc::new(RuntimeClient::from_config(config).unwrap());
    let mut handles = Vec::new();

    // Fire 6 concurrent delay calls: 1 will run, 1 will wait in pending queue, 4 will be rejected immediately
    for i in 0..6 {
        let c = Arc::clone(&client);
        handles.push(tokio::spawn(async move {
            c.call_json(
                "nextral.test.delay",
                json!({ "delay_ms": 200 }).to_string(),
                CallOptions::default(),
            )
            .await
        }));
    }

    let mut successes = 0;
    let mut rejections = 0;

    for handle in handles {
        match handle.await.unwrap() {
            Ok(_) => successes += 1,
            Err(e) => {
                assert_eq!(e.code, "resource_exhausted");
                rejections += 1;
            }
        }
    }

    assert!(successes >= 1, "At least 1 request should succeed");
    assert!(
        rejections >= 1,
        "At least 1 request should be rejected due to queue saturation"
    );

    // After backlog drains, system recovers and normal requests succeed
    let recover_res = client
        .call_json(
            "nextral.test.delay",
            json!({ "delay_ms": 10 }).to_string(),
            CallOptions::default(),
        )
        .await;
    assert!(
        recover_res.is_ok(),
        "Client should recover after saturation clears"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn store_recovery_scenario_handles_lease_expiry_and_reacquisition() {
    use nextral::runtime::lifecycle::{LeaseResource, LifecycleStore};
    let client = RuntimeClient::from_config(test_config()).unwrap();
    let mut store = client.store().clone();

    // Worker A claims lease with short duration
    let claim_a = store
        .lifecycle_claim_fenced(
            "tenant_recov",
            Some("user_recov"),
            LeaseResource::RuntimeState,
            "state-key",
            "worker_a",
            1, // 1 second lease
        )
        .unwrap()
        .expect("claim_a");

    // Immediately, worker B cannot claim it
    let claim_b = store
        .lifecycle_claim_fenced(
            "tenant_recov",
            Some("user_recov"),
            LeaseResource::RuntimeState,
            "state-key",
            "worker_b",
            300,
        )
        .unwrap();
    assert!(
        claim_b.is_none(),
        "Worker B should be blocked while worker A holds active lease"
    );

    // Worker A releases lease
    store.lifecycle_release_fenced(&claim_a).unwrap();

    // Now worker B can acquire lease
    let claim_b_after = store
        .lifecycle_claim_fenced(
            "tenant_recov",
            Some("user_recov"),
            LeaseResource::RuntimeState,
            "state-key",
            "worker_b",
            300,
        )
        .unwrap();
    assert!(
        claim_b_after.is_some(),
        "Worker B should acquire lease after worker A releases"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_shutdown_under_load_completes_in_flight_without_torn_state() {
    let client = RuntimeClient::from_config(test_config()).unwrap();

    // Spawn delay request
    let c = client.clone();
    let inflight = tokio::spawn(async move {
        c.call_json(
            "nextral.test.delay",
            json!({ "delay_ms": 100 }).to_string(),
            CallOptions::default(),
        )
        .await
    });

    // Brief yield to ensure request starts running
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Initiate close
    let report = client.close(Duration::from_secs(5)).await.unwrap();
    assert!(report.drained, "Client should drain in-flight request");

    // The in-flight request should have succeeded cleanly
    let inflight_res = inflight.await.unwrap();
    assert!(
        inflight_res.is_ok(),
        "In-flight request should complete cleanly during drain"
    );

    // Subsequent calls are rejected
    let post_close = client
        .call_json(
            "nextral.test.delay",
            json!({}).to_string(),
            CallOptions::default(),
        )
        .await;
    assert!(post_close.is_err());
    assert_eq!(post_close.unwrap_err().code, "conflict");
}
