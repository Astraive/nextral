use nextral::{
    config::{ExecutionConfig, NextralConfig, RuntimeBackend},
    contracts::{CoreError, CoreResult},
    package::{CallOptions, RuntimeClient, ShutdownReport},
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

fn test_config() -> NextralConfig {
    let mut config = NextralConfig::default();
    config.backend = RuntimeBackend::TestMemory;
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_clients_execute_concurrently_without_global_lock() {
    let mut config_a = test_config();
    config_a.control_namespace = "namespace_a".into();
    let client_a = RuntimeClient::from_config(config_a).expect("client A");

    let mut config_b = test_config();
    config_b.control_namespace = "namespace_b".into();
    let client_b = RuntimeClient::from_config(config_b).expect("client B");

    // Ingest on client A
    let ingest_a = client_a
        .call_json(
            "nextral.memory.ingest",
            serde_json::json!({
                "tenant_id": "tenant_a",
                "user_id": "user_a",
                "content": "Client A memory",
                "content_type": "fact",
                "memory_type": "semantic",
                "source_type": "manual",
                "importance_score": 1.0,
                "confidence_score": 1.0,
                "privacy_level": "private",
                "entities": [],
                "tags": [],
                "source_message_ids": [],
                "graph_hints": [],
                "policy": { "min_importance_score": 0.0, "min_confidence_score": 0.0 },
                "embedding": [1.0, 0.0]
            })
            .to_string(),
            CallOptions::default(),
        )
        .await
        .expect("ingest A");
    let ingest_a_json: serde_json::Value = serde_json::from_str(&ingest_a).unwrap();
    assert_eq!(ingest_a_json["status"], "accepted");

    // Independent read on client B sees no data from tenant A
    let list_b = client_b
        .call_json(
            "nextral.memory.list",
            serde_json::json!({
                "tenant_id": "tenant_a",
                "user_id": "user_a"
            })
            .to_string(),
            CallOptions::default(),
        )
        .await
        .expect("list B");
    let list_b_json: serde_json::Value = serde_json::from_str(&list_b).unwrap();
    assert_eq!(list_b_json["items"].as_array().unwrap().len(), 0);

    // Closing client A does not stop or affect client B
    let report_a = client_a
        .close(Duration::from_secs(5))
        .await
        .expect("close A");
    assert!(report_a.drained);

    // Client B can still execute
    let health_b = client_b
        .call_json("nextral.health", "{}".into(), CallOptions::default())
        .await
        .expect("health B");
    assert!(health_b.contains("ready"));

    let report_b = client_b
        .close(Duration::from_secs(5))
        .await
        .expect("close B");
    assert!(report_b.drained);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admission_bounds_queue_and_rejection() {
    let mut config = test_config();
    config.execution = ExecutionConfig {
        max_in_flight: 1,
        max_pending: 2,
        max_pending_bytes: 16777216,
        request_timeout_ms: 30000,
        shutdown_timeout_ms: 30000,
        lease_seconds: 30,
    };
    let client = Arc::new(RuntimeClient::from_config(config).expect("client"));

    // We start a long-running call that consumes the 1 in-flight slot
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let barrier_clone = barrier.clone();
    let client_clone1 = client.clone();

    let task1 = tokio::spawn(async move {
        client_clone1
            .call_json(
                "nextral.test.delay",
                serde_json::json!({ "delay_ms": 250 }).to_string(),
                CallOptions::default(),
            )
            .await
    });

    // Let task 1 take the in-flight permit
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Second call is queued (pending count becomes 2 = max_pending)
    let client_clone2 = client.clone();
    let task2 = tokio::spawn(async move {
        client_clone2
            .call_json(
                "nextral.memory.ingest",
                serde_json::json!({
                    "tenant_id": "t",
                    "user_id": "u",
                    "content": "Expired write",
                    "content_type": "fact",
                    "memory_type": "semantic",
                    "source_type": "manual",
                    "importance_score": 1.0,
                    "confidence_score": 1.0,
                    "privacy_level": "private",
                    "entities": [],
                    "tags": [],
                    "source_message_ids": [],
                    "graph_hints": [],
                    "policy": { "min_importance_score": 0.0, "min_confidence_score": 0.0 },
                    "embedding": [1.0, 0.0]
                })
                .to_string(),
                CallOptions {
                    timeout_ms: Some(20), // 20 ms deadline
                    cancellation_token: None,
                },
            )
            .await
    });

    // Allow queueing
    tokio::time::sleep(Duration::from_millis(5)).await;

    // Third call submitted while pending is full (pending_count == max_pending = 2):
    // Must immediately return resource_exhausted with not_started
    let err3 = client
        .call_json("nextral.health", "{}".into(), CallOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err3.code, "resource_exhausted");
    assert_eq!(err3.execution_status.as_deref(), Some("not_started"));

    // Task 2 deadline should elapse in queue without creating a canonical row
    let err2 = task2.await.unwrap().unwrap_err();
    assert_eq!(err2.code, "deadline_exceeded");
    assert_eq!(err2.execution_status.as_deref(), Some("not_started"));

    let _ = task1.await.unwrap();

    // Verify no canonical row was created by task 2
    let list = client
        .call_json(
            "nextral.memory.list",
            serde_json::json!({ "tenant_id": "t", "user_id": "u" }).to_string(),
            CallOptions::default(),
        )
        .await
        .unwrap();
    let list_json: serde_json::Value = serde_json::from_str(&list).unwrap();
    assert_eq!(list_json["items"].as_array().unwrap().len(), 0);

    let report = client.close(Duration::from_secs(5)).await.unwrap();
    assert!(report.drained);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_before_dispatch_does_not_mutate() {
    let mut config = test_config();
    config.execution.max_in_flight = 1;
    let client = Arc::new(RuntimeClient::from_config(config).unwrap());

    // Occupy the only in-flight permit
    let client_c1 = client.clone();
    let task1 = tokio::spawn(async move {
        client_c1
            .call_json(
                "nextral.test.delay",
                serde_json::json!({ "delay_ms": 250 }).to_string(),
                CallOptions::default(),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Submit a queued write with a cancellation token
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    let client_c2 = client.clone();

    let task2 = tokio::spawn(async move {
        client_c2
            .call_json(
                "nextral.memory.ingest",
                serde_json::json!({
                    "tenant_id": "cancel_tenant",
                    "user_id": "cancel_user",
                    "content": "Cancelled write",
                    "content_type": "fact",
                    "memory_type": "semantic",
                    "source_type": "manual",
                    "importance_score": 1.0,
                    "confidence_score": 1.0,
                    "privacy_level": "private",
                    "entities": [],
                    "tags": [],
                    "source_message_ids": [],
                    "graph_hints": [],
                    "policy": { "min_importance_score": 0.0, "min_confidence_score": 0.0 },
                    "embedding": [1.0, 0.0]
                })
                .to_string(),
                CallOptions {
                    timeout_ms: Some(5000),
                    cancellation_token: Some(cancel_clone),
                },
            )
            .await
    });

    // Cancel while queued
    tokio::time::sleep(Duration::from_millis(10)).await;
    cancel.cancel();

    let err2 = task2.await.unwrap().unwrap_err();
    assert_eq!(err2.code, "cancelled");
    assert_eq!(err2.execution_status.as_deref(), Some("not_started"));

    let _ = task1.await.unwrap();

    // Verify row was never created
    let list = client
        .call_json(
            "nextral.memory.list",
            serde_json::json!({ "tenant_id": "cancel_tenant", "user_id": "cancel_user" })
                .to_string(),
            CallOptions::default(),
        )
        .await
        .unwrap();
    let list_json: serde_json::Value = serde_json::from_str(&list).unwrap();
    assert_eq!(list_json["items"].as_array().unwrap().len(), 0);

    let report = client.close(Duration::from_secs(5)).await.unwrap();
    assert!(report.drained);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn byte_budget_saturation_rejects_and_recovers() {
    let mut config = test_config();
    config.execution.max_pending_bytes = 10485760; // 10 MiB
    let client = RuntimeClient::from_config(config).unwrap();

    // Oversized single payload exceeds 10 MiB limit
    let huge_payload = "a".repeat(10485761);
    let err = client
        .call_json("nextral.health", huge_payload, CallOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code, "invalid_input");
    assert_eq!(err.execution_status.as_deref(), Some("not_started"));

    // Permits recover immediately; next call succeeds
    let health = client
        .call_json("nextral.health", "{}".into(), CallOptions::default())
        .await
        .unwrap();
    assert!(health.contains("ready"));

    let report = client.close(Duration::from_secs(5)).await.unwrap();
    assert!(report.drained);
}
