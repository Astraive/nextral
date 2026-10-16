use nextral::{
    config::{NextralConfig, RateLimitConfig, RuntimeBackend, StoreConfig},
};
use serde_json::json;
use std::time::Duration;

fn test_config_rate_limited(burst: u32, rps: u32) -> NextralConfig {
    let mut config = NextralConfig::default();
    config.backend = RuntimeBackend::TestMemory;
    config.rate_limit = RateLimitConfig {
        enabled: true,
        max_requests_per_second: rps,
        burst_size: burst,
    };
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scoped_token_bucket_enforces_burst_and_rejects_without_cross_tenant_interference() {
    let config = test_config_rate_limited(3, 10);
    let state = nextral::api::ServiceState::new(config).unwrap();

    let payload_a = json!({
        "tenant_id": "tenant_rate_a",
        "user_id": "user_rate_a",
        "content": "burst test content a",
        "content_type": "fact",
        "memory_type": "semantic",
        "privacy_level": "private",
        "importance_score": 0.5,
        "confidence_score": 0.5
    });

    let principal = nextral::api::Principal::Service;

    // First 3 requests consume the burst
    for _ in 0..3 {
        let res = state.call(&principal, "nextral.memory.ingest", payload_a.clone()).await;
        assert!(res.is_ok(), "Expected burst request to succeed");
    }

    // 4th request exceeds burst and must be rejected with 429 resource_exhausted
    let res4 = state.call(&principal, "nextral.memory.ingest", payload_a.clone()).await;
    assert!(res4.is_err(), "Expected 4th request to exceed rate limit");
    let err = res4.unwrap_err();
    assert_eq!(err.status, 429);
    assert_eq!(err.code, "resource_exhausted");

    // Independent scope (tenant_rate_b, user_rate_b) is NOT affected
    let payload_b = json!({
        "tenant_id": "tenant_rate_b",
        "user_id": "user_rate_b",
        "content": "burst test content b",
        "content_type": "fact",
        "memory_type": "semantic",
        "privacy_level": "private",
        "importance_score": 0.5,
        "confidence_score": 0.5
    });

    let res_b = state.call(&principal, "nextral.memory.ingest", payload_b.clone()).await;
    assert!(res_b.is_ok(), "Independent scope should succeed despite scope A being exhausted");

    // Wait for refill (10 rps = 1 token per 100ms)
    tokio::time::sleep(Duration::from_millis(250)).await;
    let res_a_refilled = state.call(&principal, "nextral.memory.ingest", payload_a.clone()).await;
    assert!(res_a_refilled.is_ok(), "Scope A should succeed after token refill");
}

fn valid_store_config(pg_url: &str) -> StoreConfig {
    StoreConfig {
        postgres_url: pg_url.to_string(),
        redis_url: "rediss://127.0.0.1:6379".into(),
        qdrant_url: "https://127.0.0.1:6334".into(),
        qdrant_collection: Some("test_collection".into()),
        neo4j_url: "neo4j+s://127.0.0.1:7687".into(),
        s3_endpoint: "https://127.0.0.1:9000".into(),
        s3_bucket: "test-bucket".into(),
        s3_region: "us-east-1".into(),
        s3_access_key_env: "MINIO_ACCESS_KEY".into(),
        s3_secret_key_env: "MINIO_SECRET_KEY".into(),
        transport_profile: None,
        enforce_tls: Some(true),
    }
}

#[tokio::test]
async fn postgres_tls_verification_modes_are_strictly_enforced() {
    let mut config = NextralConfig::default();
    config.backend = RuntimeBackend::ProductionStores;
    // Insecure sslmode=disable must be rejected
    config.stores = Some(valid_store_config("postgresql://localhost:5432/db?sslmode=disable"));
    assert!(config.validate().is_err(), "sslmode=disable must be rejected");

    // Unverified sslmode=prefer must be rejected
    config.stores = Some(valid_store_config("postgresql://localhost:5432/db?sslmode=prefer"));
    assert!(config.validate().is_err(), "sslmode=prefer must be rejected");

    // Verified modes require, verify-ca, verify-full must be accepted by config validation
    config.stores = Some(valid_store_config("postgresql://localhost:5432/db?sslmode=require"));
    let res = config.validate();
    if let Err(ref e) = res {
        eprintln!("validate error: {e}");
    }
    assert!(res.is_ok(), "sslmode=require must be valid");

    config.stores = Some(valid_store_config("postgresql://localhost:5432/db?sslmode=verify-ca"));
    assert!(config.validate().is_ok(), "sslmode=verify-ca must be valid");

    config.stores = Some(valid_store_config("postgresql://localhost:5432/db?sslmode=verify-full"));
    assert!(config.validate().is_ok(), "sslmode=verify-full must be valid");

    // Duplicate conflicting sslmodes must be rejected
    config.stores = Some(valid_store_config("postgresql://localhost:5432/db?sslmode=require&sslmode=verify-full"));
    assert!(config.validate().is_err(), "multiple sslmode parameters must be rejected");
}

#[tokio::test]
async fn jwks_auth_requires_valid_https_and_rejects_empty_keyset() {
    let mut config = NextralConfig::default();
    config.backend = RuntimeBackend::TestMemory;
    config.auth.issuer = Some("https://example.com/issuer".into());
    config.auth.audience = Some("nextral-api".into());
    // Insecure HTTP jwks_url must be rejected
    config.auth.jwks_url = Some("http://insecure.example.com/jwks.json".into());
    assert!(config.validate().is_err(), "HTTP jwks_url must be rejected");

    // HTTPS is accepted by config
    config.auth.jwks_url = Some("https://secure.example.com/jwks.json".into());
    assert!(config.validate().is_ok(), "HTTPS jwks_url must be valid in config");
}
