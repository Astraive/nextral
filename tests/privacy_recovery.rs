#![cfg(all(
    feature = "postgres",
    feature = "redis",
    feature = "qdrant",
    feature = "neo4j",
    feature = "s3"
))]

use nextral::{
    adapters::{postgres::PostgresAdapter, s3::S3Adapter},
    config::{NextralConfig, RuntimeBackend},
    contracts::CoreError,
    memory::{ContentType, MemoryType, PrivacyLevel, SourceType},
    ports::{ArchiveObject, ArchiveReceipt, AuditWrite, ObjectArchivePort, PostgresPort},
    runtime::{
        consolidation::{
            consolidate_session, transcript_chunks, ConsolidationLane, ConsolidationRequest,
        },
        governance::{forget_memory, ForgetMemoryRequest},
        ingestion::{ingest_memory, IngestMemoryRequest, IngestStatus},
        lifecycle::{timestamp_epoch, LifecycleStore},
        prospective::{enqueue_lifecycle_job, LifecycleJobRequest, LifecycleTrigger},
        session::{append_session_message, AppendSessionMessageRequest},
        storage::RuntimeStore,
    },
    store::{MemoryIndexStore, SessionStore},
};
use serde_json::{json, Value};

fn counts(client: &mut postgres::Client, tenant: &str, user: &str, memory: &str) -> (i64, i64) {
    let mut tx = client.transaction().unwrap();
    tx.query_one(
        "SELECT set_config('nextral.tenant_id',$1,true),set_config('nextral.user_id',$2,true)",
        &[&tenant, &user],
    )
    .unwrap();
    let accepted = tx.query_one("SELECT count(*) FROM nextral_audit_events WHERE tenant_id=$1 AND user_id=$2 AND target_id=$3 AND action='write_accepted'", &[&tenant, &user, &memory]).unwrap().get(0);
    let outbox = tx
        .query_one(
            "SELECT count(*) FROM nextral_outbox_events WHERE tenant_id=$1 AND aggregate_id=$2",
            &[&tenant, &memory],
        )
        .unwrap()
        .get(0);
    tx.commit().unwrap();
    (accepted, outbox)
}

fn input(
    config: &NextralConfig,
    tenant: &str,
    user: &str,
    id: &str,
    content: &str,
    source: &str,
) -> IngestMemoryRequest {
    let mut request = IngestMemoryRequest::new(
        tenant,
        user,
        content,
        ContentType::Fact,
        MemoryType::Semantic,
        SourceType::Manual,
        config.ingestion_policy.clone(),
    );
    request.id = Some(id.into());
    request.source_message_ids = vec![source.into()];
    request.importance_score = 0.9;
    request.confidence_score = Some(0.95);
    let mut vector = vec![0.0; config.embedding.dimension as usize];
    vector[0] = 1.0;
    request.embedding = Some(vector);
    request.trace_id = Some("shared-correlation-trace".into());
    request
}

fn forget(store: &mut RuntimeStore, tenant: &str, user: &str, id: &str) {
    let response = forget_memory(
        store,
        ForgetMemoryRequest {
            tenant_id: tenant.into(),
            user_id: user.into(),
            memory_id: id.into(),
            actor: user.into(),
            reason: "privacy recovery regression".into(),
            redact: true,
        },
    )
    .unwrap();
    assert_eq!(response.status, "completed");
}

#[test]
#[ignore = "requires NEXTRAL_CONFIG and the migrated five-store fixture; run with --all-features"]
fn forget_fences_stale_jobs_and_source_only_archives() {
    let config: NextralConfig =
        serde_json::from_slice(&std::fs::read(std::env::var("NEXTRAL_CONFIG").unwrap()).unwrap())
            .unwrap();
    assert_eq!(config.backend, RuntimeBackend::ProductionStores);
    config.validate().unwrap();
    let stores = config.stores.as_ref().unwrap();
    let postgres = PostgresAdapter::new(&stores.postgres_url).unwrap();
    postgres.migrate().unwrap();
    let archive = S3Adapter::new(
        &stores.s3_endpoint,
        &stores.s3_bucket,
        &stores.s3_region,
        &stores.s3_access_key_env,
        &stores.s3_secret_key_env,
    )
    .unwrap();
    let mut store = RuntimeStore::configured(config.clone()).unwrap();
    let tenant = format!(
        "privacy-regression-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let session = "transcript";
    let mut body = Value::Null;
    let mut owner_source = String::new();
    for (user, id, content) in [
        ("owner", "owner-source", "Private owner transcript"),
        ("peer", "peer-source", "Independent peer transcript"),
    ] {
        let message = append_session_message(
            &mut store,
            AppendSessionMessageRequest {
                tenant_id: tenant.clone(),
                user_id: user.into(),
                session_id: session.into(),
                role: "user".into(),
                content: content.into(),
                idempotency_key: Some("append".into()),
                privacy_level: PrivacyLevel::Private,
            },
            20,
        )
        .unwrap();
        let request = input(&config, &tenant, user, id, content, &message.message_id);
        assert_eq!(
            ingest_memory(&mut store, request).unwrap().status,
            IngestStatus::Accepted
        );
        let messages = store.session_tail(&tenant, user, session, 20).unwrap();
        let archived =
            json!({"tenant_id":tenant,"user_id":user,"session_id":session,"messages":messages});
        store
            .lifecycle_archive(&tenant, &format!("independent/{user}"), &archived)
            .unwrap();
        if user == "owner" {
            owner_source = message.message_id;
            body = archived;
        }
    }
    assert!(store
        .get_memory(&tenant, "owner", "owner-source")
        .unwrap()
        .unwrap()
        .session_id
        .is_none());
    let updated = input(
        &config,
        &tenant,
        "owner",
        "owner-source",
        "Updated owner decision",
        &owner_source,
    );
    assert_eq!(
        ingest_memory(&mut store, updated).unwrap().status,
        IngestStatus::Accepted
    );
    let tls = native_tls::TlsConnector::builder().build().unwrap();
    let mut client = postgres::Client::connect(
        &stores.postgres_url,
        postgres_native_tls::MakeTlsConnector::new(tls),
    )
    .unwrap();
    let before = counts(&mut client, &tenant, "owner", "owner-source");
    assert_eq!(before.0, 2);
    let event = AuditWrite {
        id: Some("provided-event".into()),
        tenant_id: tenant.clone(),
        user_id: Some("owner".into()),
        actor_id: "owner".into(),
        action: "privacy_probe".into(),
        target_type: "memory".into(),
        target_id: Some("owner-source".into()),
        reason: "original event".into(),
        request_id: None,
        trace_id: None,
        metadata_json: "{}".into(),
    };
    postgres.write_audit(&event).unwrap();
    postgres.write_audit(&event).unwrap();
    let mut conflicting = event;
    conflicting.reason = "different event".into();
    let mut changed = store
        .get_memory(&tenant, "owner", "owner-source")
        .unwrap()
        .unwrap();
    let original_content = changed.content.clone();
    changed.content = "Unaudited mutation must not commit".into();
    let payload = json!({"record":changed,"nodes":[],"edges":[]});
    let result = postgres.commit_memory(
        &changed,
        &conflicting,
        &payload,
        stores.qdrant_collection.as_deref().unwrap(),
    );
    assert!(matches!(result, Err(CoreError::Conflict(_))));
    assert_eq!(
        store
            .get_memory(&tenant, "owner", "owner-source")
            .unwrap()
            .unwrap()
            .content,
        original_content
    );
    assert_eq!(
        counts(&mut client, &tenant, "owner", "owner-source"),
        before
    );

    let messages = store.session_tail(&tenant, "owner", session, 20).unwrap();
    let (content, sources) = transcript_chunks(&messages, 100).unwrap().remove(0);
    let mut candidate = input(
        &config,
        &tenant,
        "owner",
        "deep-candidate",
        &content,
        &owner_source,
    );
    candidate.source_message_ids = sources;
    candidate.embedding = None;
    let deep = ConsolidationRequest {
        tenant_id: tenant.clone(),
        user_id: "owner".into(),
        session_id: session.into(),
        lane: ConsolidationLane::Deep,
        policy: config.ingestion_policy.clone(),
        version: "privacy-failure".into(),
        summary: None,
        candidates: vec![candidate],
        chunk_messages: 100,
    };
    assert!(matches!(
        consolidate_session(&mut store, deep.clone()),
        Err(CoreError::InvalidInput(_))
    ));
    let mut receipts = Vec::new();
    let mut peer_receipt = None;
    let mut owner_object = None;
    for (key, mut manifest) in postgres.state_list(&tenant, "archive/").unwrap() {
        let receipt: ArchiveReceipt = serde_json::from_value(manifest["receipt"].clone()).unwrap();
        if manifest["user_id"] == "owner" {
            let bytes = archive.get_verified_object(&tenant, &receipt).unwrap();
            let stored: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(stored["messages"], body["messages"]);
            if key.contains("independent/owner") {
                manifest
                    .as_object_mut()
                    .unwrap()
                    .remove("source_message_ids");
                postgres.state_save(&tenant, &key, &manifest).unwrap();
                owner_object = Some(ArchiveObject {
                    tenant_id: tenant.clone(),
                    user_id: "owner".into(),
                    session_id: Some(session.into()),
                    memory_id: None,
                    object_kind: format!(
                        "lifecycle-{}",
                        nextral::memory::deterministic_id(&["independent/owner"])
                    ),
                    content_sha256: receipt.content_sha256.clone(),
                    bytes,
                });
            }
            receipts.push(receipt);
        } else if manifest["user_id"] == "peer" {
            peer_receipt = Some(receipt);
        }
    }
    let peer_receipt = peer_receipt.unwrap();
    let peer_bytes = archive.get_verified_object(&tenant, &peer_receipt).unwrap();
    let mut job = enqueue_lifecycle_job(
        &mut store,
        LifecycleJobRequest {
            tenant_id: tenant.clone(),
            user_id: "owner".into(),
            session_id: session.into(),
            trigger: LifecycleTrigger::Deep,
            run_at: nextral::memory::now_timestamp(),
            idle_seconds: None,
            consolidation: Some(deep),
            memory_ids: vec![],
            redact: true,
            max_attempts: 2,
        },
    )
    .unwrap();
    let key = format!("scheduled-job/owner/{}", job.id);
    let mut worker = RuntimeStore::configured(config.clone()).unwrap();
    let now = timestamp_epoch(&nextral::memory::now_timestamp()).unwrap();
    assert!(worker
        .lifecycle_claim(&tenant, &key, "worker", now, 300)
        .unwrap());
    job.status = "running".into();
    let mut stale = serde_json::to_value(job).unwrap();
    assert!(worker
        .lifecycle_save_claimed(&tenant, &key, "worker", &stale)
        .unwrap()
        .is_none());
    forget(&mut store, &tenant, "owner", "owner-source");
    stale["status"] = json!("completed");
    let current = worker
        .lifecycle_save_claimed(&tenant, &key, "worker", &stale)
        .unwrap()
        .unwrap();
    assert_eq!(current["status"], "cancelled");
    assert!(current["request"]["consolidation"].is_null());
    worker.lifecycle_release(&tenant, &key, "worker").unwrap();
    for receipt in receipts {
        assert!(matches!(
            archive.get_verified_object(&tenant, &receipt),
            Err(CoreError::NotFound(_))
        ));
    }
    assert!(matches!(
        archive.put_object(&owner_object.unwrap()),
        Err(CoreError::Conflict(_))
    ));
    assert_eq!(
        archive.get_verified_object(&tenant, &peer_receipt).unwrap(),
        peer_bytes
    );
    forget(&mut store, &tenant, "peer", "peer-source");
}
