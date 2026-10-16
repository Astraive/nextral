#![cfg(all(
    feature = "postgres",
    feature = "redis",
    feature = "qdrant",
    feature = "neo4j",
    feature = "s3"
))]

use nextral::{
    config::{NextralConfig, RuntimeBackend},
    domain::SessionMessage,
    memory::PrivacyLevel,
    retrieval::RetrievalRequest,
    runtime::{
        session::{assemble_working_context, eligible_sessions, EligibleSessionsRequest},
        storage::RuntimeStore,
    },
    store::SessionStore,
};

fn fixture_config() -> NextralConfig {
    let config: NextralConfig = serde_json::from_slice(
        &std::fs::read(std::env::var("NEXTRAL_CONFIG").expect("fixture config path")).unwrap(),
    )
    .unwrap();
    assert_eq!(config.backend, RuntimeBackend::ProductionStores);
    config.validate().unwrap();
    config
}

fn unique_tenant(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn append_at(
    store: &mut RuntimeStore,
    tenant: &str,
    user: &str,
    session: &str,
    key: &str,
    created_at: &str,
) -> SessionMessage {
    let mut message = SessionMessage::new(
        tenant,
        user,
        session,
        "user",
        format!("Durable transcript {session} {key}"),
        key,
    );
    message.created_at = created_at.into();
    message.source_metadata = serde_json::json!({"privacy_level":"private"});
    store.append_session_message(message.clone()).unwrap();
    message
}

#[test]
#[ignore = "requires NEXTRAL_CONFIG and migrated five-store fixture; run with --all-features"]
fn eligible_sessions_scope_max_timestamp_boundary_and_deterministic_limit() {
    let mut store = RuntimeStore::configured(fixture_config()).unwrap();
    let tenant = unique_tenant("eligibility-regression");
    let cutoff = "2020-01-02T00:00:00Z";
    // Deliberately reverse insertion order and use offsets whose lexical order differs
    // from their chronological order. Selection must compare parsed instants.
    for (session, key, at) in [
        ("tie-b", "only", "2020-01-01T03:00:00+03:00"),
        ("tie-A", "only", "2020-01-01T00:00:00Z"),
        ("tie-a", "only", "2020-01-01T00:00:00Z"),
        ("oldest", "only", "2019-12-31T23:00:00Z"),
        ("boundary", "first", "2019-12-31T23:00:00Z"),
        ("boundary", "last", cutoff),
        ("active", "old", "2019-01-01T00:00:00Z"),
        ("active", "new", "2020-01-02T00:00:00.000001Z"),
        ("newer", "only", "2020-01-03T00:00:00Z"),
        ("future", "only", "2099-01-01T00:00:00Z"),
    ] {
        append_at(&mut store, &tenant, "owner", session, key, at);
    }
    append_at(
        &mut store,
        &tenant,
        "peer",
        "peer-only",
        "only",
        "2010-01-01T00:00:00Z",
    );
    append_at(
        &mut store,
        &format!("{tenant}-peer"),
        "owner",
        "tenant-only",
        "only",
        "2010-01-01T00:00:00Z",
    );
    let request = EligibleSessionsRequest {
        tenant_id: tenant.clone(),
        user_id: "owner".into(),
        inactive_at_or_before: "2020-01-02T02:00:00+02:00".into(),
        limit: 100,
    };
    // A fresh process-equivalent runtime must discover durable PostgreSQL writes.
    let reopened = RuntimeStore::configured(fixture_config()).unwrap();
    let selected = eligible_sessions(&reopened, request.clone()).unwrap();
    let metadata = selected.iter().map(|item| {
        (
            item.session_id.as_str(),
            nextral::runtime::lifecycle::timestamp_nanos(&item.last_message_at).unwrap(),
            item.message_count,
        )
    });
    let expected = [
        ("oldest", "2019-12-31T23:00:00Z", 1),
        ("tie-A", "2020-01-01T00:00:00Z", 1),
        ("tie-a", "2020-01-01T00:00:00Z", 1),
        ("tie-b", "2020-01-01T00:00:00Z", 1),
        ("boundary", cutoff, 2),
    ]
    .into_iter()
    .map(|(session, at, count)| {
        (
            session,
            nextral::runtime::lifecycle::timestamp_nanos(at).unwrap(),
            count,
        )
    });
    assert!(metadata.eq(expected), "{selected:?}");
    let limited = eligible_sessions(
        &reopened,
        EligibleSessionsRequest {
            limit: 2,
            ..request.clone()
        },
    )
    .unwrap();
    assert_eq!(limited, selected[..2]);
    for limit in [0, 1001] {
        assert!(eligible_sessions(
            &reopened,
            EligibleSessionsRequest {
                tenant_id: tenant.clone(),
                user_id: "owner".into(),
                inactive_at_or_before: cutoff.into(),
                limit,
            }
        )
        .is_err());
    }
}

#[test]
#[ignore = "requires NEXTRAL_CONFIG and migrated five-store fixture; run with --all-features"]
fn working_context_recovers_actual_postgres_transcript_when_redis_is_unreachable() {
    let config = fixture_config();
    let tenant = unique_tenant("warm-recovery-regression");
    let mut healthy = RuntimeStore::configured(config.clone()).unwrap();
    let first = append_at(
        &mut healthy,
        &tenant,
        "owner",
        "warm-session",
        "first",
        "2020-01-01T00:00:00Z",
    );
    let second = append_at(
        &mut healthy,
        &tenant,
        "owner",
        "warm-session",
        "second",
        "2020-01-01T00:00:01Z",
    );
    append_at(
        &mut healthy,
        &tenant,
        "peer",
        "warm-session",
        "hidden",
        "2020-01-01T00:00:02Z",
    );
    // Binding without listen reserves the port without shutting down or mutating the
    // shared Redis fixture. Keep the socket alive until context assembly finishes.
    let reserved = tokio::net::TcpSocket::new_v4().unwrap();
    reserved
        .bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
        .unwrap();
    let port = reserved.local_addr().unwrap().port();
    let dimension = config.embedding.dimension as usize;
    let mut isolated = config;
    isolated.stores.as_mut().unwrap().redis_url = format!("redis://127.0.0.1:{port}/0");
    let mut degraded = RuntimeStore::configured(isolated).unwrap();
    let budget = 64;
    let mut query = RetrievalRequest::test(&tenant, "owner", "durable transcript");
    query.session_id = Some("warm-session".into());
    query.token_budget = budget;
    query.privacy_scope = vec![PrivacyLevel::Private];
    let mut vector = vec![0.0; dimension];
    vector[0] = 1.0;
    query.query_vector = Some(vector);
    let context = assemble_working_context(&mut degraded, query, 20).unwrap();
    assert_eq!(context.session_tail, vec![first, second]);
    assert!(context.token_estimate <= budget);
    assert!(
        context
            .degraded_reasons
            .iter()
            .any(|reason| reason == "redis_unavailable:warm_fallback"),
        "{context:?}"
    );
    drop(reserved);
}
