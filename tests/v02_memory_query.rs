use nextral::{
    config::{NextralConfig, RuntimeBackend},
    contracts::CoreError,
    memory::{
        ContentType, MemoryFilters, MemoryPage, MemoryQuery, MemoryRecord, MemoryStatus,
        MemoryType, PrivacyLevel, SourceType,
    },
    package::{CallOptions, RuntimeClient},
    retrieval::RetrievalRequest,
    runtime::lifecycle::LifecycleStore,
    store::MemoryIndexStore,
    testkit::TestMemoryStore,
};
use std::collections::HashSet;

fn test_config() -> NextralConfig {
    let mut config = NextralConfig::default();
    config.backend = RuntimeBackend::TestMemory;
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyset_pagination_105_records_exact_sequence_and_no_duplicates() {
    let client = RuntimeClient::from_config(test_config()).unwrap();
    let tenant = "tenant_keyset";
    let user = "user_keyset";

    // Seed 105 memories with tied timestamps across batches and byte-order-sensitive IDs
    for i in 0..105 {
        let batch_group = i / 21; // 5 groups of 21 items with identical timestamps
        let timestamp = format!("2026-01-{:02}T00:00:00.000000Z", batch_group + 1);
        let id = format!("mem_{:04}", i);

        let mut record = MemoryRecord::new(
            &id,
            tenant,
            user,
            &format!("Content for item {i}"),
            ContentType::Fact,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        record.created_at = timestamp.clone();
        record.updated_at = timestamp;

        client.store().clone().upsert_memory(record).unwrap();
    }

    // Page by 10
    let mut cursor = None;
    let mut collected = Vec::new();
    let mut page_count = 0;

    loop {
        let query = MemoryQuery {
            tenant_id: tenant.into(),
            user_id: user.into(),
            privacy_scope: vec![PrivacyLevel::Private],
            filters: MemoryFilters::default(),
            limit: 10,
            cursor: cursor.clone(),
        };

        let raw_page = client
            .call_json(
                "nextral.memory.list",
                serde_json::to_string(&query).unwrap(),
                CallOptions::default(),
            )
            .await
            .unwrap();

        let page: MemoryPage = serde_json::from_str(&raw_page).unwrap();
        page_count += 1;
        for item in &page.items {
            collected.push(item.clone());
        }

        if page.next_cursor.is_none() {
            break;
        }
        cursor = page.next_cursor;
    }

    // Exact count and deduplication check
    assert_eq!(collected.len(), 105);
    assert_eq!(page_count, 11); // 10 pages of 10, 1 page of 5
    let unique_ids: HashSet<_> = collected.iter().map(|item| &item.id).collect();
    assert_eq!(unique_ids.len(), 105);

    // Verify strict descending ordering: created_at DESC, id DESC
    for i in 0..collected.len() - 1 {
        let curr = &collected[i];
        let next = &collected[i + 1];
        let curr_time = &curr.created_at;
        let next_time = &next.created_at;
        assert!(
            curr_time > next_time || (curr_time == next_time && curr.id > next.id),
            "ordering violation: ({}, {}) not > ({}, {})",
            curr_time,
            curr.id,
            next_time,
            next.id
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filters_tags_entities_and_date_intervals() {
    let client = RuntimeClient::from_config(test_config()).unwrap();
    let tenant = "tenant_filters";
    let user = "user_filters";

    // Ingest diverse records
    let items = vec![
        ("1", "fact", vec!["tag_a", "tag_b"], vec!["ent_1"], "2026-01-01T10:00:00.000000Z"),
        ("2", "fact", vec!["tag_a"], vec!["ent_1", "ent_2"], "2026-01-02T10:00:00.000000Z"),
        ("3", "decision", vec!["tag_b"], vec!["ent_2"], "2026-01-03T10:00:00.000000Z"),
    ];

    for (id, ct, tags, ents, ts) in items {
        let mut r = MemoryRecord::new(
            id,
            tenant,
            user,
            &format!("Content {}", id),
            if ct == "decision" { ContentType::Decision } else { ContentType::Fact },
            MemoryType::Semantic,
            SourceType::Manual,
        );
        r.tags = tags.into_iter().map(String::from).collect();
        r.entities = ents.into_iter().map(String::from).collect();
        r.created_at = ts.into();
        r.updated_at = ts.into();
        client.store().clone().upsert_memory(r).unwrap();
    }

    // 1. Tags ALL: require both tag_a AND tag_b -> only item 1
    let q_tags_all = MemoryQuery {
        tenant_id: tenant.into(),
        user_id: user.into(),
        privacy_scope: vec![PrivacyLevel::Private],
        filters: MemoryFilters {
            tags_all: Some(vec!["tag_a".into(), "tag_b".into()]),
            ..Default::default()
        },
        limit: 10,
        cursor: None,
    };
    let page: MemoryPage = serde_json::from_str(&client.call_json("nextral.memory.list", serde_json::to_string(&q_tags_all).unwrap(), CallOptions::default()).await.unwrap()).unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, "1");

    // 2. Tags ANY: tag_b -> items 1 and 3
    let q_tags_any = MemoryQuery {
        tenant_id: tenant.into(),
        user_id: user.into(),
        privacy_scope: vec![PrivacyLevel::Private],
        filters: MemoryFilters {
            tags_any: Some(vec!["tag_b".into()]),
            ..Default::default()
        },
        limit: 10,
        cursor: None,
    };
    let page_any: MemoryPage = serde_json::from_str(&client.call_json("nextral.memory.list", serde_json::to_string(&q_tags_any).unwrap(), CallOptions::default()).await.unwrap()).unwrap();
    assert_eq!(page_any.items.len(), 2);

    // 3. Date interval: [2026-01-02, 2026-01-03) -> only item 2
    let q_date = MemoryQuery {
        tenant_id: tenant.into(),
        user_id: user.into(),
        privacy_scope: vec![PrivacyLevel::Private],
        filters: MemoryFilters {
            created_at_gte: Some("2026-01-02T00:00:00Z".into()),
            created_at_lt: Some("2026-01-03T00:00:00Z".into()),
            ..Default::default()
        },
        limit: 10,
        cursor: None,
    };
    let page_date: MemoryPage = serde_json::from_str(&client.call_json("nextral.memory.list", serde_json::to_string(&q_date).unwrap(), CallOptions::default()).await.unwrap()).unwrap();
    assert_eq!(page_date.items.len(), 1);
    assert_eq!(page_date.items[0].id, "2");

    // 4. Equal endpoints: [2026-01-02, 2026-01-02) -> empty page
    let q_equal = MemoryQuery {
        tenant_id: tenant.into(),
        user_id: user.into(),
        privacy_scope: vec![PrivacyLevel::Private],
        filters: MemoryFilters {
            created_at_gte: Some("2026-01-02T00:00:00Z".into()),
            created_at_lt: Some("2026-01-02T00:00:00Z".into()),
            ..Default::default()
        },
        limit: 10,
        cursor: None,
    };
    let page_equal: MemoryPage = serde_json::from_str(&client.call_json("nextral.memory.list", serde_json::to_string(&q_equal).unwrap(), CallOptions::default()).await.unwrap()).unwrap();
    assert_eq!(page_equal.items.len(), 0);

    // 5. Reversed interval: gte > lt -> invalid_input
    let q_rev = MemoryQuery {
        tenant_id: tenant.into(),
        user_id: user.into(),
        privacy_scope: vec![PrivacyLevel::Private],
        filters: MemoryFilters {
            created_at_gte: Some("2026-01-05T00:00:00Z".into()),
            created_at_lt: Some("2026-01-02T00:00:00Z".into()),
            ..Default::default()
        },
        limit: 10,
        cursor: None,
    };
    let err_rev = client.call_json("nextral.memory.list", serde_json::to_string(&q_rev).unwrap(), CallOptions::default()).await.unwrap_err();
    assert_eq!(err_rev.code, "invalid_input");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cursor_tampering_and_mismatch_validation() {
    let client = RuntimeClient::from_config(test_config()).unwrap();
    let tenant = "tenant_cursor";
    let user = "user_cursor";

    // Ingest 5 records
    for i in 0..5 {
        let r = MemoryRecord::new(
            &format!("id_{i}"),
            tenant,
            user,
            &format!("Content {i}"),
            ContentType::Fact,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        client.store().clone().upsert_memory(r).unwrap();
    }

    let q1 = MemoryQuery {
        tenant_id: tenant.into(),
        user_id: user.into(),
        privacy_scope: vec![PrivacyLevel::Private],
        filters: MemoryFilters::default(),
        limit: 2,
        cursor: None,
    };
    let page1: MemoryPage = serde_json::from_str(&client.call_json("nextral.memory.list", serde_json::to_string(&q1).unwrap(), CallOptions::default()).await.unwrap()).unwrap();
    let valid_cursor = page1.next_cursor.unwrap();

    // Tampered cursor (altered characters) fails
    let tampered = format!("{}xyz", &valid_cursor[..valid_cursor.len() - 3]);
    let q_tampered = MemoryQuery {
        tenant_id: tenant.into(),
        user_id: user.into(),
        privacy_scope: vec![PrivacyLevel::Private],
        filters: MemoryFilters::default(),
        limit: 2,
        cursor: Some(tampered),
    };
    let err = client.call_json("nextral.memory.list", serde_json::to_string(&q_tampered).unwrap(), CallOptions::default()).await.unwrap_err();
    assert_eq!(err.code, "invalid_input");

    // Reusing cursor with different filters fails
    let q_mismatch = MemoryQuery {
        tenant_id: tenant.into(),
        user_id: user.into(),
        privacy_scope: vec![PrivacyLevel::Private],
        filters: MemoryFilters {
            session_id: Some("different_session".into()),
            ..Default::default()
        },
        limit: 2,
        cursor: Some(valid_cursor.clone()),
    };
    let err_mismatch = client.call_json("nextral.memory.list", serde_json::to_string(&q_mismatch).unwrap(), CallOptions::default()).await.unwrap_err();
    assert_eq!(err_mismatch.code, "invalid_input");

    // Changing page limit with the same cursor works cleanly
    let q_limit_change = MemoryQuery {
        tenant_id: tenant.into(),
        user_id: user.into(),
        privacy_scope: vec![PrivacyLevel::Private],
        filters: MemoryFilters::default(),
        limit: 10,
        cursor: Some(valid_cursor),
    };
    let page_change: MemoryPage = serde_json::from_str(&client.call_json("nextral.memory.list", serde_json::to_string(&q_limit_change).unwrap(), CallOptions::default()).await.unwrap()).unwrap();
    assert_eq!(page_change.items.len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_tombstone_protection_in_query() {
    let client = RuntimeClient::from_config(test_config()).unwrap();
    let tenant = "tenant_tombstone";
    let user = "user_tombstone";

    // Derived memory referencing source message msg_1
    let mut derived = MemoryRecord::new(
        "mem_derived",
        tenant,
        user,
        "Derived summary text",
        ContentType::Fact,
        MemoryType::Semantic,
        SourceType::Manual,
    );
    derived.source_message_ids = vec!["msg_1".into()];
    client.store().clone().upsert_memory(derived.clone()).unwrap();

    // Default browse shows Active
    let q_active = MemoryQuery {
        tenant_id: tenant.into(),
        user_id: user.into(),
        privacy_scope: vec![PrivacyLevel::Private],
        filters: MemoryFilters::default(),
        limit: 10,
        cursor: None,
    };
    let p1: MemoryPage = serde_json::from_str(&client.call_json("nextral.memory.list", serde_json::to_string(&q_active).unwrap(), CallOptions::default()).await.unwrap()).unwrap();
    assert_eq!(p1.items.len(), 1);
    assert_eq!(p1.items[0].status, MemoryStatus::Active);

    // Tombstone source msg_1 as forgotten
    let mut store = client.store().clone();
    store
        .lifecycle_save(
            tenant,
            &format!("forgotten-source/{user}/msg_1"),
            &serde_json::json!({"status": "forgotten"}),
        )
        .unwrap();

    // Default browse excludes forgotten derived memory
    let p2: MemoryPage = serde_json::from_str(&client.call_json("nextral.memory.list", serde_json::to_string(&q_active).unwrap(), CallOptions::default()).await.unwrap()).unwrap();
    assert_eq!(p2.items.len(), 0);

    // Inactive browse shows derived memory as SoftDeleted
    let q_inactive = MemoryQuery {
        tenant_id: tenant.into(),
        user_id: user.into(),
        privacy_scope: vec![PrivacyLevel::Private],
        filters: MemoryFilters {
            statuses: Some(vec![MemoryStatus::SoftDeleted]),
            ..Default::default()
        },
        limit: 10,
        cursor: None,
    };
    let p3: MemoryPage = serde_json::from_str(&client.call_json("nextral.memory.list", serde_json::to_string(&q_inactive).unwrap(), CallOptions::default()).await.unwrap()).unwrap();
    assert_eq!(p3.items.len(), 1);
    assert_eq!(p3.items[0].status, MemoryStatus::SoftDeleted);

    // Redacted source replaces preview with [redacted]
    store
        .lifecycle_save(
            tenant,
            &format!("forgotten-source/{user}/msg_1"),
            &serde_json::json!({"status": "redacted", "redact": true}),
        )
        .unwrap();

    let p4: MemoryPage = serde_json::from_str(&client.call_json("nextral.memory.list", serde_json::to_string(&q_inactive).unwrap(), CallOptions::default()).await.unwrap()).unwrap();
    assert_eq!(p4.items.len(), 1);
    assert_eq!(p4.items[0].content_preview, "[redacted]");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retrieval_filter_pushdown_and_repair() {
    let client = RuntimeClient::from_config(test_config()).unwrap();
    let tenant = "tenant_pushdown";
    let user = "user_pushdown";

    // Record 1 has tag special
    let mut r1 = MemoryRecord::new(
        "mem_special",
        tenant,
        user,
        "Quantum computing breakthrough",
        ContentType::Fact,
        MemoryType::Semantic,
        SourceType::Manual,
    );
    r1.tags = vec!["special".into()];
    r1.embedding_dim = Some(128);
    r1.embedding = Some(vec![0.0; 128]);
    client.store().clone().upsert_memory(r1).unwrap();

    // Record 2 does not have tag special
    let mut r2 = MemoryRecord::new(
        "mem_general",
        tenant,
        user,
        "Quantum computing introduction",
        ContentType::Fact,
        MemoryType::Semantic,
        SourceType::Manual,
    );
    r2.tags = vec!["general".into()];
    r2.embedding_dim = Some(128);
    r2.embedding = Some(vec![0.0; 128]);
    client.store().clone().upsert_memory(r2).unwrap();

    // Retrieval with filtered pushdown
    let mut req = RetrievalRequest::test(tenant, user, "Quantum computing");
    req.query_vector = Some(vec![0.0; 128]);
    req.filters = Some(MemoryFilters {
        tags_any: Some(vec!["special".into()]),
        ..Default::default()
    });

    let resp_raw = client
        .call_json(
            "nextral.memory.retrieve",
            serde_json::to_string(&req).unwrap(),
            CallOptions::default(),
        )
        .await
        .unwrap();
    let resp: serde_json::Value = serde_json::from_str(&resp_raw).unwrap();
    let items = resp["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["memory_id"], "mem_special");

    // Index repair tool returns complete
    let repair_raw = client
        .call_json(
            "nextral.index.repair",
            serde_json::json!({ "tenant_id": tenant, "user_id": user, "limit": 100 }).to_string(),
            CallOptions::default(),
        )
        .await
        .unwrap();
    let repair: serde_json::Value = serde_json::from_str(&repair_raw).unwrap();
    assert_eq!(repair["complete"], true);
    assert_eq!(repair["remaining"], 0);
}
