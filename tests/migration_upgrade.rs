#![cfg(feature = "postgres")]

use nextral::{
    adapters::postgres::PostgresAdapter,
    contracts::CoreError,
    memory::{ContentType, MemoryRecord, MemoryType, SourceType},
    ports::{PostgresPort, TenantUserScope},
    runtime::lifecycle::timestamp_nanos,
};
use postgres::{Client, NoTls};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Barrier,
    },
    time::{SystemTime, UNIX_EPOCH},
};

static DATABASE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct Database {
    admin: Client,
    name: String,
    url: String,
}

impl Database {
    fn new() -> Self {
        let configuration: Value = serde_json::from_slice(
            &std::fs::read(std::env::var("NEXTRAL_CONFIG").unwrap()).unwrap(),
        )
        .unwrap();
        let owner_url = configuration["stores"]["postgres_url"].as_str().unwrap();
        let mut admin = Client::connect(owner_url, NoTls).unwrap();
        let name = format!(
            "nextral_migration_{}_{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            DATABASE_SEQUENCE.fetch_add(1, Ordering::Relaxed),
        );
        admin
            .batch_execute(&format!("CREATE DATABASE \"{name}\""))
            .unwrap();
        let mut url = reqwest::Url::parse(owner_url).unwrap();
        url.set_path(&format!("/{name}"));
        Self {
            admin,
            name,
            url: url.into(),
        }
    }

    fn client(&self) -> Client {
        Client::connect(&self.url, NoTls).unwrap()
    }

    fn adapter(&self) -> PostgresAdapter {
        PostgresAdapter::new(&self.url).unwrap()
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let result = self
            .admin
            .batch_execute(&format!("DROP DATABASE \"{}\" WITH (FORCE)", self.name));
        if !std::thread::panicking() {
            result.unwrap();
        }
    }
}

fn checksum(sql: &str) -> String {
    let mut hash = Sha256::new();
    for line in sql.lines() {
        hash.update(line.as_bytes());
        hash.update(b"\n");
    }
    format!("{:x}", hash.finalize())
}

fn history(client: &mut Client) -> Vec<(String, String)> {
    client
        .query(
            "SELECT version,checksum FROM nextral_schema_migrations ORDER BY version",
            &[],
        )
        .unwrap()
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

#[test]
#[ignore = "requires NEXTRAL_CONFIG with a PostgreSQL fixture owner allowed to create isolated databases"]
fn unknown_history_rejects_migration_and_readiness_without_changes() {
    let database = Database::new();
    let adapter = database.adapter();
    adapter.migrate().unwrap();
    let mut client = database.client();
    client.execute("INSERT INTO nextral_schema_migrations(version,checksum) VALUES('future-incompatible','unknown')", &[]).unwrap();
    let before = history(&mut client);
    assert!(matches!(adapter.migrate(), Err(CoreError::Conflict(_))));
    assert!(matches!(adapter.readiness(), Err(CoreError::Conflict(_))));
    assert_eq!(history(&mut client), before);
}

#[test]
#[ignore = "requires NEXTRAL_CONFIG with a PostgreSQL fixture owner allowed to create isolated databases"]
fn gapped_history_is_not_repaired_as_a_fresh_schema() {
    let database = Database::new();
    let adapter = database.adapter();
    adapter.migrate().unwrap();
    let mut client = database.client();
    client
        .execute(
            "DELETE FROM nextral_schema_migrations WHERE version='0001'",
            &[],
        )
        .unwrap();
    let before = history(&mut client);
    assert!(matches!(adapter.migrate(), Err(CoreError::Conflict(_))));
    assert!(matches!(adapter.readiness(), Err(CoreError::Conflict(_))));
    assert_eq!(history(&mut client), before);
}

#[test]
#[ignore = "requires NEXTRAL_CONFIG with a PostgreSQL fixture owner allowed to create isolated databases"]
fn conflicting_checksum_rejects_migration_and_readiness_without_changes() {
    let database = Database::new();
    let adapter = database.adapter();
    adapter.migrate().unwrap();
    let mut client = database.client();
    client
        .execute(
            "UPDATE nextral_schema_migrations SET checksum='changed' WHERE version='0001'",
            &[],
        )
        .unwrap();
    let before = history(&mut client);
    assert!(matches!(adapter.migrate(), Err(CoreError::Conflict(_))));
    assert!(matches!(adapter.readiness(), Err(CoreError::Conflict(_))));
    assert_eq!(history(&mut client), before);
}

#[test]
#[ignore = "requires NEXTRAL_CONFIG with a PostgreSQL fixture owner allowed to create isolated databases"]
fn concurrent_upgrade_preserves_populated_warm_transcript() {
    let database = Database::new();
    let first = database.adapter();
    let second = database.adapter();
    let mut client = database.client();
    client.batch_execute("CREATE TABLE nextral_schema_migrations(version TEXT PRIMARY KEY,checksum TEXT NOT NULL,applied_at TIMESTAMPTZ NOT NULL DEFAULT now())").unwrap();
    client.batch_execute(first.migration_sql()).unwrap();
    client
        .execute(
            "INSERT INTO nextral_schema_migrations(version,checksum) VALUES('0001',$1)",
            &[&checksum(first.migration_sql())],
        )
        .unwrap();
    client.execute("INSERT INTO nextral_session_messages(id,tenant_id,user_id,session_id,role,content,source_metadata,created_at) VALUES('legacy-message','tenant','owner','legacy-session','user','Preserve this warm transcript',$1,'2024-02-03T04:05:06Z')", &[&json!({"origin":"legacy"})]).unwrap();
    let original_history: String = client
        .query_one(
            "SELECT applied_at::text FROM nextral_schema_migrations WHERE version='0001'",
            &[],
        )
        .unwrap()
        .get(0);
    let barrier = Barrier::new(2);
    let results = std::thread::scope(|threads| {
        let left = threads.spawn(|| {
            barrier.wait();
            first.migrate()
        });
        let right = threads.spawn(|| {
            barrier.wait();
            second.migrate()
        });
        [left.join().unwrap(), right.join().unwrap()]
    });
    for result in results {
        result.unwrap();
    }
    assert_eq!(first.readiness().unwrap(), json!({"status":"ready"}));
    assert_eq!(second.readiness().unwrap(), json!({"status":"ready"}));
    let row = client.query_one("SELECT content,idempotency_key,record_json FROM nextral_session_messages WHERE tenant_id='tenant' AND user_id='owner' AND id='legacy-message'", &[]).unwrap();
    assert_eq!(row.get::<_, String>(0), "Preserve this warm transcript");
    assert_eq!(row.get::<_, String>(1), "legacy-message");
    let record: Value = row.get(2);
    assert_eq!(record["content"], "Preserve this warm transcript");
    assert_eq!(record["source_metadata"], json!({"origin":"legacy"}));
    assert_eq!(
        timestamp_nanos(record["created_at"].as_str().unwrap()).unwrap(),
        timestamp_nanos("2024-02-03T04:05:06Z").unwrap()
    );
    let applied_at: String = client
        .query_one(
            "SELECT applied_at::text FROM nextral_schema_migrations WHERE version='0001'",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(applied_at, original_history);
}

fn owned_record(user: &str) -> MemoryRecord {
    let mut record = MemoryRecord::new(
        "shared-id",
        "ownership-race",
        user,
        format!("Canonical data for {user}"),
        ContentType::Fact,
        MemoryType::Semantic,
        SourceType::Manual,
    );
    record.embedding_provider = Some("test".into());
    record.embedding_model = Some("ownership-fixture".into());
    record.embedding_dim = Some(2);
    record.embedding = Some(vec![1.0, 0.0]);
    record
}

#[test]
#[ignore = "requires NEXTRAL_CONFIG with a PostgreSQL fixture owner allowed to create isolated databases"]
fn concurrent_cross_owner_writes_cannot_reassign_canonical_ownership() {
    let database = Database::new();
    let first = database.adapter();
    first.migrate().unwrap();
    let second = database.adapter();
    let alice = owned_record("alice");
    let bob = owned_record("bob");
    let barrier = Barrier::new(2);
    let (left, right) = std::thread::scope(|threads| {
        let left = threads.spawn(|| {
            barrier.wait();
            first.upsert_memory(&alice)
        });
        let right = threads.spawn(|| {
            barrier.wait();
            second.upsert_memory(&bob)
        });
        (left.join().unwrap(), right.join().unwrap())
    });
    let (winner, loser) = match (left, right) {
        (Ok(()), Err(CoreError::Conflict(_))) => (&alice, &bob),
        (Err(CoreError::Conflict(_)), Ok(())) => (&bob, &alice),
        other => panic!("expected one accepted owner and one conflict, got {other:?}"),
    };
    let scope = |user: &str| TenantUserScope {
        tenant_id: "ownership-race".into(),
        user_id: user.into(),
    };
    assert_eq!(
        first
            .get_memory(&scope(&winner.user_id), "shared-id")
            .unwrap()
            .as_ref(),
        Some(winner)
    );
    assert_eq!(
        first
            .get_memory(&scope(&loser.user_id), "shared-id")
            .unwrap(),
        None
    );
    assert!(matches!(
        second.upsert_memory(loser),
        Err(CoreError::Conflict(_))
    ));
    assert_eq!(
        first
            .get_memory(&scope(&winner.user_id), "shared-id")
            .unwrap()
            .as_ref(),
        Some(winner)
    );
}
