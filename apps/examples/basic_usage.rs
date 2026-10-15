use nextral::{
    config::NextralConfig,
    ingestion::{ingest_memory, IngestMemoryRequest},
    memory::{ContentType, MemoryRecord, MemoryType, SourceType},
    retrieval::{retrieve, RetrievalRequest},
    store::TestMemoryStore,
    topology::{all_profiles, requires_store, StoreRole},
};

fn main() {
    let mut store = TestMemoryStore::new();

    let request = IngestMemoryRequest::new(
        "tenant_1",
        "user_1",
        "Atlas uses PostgreSQL for durable memory storage",
        ContentType::Fact,
        MemoryType::Semantic,
        SourceType::Manual,
        nextral::config::IngestionPolicy {
            min_importance_score: 0.0,
            min_confidence_score: 0.0,
        },
    );
    let response = ingest_memory(&mut store, request).unwrap();
    println!("Ingested: {:?}", response.status);

    let retrieval = retrieve(
        &mut store,
        RetrievalRequest::test("tenant_1", "user_1", "PostgreSQL"),
    )
    .unwrap();
    println!("Retrieved {} items", retrieval.items.len());
    for item in &retrieval.items {
        println!("  - {} (score: {:.3})", item.content, item.retrieval_score);
    }

    let profiles = all_profiles();
    println!("\nMemory types: {}", profiles.len());
    for profile in &profiles {
        let stores: Vec<&str> = profile
            .required_stores
            .iter()
            .map(|s| match s {
                StoreRole::Postgres => "Postgres",
                StoreRole::Redis => "Redis",
                StoreRole::Qdrant => "Qdrant",
                StoreRole::Neo4j => "Neo4j",
                StoreRole::S3 => "S3",
                StoreRole::ModelContext => "ModelContext",
            })
            .collect();
        println!(
            "  {:?}: durable={}, stores=[{}]",
            profile.memory_type, profile.durable, stores.join(", ")
        );
    }
}
