pub mod consolidation;
pub mod evaluation;
pub mod governance;
pub mod graphify;
pub mod ingestion;
pub mod intelligence;
pub mod reembed;
pub mod reminders;
pub mod retrieval;
pub mod session;
pub mod prospective;

use crate::{
    contracts::CoreResult,
    memory::MemoryRecord,
    runtime::{
        ingestion::{ingest_memory, IngestMemoryRequest, IngestMemoryResponse},
        retrieval::{RetrievalRequest, RetrievalResponse},
    },
    scoring::{self, ScoredRecord},
    store::TestMemoryStore,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeHealth {
    pub status: String,
    pub config_valid: bool,
    pub postgres: String,
    pub redis: String,
    pub qdrant: String,
    pub neo4j: String,
    pub object_store: String,
}

impl RuntimeHealth {
    pub fn configured() -> Self {
        Self {
            status: "configured".to_string(),
            config_valid: true,
            postgres: "not_checked".to_string(),
            redis: "not_checked".to_string(),
            qdrant: "not_checked".to_string(),
            neo4j: "not_checked".to_string(),
            object_store: "not_checked".to_string(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TestRuntime {
    pub store: TestMemoryStore,
}

impl TestRuntime {
    pub fn new() -> Self {
        Self {
            store: TestMemoryStore::new(),
        }
    }

    pub fn ingest(&mut self, request: IngestMemoryRequest) -> CoreResult<IngestMemoryResponse> {
        ingest_memory(&mut self.store, request)
    }

    pub fn retrieve(&mut self, request: RetrievalRequest) -> CoreResult<RetrievalResponse> {
        retrieval::retrieve(&mut self.store, request)
    }
}

pub async fn scored_keyword_search(
    records: &[MemoryRecord],
    query: &str,
) -> CoreResult<Vec<ScoredRecord>> {
    tokio::task::yield_now().await;
    let matches = retrieval::keyword_search(records, query)?;
    scoring::rank_records(&matches, query)
}
