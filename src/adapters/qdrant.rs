use crate::{
    adapters::AdapterHealth,
    contracts::{CoreError, CoreResult},
    ports::{QdrantPort, VectorPoint, VectorSearchHit, VectorSearchRequest},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QdrantAdapter {
    pub url: String,
    pub collection: String,
}

impl QdrantAdapter {
    pub fn new(url: impl Into<String>, collection: impl Into<String>) -> CoreResult<Self> {
        let url = url.into();
        let collection = collection.into();
        if url.trim().is_empty() || collection.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "qdrant_url and collection are required".to_string(),
            ));
        }
        Ok(Self { url, collection })
    }

    pub fn collection_schema_json(&self) -> &'static str {
        include_str!("../../migrations/qdrant/memory_collection.json")
    }

    pub fn health(&self) -> AdapterHealth {
        AdapterHealth::configured("qdrant")
    }
}

impl QdrantPort for QdrantAdapter {
    fn ensure_collection(
        &self,
        _collection: &str,
        _dimension: u32,
        _distance: &str,
    ) -> CoreResult<()> {
        Err(CoreError::InvalidInput(
            "qdrant driver execution is not enabled in this build".to_string(),
        ))
    }

    fn upsert_point(&self, _collection: &str, _point: &VectorPoint) -> CoreResult<()> {
        Err(CoreError::InvalidInput(
            "qdrant driver execution is not enabled in this build".to_string(),
        ))
    }

    fn search(
        &self,
        _collection: &str,
        _request: &VectorSearchRequest,
    ) -> CoreResult<Vec<VectorSearchHit>> {
        Err(CoreError::InvalidInput(
            "qdrant driver execution is not enabled in this build".to_string(),
        ))
    }

    fn delete_point(
        &self,
        _collection: &str,
        _tenant_id: &str,
        _memory_id: &str,
    ) -> CoreResult<()> {
        Err(CoreError::InvalidInput(
            "qdrant driver execution is not enabled in this build".to_string(),
        ))
    }
}
