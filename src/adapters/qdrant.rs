use crate::{
    adapters::{transport::{maybe_add_bearer_auth, validate_transport_url, TransportHardeningProfile}, AdapterHealth},
    contracts::{CoreError, CoreResult},
    ports::{QdrantPort, VectorPoint, VectorSearchHit, VectorSearchRequest},
};
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QdrantAdapter {
    pub url: String,
    pub collection: String,
    pub hardening: TransportHardeningProfile,
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
        let hardening = TransportHardeningProfile::baseline(Some("NEXTRAL_QDRANT_API_KEY"));
        validate_transport_url(&url, hardening.require_tls)
            .map_err(CoreError::InvalidInput)?;
        Ok(Self { url, collection, hardening })
    }

    pub fn collection_schema_json(&self) -> &'static str {
        include_str!("../../migrations/qdrant/memory_collection.json")
    }

    pub fn health(&self) -> AdapterHealth {
        AdapterHealth::configured("qdrant")
    }

    pub fn readiness(&self) -> CoreResult<Value> {
        let client = Client::builder()
            .connect_timeout(Duration::from_millis(self.hardening.connect_timeout_ms))
            .timeout(Duration::from_millis(self.hardening.request_timeout_ms))
            .build()
            .map_err(|error| CoreError::Io(error.to_string()))?;
        let response = maybe_add_bearer_auth(
            client.get(format!("{}/collections", self.url.trim_end_matches('/'))),
            self.hardening.token_env.as_deref(),
        )
            .send()
            .map_err(|error| CoreError::Io(error.to_string()))?;
        let status = response.status();
        let body = response
            .json::<Value>()
            .map_err(|error| CoreError::Serialization(error.to_string()))?;
        if !status.is_success() {
            return Err(CoreError::Io(format!("qdrant readiness failed: {status}")));
        }
        Ok(body)
    }
}

impl QdrantPort for QdrantAdapter {
    fn ensure_collection(
        &self,
        collection: &str,
        dimension: u32,
        distance: &str,
    ) -> CoreResult<()> {
        let payload = json!({
            "vectors": {
                "size": dimension,
                "distance": distance.to_uppercase(),
            }
        });
        let client = Client::new();
        let response = maybe_add_bearer_auth(
            client.put(format!(
                "{}/collections/{}",
                self.url.trim_end_matches('/'),
                collection
            )),
            self.hardening.token_env.as_deref(),
        )
        .json(&payload)
            .send()
            .map_err(|error| CoreError::Io(error.to_string()))?;
        if !response.status().is_success() {
            return Err(CoreError::Io(format!(
                "qdrant ensure_collection failed: {}",
                response.status()
            )));
        }
        Ok(())
    }

    fn upsert_point(&self, collection: &str, point: &VectorPoint) -> CoreResult<()> {
        let payload = json!({
            "points": [{
                "id": point.memory_id,
                "vector": point.vector,
                "payload": {
                    "tenant_id": point.tenant_id,
                    "user_id": point.user_id,
                    "privacy_level": point.privacy_level,
                    "status": point.status,
                    "content_type": point.content_type,
                    "memory_type": point.memory_type,
                    "schema_version": point.schema_version
                }
            }]
        });
        let response = maybe_add_bearer_auth(
            Client::new().put(format!(
                "{}/collections/{}/points",
                self.url.trim_end_matches('/'),
                collection
            )),
            self.hardening.token_env.as_deref(),
        )
        .json(&payload)
            .send()
            .map_err(|error| CoreError::Io(error.to_string()))?;
        if !response.status().is_success() {
            return Err(CoreError::Io(format!(
                "qdrant upsert_point failed: {}",
                response.status()
            )));
        }
        Ok(())
    }

    fn search(
        &self,
        collection: &str,
        request: &VectorSearchRequest,
    ) -> CoreResult<Vec<VectorSearchHit>> {
        let payload = json!({
            "vector": request.query_vector,
            "limit": request.top_k,
            "with_payload": true,
            "filter": {
                "must": [
                    { "key": "tenant_id", "match": { "value": request.scope.tenant_id }},
                    { "key": "user_id", "match": { "value": request.scope.user_id }},
                    { "key": "status", "match": { "value": "active" }},
                ]
            }
        });
        let response = maybe_add_bearer_auth(
            Client::new().post(format!(
                "{}/collections/{}/points/search",
                self.url.trim_end_matches('/'),
                collection
            )),
            self.hardening.token_env.as_deref(),
        )
        .json(&payload)
            .send()
            .map_err(|error| CoreError::Io(error.to_string()))?;
        if !response.status().is_success() {
            return Err(CoreError::Io(format!(
                "qdrant search failed: {}",
                response.status()
            )));
        }
        let body = response
            .json::<Value>()
            .map_err(|error| CoreError::Serialization(error.to_string()))?;
        let hits = body["result"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entry| {
                Some(VectorSearchHit {
                    memory_id: entry.get("id")?.as_str()?.to_string(),
                    score: entry.get("score")?.as_f64()? as f32,
                })
            })
            .collect();
        Ok(hits)
    }

    fn delete_point(
        &self,
        collection: &str,
        tenant_id: &str,
        memory_id: &str,
    ) -> CoreResult<()> {
        let payload = json!({
            "points": [memory_id],
            "filter": {
                "must": [
                    { "key": "tenant_id", "match": { "value": tenant_id }}
                ]
            }
        });
        let response = maybe_add_bearer_auth(
            Client::new().post(format!(
                "{}/collections/{}/points/delete",
                self.url.trim_end_matches('/'),
                collection
            )),
            self.hardening.token_env.as_deref(),
        )
        .json(&payload)
            .send()
            .map_err(|error| CoreError::Io(error.to_string()))?;
        if !response.status().is_success() {
            return Err(CoreError::Io(format!(
                "qdrant delete_point failed: {}",
                response.status()
            )));
        }
        Ok(())
    }
}
