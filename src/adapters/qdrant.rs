use crate::{
    adapters::{
        transport::{
            env_secret, hardened_client, store_failure, validate_transport_url,
            TransportHardeningProfile,
        },
        AdapterHealth,
    },
    contracts::{CoreError, CoreResult},
    memory::MemoryRecord,
    ports::{QdrantPort, TenantUserScope, VectorPoint, VectorSearchHit, VectorSearchRequest},
    runtime::observability::StoreBackend,
};
use reqwest::{Method, StatusCode};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QdrantAdapter {
    pub url: String,
    pub collection: String,
    pub hardening: TransportHardeningProfile,
}

impl QdrantAdapter {
    pub fn new(url: impl Into<String>, collection: impl Into<String>) -> CoreResult<Self> {
        let adapter = Self {
            url: url.into(),
            collection: collection.into(),
            hardening: TransportHardeningProfile::baseline(Some("NEXTRAL_QDRANT_API_KEY")),
        };
        validate_transport_url(&adapter.url, false).map_err(CoreError::InvalidInput)?;
        validate_name(&adapter.collection)?;
        Ok(adapter)
    }
    pub fn collection_schema_json(&self) -> &'static str {
        include_str!("../../migrations/qdrant/memory_collection.json")
    }
    pub fn health(&self) -> AdapterHealth {
        AdapterHealth::configured("qdrant")
    }

    fn request(
        &self,
        method: Method,
        path: &str,
        payload: Option<&Value>,
    ) -> CoreResult<(StatusCode, Value)> {
        validate_transport_url(&self.url, self.hardening.require_tls)
            .map_err(CoreError::InvalidInput)?;
        let mut request = hardened_client(&self.hardening)?
            .request(method, format!("{}{path}", self.url.trim_end_matches('/')));
        if let Some(name) = self.hardening.token_env.as_deref() {
            // Blank optional local credentials mean an explicitly unsecured development server.
            if std::env::var(name).is_ok_and(|value| !value.trim().is_empty())
                || self.hardening.require_tls
            {
                request = request.header("api-key", env_secret(name)?);
            }
        }
        if let Some(payload) = payload {
            request = request.json(payload);
        }
        let response = request.send().map_err(|e| {
            store_failure(
                StoreBackend::Qdrant,
                CoreError::Io(format!("Qdrant request failed: {e}")),
            )
        })?;
        let status = response.status();
        let bytes = response
            .bytes()
            .map_err(|e| store_failure(StoreBackend::Qdrant, CoreError::Io(e.to_string())))?;
        if !status.is_success() {
            return Ok((status, Value::Null));
        }
        let body = serde_json::from_slice(&bytes).map_err(|e| {
            store_failure(
                StoreBackend::Qdrant,
                CoreError::Serialization(format!("invalid Qdrant response: {e}")),
            )
        })?;
        Ok((status, body))
    }
    pub(crate) fn call(
        &self,
        method: Method,
        path: &str,
        payload: Option<&Value>,
    ) -> CoreResult<Value> {
        let (status, body) = self.request(method, path, payload)?;
        if !status.is_success() {
            return Err(store_failure(
                StoreBackend::Qdrant,
                CoreError::Io(format!("Qdrant {path} failed: {status}")),
            ));
        }
        if body.get("status").and_then(Value::as_str) != Some("ok") {
            return Err(store_failure(
                StoreBackend::Qdrant,
                CoreError::Io(format!(
                    "Qdrant {path} returned a malformed or failed status"
                )),
            ));
        }
        Ok(body)
    }
    pub fn readiness(&self) -> CoreResult<Value> {
        self.call(Method::GET, "/collections", None)
    }

    pub fn upsert_record(
        &self,
        collection: &str,
        point: &VectorPoint,
        record: &MemoryRecord,
    ) -> CoreResult<()> {
        if record.id != point.memory_id
            || record.tenant_id != point.tenant_id
            || record.user_id != point.user_id
        {
            return Err(CoreError::InvalidInput(
                "vector record identity mismatch".into(),
            ));
        }
        let mut payload = serde_json::to_value(record)?;
        payload
            .as_object_mut()
            .ok_or_else(|| CoreError::Serialization("memory record is not an object".into()))?
            .remove("embedding");
        // Indexed fields must agree with the canonical record, not a stale caller copy.
        for (field, expected) in [
            ("privacy_level", serde_json::to_value(&point.privacy_level)?),
            ("status", serde_json::to_value(&point.status)?),
            ("content_type", json!(point.content_type)),
            ("memory_type", json!(point.memory_type)),
            ("schema_version", json!(point.schema_version)),
        ] {
            if payload[field] != expected {
                return Err(CoreError::InvalidInput(format!(
                    "vector {field} does not match canonical record"
                )));
            }
        }
        payload["memory_id"] = json!(record.id);
        self.upsert_payload(collection, point, payload)
    }
    fn upsert_payload(
        &self,
        collection: &str,
        point: &VectorPoint,
        payload: Value,
    ) -> CoreResult<()> {
        validate_name(collection)?;
        validate_identity(&point.tenant_id, &point.user_id, &point.memory_id)?;
        validate_vector(&point.vector)?;
        self.call(Method::PUT,&format!("/collections/{collection}/points?wait=true"),Some(&json!({"points":[{"id":point_id(&point.tenant_id,&point.user_id,&point.memory_id),"vector":point.vector,"payload":payload}]})))?;
        Ok(())
    }
    pub fn delete_scoped(
        &self,
        collection: &str,
        scope: &TenantUserScope,
        memory_id: &str,
    ) -> CoreResult<()> {
        validate_identity(&scope.tenant_id, &scope.user_id, memory_id)?;
        let mut filter = scope_filter(&scope.tenant_id, Some(&scope.user_id));
        filter["must"]
            .as_array_mut()
            .ok_or_else(|| CoreError::Serialization("invalid filter".into()))?
            .push(json!({"key":"memory_id","match":{"value":memory_id}}));
        self.delete_filter(collection, filter)
    }
    fn delete_filter(&self, collection: &str, filter: Value) -> CoreResult<()> {
        validate_name(collection)?;
        self.call(
            Method::POST,
            &format!("/collections/{collection}/points/delete?wait=true"),
            Some(&json!({"filter":filter})),
        )?;
        Ok(())
    }
    pub fn count_scoped(&self, collection: &str, scope: &TenantUserScope) -> CoreResult<u64> {
        if scope.tenant_id.is_empty() || scope.user_id.is_empty() {
            return Err(CoreError::InvalidInput(
                "count requires tenant and user".into(),
            ));
        }
        self.count_filter(
            collection,
            scope_filter(&scope.tenant_id, Some(&scope.user_id)),
        )
    }
    fn count_filter(&self, collection: &str, filter: Value) -> CoreResult<u64> {
        validate_name(collection)?;
        self.call(
            Method::POST,
            &format!("/collections/{collection}/points/count"),
            Some(&json!({"filter":filter,"exact":true})),
        )?["result"]["count"]
            .as_u64()
            .ok_or_else(|| CoreError::Serialization("Qdrant count missing".into()))
    }
    pub fn scroll_scoped(
        &self,
        collection: &str,
        scope: &TenantUserScope,
        offset: Option<&str>,
        limit: usize,
    ) -> CoreResult<Value> {
        validate_name(collection)?;
        if scope.tenant_id.is_empty() || scope.user_id.is_empty() || limit == 0 || limit > 10000 {
            return Err(CoreError::InvalidInput(
                "scroll requires scope and limit 1..10000".into(),
            ));
        }
        let body=self.call(Method::POST,&format!("/collections/{collection}/points/scroll"),Some(&json!({"filter":scope_filter(&scope.tenant_id,Some(&scope.user_id)),"limit":limit,"offset":offset,"with_payload":true,"with_vector":true})))?;
        let points = body["result"]["points"]
            .as_array()
            .ok_or_else(|| CoreError::Serialization("Qdrant scroll points missing".into()))?;
        if points.iter().any(|point| {
            point["payload"]["tenant_id"] != scope.tenant_id
                || point["payload"]["user_id"] != scope.user_id
        }) {
            return Err(CoreError::Io(
                "Qdrant returned an out-of-scope scroll point".into(),
            ));
        }
        Ok(body)
    }
    pub fn read_scoped_vector(
        &self,
        collection: &str,
        scope: &TenantUserScope,
        memory_id: &str,
    ) -> CoreResult<Option<Vec<f32>>> {
        validate_name(collection)?;
        validate_identity(&scope.tenant_id, &scope.user_id, memory_id)?;
        let mut filter = scope_filter(&scope.tenant_id, Some(&scope.user_id));
        filter["must"]
            .as_array_mut()
            .ok_or_else(|| CoreError::Serialization("invalid filter".into()))?
            .push(json!({"key":"memory_id","match":{"value":memory_id}}));
        let body = self.call(
            Method::POST,
            &format!("/collections/{collection}/points/scroll"),
            Some(&json!({"filter":filter,"limit":2,"with_vector":true,"with_payload":true})),
        )?;
        let points = body["result"]["points"]
            .as_array()
            .ok_or_else(|| CoreError::Serialization("Qdrant scroll points missing".into()))?;
        if points.len() > 1 {
            return Err(CoreError::Conflict(
                "multiple vector points have the same scoped canonical memory_id".into(),
            ));
        }
        let Some(point) = points.first() else {
            return Ok(None);
        };
        if point["payload"]["tenant_id"] != scope.tenant_id
            || point["payload"]["user_id"] != scope.user_id
            || point["payload"]["memory_id"] != memory_id
        {
            return Err(CoreError::Io(
                "Qdrant returned an out-of-scope vector".into(),
            ));
        }
        let vector: Vec<f32> = serde_json::from_value(point["vector"].clone())?;
        validate_vector(&vector)?;
        Ok(Some(vector))
    }
    pub fn collections(&self) -> CoreResult<Vec<String>> {
        let body = self.call(Method::GET, "/collections", None)?;
        body["result"]["collections"]
            .as_array()
            .ok_or_else(|| CoreError::Serialization("Qdrant collections missing".into()))?
            .iter()
            .map(|entry| {
                entry["name"].as_str().map(str::to_string).ok_or_else(|| {
                    CoreError::Serialization("Qdrant collection name missing".into())
                })
            })
            .collect()
    }
    pub fn resolve_alias(&self, alias: &str) -> CoreResult<Option<String>> {
        validate_name(alias)?;
        let body = self.call(Method::GET, "/aliases", None)?;
        let aliases = body["result"]["aliases"]
            .as_array()
            .ok_or_else(|| CoreError::Serialization("Qdrant aliases missing".into()))?;
        aliases
            .iter()
            .find(|entry| entry["alias_name"] == alias)
            .map(|entry| {
                entry["collection_name"]
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| CoreError::Serialization("Qdrant alias target missing".into()))
            })
            .transpose()
    }
    pub fn switch_alias(&self, alias: &str, target: &str) -> CoreResult<()> {
        validate_name(alias)?;
        validate_name(target)?;
        self.call(Method::GET, &format!("/collections/{target}"), None)?;
        let mut actions = Vec::new();
        if self.resolve_alias(alias)?.is_some() {
            actions.push(json!({"delete_alias":{"alias_name":alias}}));
        }
        actions.push(json!({"create_alias":{"alias_name":alias,"collection_name":target}}));
        self.call(
            Method::POST,
            "/collections/aliases",
            Some(&json!({"actions":actions})),
        )?;
        Ok(())
    }
}

impl QdrantPort for QdrantAdapter {
    fn ensure_collection(
        &self,
        collection: &str,
        dimension: u32,
        distance: &str,
    ) -> CoreResult<()> {
        validate_name(collection)?;
        if dimension == 0 {
            return Err(CoreError::InvalidInput(
                "vector dimension must be positive".into(),
            ));
        }
        let metric = match distance.to_ascii_lowercase().as_str() {
            "cosine" => "Cosine",
            "dot" => "Dot",
            "euclid" => "Euclid",
            "manhattan" => "Manhattan",
            _ => {
                return Err(CoreError::InvalidInput(
                    "unsupported vector distance".into(),
                ))
            }
        };
        let path = format!("/collections/{collection}");
        let (status, mut body) = self.request(Method::GET, &path, None)?;
        if status == StatusCode::NOT_FOUND {
            let (created, _) = self.request(
                Method::PUT,
                &path,
                Some(&json!({"vectors":{"size":dimension,"distance":metric}})),
            )?;
            if !created.is_success() && created != StatusCode::CONFLICT {
                return Err(store_failure(
                    StoreBackend::Qdrant,
                    CoreError::Io(format!("Qdrant collection create failed: {created}")),
                ));
            }
            body = self.call(Method::GET, &path, None)?;
        } else if !status.is_success() {
            return Err(store_failure(
                StoreBackend::Qdrant,
                CoreError::Io(format!("Qdrant collection lookup failed: {status}")),
            ));
        }
        verify_collection(&body, dimension, metric)
    }
    fn upsert_point(&self, collection: &str, point: &VectorPoint) -> CoreResult<()> {
        let mut payload = serde_json::to_value(point)?;
        payload
            .as_object_mut()
            .ok_or_else(|| CoreError::Serialization("invalid point".into()))?
            .remove("vector");
        self.upsert_payload(collection, point, payload)
    }
    fn search(
        &self,
        collection: &str,
        request: &VectorSearchRequest,
    ) -> CoreResult<Vec<VectorSearchHit>> {
        validate_name(collection)?;
        validate_vector(&request.query_vector)?;
        if request.scope.tenant_id.is_empty()
            || request.scope.user_id.is_empty()
            || request.top_k == 0
        {
            return Err(CoreError::InvalidInput(
                "vector search requires tenant, user and positive top_k".into(),
            ));
        }
        if request.privacy_scope.is_empty() {
            return Ok(Vec::new());
        }
        let mut filter = scope_filter(&request.scope.tenant_id, Some(&request.scope.user_id));
        let must = filter["must"]
            .as_array_mut()
            .ok_or_else(|| CoreError::Serialization("invalid filter".into()))?;
        must.push(json!({"key":"status","match":{"value":"active"}}));
        must.push(json!({"key":"privacy_level","match":{"any":request.privacy_scope}}));
        if let Some(ref filters) = request.filters {
            if let Some(ref sid) = filters.session_id {
                must.push(json!({"key": "session_id", "match": {"value": sid}}));
            }
            if let Some(ref st) = filters.source_types {
                let any_st: Vec<String> = st
                    .iter()
                    .map(|s| {
                        serde_json::to_value(s)
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .to_string()
                    })
                    .collect();
                must.push(json!({"key": "source_type", "match": {"any": any_st}}));
            }
            if let Some(ref tags_any) = filters.tags_any {
                must.push(json!({"key": "tags", "match": {"any": tags_any}}));
            }
            if let Some(ref tags_all) = filters.tags_all {
                for tag in tags_all {
                    must.push(json!({"key": "tags", "match": {"value": tag}}));
                }
            }
            if let Some(ref entities_any) = filters.entities_any {
                must.push(json!({"key": "entities", "match": {"any": entities_any}}));
            }
            if let Some(ref entities_all) = filters.entities_all {
                for entity in entities_all {
                    must.push(json!({"key": "entities", "match": {"value": entity}}));
                }
            }
            if let Some(ref gte) = filters.created_at_gte {
                let micros = crate::runtime::lifecycle::timestamp_nanos(gte)? / 1000;
                must.push(json!({"key": "created_at", "range": {"gte": micros}}));
            }
            if let Some(ref lt) = filters.created_at_lt {
                let micros = crate::runtime::lifecycle::timestamp_nanos(lt)? / 1000;
                must.push(json!({"key": "created_at", "range": {"lt": micros}}));
            }
        }
        let body=self.call(Method::POST,&format!("/collections/{collection}/points/search"),Some(&json!({"vector":request.query_vector,"limit":request.top_k,"with_payload":true,"filter":filter})))?;
        parse_hits(&body, request)
    }
    fn delete_point(&self, collection: &str, tenant_id: &str, memory_id: &str) -> CoreResult<()> {
        if tenant_id.is_empty() || memory_id.is_empty() {
            return Err(CoreError::InvalidInput(
                "delete requires tenant and memory ID".into(),
            ));
        }
        let mut filter = scope_filter(tenant_id, None);
        filter["must"]
            .as_array_mut()
            .ok_or_else(|| CoreError::Serialization("invalid filter".into()))?
            .push(json!({"key":"memory_id","match":{"value":memory_id}}));
        self.delete_filter(collection, filter)
    }
}
fn scope_filter(tenant: &str, user: Option<&str>) -> Value {
    let mut must = vec![json!({"key":"tenant_id","match":{"value":tenant}})];
    if let Some(user) = user {
        must.push(json!({"key":"user_id","match":{"value":user}}));
    }
    json!({"must":must})
}
fn validate_name(name: &str) -> CoreResult<()> {
    if name.is_empty()
        || name.len() > 255
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        || matches!(name, "." | "..")
    {
        return Err(CoreError::InvalidInput(
            "invalid Qdrant collection or alias name".into(),
        ));
    }
    Ok(())
}
fn validate_identity(tenant: &str, user: &str, id: &str) -> CoreResult<()> {
    if tenant.is_empty() || user.is_empty() || id.is_empty() {
        return Err(CoreError::InvalidInput(
            "vector tenant, user and memory ID required".into(),
        ));
    }
    Ok(())
}
fn validate_vector(vector: &[f32]) -> CoreResult<()> {
    if vector.is_empty() || vector.iter().any(|v| !v.is_finite()) {
        return Err(CoreError::InvalidInput(
            "vector must be nonempty and finite".into(),
        ));
    }
    Ok(())
}
/// Version-8 UUID over length-prefixed UTF-8 identity fields.
pub fn point_id(tenant: &str, user: &str, memory_id: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(b"nextral:qdrant:v1");
    for part in [tenant, user, memory_id] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part.as_bytes());
    }
    let digest = hash.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 15) | 0x80;
    bytes[8] = (bytes[8] & 63) | 0x80;
    let hex = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}
fn verify_collection(body: &Value, dimension: u32, metric: &str) -> CoreResult<()> {
    let vectors = &body["result"]["config"]["params"]["vectors"];
    if vectors["size"].as_u64() != Some(dimension as u64)
        || !vectors["distance"]
            .as_str()
            .is_some_and(|distance| distance.eq_ignore_ascii_case(metric))
    {
        return Err(CoreError::Conflict(format!(
            "Qdrant collection dimension/metric mismatch: expected {dimension}/{metric}"
        )));
    }
    Ok(())
}
fn parse_hits(body: &Value, request: &VectorSearchRequest) -> CoreResult<Vec<VectorSearchHit>> {
    body["result"]
        .as_array()
        .ok_or_else(|| CoreError::Serialization("Qdrant result missing".into()))?
        .iter()
        .map(|entry| {
            let payload = &entry["payload"];
            let privacy = serde_json::from_value(payload["privacy_level"].clone())?;
            if payload["tenant_id"] != request.scope.tenant_id
                || payload["user_id"] != request.scope.user_id
                || payload["status"] != "active"
                || !request.privacy_scope.contains(&privacy)
            {
                return Err(CoreError::Io(
                    "Qdrant returned an out-of-scope search hit".into(),
                ));
            }
            let id = payload["memory_id"]
                .as_str()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| {
                    CoreError::Serialization(
                        "canonical memory_id missing from Qdrant payload".into(),
                    )
                })?;
            let score = entry["score"]
                .as_f64()
                .ok_or_else(|| CoreError::Serialization("Qdrant score missing".into()))?
                as f32;
            if !score.is_finite() {
                return Err(CoreError::Serialization("Qdrant score not finite".into()));
            }
            Ok(VectorSearchHit {
                memory_id: id.to_string(),
                score,
            })
        })
        .collect()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn arbitrary_memory_ids_map_to_stable_scoped_uuid() {
        let id = point_id("tenant", "user", "mem/not-a-uuid");
        assert_eq!(id.len(), 36);
        assert_eq!(&id[14..15], "8");
        assert_eq!(id, point_id("tenant", "user", "mem/not-a-uuid"));
        assert_ne!(id, point_id("other", "user", "mem/not-a-uuid"));
        assert_ne!(point_id("ab", "c", "d"), point_id("a", "bc", "d"));
    }
    #[test]
    fn search_returns_canonical_id_and_rejects_cross_tenant_payload() {
        let request = VectorSearchRequest {
            scope: TenantUserScope {
                tenant_id: "t".into(),
                user_id: "u".into(),
            },
            query_vector: vec![1.0],
            privacy_scope: vec![crate::memory::PrivacyLevel::Private],
            top_k: 1,
            filters: None,
        };
        let mut body = json!({"result":[{"id":point_id("t","u","mem_1"),"score":0.5,"payload":{"tenant_id":"t","user_id":"u","memory_id":"mem_1","privacy_level":"private","status":"active"}}]});
        assert_eq!(parse_hits(&body, &request).unwrap()[0].memory_id, "mem_1");
        body["result"][0]["payload"]["tenant_id"] = json!("other");
        assert!(parse_hits(&body, &request).is_err());
    }
    #[test]
    fn collection_verification_rejects_dimension_and_metric_drift() {
        let body =
            json!({"result":{"config":{"params":{"vectors":{"size":3,"distance":"Cosine"}}}}});
        assert!(verify_collection(&body, 3, "Cosine").is_ok());
        assert!(verify_collection(&body, 4, "Cosine").is_err());
        assert!(verify_collection(&body, 3, "Dot").is_err());
    }
    #[test]
    #[ignore = "requires Qdrant at NEXTRAL_QDRANT_URL with optional NEXTRAL_QDRANT_API_KEY"]
    fn real_qdrant_non_uuid_round_trip_and_scoped_delete() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let collection = format!("adapter_test_{unique}");
        let adapter =
            QdrantAdapter::new(std::env::var("NEXTRAL_QDRANT_URL").unwrap(), &collection).unwrap();
        adapter.ensure_collection(&collection, 3, "cosine").unwrap();
        adapter.ensure_collection(&collection, 3, "Cosine").unwrap();
        assert!(adapter.ensure_collection(&collection, 4, "Cosine").is_err());
        let a = TenantUserScope {
            tenant_id: "tenant-a".into(),
            user_id: "user".into(),
        };
        let b = TenantUserScope {
            tenant_id: "tenant-b".into(),
            user_id: "user".into(),
        };
        let peer = TenantUserScope {
            tenant_id: "tenant-a".into(),
            user_id: "peer".into(),
        };
        for scope in [&a, &b, &peer] {
            let vector = if scope.user_id == "peer" {
                vec![0.0, 1.0, 0.0]
            } else {
                vec![1.0, 0.0, 0.0]
            };
            adapter
                .upsert_point(
                    &collection,
                    &VectorPoint {
                        memory_id: "mem/non-uuid".into(),
                        tenant_id: scope.tenant_id.clone(),
                        user_id: scope.user_id.clone(),
                        vector,
                        privacy_level: crate::memory::PrivacyLevel::Private,
                        status: crate::memory::MemoryStatus::Active,
                        content_type: "fact".into(),
                        memory_type: "semantic".into(),
                        schema_version: "1.0.0".into(),
                    },
                )
                .unwrap();
        }
        let request = VectorSearchRequest {
            scope: a.clone(),
            query_vector: vec![1.0, 0.0, 0.0],
            privacy_scope: vec![crate::memory::PrivacyLevel::Private],
            top_k: 5,
            filters: None,
        };
        assert_eq!(
            adapter.search(&collection, &request).unwrap()[0].memory_id,
            "mem/non-uuid"
        );
        assert_eq!(adapter.count_scoped(&collection, &a).unwrap(), 1);
        assert_eq!(
            adapter
                .read_scoped_vector(&collection, &a, "mem/non-uuid")
                .unwrap(),
            Some(vec![1.0, 0.0, 0.0])
        );
        assert_eq!(
            adapter
                .read_scoped_vector(&collection, &peer, "mem/non-uuid")
                .unwrap(),
            Some(vec![0.0, 1.0, 0.0])
        );
        adapter
            .delete_scoped(&collection, &a, "mem/non-uuid")
            .unwrap();
        assert_eq!(adapter.count_scoped(&collection, &a).unwrap(), 0);
        assert_eq!(adapter.count_scoped(&collection, &b).unwrap(), 1);
        assert_eq!(
            adapter
                .read_scoped_vector(&collection, &b, "mem/non-uuid")
                .unwrap(),
            Some(vec![1.0, 0.0, 0.0])
        );
        assert_eq!(adapter.count_scoped(&collection, &peer).unwrap(), 1);
        assert_eq!(
            adapter
                .read_scoped_vector(&collection, &a, "mem/non-uuid")
                .unwrap(),
            None
        );
        adapter
            .call(Method::DELETE, &format!("/collections/{collection}"), None)
            .unwrap();
    }
}
