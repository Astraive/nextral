use crate::contracts::{CoreError, CoreResult};
use crate::{
    adapters::{
        neo4j::Neo4jAdapter, postgres::PostgresAdapter, qdrant::QdrantAdapter, redis::RedisAdapter,
        s3::S3Adapter,
    },
    config::IngestionPolicy,
    ingestion::IngestMemoryRequest,
    memory::{ContentType, MemoryRecord, MemoryType, SourceType},
    ports::{
        AuditWrite, CacheEntry, Neo4jPort, ObjectArchivePort, PostgresPort, QdrantPort, RedisPort,
        TenantUserScope, VectorSearchRequest,
    },
    retrieval::RetrievalRequest,
    runtime::{
        consolidation::{consolidate_session, ConsolidationRequest},
        evaluation::PromotionEvidence,
        governance::{forget_memory, ForgetMemoryRequest},
        intelligence::{decision_for, ExperimentRegistry, RuntimeLane, SafetyPolicy, Severity},
        reembed::plan_reembed,
        reminders::{
            execute_due_reminders, schedule_reminder, ExecuteDueRemindersRequest,
            ScheduleReminderRequest,
        },
        session::{
            append_session_message, assemble_working_context, eligible_sessions,
            AppendSessionMessageRequest, EligibleSessionsRequest,
        },
    },
    runtime::{lifecycle::LifecycleStore, storage::RuntimeStore},
    store::{MemoryIndexStore, TestMemoryStore},
};
use serde::{Deserialize, Serialize};
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PackageError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AdapterSmokeRequest {
    pub postgres_url: String,
    pub redis_url: String,
    pub qdrant_url: String,
    pub neo4j_url: String,
    pub s3_endpoint: String,
    pub s3_bucket: String,
    pub s3_region: String,
    pub s3_access_key_env: String,
    pub s3_secret_key_env: String,
    pub redis_key_prefix: String,
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: String,
    pub memory_id: String,
    pub memory_content: String,
    pub embedding_provider: String,
    pub embedding_model: String,
    pub embedding_dim: u32,
    pub reminder_title: String,
    pub reminder_due_at: String,
    pub timezone: String,
    pub cache_key: String,
    pub cache_value_json: String,
    pub cache_ttl_seconds: u64,
    pub qdrant_collection: String,
    pub qdrant_dimension: u32,
    pub qdrant_query_vector: Vec<f32>,
    pub qdrant_point_vector: Vec<f32>,
    pub qdrant_distance: String,
    pub graph_hops: u8,
    pub archive_kind: String,
    pub archive_bytes: String,
    pub archive_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct McpCallRequest {
    pub tool: String,
    pub payload_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExperimentCreateRequest {
    pub lane: RuntimeLane,
    pub policy_version: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExperimentPromoteRequest {
    pub experiment_id: String,
    pub evidence: PromotionEvidence,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExperimentRollbackRequest {
    pub experiment_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SafetyPolicySetRequest {
    pub severity: Severity,
    pub action: crate::runtime::intelligence::DecisionAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BatchIngestRequest {
    pub items: Vec<IngestMemoryRequest>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BatchRetrieveRequest {
    pub queries: Vec<RetrievalRequest>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BatchForgetRequest {
    pub items: Vec<ForgetMemoryRequest>,
    pub idempotency_key: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphifyRequest {
    tenant_id: String,
    user_id: String,
    memory_id: String,
    #[serde(default)]
    hints: Vec<crate::graph::GraphHint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchResponse {
    pub total: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub errors: Vec<String>,
    pub results: Vec<serde_json::Value>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct RuntimeControlPlane {
    registry: ExperimentRegistry,
    safety_policy: SafetyPolicy,
}

fn load_control(store: &RuntimeStore) -> CoreResult<RuntimeControlPlane> {
    store
        .lifecycle_load("__nextral_control", "control-plane")?
        .map(serde_json::from_value)
        .transpose()
        .map_err(CoreError::from)
        .map(|value| value.unwrap_or_default())
}

fn with_control(
    store: &mut RuntimeStore,
    operation: impl FnOnce(&mut RuntimeControlPlane) -> CoreResult<serde_json::Value>,
) -> CoreResult<serde_json::Value> {
    let owner = crate::runtime::lifecycle::worker_owner();
    if !store.lifecycle_claim(
        "__nextral_control",
        "control-lease",
        &owner,
        crate::runtime::lifecycle::timestamp_epoch(&crate::memory::now_timestamp())?,
        300,
    )? {
        return Err(CoreError::Conflict(
            "control plane is leased by another worker".into(),
        ));
    }
    let result = (|| {
        let mut control = load_control(store)?;
        let result = operation(&mut control)?;
        let receipt = store.lifecycle_save(
            "__nextral_control",
            "control-plane",
            &serde_json::to_value(control)?,
        )?;
        if !crate::runtime::lifecycle::acknowledged(&receipt) {
            return Err(CoreError::Io(
                "control plane persistence was not acknowledged".into(),
            ));
        }
        Ok(result)
    })();
    let release = store.lifecycle_release("__nextral_control", "control-lease", &owner);
    match result {
        Ok(value) => {
            release?;
            Ok(value)
        }
        Err(error) => {
            let _ = release;
            Err(error)
        }
    }
}

fn invalid_json_input(_: serde_json::Error) -> CoreError {
    CoreError::InvalidInput("invalid input format".into())
}

static RUNTIME: OnceLock<Mutex<RuntimeStore>> = OnceLock::new();

pub fn configure_runtime_json(config_json: &str) -> Result<String, PackageError> {
    let config = crate::config::load_config(config_json)?;
    crate::runtime::observability::configure(&config.observability)?;
    let backend = serde_json::to_value(&config.backend).map_err(CoreError::from)?;
    let configured = RuntimeStore::configured(config)?;
    if let Some(current) = RUNTIME.get() {
        *current
            .lock()
            .map_err(|e| CoreError::Conflict(e.to_string()))? = configured;
    } else {
        RUNTIME
            .set(Mutex::new(configured))
            .map_err(|_| CoreError::Conflict("runtime was configured concurrently".into()))?;
    }
    Ok(serde_json::json!({"backend":backend,"status":"configured"}).to_string())
}

fn runtime_store() -> CoreResult<&'static Mutex<RuntimeStore>> {
    if let Some(store) = RUNTIME.get() {
        return Ok(store);
    }
    if let Ok(path) = std::env::var("NEXTRAL_CONFIG") {
        let config = std::fs::read_to_string(path).map_err(|e| CoreError::Io(e.to_string()))?;
        configure_runtime_json(&config).map_err(|e| CoreError::InvalidInput(e.message))?;
    } else {
        #[cfg(test)]
        {
            let _ = RUNTIME.set(Mutex::new(RuntimeStore::Test(Box::default())));
        }
        #[cfg(not(test))]
        {
            return Err(CoreError::InvalidInput(
                "configure_runtime_json or NEXTRAL_CONFIG is required".into(),
            ));
        }
    }
    RUNTIME
        .get()
        .ok_or_else(|| CoreError::Conflict("runtime initialization failed".into()))
}

pub fn runtime_readiness_json() -> Result<String, PackageError> {
    Ok(runtime_store()?
        .lock()
        .map_err(|e| CoreError::Conflict(e.to_string()))?
        .readiness()
        .to_string())
}

pub fn run_service_hosts_json(config_json: &str, modes_json: &str) -> Result<String, PackageError> {
    crate::api::run_service_hosts_json(config_json, modes_json).map_err(PackageError::from)
}

pub fn e2e_smoke_json() -> Result<String, PackageError> {
    let mut store = TestMemoryStore::new();
    let mut request = IngestMemoryRequest::new(
        "test_tenant",
        "test_user",
        "Host supplied PostgreSQL memory",
        ContentType::Fact,
        MemoryType::Semantic,
        SourceType::Manual,
        IngestionPolicy {
            min_importance_score: 0.0,
            min_confidence_score: 0.0,
        },
    );
    request.importance_score = 0.9;
    request.confidence_score = Some(0.9);
    let ingested = crate::ingestion::ingest_memory(&mut store, request)?;
    let retrieved = crate::retrieval::retrieve(
        &mut store,
        RetrievalRequest::test("test_tenant", "test_user", "PostgreSQL"),
    )?;
    Ok(
        serde_json::json!({"backend":"test_memory","ingested":ingested,"retrieved":retrieved})
            .to_string(),
    )
}

pub fn adapter_smoke_json(request_json: &str) -> Result<String, PackageError> {
    let request: AdapterSmokeRequest =
        serde_json::from_str(request_json).map_err(invalid_json_input)?;
    validate_adapter_smoke_request(&request)?;

    let postgres = PostgresAdapter::new(&request.postgres_url)?;
    let redis = RedisAdapter::new(&request.redis_url, &request.redis_key_prefix)?;
    let qdrant = QdrantAdapter::new(&request.qdrant_url, &request.qdrant_collection)?;
    let neo4j = Neo4jAdapter::new(&request.neo4j_url)?;
    let s3 = S3Adapter::new(
        &request.s3_endpoint,
        &request.s3_bucket,
        &request.s3_region,
        &request.s3_access_key_env,
        &request.s3_secret_key_env,
    )?;
    postgres.migrate()?;

    let mut record = MemoryRecord::new(
        &request.memory_id,
        &request.tenant_id,
        &request.user_id,
        &request.memory_content,
        ContentType::Fact,
        MemoryType::Semantic,
        SourceType::Manual,
    );
    record.session_id = Some(request.session_id.clone());
    record.embedding_provider = Some(request.embedding_provider.clone());
    record.embedding_model = Some(request.embedding_model.clone());
    record.embedding_dim = Some(request.embedding_dim);
    record.entities = vec![request.memory_id.clone()];
    postgres.upsert_memory(&record)?;

    let scope = TenantUserScope {
        tenant_id: request.tenant_id.clone(),
        user_id: request.user_id.clone(),
    };
    let loaded = postgres
        .get_memory(&scope, &request.memory_id)?
        .ok_or_else(|| CoreError::NotFound("adapter smoke memory was not loaded".to_string()))?;
    let message_id = postgres.append_session_message(
        &scope,
        &request.session_id,
        "user",
        &request.memory_content,
    )?;
    let reminder = crate::prospective::ReminderRecord::new(
        &request.tenant_id,
        &request.user_id,
        &request.memory_id,
        crate::prospective::ReminderKind::FollowUp,
        &request.reminder_title,
        &request.reminder_due_at,
        &request.timezone,
    )?;
    postgres.upsert_reminder(&reminder)?;
    postgres.write_audit(&AuditWrite {
        id: None,
        tenant_id: request.tenant_id.clone(),
        user_id: Some(request.user_id.clone()),
        actor_id: request.user_id.clone(),
        action: "adapter_smoke".to_string(),
        target_type: "memory".to_string(),
        target_id: Some(request.memory_id.clone()),
        reason: "adapter smoke verification".to_string(),
        request_id: None,
        trace_id: None,
        metadata_json: "{}".to_string(),
    })?;
    let outbox_id = postgres.enqueue_outbox(
        &request.tenant_id,
        "adapter_smoke.completed",
        &request.memory_id,
        "{}",
    )?;
    qdrant.ensure_collection(
        &request.qdrant_collection,
        request.qdrant_dimension,
        &request.qdrant_distance,
    )?;
    qdrant.upsert_point(
        &request.qdrant_collection,
        &crate::ports::VectorPoint {
            memory_id: request.memory_id.clone(),
            tenant_id: request.tenant_id.clone(),
            user_id: request.user_id.clone(),
            vector: request.qdrant_point_vector.clone(),
            privacy_level: crate::memory::PrivacyLevel::Private,
            status: crate::memory::MemoryStatus::Active,
            content_type: "fact".to_string(),
            memory_type: "semantic".to_string(),
            schema_version: "1.0.0".to_string(),
        },
    )?;
    let vector_hits = qdrant.search(
        &request.qdrant_collection,
        &VectorSearchRequest {
            scope: scope.clone(),
            query_vector: request.qdrant_query_vector.clone(),
            privacy_scope: vec![crate::memory::PrivacyLevel::Private],
            top_k: 3,
        },
    )?;
    neo4j.merge_node(&crate::graph::GraphNode::new(
        &request.tenant_id,
        &request.user_id,
        "Entity",
        &request.memory_id,
        0.8,
    )?)?;
    let related_ids = neo4j.related_memory_ids(
        &scope,
        std::slice::from_ref(&request.memory_id),
        request.graph_hops,
    )?;
    let archive_receipt = s3.put_object(&crate::ports::ArchiveObject {
        tenant_id: request.tenant_id.clone(),
        user_id: request.user_id.clone(),
        session_id: Some(request.session_id.clone()),
        memory_id: Some(request.memory_id.clone()),
        object_kind: request.archive_kind.clone(),
        content_sha256: request.archive_sha256.clone(),
        bytes: request.archive_bytes.as_bytes().to_vec(),
    })?;

    redis.put_cache(&CacheEntry {
        key: request.cache_key.clone(),
        value_json: request.cache_value_json.clone(),
        ttl_seconds: request.cache_ttl_seconds,
    })?;
    let cached = redis.get_cache(&request.cache_key)?.ok_or_else(|| {
        CoreError::NotFound("adapter smoke cache value was not loaded".to_string())
    })?;
    let lease_key = format!("lease:{}", request.cache_key);
    let lease_owner = format!("{}:{}", request.tenant_id, request.user_id);
    let lease_acquired =
        redis.acquire_lease(&lease_key, &lease_owner, request.cache_ttl_seconds)?;
    redis.release_lease(&lease_key, &lease_owner)?;

    let payload = serde_json::json!({
        "status": "ok",
        "backend": "production_adapters",
        "memory_id": loaded.id,
        "message_id": message_id,
        "reminder_id": reminder.id,
        "outbox_id": outbox_id,
        "cache_key": request.cache_key,
        "cache_value": cached,
        "lease_acquired": lease_acquired,
        "vector_hits": vector_hits.len(),
        "graph_related_ids": related_ids,
        "archive_object_key": archive_receipt.object_key
    });
    Ok(payload.to_string())
}

const MAX_PAYLOAD_BYTES: usize = 10 * 1024 * 1024;

fn validate_tenant_user(payload: &serde_json::Value) -> Result<(), PackageError> {
    let tenant_id = payload
        .get("tenant_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let user_id = payload
        .get("user_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    crate::store::validate_scope_component(tenant_id, "tenant_id")?;
    crate::store::validate_scope_component(user_id, "user_id")?;
    if tenant_id.trim().is_empty() || user_id.trim().is_empty() {
        return Err(PackageError {
            code: "invalid_input".into(),
            message: "tenant_id and user_id are required".into(),
        });
    }
    if tenant_id.len() > 128 || user_id.len() > 128 {
        return Err(PackageError {
            code: "invalid_input".into(),
            message: "tenant_id and user_id must not exceed 128 characters".into(),
        });
    }
    Ok(())
}

pub fn mcp_call_json(request_json: &str) -> Result<String, PackageError> {
    let mut span = crate::runtime::observability::start_span("nextral.dispatch");
    let result = mcp_call_impl(request_json);
    if result.is_err() {
        span.fail();
    }
    result
}

fn mcp_call_impl(request_json: &str) -> Result<String, PackageError> {
    if request_json.len() > MAX_PAYLOAD_BYTES {
        return Err(CoreError::InvalidInput("request exceeds maximum payload size".into()).into());
    }
    let request: McpCallRequest = serde_json::from_str(request_json).map_err(invalid_json_input)?;
    if request.payload_json.len() > MAX_PAYLOAD_BYTES {
        return Err(CoreError::InvalidInput("tool payload exceeds maximum size".into()).into());
    }
    let payload: serde_json::Value =
        serde_json::from_str(&request.payload_json).map_err(invalid_json_input)?;
    if request.tool == "nextral.health" {
        return runtime_readiness_json();
    }
    let mut guard = runtime_store()?
        .lock()
        .map_err(|e| CoreError::Conflict(e.to_string()))?;
    let store = &mut *guard;
    if request.tool.starts_with("nextral.")
        && !matches!(
            request.tool.as_str(),
            "nextral.database.migrate"
                | "nextral.reembed.plan"
                | "nextral.jobs.reembed.execute"
                | "nextral.outbox.run"
                | "nextral.batch.ingest"
                | "nextral.batch.retrieve"
                | "nextral.batch.forget"
        )
    {
        validate_tenant_user(&payload)?;
    }
    let tenant = payload["tenant_id"].as_str().unwrap_or("");
    let user = payload["user_id"].as_str().unwrap_or("");
    let string_field = |name: &str| -> CoreResult<&str> {
        payload
            .get(name)
            .and_then(serde_json::Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| CoreError::InvalidInput(format!("{name} required")))
    };
    macro_rules! decode {
        ($ty:ty) => {
            serde_json::from_value::<$ty>(payload.clone()).map_err(invalid_json_input)?
        };
    }
    macro_rules! value {
        ($expr:expr) => {
            serde_json::to_value($expr).map_err(CoreError::from)?
        };
    }
    let result = match request.tool.as_str() {
        "nextral.database.migrate" => store.migrate()?,
        "nextral.outbox.run" => store.drain_outbox(string_field("tenant_id")?)?,
        "nextral.memory.ingest" => {
            let result = crate::ingestion::ingest_memory(store, decode!(IngestMemoryRequest))?;
            value!(result)
        }
        "nextral.memory.retrieve" | "nextral.runtime.scored_search" => {
            let mut result = crate::retrieval::retrieve(store, decode!(RetrievalRequest))?;
            let control = load_control(store)?;
            result.decision_action = decision_for(&control.safety_policy, &result.severity);
            result.lane = control.registry.current_lane.clone();
            result.policy_version = control.registry.active_policy_version.clone();
            value!(result)
        }
        "nextral.graph.query" => {
            let response = crate::retrieval::retrieve(store, decode!(RetrievalRequest))?;
            serde_json::json!({"items":response.items.into_iter().filter(|i|!matches!(i.source_path,crate::retrieval::SourcePath::Vector)).collect::<Vec<_>>()})
        }
        "nextral.memory.get" => {
            value!(store.get_memory(tenant, user, string_field("memory_id")?)?)
        }
        "nextral.memory.forget" => value!(forget_memory(store, decode!(ForgetMemoryRequest))?),
        "nextral.memory.list"
        | "nextral.memory.search_by_tag"
        | "nextral.memory.search_by_entity" => {
            let privacy = payload
                .get("privacy_scope")
                .map(|v| serde_json::from_value(v.clone()))
                .transpose()
                .map_err(invalid_json_input)?
                .unwrap_or_else(|| {
                    vec![
                        crate::memory::PrivacyLevel::Private,
                        crate::memory::PrivacyLevel::Shared,
                        crate::memory::PrivacyLevel::Sensitive,
                    ]
                });
            let mut records = store.list_memories(
                tenant,
                user,
                &privacy,
                payload["include_inactive"].as_bool().unwrap_or(false),
            )?;
            if request.tool == "nextral.memory.search_by_tag" {
                let tag = string_field("tag")?;
                records.retain(|r| r.has_tag(tag));
            }
            if request.tool == "nextral.memory.search_by_entity" {
                let entity = string_field("entity")?;
                records.retain(|r| r.has_entity(entity));
            }
            value!(records)
        }
        "nextral.memory.update" => {
            let mut record = store
                .get_memory(tenant, user, string_field("memory_id")?)?
                .ok_or_else(|| CoreError::NotFound("memory not found".into()))?;
            record.patch(
                payload
                    .get("content")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned),
                payload
                    .get("content_type")
                    .map(|v| serde_json::from_value(v.clone()))
                    .transpose()
                    .map_err(invalid_json_input)?,
                payload
                    .get("importance_score")
                    .and_then(|v| v.as_f64())
                    .map(|v| v as f32),
                payload
                    .get("confidence_score")
                    .map(|v| v.as_f64().map(|v| v as f32)),
                payload
                    .get("privacy_level")
                    .map(|v| serde_json::from_value(v.clone()))
                    .transpose()
                    .map_err(invalid_json_input)?,
            )?;
            for (name, add, entity) in [
                ("add_tags", true, false),
                ("remove_tags", false, false),
                ("add_entities", true, true),
                ("remove_entities", false, true),
            ] {
                if let Some(values) = payload.get(name) {
                    for item in serde_json::from_value::<Vec<String>>(values.clone())
                        .map_err(invalid_json_input)?
                    {
                        match (add, entity) {
                            (true, false) => record.add_tag(item),
                            (false, false) => {
                                record.remove_tag(&item);
                            }
                            (true, true) => record.add_entity(item),
                            (false, true) => {
                                record.remove_entity(&item);
                            }
                        }
                    }
                }
            }
            if payload.get("content").is_some() || payload.get("embedding").is_some() {
                let embedding = payload
                    .get("embedding")
                    .map(|v| serde_json::from_value::<Vec<f32>>(v.clone()))
                    .transpose()
                    .map_err(invalid_json_input)?;
                store.prepare_embedding(&mut record, embedding.as_deref())?;
            }
            store.update_memory(record.clone())?;
            value!(record)
        }
        "nextral.policy.upsert" => {
            let privacy = serde_json::from_value::<crate::memory::PrivacyLevel>(
                payload["privacy_level"].clone(),
            )
            .map_err(invalid_json_input)?;
            let mut policy = crate::domain::ProceduralPolicy::new(
                tenant,
                user,
                string_field("name")?,
                string_field("body")?,
                privacy,
            )?;
            if let Some(id) = payload.get("policy_id") {
                let id = id
                    .as_str()
                    .ok_or_else(|| CoreError::InvalidInput("policy_id must be a string".into()))?;
                crate::store::validate_scope_component(id, "policy_id")?;
                policy.id = id.into();
            }
            policy.enabled = payload
                .get("enabled")
                .map(|v| serde_json::from_value::<bool>(v.clone()))
                .transpose()
                .map_err(invalid_json_input)?
                .unwrap_or(true);
            let (policy, receipts) = store.policy_upsert(policy)?;
            serde_json::json!({"policy":policy,"receipts":receipts})
        }
        "nextral.policy.list" => {
            let requested = payload
                .get("privacy_scope")
                .map(|v| serde_json::from_value::<Vec<crate::memory::PrivacyLevel>>(v.clone()))
                .transpose()
                .map_err(invalid_json_input)?;
            let privacy = store.policy_privacy_scope(requested);
            let include_disabled = payload
                .get("include_disabled")
                .map(|v| serde_json::from_value::<bool>(v.clone()))
                .transpose()
                .map_err(invalid_json_input)?
                .unwrap_or(false);
            let mut policies = store.lifecycle_policies(tenant, user)?;
            policies.retain(|p| {
                p.tenant_id == tenant
                    && p.user_id == user
                    && privacy.contains(&p.privacy_level)
                    && (include_disabled || p.enabled)
            });
            policies.sort_by(|a, b| a.id.cmp(&b.id));
            value!(policies)
        }
        "nextral.policy.remove" => {
            let id = string_field("policy_id")?;
            crate::store::validate_scope_component(id, "policy_id")?;
            serde_json::json!({"policy_id":id,"status":"removed","receipts":store.policy_remove(tenant,user,id)?})
        }
        "nextral.memory.stats" => {
            let records = store.list_memories(
                tenant,
                user,
                &[
                    crate::memory::PrivacyLevel::Private,
                    crate::memory::PrivacyLevel::Sensitive,
                    crate::memory::PrivacyLevel::Shared,
                    crate::memory::PrivacyLevel::Restricted,
                ],
                true,
            )?;
            let mut by_type = std::collections::BTreeMap::new();
            let mut by_content = std::collections::BTreeMap::new();
            for r in &records {
                *by_type
                    .entry(format!("{:?}", r.memory_type))
                    .or_insert(0usize) += 1;
                *by_content
                    .entry(format!("{:?}", r.content_type))
                    .or_insert(0usize) += 1;
            }
            serde_json::json!({"tenant_id":tenant,"user_id":user,"active_count":store.count_memories(tenant,user,false)?,"total_count":records.len(),"entity_count":store.list_entities(tenant,user)?.len(),"tag_count":store.list_tags(tenant,user)?.len(),"avg_importance":if records.is_empty(){0.0}else{records.iter().map(|r|r.importance_score).sum::<f32>()/records.len() as f32},"by_memory_type":by_type,"by_content_type":by_content})
        }
        "nextral.session.append" => value!(append_session_message(
            store,
            decode!(AppendSessionMessageRequest),
            20
        )?),
        "nextral.session.eligible" => {
            value!(eligible_sessions(store, decode!(EligibleSessionsRequest))?)
        }
        "nextral.session.context" => value!(assemble_working_context(
            store,
            decode!(RetrievalRequest),
            20
        )?),
        "nextral.consolidation.run" => {
            value!(consolidate_session(store, decode!(ConsolidationRequest))?)
        }
        "nextral.reminders.schedule" => {
            value!(schedule_reminder(store, decode!(ScheduleReminderRequest))?)
        }
        "nextral.reminders.due" => value!(execute_due_reminders(
            store,
            decode!(ExecuteDueRemindersRequest)
        )?),
        "nextral.reminders.list" => value!(store.reminders(tenant, user)?),
        "nextral.jobs.reembed.execute" => value!(crate::runtime::reembed::execute_reembed(
            store,
            decode!(crate::runtime::reembed::ReembedExecutionRequest)
        )?),
        "nextral.jobs.enqueue" => value!(crate::runtime::prospective::enqueue_lifecycle_job(
            store,
            decode!(crate::runtime::prospective::LifecycleJobRequest)
        )?),
        "nextral.jobs.run" => value!(crate::runtime::prospective::execute_lifecycle_jobs(
            store,
            decode!(crate::runtime::prospective::RunLifecycleJobsRequest)
        )?),
        "nextral.jobs.list" => {
            let mut jobs = store.lifecycle_list(tenant, &format!("scheduled-job/{user}/"))?;
            for checkpoint in store.lifecycle_list(tenant, &format!("reembed/{user}/"))? {
                let report = checkpoint["report"].clone();
                jobs.push(serde_json::json!({"id":report["plan_id"],"job_type":"reembed","status":report["status"],"report":report}));
            }
            value!(jobs)
        }
        "nextral.jobs.get" => {
            let id = string_field("job_id")?;
            crate::store::validate_scope_component(id, "job_id")?;
            if let Some(job) =
                store.lifecycle_load(tenant, &format!("scheduled-job/{user}/{id}"))?
            {
                job
            } else if let Some(checkpoint) =
                store.lifecycle_load(tenant, &format!("reembed/{user}/{id}"))?
            {
                let report = checkpoint["report"].clone();
                serde_json::json!({"id":report["plan_id"],"job_type":"reembed","status":report["status"],"report":report})
            } else {
                serde_json::Value::Null
            }
        }
        "nextral.reembed.plan" => {
            let p = decode!(ReembedPlanRequest);
            value!(plan_reembed(
                &p.tenant_id,
                &p.source_collection,
                &p.shadow_collection,
                &p.target_embedding_provider,
                &p.target_embedding_model
            )?)
        }
        "nextral.graph.graphify" => {
            let p = decode!(GraphifyRequest);
            value!(crate::runtime::graphify::graphify_memory(
                store,
                &p.tenant_id,
                &p.user_id,
                &p.memory_id,
                &p.hints,
            )?)
        }
        "nextral.batch.ingest" | "nextral.batch.retrieve" | "nextral.batch.forget" => {
            let array_name = if request.tool == "nextral.batch.retrieve" {
                "queries"
            } else {
                "items"
            };
            let items = payload[array_name]
                .as_array()
                .ok_or_else(|| CoreError::InvalidInput(format!("{array_name} required")))?;
            let maximum = match store {
                RuntimeStore::Production(s) => s.config.batch.max_batch_size,
                RuntimeStore::Test(_) => 100,
            };
            if items.len() > maximum {
                return Err(
                    CoreError::InvalidInput(format!("batch exceeds {maximum} items")).into(),
                );
            }
            if items.is_empty() {
                return Ok(serde_json::to_string(&BatchResponse {
                    total: 0,
                    succeeded: 0,
                    failed: 0,
                    errors: Vec::new(),
                    results: Vec::new(),
                })
                .map_err(CoreError::from)?);
            }
            let batch_tenant = items[0]["tenant_id"].as_str().unwrap_or("");
            let batch_user = items[0]["user_id"].as_str().unwrap_or("");
            for item in items {
                validate_tenant_user(item)?;
                if item["tenant_id"].as_str() != Some(batch_tenant)
                    || item["user_id"].as_str() != Some(batch_user)
                {
                    return Err(CoreError::InvalidInput(
                        "batch must have one tenant/user scope".into(),
                    )
                    .into());
                }
            }
            let fingerprint =
                crate::memory::deterministic_id(&[&request.tool, &payload[array_name].to_string()]);
            let key = payload["idempotency_key"].as_str().map(|k| {
                format!(
                    "idempotency/{}",
                    crate::memory::deterministic_id(&[batch_tenant, batch_user, &request.tool, k])
                )
            });
            let lease = key.as_ref().map(|key| format!("lease/{key}"));
            let owner = crate::runtime::lifecycle::worker_owner();
            if let Some(lease) = &lease {
                if !store.lifecycle_claim(
                    batch_tenant,
                    lease,
                    &owner,
                    crate::runtime::lifecycle::timestamp_epoch(&crate::memory::now_timestamp())?,
                    300,
                )? {
                    return Err(CoreError::Conflict(
                        "idempotent batch is leased by another worker".into(),
                    )
                    .into());
                }
            }
            if let Some(key) = &key {
                if let Some(cached) = store.lifecycle_load(batch_tenant, key)? {
                    if cached["fingerprint"].as_str() != Some(fingerprint.as_str()) {
                        if let Some(lease) = &lease {
                            store.lifecycle_release(batch_tenant, lease, &owner)?;
                        }
                        return Err(CoreError::Conflict(
                            "idempotency key reused with different request".into(),
                        )
                        .into());
                    }
                    if request.tool != "nextral.batch.retrieve"
                        && cached["response"]["failed"].as_u64() == Some(0)
                    {
                        if let Some(lease) = &lease {
                            store.lifecycle_release(batch_tenant, lease, &owner)?;
                        }
                        return Ok(cached["response"].to_string());
                    }
                }
            }
            let mut response = BatchResponse {
                total: items.len(),
                succeeded: 0,
                failed: 0,
                errors: Vec::new(),
                results: Vec::new(),
            };
            for item in items {
                if let Some(lease) = &lease {
                    if !store.lifecycle_claim(batch_tenant,lease,&owner,crate::runtime::lifecycle::timestamp_epoch(&crate::memory::now_timestamp())?,300)?{return Err(CoreError::Conflict("batch idempotency lease lost".into()).into());}
                }
                let result: CoreResult<serde_json::Value> = (|| match request.tool.as_str() {
                    "nextral.batch.ingest" => {
                        Ok(serde_json::to_value(crate::ingestion::ingest_memory(
                            store,
                            serde_json::from_value(item.clone()).map_err(invalid_json_input)?,
                        )?)?)
                    }
                    "nextral.batch.retrieve" => {
                        Ok(serde_json::to_value(crate::retrieval::retrieve(
                            store,
                            serde_json::from_value(item.clone()).map_err(invalid_json_input)?,
                        )?)?)
                    }
                    _ => Ok(serde_json::to_value(forget_memory(
                        store,
                        serde_json::from_value(item.clone()).map_err(invalid_json_input)?,
                    )?)?),
                })();
                match result {
                    Ok(value) => {
                        if matches!(
                            value["status"].as_str(),
                            Some("rejected" | "error" | "incomplete")
                        ) {
                            response.failed += 1;
                            response
                                .errors
                                .push("item did not complete; inspect its result".into());
                        } else {
                            response.succeeded += 1;
                        }
                        response.results.push(value);
                    }
                    Err(error) => {
                        response.failed += 1;
                        response.errors.push(error.to_string());
                    }
                }
            }
            let result = value!(response);
            if let Some(key) = key {
                store.lifecycle_save(
                    batch_tenant,
                    &key,
                    &serde_json::json!({"tenant_id":batch_tenant,"user_id":batch_user,"tool":request.tool,"fingerprint":fingerprint,"response":(request.tool != "nextral.batch.retrieve").then_some(&result)}),
                )?;
            }
            if let Some(lease) = &lease {
                store.lifecycle_release(batch_tenant, lease, &owner)?;
            }
            result
        }
        "experiments.create" => {
            let p = decode!(ExperimentCreateRequest);
            with_control(store, |c| {
                Ok(serde_json::to_value(c.registry.create(
                    p.lane,
                    p.policy_version,
                    p.description,
                ))?)
            })?
        }
        "experiments.promote" => {
            let p = decode!(ExperimentPromoteRequest);
            with_control(store, |c| {
                Ok(serde_json::to_value(
                    c.registry.promote(&p.experiment_id, &p.evidence)?,
                )?)
            })?
        }
        "experiments.rollback" => {
            let p = decode!(ExperimentRollbackRequest);
            with_control(store, |c| {
                Ok(serde_json::to_value(
                    c.registry.rollback(&p.experiment_id, &p.reason)?,
                )?)
            })?
        }
        "experiments.status" => {
            let c = load_control(store)?;
            value!(c.registry.status(payload["experiment_id"].as_str()))
        }
        "safety.policy.get" => {
            let c = load_control(store)?;
            value!(&c.safety_policy)
        }
        "safety.policy.set" => {
            let p = decode!(SafetyPolicySetRequest);
            with_control(store, |c| {
                c.safety_policy.actions.insert(p.severity, p.action);
                Ok(serde_json::to_value(&c.safety_policy)?)
            })?
        }
        other => return Err(CoreError::InvalidInput(format!("unknown MCP tool: {other}")).into()),
    };
    if matches!(
        request.tool.as_str(),
        "nextral.consolidation.run" | "nextral.jobs.run"
    ) {
        store.drain_outbox(tenant)?;
    }
    Ok(result.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReembedPlanRequest {
    pub tenant_id: String,
    pub source_collection: String,
    pub shadow_collection: String,
    pub target_embedding_provider: String,
    pub target_embedding_model: String,
}

pub fn reembed_plan_json(request_json: &str) -> Result<String, PackageError> {
    let request: ReembedPlanRequest =
        serde_json::from_str(request_json).map_err(invalid_json_input)?;
    let plan = plan_reembed(
        &request.tenant_id,
        &request.source_collection,
        &request.shadow_collection,
        &request.target_embedding_provider,
        &request.target_embedding_model,
    )?;
    Ok(serde_json::to_string(&plan).map_err(CoreError::from)?)
}

pub fn ingest_request_schema_json() -> String {
    let request = IngestMemoryRequest::new(
        "tenant_id",
        "user_id",
        "content",
        ContentType::Fact,
        MemoryType::Semantic,
        SourceType::Manual,
        IngestionPolicy {
            min_importance_score: 0.0,
            min_confidence_score: 0.0,
        },
    );
    serde_json::to_string(&request).unwrap_or_else(|_| "{}".to_string())
}

fn validate_adapter_smoke_request(request: &AdapterSmokeRequest) -> CoreResult<()> {
    let required = [
        ("postgres_url", request.postgres_url.as_str()),
        ("redis_url", request.redis_url.as_str()),
        ("qdrant_url", request.qdrant_url.as_str()),
        ("neo4j_url", request.neo4j_url.as_str()),
        ("s3_endpoint", request.s3_endpoint.as_str()),
        ("s3_bucket", request.s3_bucket.as_str()),
        ("s3_region", request.s3_region.as_str()),
        ("s3_access_key_env", request.s3_access_key_env.as_str()),
        ("s3_secret_key_env", request.s3_secret_key_env.as_str()),
        ("redis_key_prefix", request.redis_key_prefix.as_str()),
        ("tenant_id", request.tenant_id.as_str()),
        ("user_id", request.user_id.as_str()),
        ("session_id", request.session_id.as_str()),
        ("memory_id", request.memory_id.as_str()),
        ("memory_content", request.memory_content.as_str()),
        ("embedding_provider", request.embedding_provider.as_str()),
        ("embedding_model", request.embedding_model.as_str()),
        ("reminder_title", request.reminder_title.as_str()),
        ("reminder_due_at", request.reminder_due_at.as_str()),
        ("timezone", request.timezone.as_str()),
        ("cache_key", request.cache_key.as_str()),
        ("cache_value_json", request.cache_value_json.as_str()),
        ("qdrant_collection", request.qdrant_collection.as_str()),
        ("qdrant_distance", request.qdrant_distance.as_str()),
        ("archive_kind", request.archive_kind.as_str()),
        ("archive_bytes", request.archive_bytes.as_str()),
        ("archive_sha256", request.archive_sha256.as_str()),
    ];
    for (name, value) in required {
        if value.trim().is_empty() {
            return Err(CoreError::InvalidInput(format!("{name} is required")));
        }
    }
    if request.embedding_dim == 0 {
        return Err(CoreError::InvalidInput(
            "embedding_dim must be greater than zero".to_string(),
        ));
    }
    if request.cache_ttl_seconds == 0 {
        return Err(CoreError::InvalidInput(
            "cache_ttl_seconds must be greater than zero".to_string(),
        ));
    }
    if request.qdrant_dimension == 0 {
        return Err(CoreError::InvalidInput(
            "qdrant_dimension must be greater than zero".to_string(),
        ));
    }
    if request.qdrant_query_vector.is_empty() || request.qdrant_point_vector.is_empty() {
        return Err(CoreError::InvalidInput(
            "qdrant vectors are required".to_string(),
        ));
    }
    if request.qdrant_query_vector.len() != request.qdrant_point_vector.len() {
        return Err(CoreError::InvalidInput(
            "qdrant vectors must have the same length".to_string(),
        ));
    }
    if request.qdrant_point_vector.len() != request.qdrant_dimension as usize {
        return Err(CoreError::InvalidInput(
            "qdrant vectors must match qdrant_dimension".to_string(),
        ));
    }
    serde_json::from_str::<serde_json::Value>(&request.cache_value_json)
        .map_err(invalid_json_input)?;
    Ok(())
}

fn sanitize_error_message(msg: &str) -> String {
    let sanitized = msg
        .replace(['\n', '\r'], " ")
        .chars()
        .filter(|c| !c.is_control() || *c == '\t')
        .take(500)
        .collect::<String>();
    if sanitized.len() > 200 {
        format!("{}...", &sanitized[..200])
    } else {
        sanitized
    }
}

impl From<CoreError> for PackageError {
    fn from(error: CoreError) -> Self {
        let (code, raw_message) = match &error {
            CoreError::InvalidInput(msg) => ("invalid_input", msg.as_str()),
            CoreError::NotFound(msg) => ("not_found", msg.as_str()),
            CoreError::Conflict(msg) => ("conflict", msg.as_str()),
            CoreError::Io(_) => ("io", "internal error"),
            CoreError::Serialization(_) => ("serialization", "invalid input format"),
        };
        Self {
            code: code.to_string(),
            message: sanitize_error_message(raw_message),
        }
    }
}
