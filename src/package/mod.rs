use crate::contracts::{CoreError, CoreResult};
use crate::{
    adapters::{neo4j::Neo4jAdapter, postgres::PostgresAdapter, qdrant::QdrantAdapter, redis::RedisAdapter, s3::S3Adapter},
    config::IngestionPolicy,
    ingestion::IngestMemoryRequest,
    memory::{ContentType, MemoryRecord, MemoryType, SourceType},
    ports::{AuditWrite, CacheEntry, Neo4jPort, ObjectArchivePort, PostgresPort, QdrantPort, RedisPort, TenantUserScope, VectorSearchRequest},
    retrieval::RetrievalRequest,
    runtime::{
        consolidation::{consolidate_session, ConsolidationLane, ConsolidationRequest},
        governance::{forget_memory, ForgetMemoryRequest},
        intelligence::{
            classify_severity, decision_for, ExperimentRegistry, RuntimeLane, SafetyPolicy,
            Severity,
        },
        evaluation::{canary_replay_gate, report_from_severity},
        reembed::plan_reembed,
        reminders::{execute_due_reminders, schedule_reminder, ExecuteDueRemindersRequest, ScheduleReminderRequest},
        session::{append_session_message, assemble_working_context, AppendSessionMessageRequest},
    },
    store::TestMemoryStore,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExperimentPromoteRequest {
    pub experiment_id: String,
    pub severity: Severity,
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

#[derive(Debug)]
struct RuntimeControlPlane {
    registry: ExperimentRegistry,
    safety_policy: SafetyPolicy,
}

impl Default for RuntimeControlPlane {
    fn default() -> Self {
        Self {
            registry: ExperimentRegistry::default(),
            safety_policy: SafetyPolicy::default(),
        }
    }
}

fn control_plane() -> &'static Mutex<RuntimeControlPlane> {
    static INSTANCE: OnceLock<Mutex<RuntimeControlPlane>> = OnceLock::new();
    INSTANCE.get_or_init(|| Mutex::new(RuntimeControlPlane::default()))
}

pub fn e2e_smoke_json() -> Result<String, PackageError> {
    let mut store = TestMemoryStore::new();
    let appended = append_session_message(
        &mut store,
        AppendSessionMessageRequest {
            tenant_id: "test_tenant".to_string(),
            user_id: "test_user".to_string(),
            session_id: "test_session".to_string(),
            role: "user".to_string(),
            content: "Use PostgreSQL for Atlas and remind me Friday".to_string(),
            idempotency_key: Some("testkit-message".to_string()),
        },
        20,
    )?;
    let consolidated = consolidate_session(
        &mut store,
        ConsolidationRequest {
            tenant_id: "test_tenant".to_string(),
            user_id: "test_user".to_string(),
            session_id: "test_session".to_string(),
            lane: ConsolidationLane::Fast,
            policy: IngestionPolicy {
                min_importance_score: 0.1,
                min_confidence_score: 0.1,
            },
        },
    )?;
    let memory_id = consolidated.accepted[0]
        .record_id
        .clone()
        .unwrap_or_default();
    let scheduled = schedule_reminder(
        &mut store,
        ScheduleReminderRequest {
            tenant_id: "test_tenant".to_string(),
            user_id: "test_user".to_string(),
            source_memory_id: memory_id.clone(),
            kind: crate::prospective::ReminderKind::FollowUp,
            title: "Check Atlas migration".to_string(),
            due_at: "9999999999".to_string(),
            timezone: "configured-by-user".to_string(),
            trace_id: None,
        },
    )?;
    let mut retrieval = RetrievalRequest::test("test_tenant", "test_user", "PostgreSQL");
    retrieval.session_id = Some("test_session".to_string());
    let context = assemble_working_context(&mut store, retrieval, 20)?;
    let forgotten = forget_memory(
        &mut store,
        ForgetMemoryRequest {
            tenant_id: "test_tenant".to_string(),
            user_id: "test_user".to_string(),
            memory_id,
            actor: "test_user".to_string(),
            reason: "testkit redaction".to_string(),
            redact: true,
        },
    )?;
    let payload = serde_json::json!({
        "status": "ok",
        "backend": "testkit",
        "message_id": appended.message_id,
        "consolidation_job": consolidated.job.id,
        "reminder_id": scheduled.reminder.id,
        "working_context_items": context.retrieved_memory_ids.len(),
        "redacted": forgotten.redaction_transition.is_some()
    });
    Ok(payload.to_string())
}

pub fn adapter_smoke_json(request_json: &str) -> Result<String, PackageError> {
    let request: AdapterSmokeRequest =
        serde_json::from_str(request_json).map_err(CoreError::from)?;
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
        &request.user_id,
        "Entity",
        &request.memory_id,
        0.8,
    )?)?;
    let related_ids = neo4j.related_memory_ids(&scope, std::slice::from_ref(&request.memory_id), request.graph_hops)?;
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

pub fn mcp_call_json(request_json: &str) -> Result<String, PackageError> {
    let request: McpCallRequest = serde_json::from_str(request_json).map_err(CoreError::from)?;
    match request.tool.as_str() {
        "nextral.memory.ingest" => {
            let payload: IngestMemoryRequest =
                serde_json::from_str(&request.payload_json).map_err(CoreError::from)?;
            let mut runtime = crate::runtime::TestRuntime::new();
            Ok(serde_json::to_string(&runtime.ingest(payload)?).map_err(CoreError::from)?)
        }
        "nextral.memory.retrieve" => {
            let payload: RetrievalRequest =
                serde_json::from_str(&request.payload_json).map_err(CoreError::from)?;
            let mut runtime = crate::runtime::TestRuntime::new();
            let mut response = runtime.retrieve(payload)?;
            let control = control_plane()
                .lock()
                .map_err(|error| CoreError::Conflict(error.to_string()))?;
            let severity = classify_severity(
                response.quality_score,
                if response.items.is_empty() { 1.0 } else { 0.0 },
                !response.telemetry.degraded_reasons.is_empty(),
            );
            response.severity = severity.clone();
            response.decision_action = decision_for(&control.safety_policy, &severity);
            response.lane = control.registry.current_lane.clone();
            response.policy_version = control.registry.active_policy_version.clone();
            Ok(serde_json::to_string(&response).map_err(CoreError::from)?)
        }
        "nextral.memory.forget" => {
            let payload: ForgetMemoryRequest =
                serde_json::from_str(&request.payload_json).map_err(CoreError::from)?;
            let mut store = TestMemoryStore::new();
            Ok(serde_json::to_string(&forget_memory(&mut store, payload)?).map_err(CoreError::from)?)
        }
        "nextral.reminders.due" => {
            let payload: ExecuteDueRemindersRequest =
                serde_json::from_str(&request.payload_json).map_err(CoreError::from)?;
            let mut store = TestMemoryStore::new();
            Ok(serde_json::to_string(&execute_due_reminders(&mut store, payload)?).map_err(CoreError::from)?)
        }
        "experiments.create" => {
            let payload: ExperimentCreateRequest =
                serde_json::from_str(&request.payload_json).map_err(CoreError::from)?;
            let mut control = control_plane()
                .lock()
                .map_err(|error| CoreError::Conflict(error.to_string()))?;
            let experiment = control
                .registry
                .create(payload.lane, payload.policy_version, payload.description);
            Ok(serde_json::to_string(&experiment).map_err(CoreError::from)?)
        }
        "experiments.promote" => {
            let payload: ExperimentPromoteRequest =
                serde_json::from_str(&request.payload_json).map_err(CoreError::from)?;
            let mut control = control_plane()
                .lock()
                .map_err(|error| CoreError::Conflict(error.to_string()))?;
            let report = report_from_severity(&payload.severity);
            if !canary_replay_gate(&report) {
                let quarantined = control
                    .registry
                    .promote(&payload.experiment_id, Severity::Destructive)
                    .ok_or_else(|| {
                        CoreError::NotFound(format!(
                            "experiment {} not found",
                            payload.experiment_id
                        ))
                    })?;
                return Ok(serde_json::to_string(&serde_json::json!({
                    "status": "blocked_by_replay_gate",
                    "report": report,
                    "experiment": quarantined
                }))
                .map_err(CoreError::from)?);
            }
            let experiment = control
                .registry
                .promote(&payload.experiment_id, payload.severity)
                .ok_or_else(|| {
                    CoreError::NotFound(format!(
                        "experiment {} not found",
                        payload.experiment_id
                    ))
                })?;
            Ok(serde_json::to_string(&experiment).map_err(CoreError::from)?)
        }
        "experiments.rollback" => {
            let payload: ExperimentRollbackRequest =
                serde_json::from_str(&request.payload_json).map_err(CoreError::from)?;
            let mut control = control_plane()
                .lock()
                .map_err(|error| CoreError::Conflict(error.to_string()))?;
            let experiment = control
                .registry
                .rollback(&payload.experiment_id, &payload.reason)
                .ok_or_else(|| {
                    CoreError::NotFound(format!(
                        "experiment {} not found",
                        payload.experiment_id
                    ))
                })?;
            Ok(serde_json::to_string(&experiment).map_err(CoreError::from)?)
        }
        "experiments.status" => {
            let id = if request.payload_json.trim().is_empty() {
                None
            } else {
                Some(
                    serde_json::from_str::<serde_json::Value>(&request.payload_json)
                        .map_err(CoreError::from)?
                        .get("experiment_id")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                )
            };
            let control = control_plane()
                .lock()
                .map_err(|error| CoreError::Conflict(error.to_string()))?;
            let status = control.registry.status(id.as_deref());
            Ok(serde_json::to_string(&status).map_err(CoreError::from)?)
        }
        "safety.policy.get" => {
            let control = control_plane()
                .lock()
                .map_err(|error| CoreError::Conflict(error.to_string()))?;
            Ok(serde_json::to_string(&control.safety_policy).map_err(CoreError::from)?)
        }
        "safety.policy.set" => {
            let payload: SafetyPolicySetRequest =
                serde_json::from_str(&request.payload_json).map_err(CoreError::from)?;
            let mut control = control_plane()
                .lock()
                .map_err(|error| CoreError::Conflict(error.to_string()))?;
            control
                .safety_policy
                .actions
                .insert(payload.severity, payload.action);
            Ok(serde_json::to_string(&control.safety_policy).map_err(CoreError::from)?)
        }
        "nextral.graph.query" => {
            let payload: RetrievalRequest =
                serde_json::from_str(&request.payload_json).map_err(CoreError::from)?;
            let mut store = TestMemoryStore::new();
            let response = crate::runtime::retrieval::retrieve(&mut store, payload)?;
            let graph_only: Vec<_> = response
                .items
                .into_iter()
                .filter(|item| !matches!(item.source_path, crate::retrieval::SourcePath::Vector))
                .collect();
            Ok(serde_json::to_string(&serde_json::json!({ "items": graph_only })).map_err(CoreError::from)?)
        }
        other => Err(PackageError {
            code: "invalid_input".to_string(),
            message: format!("unknown MCP tool: {other}"),
        }),
    }
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
        serde_json::from_str(request_json).map_err(CoreError::from)?;
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
        .map_err(CoreError::from)?;
    Ok(())
}

impl From<CoreError> for PackageError {
    fn from(error: CoreError) -> Self {
        let code = match &error {
            CoreError::InvalidInput(_) => "invalid_input",
            CoreError::NotFound(_) => "not_found",
            CoreError::Conflict(_) => "conflict",
            CoreError::Io(_) => "io",
            CoreError::Serialization(_) => "serialization",
        };
        Self {
            code: code.to_string(),
            message: error.to_string(),
        }
    }
}
