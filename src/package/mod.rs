use crate::contracts::{CoreError, CoreResult};
use crate::{
    adapters::{
        neo4j::Neo4jAdapter, postgres::PostgresAdapter, qdrant::QdrantAdapter, redis::RedisAdapter,
        s3::S3Adapter,
    },
    config::{IngestionPolicy, NextralConfig},
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
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use parking_lot::Mutex;
use std::time::Duration;
use crate::providers::Clock;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PackageError {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_status: Option<String>,
}

impl std::fmt::Display for PackageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for PackageError {}

#[derive(Debug, Clone, Default)]
pub struct CallOptions {
    pub timeout_ms: Option<u64>,
    pub cancellation_token: Option<tokio_util::sync::CancellationToken>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShutdownReport {
    pub drained: bool,
    pub in_flight_remaining: usize,
}

pub struct ServiceHandle {
    pub(crate) shutdown_token: tokio_util::sync::CancellationToken,
    pub(crate) join_handle: Mutex<Option<tokio::task::JoinHandle<CoreResult<crate::api::ServiceRuntimeReport>>>>,
    pub(crate) binds: Vec<String>,
}

impl ServiceHandle {
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), PackageError> {
        self.shutdown_token.cancel();
        let handle = {
            let mut guard = self.join_handle.lock();
            guard.take()
        };
        if let Some(handle) = handle {
            let _ = tokio::time::timeout(timeout, handle).await;
        }
        Ok(())
    }

    pub fn binds(&self) -> &[String] {
        &self.binds
    }
}

#[derive(Clone)]
pub struct WorkerHandle {
    pub(crate) shutdown_token: tokio_util::sync::CancellationToken,
    pub(crate) join_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    pub(crate) tick_lock: Arc<tokio::sync::Mutex<()>>,
    pub(crate) client: RuntimeClient,
    pub(crate) config: crate::runtime::prospective::WorkerConfig,
}
impl std::fmt::Debug for WorkerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerHandle").finish()
    }
}


impl WorkerHandle {
    pub async fn tick(&self) -> Result<crate::runtime::prospective::WorkerTickReport, PackageError> {
        let Ok(_guard) = self.tick_lock.try_lock() else {
            return Err(PackageError {
                code: "conflict".into(),
                message: "worker tick already in progress".into(),
                execution_status: None,
            });
        };
        let store = self.client.store().clone();
        let config = self.config.clone();
        let report = tokio::task::spawn_blocking(move || {
            crate::runtime::prospective::run_worker_tick(&store, &config)
        })
        .await
        .map_err(|e| PackageError {
            code: "io".into(),
            message: e.to_string(),
            execution_status: None,
        })??;
        Ok(report)
    }

    pub async fn shutdown(&self, timeout: Duration) -> Result<(), PackageError> {
        self.shutdown_token.cancel();
        let handle = {
            let mut guard = self.join_handle.lock();
            guard.take()
        };
        if let Some(handle) = handle {
            let _ = tokio::time::timeout(timeout, handle).await;
        }
        Ok(())
    }
    pub fn tick_lock(&self) -> Arc<tokio::sync::Mutex<()>> {
        self.tick_lock.clone()
    }
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

#[derive(Clone)]
pub struct RuntimeClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    config: NextralConfig,
    store: RuntimeStore,
    clock: Arc<dyn Clock>,
    #[allow(dead_code)]
    telemetry_lease: Option<crate::runtime::observability::ObservabilityLease>,
    in_flight_semaphore: Arc<tokio::sync::Semaphore>,
    pending_count: AtomicUsize,
    pending_bytes: AtomicUsize,
    closed: AtomicBool,
    close_token: tokio_util::sync::CancellationToken,
    active_service: Mutex<Option<Arc<ServiceHandle>>>,
    active_worker: Mutex<Option<Arc<WorkerHandle>>>,
}

struct AdmissionGuard {
    inner: Arc<ClientInner>,
    bytes: usize,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        self.inner.pending_count.fetch_sub(1, Ordering::Release);
        self.inner.pending_bytes.fetch_sub(self.bytes, Ordering::Release);
    }
}

impl RuntimeClient {
    pub fn from_config(config: NextralConfig) -> Result<Self, PackageError> {
        Self::from_config_with_clock(config, Arc::new(crate::providers::SystemClock))
    }

    pub fn from_config_with_clock(
        config: NextralConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, PackageError> {
        config.validate().map_err(PackageError::from)?;
        let telemetry_lease = crate::runtime::observability::acquire_lease(&config.observability)
            .map_err(PackageError::from)?;
        let store = RuntimeStore::configured(config.clone()).map_err(PackageError::from)?;
        let in_flight = config.execution.max_in_flight;
        Ok(Self {
            inner: Arc::new(ClientInner {
                config,
                store,
                clock,
                telemetry_lease: Some(telemetry_lease),
                in_flight_semaphore: Arc::new(tokio::sync::Semaphore::new(in_flight)),
                pending_count: AtomicUsize::new(0),
                pending_bytes: AtomicUsize::new(0),
                closed: AtomicBool::new(false),
                close_token: tokio_util::sync::CancellationToken::new(),
                active_service: Mutex::new(None),
                active_worker: Mutex::new(None),
            }),
        })
    }

    pub fn config(&self) -> &NextralConfig {
        &self.inner.config
    }

    pub fn store(&self) -> &RuntimeStore {
        &self.inner.store
    }

    pub fn readiness(&self) -> serde_json::Value {
        self.inner.store.readiness()
    }

    pub async fn call_json(
        &self,
        tool: &str,
        payload_json: String,
        options: CallOptions,
    ) -> Result<String, PackageError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(PackageError {
                code: "conflict".into(),
                message: "client is closed".into(),
                execution_status: Some("not_started".into()),
            });
        }
        let payload_bytes = payload_json.len();
        if payload_bytes > MAX_PAYLOAD_BYTES {
            return Err(PackageError {
                code: "invalid_input".into(),
                message: "request exceeds maximum payload size".into(),
                execution_status: Some("not_started".into()),
            });
        }
        let max_pending = self.inner.config.execution.max_pending;
        let max_pending_bytes = self.inner.config.execution.max_pending_bytes;

        let mut current_count = self.inner.pending_count.load(Ordering::Relaxed);
        loop {
            if current_count >= max_pending {
                crate::runtime::observability::record_admission(false);
                return Err(PackageError {
                  code: "resource_exhausted".into(),
                  message: "admission capacity exceeded".into(),
                  execution_status: Some("not_started".into()),
                });
            }
            match self.inner.pending_count.compare_exchange_weak(
                current_count,
                current_count + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => current_count = c,
            }
        }

        let mut current_bytes = self.inner.pending_bytes.load(Ordering::Relaxed);
        loop {
            if current_bytes.saturating_add(payload_bytes) > max_pending_bytes {
                self.inner.pending_count.fetch_sub(1, Ordering::Release);
                crate::runtime::observability::record_admission(false);
                return Err(PackageError {
                    code: "resource_exhausted".into(),
                    message: "admission byte capacity exceeded".into(),
                    execution_status: Some("not_started".into()),
                });
            }
            match self.inner.pending_bytes.compare_exchange_weak(
                current_bytes,
                current_bytes + payload_bytes,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(b) => current_bytes = b,
            }
        }

        let admission_guard = AdmissionGuard {
            inner: Arc::clone(&self.inner),
            bytes: payload_bytes,
        };
        crate::runtime::observability::record_admission(true);

        let request_timeout_ms = self.inner.config.execution.request_timeout_ms;
        let effective_timeout_ms = options.timeout_ms.map_or(request_timeout_ms, |t| t.min(request_timeout_ms));
        let deadline = self.inner.clock.monotonic_now() + Duration::from_millis(effective_timeout_ms);

        let sem = Arc::clone(&self.inner.in_flight_semaphore);
        let cancel = options.cancellation_token.clone();
        let close_token = self.inner.close_token.clone();
        let now = self.inner.clock.monotonic_now();
        let queue_timeout = deadline.saturating_duration_since(now);
        if queue_timeout.is_zero() {
            crate::runtime::observability::record_queue_timeout();
            return Err(PackageError {
                code: "deadline_exceeded".into(),
                message: "deadline exceeded before admission".into(),
                execution_status: Some("not_started".into()),
            });
        }

        let permit = tokio::select! {
            permit = sem.acquire_owned() => {
                match permit {
                    Ok(p) => p,
                    Err(_) => {
                        return Err(PackageError {
                            code: "conflict".into(),
                            message: "client is closed".into(),
                            execution_status: Some("not_started".into()),
                        });
                    }
                }
            }
            _ = tokio::time::sleep(queue_timeout) => {
                crate::runtime::observability::record_queue_timeout();
                return Err(PackageError {
                    code: "deadline_exceeded".into(),
                    message: "request deadline exceeded in queue".into(),
                    execution_status: Some("not_started".into()),
                });
            }
            _ = close_token.cancelled() => {
                return Err(PackageError {
                    code: "conflict".into(),
                    message: "client closed".into(),
                    execution_status: Some("not_started".into()),
                });
            }
            _ = async {
                if let Some(ref c) = cancel {
                    c.cancelled().await
                } else {
                    std::future::pending::<()>().await
                }
            } => {
                return Err(PackageError {
                    code: "cancelled".into(),
                    message: "request cancelled".into(),
                    execution_status: Some("not_started".into()),
                });
            }
        };

        let remaining = deadline.saturating_duration_since(self.inner.clock.monotonic_now());
        let tool_owned = tool.to_string();
        let store = self.inner.store.clone();
        let config = self.inner.config.clone();

        let blocking_task = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _guard = admission_guard;
            dispatch_tool(&store, &config, &tool_owned, &payload_json)
        });

        let res = tokio::select! {
            task_res = blocking_task => {
                match task_res {
                    Ok(res) => res,
                    Err(join_err) => {
                        Err(PackageError {
                            code: "io".into(),
                            message: format!("task panicked: {join_err}"),
                            execution_status: Some("outcome_unknown".into()),
                        })
                    }
                }
            }
            _ = tokio::time::sleep(remaining) => {
                crate::runtime::observability::record_execution_timeout();
                Err(PackageError {
                    code: "deadline_exceeded".into(),
                    message: "runtime request deadline exceeded".into(),
                    execution_status: Some("outcome_unknown".into()),
                })
            }
            _ = async {
                if let Some(ref c) = cancel {
                    c.cancelled().await
                } else {
                    std::future::pending::<()>().await
                }
            } => {
                Err(PackageError {
                    code: "cancelled".into(),
                    message: "request cancelled during execution".into(),
                    execution_status: Some("outcome_unknown".into()),
                })
            }
        };
        res
    }

    pub async fn close(&self, timeout: Duration) -> Result<ShutdownReport, PackageError> {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.close_token.cancel();

        let service_handle = {
            let mut guard = self.inner.active_service.lock();
            guard.take()
        };
        if let Some(service) = service_handle {
            let _ = service.shutdown(timeout).await;
        }

        let worker_handle = {
            let mut guard = self.inner.active_worker.lock();
            guard.take()
        };
        if let Some(worker) = worker_handle {
            let _ = worker.shutdown(timeout).await;
        }

        let start = std::time::Instant::now();
        let max_in_flight = self.inner.config.execution.max_in_flight;
        loop {
            let available = self.inner.in_flight_semaphore.available_permits();
            let in_flight = max_in_flight.saturating_sub(available);
            if in_flight == 0 {
                return Ok(ShutdownReport {
                    drained: true,
                    in_flight_remaining: 0,
                });
            }
            if start.elapsed() >= timeout {
                return Ok(ShutdownReport {
                    drained: false,
                    in_flight_remaining: in_flight,
                });
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn serve(&self) -> Result<ServiceHandle, PackageError> {
        {
            let active = self.inner.active_service.lock();
            if active.is_some() {
                return Err(PackageError {
                    code: "conflict".into(),
                    message: "service host already running for this client".into(),
                    execution_status: None,
                });
            }
        }
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        let token = shutdown_token.clone();
        let client_clone = self.clone();

        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

        let join_handle = tokio::spawn(async move {
            crate::api::serve_client(client_clone, token, ready_tx).await
        });

        let binds = ready_rx.await.map_err(|_| PackageError {
            code: "io".into(),
            message: "service host startup failed".into(),
            execution_status: None,
        })??;

        let handle = Arc::new(ServiceHandle {
            shutdown_token: shutdown_token.clone(),
            join_handle: Mutex::new(Some(join_handle)),
            binds: binds.clone(),
        });
        {
            let mut active = self.inner.active_service.lock();
            *active = Some(handle.clone());
        }
        Ok(ServiceHandle {
            shutdown_token,
            join_handle: Mutex::new(None),
            binds,
        })
    }

    pub async fn start_worker(
        &self,
        config: crate::runtime::prospective::WorkerConfig,
    ) -> Result<WorkerHandle, PackageError> {
        config.validate().map_err(PackageError::from)?;
        {
            let active = self.inner.active_worker.lock();
            if active.is_some() {
                return Err(PackageError {
                    code: "conflict".into(),
                    message: "worker already running for this client".into(),
                    execution_status: None,
                });
            }
        }
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        let token = shutdown_token.clone();
        let tick_lock = Arc::new(tokio::sync::Mutex::new(()));
        let tick_lock_loop = tick_lock.clone();
        let store = self.store().clone();
        let worker_cfg = config.clone();
        let interval = Duration::from_millis(config.poll_interval_ms);

        let join_handle = tokio::spawn(async move {
            while !token.is_cancelled() {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = tokio::time::sleep(interval) => {
                        if let Ok(_guard) = tick_lock_loop.try_lock() {
                            let s = store.clone();
                            let c = worker_cfg.clone();
                            let _ = tokio::task::spawn_blocking(move || {
                                crate::runtime::prospective::run_worker_tick(&s, &c)
                            }).await;
                        }
                    }
                }
            }
        });

        let handle = WorkerHandle {
            shutdown_token: shutdown_token.clone(),
            join_handle: Arc::new(Mutex::new(Some(join_handle))),
            tick_lock,
            client: self.clone(),
            config,
        };
        {
            let mut active = self.inner.active_worker.lock();
            *active = Some(Arc::new(handle.clone()));
        }
        Ok(handle)
    }
}

impl Drop for RuntimeClient {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            self.inner.closed.store(true, Ordering::Release);
            self.inner.close_token.cancel();
        }
    }
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
            filters: None,
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
            execution_status: None,
        });
    }
    if tenant_id.len() > 128 || user_id.len() > 128 {
        return Err(PackageError {
            code: "invalid_input".into(),
            message: "tenant_id and user_id must not exceed 128 characters".into(),
            execution_status: None,
        });
    }
    Ok(())
}

pub fn dispatch_tool(
    store: &RuntimeStore,
    config: &NextralConfig,
    tool: &str,
    payload_json: &str,
) -> Result<String, PackageError> {
    let mut span = crate::runtime::observability::start_span("nextral.dispatch");
    let result = dispatch_tool_impl(store, config, tool, payload_json);
    if result.is_err() {
        span.fail();
    }
    result
}

fn dispatch_tool_impl(
    store: &RuntimeStore,
    _config: &NextralConfig,
    tool: &str,
    payload_json: &str,
) -> Result<String, PackageError> {
    let payload: serde_json::Value =
        serde_json::from_str(payload_json).map_err(invalid_json_input)?;
    if tool == "nextral.health" {
        return Ok(store.readiness().to_string());
    }
    let mut store = store.clone();
    if tool.starts_with("nextral.")
        && !matches!(
            tool,
            "nextral.database.migrate"
                | "nextral.reembed.plan"
                | "nextral.jobs.reembed.execute"
                | "nextral.outbox.run"
                | "nextral.batch.ingest"
                | "nextral.batch.retrieve"
                | "nextral.batch.forget"
                | "nextral.test.delay"
                | "nextral.index.repair"
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
    let store = &mut store;
    let result = match tool {
        "nextral.test.delay" => {
            let ms = payload.get("delay_ms").and_then(|v| v.as_u64()).unwrap_or(100);
            std::thread::sleep(Duration::from_millis(ms));
            serde_json::json!({"status": "delayed"})
        }
        "nextral.database.migrate" => store.migrate()?,
        "nextral.outbox.run" => store.drain_outbox(string_field("tenant_id")?)?,
        "nextral.memory.ingest" => {
            let result = crate::ingestion::ingest_memory(store, decode!(IngestMemoryRequest))?;
            value!(result)
        }
        "nextral.memory.retrieve" => {
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
        "nextral.memory.list" | "nextral.memory.query" => {
            let mut query = decode!(crate::memory::MemoryQuery);
            query.tenant_id = tenant.to_string();
            query.user_id = user.to_string();
            let page = store.query_memories(&query)?;
            value!(page)
        }
        "nextral.index.repair" => {
            let limit = payload.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
            if limit == 0 || limit > 1000 {
                return Err(CoreError::InvalidInput("limit must be between 1 and 1000".into()).into());
            }
            let res = match store {
                RuntimeStore::Test(_) => serde_json::json!({
                    "repaired": 0,
                    "checked": 0,
                    "updated": 0,
                    "removed": 0,
                    "remaining": 0,
                    "complete": true,
                }),
                RuntimeStore::Production(p) => {
                    let mut r = p.index_repair(tenant, user, limit)?;
                    let repaired = r.get("repaired").and_then(|v| v.as_u64()).or_else(|| {
                        let u = r.get("updated").and_then(|v| v.as_u64()).unwrap_or(0);
                        let rm = r.get("removed").and_then(|v| v.as_u64()).unwrap_or(0);
                        Some(u + rm)
                    }).unwrap_or(0);
                    r["repaired"] = serde_json::json!(repaired);
                    r
                }
            };
            value!(res)
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
            let mut jobs: Vec<serde_json::Value> = store
                .lifecycle_job_list(tenant, user)?
                .into_iter()
                .map(|j| serde_json::to_value(j).unwrap())
                .collect();
            for checkpoint in store.lifecycle_list(tenant, &format!("reembed/{user}/"))? {
                let report = checkpoint["report"].clone();
                jobs.push(serde_json::json!({"id":report["plan_id"],"job_type":"reembed","status":report["status"],"report":report}));
            }
            value!(jobs)
        }
        "nextral.jobs.get" => {
            let id = string_field("job_id")?;
            crate::store::validate_scope_component(id, "job_id")?;
            if let Some(job) = store.lifecycle_job_get(tenant, user, id)? {
                value!(job)
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
            let array_name = if tool == "nextral.batch.retrieve" {
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
                crate::memory::deterministic_id(&[&tool, &payload[array_name].to_string()]);
            let key = payload["idempotency_key"].as_str().map(|k| {
                format!(
                    "idempotency/{}",
                    crate::memory::deterministic_id(&[batch_tenant, batch_user, &tool, k])
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
                    if tool != "nextral.batch.retrieve"
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
                let result: CoreResult<serde_json::Value> = (|| match tool {
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
                    &serde_json::json!({"tenant_id":batch_tenant,"user_id":batch_user,"tool":tool,"fingerprint":fingerprint,"response":(tool != "nextral.batch.retrieve").then_some(&result)}),
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
        tool,
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
            CoreError::ResourceExhausted(msg) => ("resource_exhausted", msg.as_str()),
            CoreError::DeadlineExceeded(msg) => ("deadline_exceeded", msg.as_str()),
            CoreError::Cancelled(msg) => ("cancelled", msg.as_str()),
            CoreError::LeaseLost(msg) => ("lease_lost", msg.as_str()),
            CoreError::IndexNotReady(msg) => ("index_not_ready", msg.as_str()),
            CoreError::Io(_) => ("io", "internal error"),
            CoreError::Serialization(_) => ("serialization", "invalid input format"),
        };
        Self {
            code: code.to_string(),
            message: sanitize_error_message(raw_message),
            execution_status: None,
        }
    }
}
