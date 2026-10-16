pub mod auth;
pub use auth::Principal;
pub mod graphql;
pub mod grpc;

pub mod http;
#[cfg(test)]
mod tests;

use crate::{
    config::NextralConfig,
    contracts::{CoreError, CoreResult},
};
use auth::Authenticator;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    future::Future,
    sync::{Arc, LazyLock},
    time::Duration,
};
use tokio::sync::{watch, Semaphore};

pub(crate) const MAX_BODY: usize = 10 * 1024 * 1024;
pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_IN_FLIGHT: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ServiceMode {
    Http,
    Grpc,
    Graphql,
    All,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceStartupPlan {
    pub modes: Vec<ServiceMode>,
    pub http_bind: Option<String>,
    pub grpc_bind: Option<String>,
    pub graphql_bind: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceRuntimeReport {
    pub status: String,
    pub binds: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackendReadiness {
    pub backend: String,
    pub status: String,
    pub degraded_signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StartupReadinessMatrix {
    pub fail_fast: bool,
    pub backends: Vec<BackendReadiness>,
}

#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: u16,
    pub code: String,
    pub message: String,
}
impl ApiError {
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::new(400, "invalid_input", message)
    }
    pub(crate) fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(401, "unauthenticated", message)
    }
    pub(crate) fn forbidden(message: impl Into<String>) -> Self {
        Self::new(403, "permission_denied", message)
    }
    pub(crate) fn new(status: u16, code: &str, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
        }
    }
}
impl From<crate::package::PackageError> for ApiError {
    fn from(error: crate::package::PackageError) -> Self {
        let status = match error.code.as_str() {
            "invalid_input" | "serialization" | "serialization_error" => 400,
            "not_found" => 404,
            "conflict" => 409,
            "unauthenticated" => 401,
            "permission_denied" => 403,
            "resource_exhausted" => 429,
            "deadline_exceeded" => 504,
            "cancelled" => 408,
            "lease_lost" => 409,
            "index_not_ready" => 503,
            "io" | "io_error" | "unavailable" | "configuration" => 503,
            _ => 500,
        };
        Self {
            status,
            code: error.code,
            message: error.message,
        }
    }
}

struct TokenBucket {
    tokens: f64,
    last_refill: std::time::Instant,
}

pub(crate) struct ScopedRateLimiter {
    enabled: bool,
    max_rps: f64,
    burst_size: f64,
    buckets: parking_lot::Mutex<std::collections::HashMap<(String, String), TokenBucket>>,
}

impl ScopedRateLimiter {
    pub(crate) fn new(config: &crate::config::RateLimitConfig) -> Self {
        Self {
            enabled: config.enabled,
            max_rps: config.max_requests_per_second as f64,
            burst_size: config.burst_size as f64,
            buckets: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub(crate) fn check_and_consume(&self, tenant_id: &str, user_id: &str) -> bool {
        if !self.enabled {
            return true;
        }
        let now = std::time::Instant::now();
        let mut buckets = self.buckets.lock();
        if buckets.len() > 10_000 {
            buckets.retain(|_, b| now.duration_since(b.last_refill) < std::time::Duration::from_secs(3600));
        }
        let key = (tenant_id.to_string(), user_id.to_string());
        let bucket = buckets.entry(key).or_insert_with(|| TokenBucket {
            tokens: self.burst_size,
            last_refill: now,
        });
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.max_rps).min(self.burst_size);
        bucket.last_refill = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[derive(Clone)]
pub struct ServiceState {
    pub config: Arc<NextralConfig>,
    pub(crate) auth: Authenticator,
    pub client: Arc<crate::package::RuntimeClient>,
    permits: Arc<Semaphore>,
    request_permits: Arc<Semaphore>,
    pub(crate) rate_limiter: Arc<ScopedRateLimiter>,
}

impl ServiceState {
    pub fn new(config: NextralConfig) -> CoreResult<Self> {
        config.validate()?;
        let auth = Authenticator::new(&config)?;
        let client = Arc::new(
            crate::package::RuntimeClient::from_config(config.clone())
                .map_err(|e| CoreError::InvalidInput(e.message))?,
        );
        Ok(Self {
            config: Arc::new(config.clone()),
            auth,
            client,
            permits: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            request_permits: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            rate_limiter: Arc::new(ScopedRateLimiter::new(&config.rate_limit)),
        })
    }

    pub(crate) fn new_with_client(
        config: NextralConfig,
        client: Arc<crate::package::RuntimeClient>,
    ) -> CoreResult<Self> {
        config.validate()?;
        let auth = Authenticator::new(&config)?;
        Ok(Self {
            config: Arc::new(config.clone()),
            auth,
            client,
            permits: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            request_permits: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            rate_limiter: Arc::new(ScopedRateLimiter::new(&config.rate_limit)),
        })
    }

    pub(crate) async fn probe(&self) -> Result<StartupReadinessMatrix, ApiError> {
        let matrix = serde_json::from_value(self.client.readiness())
            .map_err(|e| ApiError::new(503, "unavailable", e.to_string()))?;
        Ok(matrix)
    }
    pub async fn call(
        &self,
        principal: &Principal,
        tool: &'static str,
        mut payload: Value,
    ) -> Result<Value, ApiError> {
        principal.authorize(&payload)?;
        let tenant_str = payload.get("tenant_id").and_then(Value::as_str).unwrap_or("");
        let user_str = payload.get("user_id").and_then(Value::as_str).unwrap_or("");
        if !self.rate_limiter.check_and_consume(tenant_str, user_str) {
            return Err(ApiError::new(
                429,
                "resource_exhausted",
                "rate limit exceeded for scope",
            ));
        }
        self.prepare(tool, &mut payload)?;
        let permit = Arc::clone(&self.permits).try_acquire_owned().map_err(|_| {
            ApiError::new(429, "resource_exhausted", "runtime request limit reached")
        })?;
        let tenant = payload["tenant_id"].clone();
        let user = payload["user_id"].clone();
        let workflow_id = format!(
            "forget/{}/{}",
            payload["user_id"].as_str().unwrap_or(""),
            payload["memory_id"].as_str().unwrap_or("")
        );
        let due = payload
            .get("due_at_or_before")
            .filter(|v| !v.is_null())
            .map(|value| {
                let text = value.as_str().ok_or_else(|| {
                    ApiError::invalid("due_at_or_before must be a timestamp string")
                })?;
                crate::runtime::lifecycle::timestamp_epoch(text)
                    .map_err(|e| ApiError::invalid(e.to_string()))
            })
            .transpose()?;
        let response = self
            .client
            .call_json(
                tool,
                payload.to_string(),
                crate::package::CallOptions {
                    timeout_ms: Some(REQUEST_TIMEOUT.as_millis() as u64),
                    cancellation_token: None,
                },
            )
            .await
            .map_err(ApiError::from)?;
        let _permit = permit;
        let mut value: Value = serde_json::from_str(&response)
            .map_err(|e| ApiError::new(500, "internal", e.to_string()))?;
        match tool {
            "nextral.memory.ingest" => {
                let id = value.get("record_id").cloned().unwrap_or(Value::Null);
                value["memory_id"] = id.clone();
                value["dedupe_key"] = id;
                if value["status"] == "rejected" {
                    return Err(ApiError::invalid(value["validation_errors"].to_string()));
                }
            }
            "nextral.memory.retrieve" => {
                value["tenant_id"] = tenant;
                value["user_id"] = user;
            }
            "nextral.memory.forget" => {
                value["workflow_id"] = json!(workflow_id);
            }
            "nextral.memory.get" if !value.is_null() && !self.visible(&value)? => {
                return Err(ApiError::forbidden(
                    "memory privacy exceeds configured policy",
                ));
            }
            "nextral.memory.get" => {}
            "nextral.reminders.list" => {
                let reminders = value.as_array_mut().ok_or_else(|| {
                    ApiError::new(500, "internal", "runtime reminder list must be an array")
                })?;
                let mut selected = Vec::with_capacity(reminders.len());
                for reminder in reminders.drain(..) {
                    if !self.visible(&reminder)? {
                        continue;
                    }
                    if let Some(cutoff) = due {
                        let timestamp =
                            reminder
                                .get("due_at")
                                .and_then(Value::as_str)
                                .ok_or_else(|| {
                                    ApiError::new(500, "internal", "reminder due_at missing")
                                })?;
                        let timestamp = crate::runtime::lifecycle::timestamp_epoch(timestamp)
                            .map_err(|e| ApiError::new(500, "internal", e.to_string()))?;
                        let status = reminder["status"].as_str().unwrap_or("");
                        if timestamp > cutoff
                            || !matches!(status, "scheduled" | "retry_scheduled" | "due")
                        {
                            continue;
                        }
                    }
                    selected.push(reminder);
                }
                *reminders = selected;
            }
            _ => {}
        }
        Ok(value)
    }
    fn visible(&self, value: &Value) -> Result<bool, ApiError> {
        let allowed = serde_json::to_value(&self.config.retrieval_policy.privacy_scope)
            .map_err(|e| ApiError::new(500, "internal", e.to_string()))?;
        Ok(allowed
            .as_array()
            .is_some_and(|scope| scope.contains(&value["privacy_level"])))
    }

    fn prepare(&self, tool: &str, payload: &mut Value) -> Result<(), ApiError> {
        let fields = payload
            .as_object_mut()
            .ok_or_else(|| ApiError::invalid("request must be an object"))?;
        for name in ["embedding", "query_vector"] {
            if let Some(value) = fields.get(name).filter(|value| !value.is_null()) {
                let vector = value
                    .as_array()
                    .ok_or_else(|| ApiError::invalid(format!("{name} must be a numeric vector")))?;
                if vector.is_empty()
                    || vector.iter().any(|value| {
                        !value
                            .as_f64()
                            .is_some_and(|number| (number as f32).is_finite())
                    })
                {
                    return Err(ApiError::invalid(format!(
                        "{name} must be a nonempty vector of finite floats"
                    )));
                }
            }
        }
        match tool {
            "nextral.memory.ingest" => {
                // The host controls policy; clients cannot lower configured write thresholds.
                fields.insert(
                    "policy".into(),
                    serde_json::to_value(&self.config.ingestion_policy)
                        .map_err(|e| ApiError::invalid(e.to_string()))?,
                );
                default(fields, "source_type", json!("realtime"));
                default(fields, "importance_score", json!(0.0));
                default(fields, "confidence_score", Value::Null);
                for name in ["entities", "tags", "graph_hints", "source_message_ids"] {
                    default(fields, name, json!([]));
                }
            }
            "nextral.memory.retrieve" => {
                for name in ["token_budget", "top_k_vector", "max_graph_hops"] {
                    if fields
                        .get(name)
                        .and_then(Value::as_i64)
                        .is_some_and(|v| v < 1)
                    {
                        return Err(ApiError::invalid(format!("{name} must be positive")));
                    }
                }
                default(
                    fields,
                    "token_budget",
                    json!(self.config.retrieval_policy.token_budget),
                );
                default(
                    fields,
                    "top_k_vector",
                    json!(self.config.retrieval_policy.top_k_vector),
                );
                default(
                    fields,
                    "max_graph_hops",
                    json!(self.config.retrieval_policy.max_graph_hops),
                );
                default(
                    fields,
                    "privacy_scope",
                    json!(self.config.retrieval_policy.privacy_scope),
                );
                default(fields, "entities", json!([]));
                if fields["token_budget"]
                    .as_u64()
                    .is_some_and(|v| v > u64::from(self.config.retrieval_policy.token_budget))
                {
                    return Err(ApiError::invalid("token_budget exceeds configured limit"));
                }
                if fields["top_k_vector"].as_u64().is_some_and(|v| v > 1000)
                    || fields["max_graph_hops"].as_u64().is_some_and(|v| v > 10)
                {
                    return Err(ApiError::invalid("retrieval bounds exceeded"));
                }
                if let Some(scope) = fields["privacy_scope"].as_array() {
                    let allowed = serde_json::to_value(&self.config.retrieval_policy.privacy_scope)
                        .map_err(|e| ApiError::invalid(e.to_string()))?;
                    if scope
                        .iter()
                        .any(|v| !allowed.as_array().is_some_and(|a| a.contains(v)))
                    {
                        return Err(ApiError::forbidden(
                            "privacy scope exceeds configured policy",
                        ));
                    }
                }
            }
            "nextral.memory.forget" => {
                if let Some(actor) = fields.remove("actor_id") {
                    fields.insert("actor".into(), actor);
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn default(fields: &mut serde_json::Map<String, Value>, key: &str, value: Value) {
    if fields.get(key).is_none_or(Value::is_null) {
        fields.insert(key.into(), value);
    }
}

pub fn startup_plan(config: &NextralConfig, mode: ServiceMode) -> CoreResult<ServiceStartupPlan> {
    config.validate()?;
    Ok(ServiceStartupPlan {
        modes: if mode == ServiceMode::All {
            vec![ServiceMode::Http, ServiceMode::Grpc, ServiceMode::Graphql]
        } else {
            vec![mode]
        },
        http_bind: config.service.http_bind.clone(),
        grpc_bind: config.service.grpc_bind.clone(),
        graphql_bind: config.service.graphql_bind.clone(),
    })
}

pub fn startup_readiness_matrix(config: &NextralConfig) -> CoreResult<StartupReadinessMatrix> {
    config.validate()?;
    let store = crate::runtime::storage::RuntimeStore::configured(config.clone())?;
    Ok(serde_json::from_value(store.readiness())?)
}


fn shutdown_sender() -> &'static watch::Sender<bool> {
    static SHUTDOWN: LazyLock<watch::Sender<bool>> = LazyLock::new(|| watch::channel(false).0);
    &SHUTDOWN
}


pub fn run_service_hosts(
    config: &NextralConfig,
    mode: ServiceMode,
) -> CoreResult<ServiceRuntimeReport> {
    let modes = if mode == ServiceMode::All {
        vec![ServiceMode::Http, ServiceMode::Grpc, ServiceMode::Graphql]
    } else {
        vec![mode]
    };
    run_blocking(config.clone(), modes)
}


fn run_blocking(
    config: NextralConfig,
    modes: Vec<ServiceMode>,
) -> CoreResult<ServiceRuntimeReport> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(CoreError::InvalidInput(
            "blocking service host cannot run inside a Tokio runtime; use serve_with_shutdown"
                .into(),
        ));
    }
    config.validate()?;
    let state = ServiceState::new(config)?;
    shutdown_sender().send_replace(false);
    let mut shutdown = shutdown_sender().subscribe();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .max_blocking_threads(MAX_IN_FLIGHT + 4)
        .build()?;
    let result = runtime.block_on(serve_state(state, modes, async move {
        tokio::select! {
            _ = shutdown.changed() => {},
            _ = shutdown_signal() => {},
        }
    }));
    runtime.shutdown_timeout(REQUEST_TIMEOUT);
    result
}

pub async fn serve_client(
    client: crate::package::RuntimeClient,
    shutdown_token: tokio_util::sync::CancellationToken,
    ready_sender: tokio::sync::oneshot::Sender<Result<Vec<String>, crate::package::PackageError>>,
) -> CoreResult<ServiceRuntimeReport> {
    let config = client.config().clone();
    let state = ServiceState::new_with_client(config.clone(), Arc::new(client))?;
    let mut modes = Vec::new();
    if config.service.http_bind.is_some() {
        modes.push(ServiceMode::Http);
    }
    if config.service.grpc_bind.is_some() {
        modes.push(ServiceMode::Grpc);
    }
    if config.service.graphql_bind.is_some() {
        modes.push(ServiceMode::Graphql);
    }
    if modes.is_empty() {
        modes.push(ServiceMode::Http);
    }
    serve_state_ready(
        state,
        modes,
        async move {
            shutdown_token.cancelled().await;
        },
        Some(ready_sender),
    )
    .await
}

/// Serves an already configured runtime until the supplied future resolves.
pub async fn serve_with_shutdown<F>(
    config: NextralConfig,
    modes: Vec<ServiceMode>,
    shutdown: F,
) -> CoreResult<ServiceRuntimeReport>
where
    F: Future<Output = ()> + Send + 'static,
{
    let state = tokio::task::spawn_blocking(move || ServiceState::new(config))
        .await
        .map_err(|e| CoreError::Io(e.to_string()))??;
    let result = serve_state(state, modes, shutdown).await;
    let telemetry = tokio::task::spawn_blocking(crate::runtime::observability::shutdown)
        .await
        .map_err(|error| CoreError::Io(error.to_string()))?;
    result.and_then(|report| {
        telemetry?;
        Ok(report)
    })
}

async fn serve_state<F>(
    state: ServiceState,
    modes: Vec<ServiceMode>,
    shutdown: F,
) -> CoreResult<ServiceRuntimeReport>
where
    F: Future<Output = ()> + Send + 'static,
{
    serve_state_ready(state, modes, shutdown, None).await
}

pub(crate) async fn serve_state_ready<F>(
    state: ServiceState,
    modes: Vec<ServiceMode>,
    shutdown: F,
    mut ready_sender: Option<tokio::sync::oneshot::Sender<Result<Vec<String>, crate::package::PackageError>>>,
) -> CoreResult<ServiceRuntimeReport>
where
    F: Future<Output = ()> + Send + 'static,
{
    let mut active = Vec::new();
    for mode in modes {
        for mode in if mode == ServiceMode::All {
            vec![ServiceMode::Http, ServiceMode::Grpc, ServiceMode::Graphql]
        } else {
            vec![mode]
        } {
            if !active.contains(&mode) {
                active.push(mode);
            }
        }
    }
    if active.is_empty() {
        let err = CoreError::InvalidInput("at least one service mode is required".into());
        if let Some(tx) = ready_sender.take() {
            let _ = tx.send(Err(crate::package::PackageError::from(CoreError::InvalidInput("at least one service mode is required".into()))));
        }
        return Err(err);
    }
    let mut listeners = Vec::new();
    let mut binds = Vec::new();
    for mode in active {
        let (scheme, bind) = match mode {
            ServiceMode::Http => ("http", state.config.service.http_bind.as_deref()),
            ServiceMode::Grpc => ("grpc", state.config.service.grpc_bind.as_deref()),
            ServiceMode::Graphql => ("graphql", state.config.service.graphql_bind.as_deref()),
            ServiceMode::All => unreachable!(),
        };
        let bind = match bind {
            Some(b) => b,
            None => {
                let err = CoreError::InvalidInput(format!("{scheme} bind is not configured"));
                if let Some(tx) = ready_sender.take() {
                    let _ = tx.send(Err(crate::package::PackageError::from(err.clone())));
                }
                return Err(err);
            }
        };
        let listener = match tokio::net::TcpListener::bind(bind).await {
            Ok(l) => l,
            Err(e) => {
                let err = CoreError::Io(format!("{scheme} bind {bind}: {e}"));
                if let Some(tx) = ready_sender.take() {
                    let _ = tx.send(Err(crate::package::PackageError::from(err.clone())));
                }
                return Err(err);
            }
        };
        let addr = match listener.local_addr() {
            Ok(a) => a,
            Err(e) => {
                let err = CoreError::Io(e.to_string());
                if let Some(tx) = ready_sender.take() {
                    let _ = tx.send(Err(crate::package::PackageError::from(err.clone())));
                }
                return Err(err);
            }
        };
        binds.push(format!("{scheme}://{addr}"));
        listeners.push((mode, listener));
    }
    if let Some(tx) = ready_sender.take() {
        let _ = tx.send(Ok(binds.clone()));
    }
    let schema = graphql::build_schema()?;
    let (stop, _) = watch::channel(false);
    let mut tasks = tokio::task::JoinSet::new();
    for (mode, listener) in listeners {
        let state = state.clone();
        let shutdown = stop.subscribe();
        let schema = schema.clone();
        tasks.spawn(async move {
            match mode {
                ServiceMode::Grpc => grpc::serve(listener, state, shutdown).await,
                ServiceMode::Http => http::serve(listener, http::router(state), shutdown).await,
                ServiceMode::Graphql => {
                    http::serve(listener, graphql::router(state, schema), shutdown).await
                }
                ServiceMode::All => unreachable!(),
            }
        });
    }
    tokio::pin!(shutdown);
    let failure = tokio::select! {
        _ = &mut shutdown => None,
        result = tasks.join_next() => Some(match result {
            Some(Ok(Err(error))) => error,
            Some(Err(error)) => CoreError::Io(error.to_string()),
            _ => CoreError::Io("service host stopped unexpectedly".into()),
        }),
    };
    stop.send_replace(true);
    let drain = async {
        while let Some(result) = tasks.join_next().await {
            result.map_err(|e| CoreError::Io(e.to_string()))??;
        }
        Ok::<_, CoreError>(())
    };
    tokio::time::timeout(REQUEST_TIMEOUT + Duration::from_secs(5), drain)
        .await
        .map_err(|_| CoreError::Io("graceful shutdown deadline exceeded".into()))??;
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(ServiceRuntimeReport {
        status: "stopped".into(),
        binds,
    })
}

pub(crate) async fn stopped(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
