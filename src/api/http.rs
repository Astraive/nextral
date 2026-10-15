use super::{auth::Principal, ApiError, ServiceState, MAX_BODY, REQUEST_TIMEOUT};
use axum::{
    extract::{rejection::JsonRejection, DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use tokio::{net::TcpListener, sync::watch};

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let mut response = (
            status,
            Json(json!({"error":{"code":self.code,"message":self.message}})),
        )
            .into_response();
        if status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                axum::http::header::WWW_AUTHENTICATE,
                axum::http::HeaderValue::from_static("Bearer"),
            );
        }
        response
    }
}

pub(crate) fn principal(state: &ServiceState, headers: &HeaderMap) -> Result<Principal, ApiError> {
    let authorization = headers
        .get(axum::http::header::AUTHORIZATION)
        .map(|v| v.to_str())
        .transpose()
        .map_err(|_| ApiError::unauthorized("invalid authorization header"))?;
    state.auth.authenticate(authorization)
}

pub(crate) async fn limit_requests(
    State(state): State<ServiceState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let Ok(_permit) = state.request_permits.clone().try_acquire_owned() else {
        return ApiError::new(429, "resource_exhausted", "service request limit reached")
            .into_response();
    };
    next.run(request).await
}

pub(crate) fn router(state: ServiceState) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/v1/memories", post(ingest))
        .route("/v1/retrieval/query", post(retrieve))
        .route("/v1/memories/{memory_id}/forget", post(forget))
        .route("/v1/reminders", post(schedule))
        .fallback(|| async { ApiError::new(404, "not_found", "route not found") })
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .layer(tower_http::timeout::TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            REQUEST_TIMEOUT,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            limit_requests,
        ))
        .with_state(state)
}

pub(crate) async fn health() -> Json<Value> {
    Json(json!({"status":"alive"}))
}

pub(crate) async fn ready(State(state): State<ServiceState>) -> Result<Response, ApiError> {
    let matrix = state.probe().await?;
    let status = if matrix.fail_fast {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };
    Ok((status, Json(matrix)).into_response())
}

fn body(body: Result<Json<Value>, JsonRejection>) -> Result<Value, ApiError> {
    body.map(|Json(value)| value)
        .map_err(|e| ApiError::new(e.status().as_u16(), "invalid_input", e.body_text()))
}

async fn ingest(
    State(state): State<ServiceState>,
    headers: HeaderMap,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Response, ApiError> {
    let actor = principal(&state, &headers)?;
    let response = state
        .call(&actor, "nextral.memory.ingest", body(input)?)
        .await?;
    Ok((StatusCode::ACCEPTED, Json(response)).into_response())
}

async fn retrieve(
    State(state): State<ServiceState>,
    headers: HeaderMap,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Json<Value>, ApiError> {
    let actor = principal(&state, &headers)?;
    let response = state
        .call(&actor, "nextral.memory.retrieve", body(input)?)
        .await?;
    if response["status"] == "error" || response["decision_action"] == "block" {
        return Err(ApiError::new(
            503,
            "unavailable",
            "retrieval is unavailable under active policy",
        ));
    }
    Ok(Json(response))
}

async fn forget(
    State(state): State<ServiceState>,
    Path(memory_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Response, ApiError> {
    let actor = principal(&state, &headers)?;
    let mut input = body(input)?;
    let fields = input
        .as_object_mut()
        .ok_or_else(|| ApiError::invalid("request must be an object"))?;
    if fields
        .get("memory_id")
        .and_then(Value::as_str)
        .is_some_and(|v| v != memory_id)
    {
        return Err(ApiError::invalid("body memory_id does not match path"));
    }
    fields.insert("memory_id".into(), json!(memory_id));
    Ok((
        StatusCode::ACCEPTED,
        Json(state.call(&actor, "nextral.memory.forget", input).await?),
    )
        .into_response())
}

async fn schedule(
    State(state): State<ServiceState>,
    headers: HeaderMap,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Response, ApiError> {
    let actor = principal(&state, &headers)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .call(&actor, "nextral.reminders.schedule", body(input)?)
                .await?,
        ),
    )
        .into_response())
}

pub(crate) async fn serve(
    listener: TcpListener,
    router: Router,
    shutdown: watch::Receiver<bool>,
) -> crate::contracts::CoreResult<()> {
    axum::serve(listener, router)
        .with_graceful_shutdown(super::stopped(shutdown))
        .await?;
    Ok(())
}

pub fn health_response(
    health: &crate::runtime::RuntimeHealth,
) -> crate::contracts::CoreResult<String> {
    Ok(serde_json::to_string(health)?)
}
