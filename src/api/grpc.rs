use super::{ApiError, ServiceState, MAX_BODY, REQUEST_TIMEOUT};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use tokio::{net::TcpListener, sync::watch};
use tonic::{Request, Response, Status};

pub mod proto {
    tonic::include_proto!("nextral.v1");
}
use proto::{
    memory_service_server::{MemoryService, MemoryServiceServer},
    reminder_service_server::{ReminderService, ReminderServiceServer},
    *,
};

#[derive(Clone)]
struct Services(ServiceState);

impl From<ApiError> for Status {
    fn from(error: ApiError) -> Self {
        match error.status {
            400 | 413 | 415 | 422 => Status::invalid_argument(error.message),
            401 => Status::unauthenticated(error.message),
            403 => Status::permission_denied(error.message),
            404 => Status::not_found(error.message),
            409 => Status::already_exists(error.message),
            429 => Status::resource_exhausted(error.message),
            503 => Status::unavailable(error.message),
            504 => Status::deadline_exceeded(error.message),
            _ => Status::internal(error.message),
        }
    }
}

impl Services {
    async fn invoke<T: Serialize>(
        &self,
        request: Request<T>,
        tool: &'static str,
    ) -> Result<Value, Status> {
        let auth = request
            .metadata()
            .get("authorization")
            .map(|v| v.to_str())
            .transpose()
            .map_err(|_| Status::unauthenticated("invalid authorization metadata"))?;
        let principal = self.0.auth.authenticate(auth)?;
        let mut payload = serde_json::to_value(request.into_inner())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        // Protobuf repeated fields represent an omitted host vector as an empty list.
        if let Some(fields) = payload.as_object_mut() {
            for name in ["embedding", "query_vector"] {
                if fields
                    .get(name)
                    .and_then(Value::as_array)
                    .is_some_and(Vec::is_empty)
                {
                    fields.remove(name);
                }
            }
        }
        Ok(self.0.call(&principal, tool, payload).await?)
    }
}

fn response<T: DeserializeOwned>(value: Value) -> Result<Response<T>, Status> {
    serde_json::from_value(value)
        .map(Response::new)
        .map_err(|e| Status::internal(format!("runtime response violates protocol contract: {e}")))
}

#[tonic::async_trait]
impl MemoryService for Services {
    async fn ingest(
        &self,
        request: Request<IngestMemoryRequest>,
    ) -> Result<Response<IngestMemoryResponse>, Status> {
        let result = self.invoke(request, "nextral.memory.ingest").await?;
        response(result)
    }
    async fn retrieve(
        &self,
        request: Request<RetrieveMemoryRequest>,
    ) -> Result<Response<RetrieveMemoryResponse>, Status> {
        let result = self.invoke(request, "nextral.memory.retrieve").await?;
        if result["status"] == "error" || result["decision_action"] == "block" {
            return Err(Status::unavailable(
                "retrieval unavailable under active policy",
            ));
        }
        response(result)
    }
    async fn forget(
        &self,
        request: Request<ForgetMemoryRequest>,
    ) -> Result<Response<ForgetMemoryResponse>, Status> {
        response(self.invoke(request, "nextral.memory.forget").await?)
    }
    async fn query(
        &self,
        request: Request<MemoryQueryRequest>,
    ) -> Result<Response<MemoryPage>, Status> {
        let result = self.invoke(request, "nextral.memory.list").await?;
        response(result)
    }
    async fn repair_index(
        &self,
        request: Request<RepairIndexRequest>,
    ) -> Result<Response<RepairIndexResponse>, Status> {
        let result = self.invoke(request, "nextral.index.repair").await?;
        response(result)
    }
}

#[tonic::async_trait]
impl ReminderService for Services {
    async fn schedule(
        &self,
        request: Request<ScheduleReminderRequest>,
    ) -> Result<Response<ScheduleReminderResponse>, Status> {
        let result = self.invoke(request, "nextral.reminders.schedule").await?;
        response(json!({"reminder_id": result["reminder"]["id"]}))
    }
    async fn list_due(
        &self,
        request: Request<ListDueRemindersRequest>,
    ) -> Result<Response<ListDueRemindersResponse>, Status> {
        // ListDue is a read, not the reminder-dispatch worker operation.
        let result = self.invoke(request, "nextral.reminders.list").await?;
        let reminders = result
            .as_array()
            .ok_or_else(|| Status::internal("runtime reminder list must be an array"))?;
        response(json!({"reminder_ids": reminders.iter().map(|r| &r["id"]).collect::<Vec<_>>()}))
    }
}

pub(crate) async fn serve(
    listener: TcpListener,
    state: ServiceState,
    shutdown: watch::Receiver<bool>,
) -> crate::contracts::CoreResult<()> {
    let (reporter, health) = tonic_health::server::health_reporter();
    for service in ["", "nextral.v1.MemoryService", "nextral.v1.ReminderService"] {
        reporter
            .set_service_status(service, tonic_health::ServingStatus::NotServing)
            .await;
    }
    let monitor_state = state.clone();
    let monitor_shutdown = shutdown.clone();
    let monitor = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = super::stopped(monitor_shutdown.clone()) => break,
                _ = interval.tick() => {},
            }
            let ready = monitor_state
                .probe()
                .await
                .is_ok_and(|matrix| !matrix.fail_fast);
            let status = if ready {
                tonic_health::ServingStatus::Serving
            } else {
                tonic_health::ServingStatus::NotServing
            };
            for service in ["", "nextral.v1.MemoryService", "nextral.v1.ReminderService"] {
                reporter.set_service_status(service, status).await;
            }
        }
    });
    let services = Services(state);
    let result = tonic::transport::Server::builder()
        .concurrency_limit_per_connection(64)
        .load_shed(true)
        .layer(tower::limit::GlobalConcurrencyLimitLayer::new(64))
        .max_concurrent_streams(64)
        .timeout(REQUEST_TIMEOUT)
        .http2_max_header_list_size(32 * 1024)
        .add_service(
            MemoryServiceServer::new(services.clone())
                .max_decoding_message_size(MAX_BODY)
                .max_encoding_message_size(MAX_BODY),
        )
        .add_service(
            ReminderServiceServer::new(services)
                .max_decoding_message_size(MAX_BODY)
                .max_encoding_message_size(MAX_BODY),
        )
        .add_service(health)
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            super::stopped(shutdown),
        )
        .await
        .map_err(|e| crate::contracts::CoreError::Io(e.to_string()));
    monitor.abort();
    result
}
