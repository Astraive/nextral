//! Private JSON transport for the typed JavaScript package.

use napi::{Error, Result};
use napi_derive::napi;
use std::sync::Arc;
use nextral::package::{CallOptions, RuntimeClient};

fn package_error(error: nextral::package::PackageError) -> Error {
    Error::from_reason(
        serde_json::json!({
            "code": error.code,
            "message": error.message,
            "execution_status": error.execution_status,
        })
        .to_string(),
    )
}

#[napi]
pub fn lexical_score(text: String, query: String) -> Result<f64> {
    nextral::scoring::try_lexical_score(&text, &query)
        .map(f64::from)
        .map_err(|error| package_error(error.into()))
}

#[napi]
pub fn validate_config(config_json: String) -> Result<String> {
    nextral::config::validate_config_json(&config_json).map_err(|error| package_error(error.into()))
}

#[napi]
pub struct Client {
    client: Arc<RuntimeClient>,
}

#[napi]
pub struct CallTicket {
    cancellation_token: tokio_util::sync::CancellationToken,
}

#[napi]
impl CallTicket {
    #[napi]
    pub fn cancel(&self) {
        self.cancellation_token.cancel();
    }
}

#[napi]
pub struct ServiceHandle {
    handle: Arc<nextral::package::ServiceHandle>,
}

#[napi]
impl ServiceHandle {
    #[napi]
    pub async fn shutdown(&self, timeout_ms: Option<u32>) -> Result<()> {
        let timeout = std::time::Duration::from_millis(timeout_ms.unwrap_or(30000) as u64);
        self.handle.shutdown(timeout).await.map_err(package_error)
    }

    #[napi]
    pub fn binds(&self) -> Vec<String> {
        self.handle.binds().to_vec()
    }
}
#[napi]
pub struct WorkerHandle {
    handle: Arc<nextral::package::WorkerHandle>,
}

#[napi]
impl WorkerHandle {
    #[napi]
    pub async fn tick(&self) -> Result<String> {
        let report = self.handle.tick().await.map_err(package_error)?;
        serde_json::to_string(&report).map_err(|e| Error::from_reason(e.to_string()))
    }

    #[napi]
    pub async fn shutdown(&self, timeout_ms: Option<u32>) -> Result<()> {
        let timeout = std::time::Duration::from_millis(timeout_ms.unwrap_or(30000) as u64);
        self.handle.shutdown(timeout).await.map_err(package_error)
    }
}


#[napi]
impl Client {
    #[napi(factory)]
    pub fn create(config_json: String) -> Result<Self> {
        let config = nextral::config::load_config(&config_json).map_err(|e| package_error(e.into()))?;
        let client = RuntimeClient::from_config(config).map_err(package_error)?;
        Ok(Self {
            client: Arc::new(client),
        })
    }

    #[napi]
    pub fn create_ticket(&self) -> CallTicket {
        CallTicket {
            cancellation_token: tokio_util::sync::CancellationToken::new(),
        }
    }

    #[napi]
    pub async fn call_json_with_ticket(
        &self,
        tool: String,
        payload_json: String,
        timeout_ms: Option<u32>,
        ticket: &CallTicket,
    ) -> Result<String> {
        self.client
            .call_json(
                &tool,
                payload_json,
                CallOptions {
                    timeout_ms: timeout_ms.map(|t| t as u64),
                    cancellation_token: Some(ticket.cancellation_token.clone()),
                },
            )
            .await
            .map_err(package_error)
    }

    #[napi]
    pub async fn close(&self, timeout_ms: Option<u32>) -> Result<String> {
        let timeout = std::time::Duration::from_millis(timeout_ms.unwrap_or(30000) as u64);
        let report = self.client.close(timeout).await.map_err(package_error)?;
        serde_json::to_string(&report).map_err(|e| Error::from_reason(e.to_string()))
    }

    #[napi]
    pub async fn serve(&self) -> Result<ServiceHandle> {
        let handle = self.client.serve().await.map_err(package_error)?;
        Ok(ServiceHandle {
            handle: Arc::new(handle),
        })
    }

    #[napi]
    pub async fn start_worker(&self, config_json: String) -> Result<WorkerHandle> {
        let config: nextral::runtime::prospective::WorkerConfig =
            serde_json::from_str(&config_json).map_err(|e| Error::from_reason(e.to_string()))?;
        let handle = self.client.start_worker(config).await.map_err(package_error)?;
        Ok(WorkerHandle {
            handle: Arc::new(handle),
        })
    }
}
