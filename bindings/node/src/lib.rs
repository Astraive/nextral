//! Private JSON transport for the typed JavaScript package.

use napi::{bindgen_prelude::AsyncTask, Env, Error, Result, Task};
use napi_derive::napi;

fn package_error(error: nextral::package::PackageError) -> Error {
    Error::from_reason(
        serde_json::json!({ "code": error.code, "message": error.message }).to_string(),
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
pub fn configure_runtime(config_json: String) -> Result<String> {
    nextral::package::configure_runtime_json(&config_json).map_err(package_error)
}

#[napi]
pub fn mcp_call(request_json: String) -> Result<String> {
    nextral::package::mcp_call_json(&request_json).map_err(package_error)
}

pub struct ServiceHostsTask {
    config_json: String,
    modes_json: String,
}

impl Task for ServiceHostsTask {
    type Output = String;
    type JsValue = String;

    fn compute(&mut self) -> Result<Self::Output> {
        nextral::package::run_service_hosts_json(&self.config_json, &self.modes_json)
            .map_err(package_error)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(output)
    }
}

#[napi]
pub fn run_service_hosts(config_json: String, modes_json: String) -> AsyncTask<ServiceHostsTask> {
    AsyncTask::new(ServiceHostsTask {
        config_json,
        modes_json,
    })
}

#[napi]
pub fn stop_serving() {
    nextral::api::request_service_shutdown();
}
