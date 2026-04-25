use napi::bindgen_prelude::{Error, Result};
use napi_derive::napi;

#[napi]
pub fn lexical_score(text: String, query: String) -> Result<f64> {
    nextral::scoring::try_lexical_score(&text, &query)
        .map(|score| score as f64)
        .map_err(|error| Error::from_reason(error.to_string()))
}

#[napi]
pub fn validate_config(config_json: String) -> Result<String> {
    nextral::config::validate_config_json(&config_json)
        .map_err(|error| Error::from_reason(error.to_string()))
}

#[napi]
pub fn e2e_smoke() -> Result<String> {
    nextral::package::e2e_smoke_json().map_err(|error| Error::from_reason(error.message))
}

#[napi]
pub fn reembed_plan(request_json: String) -> Result<String> {
    nextral::package::reembed_plan_json(&request_json)
        .map_err(|error| Error::from_reason(error.message))
}

#[napi]
pub fn ingest_request_schema() -> Result<String> {
    Ok(nextral::package::ingest_request_schema_json())
}
