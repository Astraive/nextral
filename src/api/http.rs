use crate::{contracts::CoreResult, runtime::RuntimeHealth};

pub fn health_response(health: &RuntimeHealth) -> CoreResult<String> {
    Ok(serde_json::to_string(health)?)
}
