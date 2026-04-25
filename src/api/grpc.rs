use crate::{contracts::CoreResult, runtime::RuntimeHealth};

pub fn health_payload(health: &RuntimeHealth) -> CoreResult<Vec<u8>> {
    Ok(serde_json::to_vec(health)?)
}
