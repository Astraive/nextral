pub mod graphql;
pub mod grpc;
pub mod http;

use crate::{config::NextralConfig, contracts::CoreResult};
use serde::{Deserialize, Serialize};

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

pub fn startup_plan(config: &NextralConfig, mode: ServiceMode) -> CoreResult<ServiceStartupPlan> {
    config.validate()?;
    let modes = match mode {
        ServiceMode::All => vec![ServiceMode::Http, ServiceMode::Grpc, ServiceMode::Graphql],
        single => vec![single],
    };
    Ok(ServiceStartupPlan {
        modes,
        http_bind: config.service.http_bind.clone(),
        grpc_bind: config.service.grpc_bind.clone(),
        graphql_bind: config.service.graphql_bind.clone(),
    })
}
