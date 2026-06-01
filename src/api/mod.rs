pub mod graphql;
pub mod grpc;
pub mod http;

use crate::{config::NextralConfig, contracts::CoreResult};
use serde::{Deserialize, Serialize};
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    thread,
};

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

pub fn run_service_hosts(config: &NextralConfig, mode: ServiceMode) -> CoreResult<ServiceRuntimeReport> {
    let plan = startup_plan(config, mode)?;

    // Check readiness matrix before starting services
    let readiness = startup_readiness_matrix(config)?;
    if readiness.fail_fast {
        let unavailable: Vec<&str> = readiness.backends.iter()
            .filter(|b| b.status == "unavailable")
            .map(|b| b.backend.as_str())
            .collect();
        return Err(crate::contracts::CoreError::InvalidInput(format!(
            "Cannot start services: required backends unavailable: {:?}",
            unavailable
        )));
    }

    let mut binds = Vec::new();
    let mut handles = Vec::new();
    for active_mode in plan.modes {
        match active_mode {
            ServiceMode::Http => {
                if let Some(bind) = plan.http_bind.clone() {
                    binds.push(format!("http://{bind}"));
                    handles.push(spawn_listener(bind, "http"));
                }
            }
            ServiceMode::Grpc => {
                if let Some(bind) = plan.grpc_bind.clone() {
                    binds.push(format!("grpc://{bind}"));
                    handles.push(spawn_listener(bind, "grpc"));
                }
            }
            ServiceMode::Graphql => {
                if let Some(bind) = plan.graphql_bind.clone() {
                    binds.push(format!("graphql://{bind}"));
                    handles.push(spawn_listener(bind, "graphql"));
                }
            }
            ServiceMode::All => {}
        }
    }
    for handle in handles {
        if let Err(error) = handle.join() {
            eprintln!("Service listener thread panicked: {:?}", error);
        }
    }
    Ok(ServiceRuntimeReport {
        status: "stopped".to_string(),
        binds,
    })
}

pub fn startup_readiness_matrix(config: &NextralConfig) -> CoreResult<StartupReadinessMatrix> {
    config.validate()?;
    let mut backends = Vec::new();
    if let Some(stores) = &config.stores {
        backends.push(BackendReadiness {
            backend: "postgres".to_string(),
            status: if stores.postgres_url.trim().is_empty() {
                "unavailable".to_string()
            } else {
                "configured".to_string()
            },
            degraded_signature: Some("degraded:postgres_unavailable".to_string()),
        });
        backends.push(BackendReadiness {
            backend: "redis".to_string(),
            status: if stores.redis_url.trim().is_empty() {
                "unavailable".to_string()
            } else {
                "configured".to_string()
            },
            degraded_signature: Some("degraded:redis_unavailable".to_string()),
        });
        backends.push(BackendReadiness {
            backend: "qdrant".to_string(),
            status: if stores.qdrant_url.trim().is_empty() {
                "unavailable".to_string()
            } else {
                "configured".to_string()
            },
            degraded_signature: Some("degraded:qdrant_unavailable".to_string()),
        });
        backends.push(BackendReadiness {
            backend: "neo4j".to_string(),
            status: if stores.neo4j_url.trim().is_empty() {
                "unavailable".to_string()
            } else {
                "configured".to_string()
            },
            degraded_signature: Some("degraded:neo4j_unavailable".to_string()),
        });
        backends.push(BackendReadiness {
            backend: "s3".to_string(),
            status: if stores.s3_endpoint.trim().is_empty() {
                "unavailable".to_string()
            } else {
                "configured".to_string()
            },
            degraded_signature: Some("degraded:s3_unavailable".to_string()),
        });
    }
    let fail_fast = backends.iter().any(|entry| entry.status == "unavailable");
    Ok(StartupReadinessMatrix {
        fail_fast,
        backends,
    })
}

fn spawn_listener(bind: String, mode: &'static str) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let listener = match TcpListener::bind(&bind) {
            Ok(listener) => listener,
            Err(_) => return,
        };
        for stream in listener.incoming() {
            if let Ok(mut stream) = stream {
                let _ = write_response(&mut stream, mode);
            }
        }
    })
}

fn write_response(stream: &mut TcpStream, mode: &str) -> std::io::Result<()> {
    let body = format!("nextral {mode} service alive");
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(response.as_bytes())
}
