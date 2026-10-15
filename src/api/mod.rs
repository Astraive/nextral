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

pub fn run_service_hosts(
    config: &NextralConfig,
    mode: ServiceMode,
) -> CoreResult<ServiceRuntimeReport> {
    let plan = startup_plan(config, mode)?;

    // Check readiness matrix before starting services
    let readiness = startup_readiness_matrix(config)?;
    if readiness.fail_fast {
        let unavailable: Vec<&str> = readiness
            .backends
            .iter()
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
    let mut errors = Vec::new();
    for handle in handles {
        match handle.join() {
            Ok(Some(error)) => errors.push(error),
            Ok(None) => {}
            Err(error) => errors.push(format!("listener thread panicked: {:?}", error)),
        }
    }
    if !errors.is_empty() {
        return Err(crate::contracts::CoreError::Io(format!(
            "service startup failed: {}",
            errors.join("; ")
        )));
    }
    Ok(ServiceRuntimeReport {
        status: "running".to_string(),
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

fn spawn_listener(bind: String, mode: &'static str) -> thread::JoinHandle<Option<String>> {
    thread::spawn(move || {
        let listener = match TcpListener::bind(&bind) {
            Ok(listener) => listener,
            Err(error) => return Some(format!("{mode} bind failed on {bind}: {error}")),
        };
        let connection_count = std::sync::atomic::AtomicUsize::new(0);
        for mut stream in listener.incoming().flatten() {
            let count = connection_count.load(std::sync::atomic::Ordering::Relaxed);
            if count >= MAX_CONNECTIONS {
                let _ = stream.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                continue;
            }
            connection_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let _ = handle_connection(&mut stream, mode);
            connection_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
        None
    })
}

const MAX_REQUEST_BODY_BYTES: usize = 10 * 1024 * 1024;
const MAX_CONNECTIONS: usize = 256;

fn handle_connection(stream: &mut TcpStream, mode: &str) -> std::io::Result<()> {
    use std::io::Read;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(30)))?;
    let mut buf = [0u8; 8192];
    let n = stream.read(&mut buf)?;
    if n == 0 {
        return Ok(());
    }
    let request = String::from_utf8_lossy(&buf[..n]);
    let mut lines = request.lines();
    let request_line = lines.next().unwrap_or("");
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    let method = parts.first().unwrap_or(&"");
    let path = parts.get(1).unwrap_or(&"");

    if !matches!(
        *method,
        "GET" | "POST" | "PUT" | "DELETE" | "PATCH" | "HEAD" | "OPTIONS"
    ) {
        let error_body = serde_json::json!({"error": "method not allowed"}).to_string();
        let response = format!(
            "HTTP/1.1 405 Method Not Allowed\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            error_body.len(),
            error_body
        );
        return stream.write_all(response.as_bytes());
    }

    let content_length: usize = lines
        .filter_map(|line| {
            line.strip_prefix("Content-Length:")
                .map(|v| v.trim().parse().unwrap_or(0))
        })
        .next()
        .unwrap_or(0);

    if content_length > MAX_REQUEST_BODY_BYTES {
        let error_body = serde_json::json!({"error": "request body too large", "max_bytes": MAX_REQUEST_BODY_BYTES}).to_string();
        let response = format!(
            "HTTP/1.1 413 Payload Too Large\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            error_body.len(),
            error_body
        );
        return stream.write_all(response.as_bytes());
    }

    let body_start = request.find("\r\n\r\n").map(|i| i + 4).unwrap_or(n);
    let mut body = request[body_start..].to_string();
    if body.len() < content_length {
        let remaining = content_length - body.len();
        if remaining > MAX_REQUEST_BODY_BYTES {
            let error_body = serde_json::json!({"error": "request body too large"}).to_string();
            let response = format!(
                "HTTP/1.1 413 Payload Too Large\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                error_body.len(),
                error_body
            );
            return stream.write_all(response.as_bytes());
        }
        let mut extra = vec![0u8; remaining];
        stream.read_exact(&mut extra)?;
        body.push_str(&String::from_utf8_lossy(&extra));
    }

    let (status_code, response_body) = route_request(mode, method, path, &body);
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\nX-Content-Type-Options: nosniff\r\nX-Frame-Options: DENY\r\n\r\n{}",
        status_code.0,
        status_code.1,
        response_body.len(),
        response_body
    );
    stream.write_all(response.as_bytes())
}

pub fn route_request(
    mode: &str,
    method: &str,
    path: &str,
    body: &str,
) -> ((u16, &'static str), String) {
    let path = path.trim_end_matches('/');
    let result = match (method, path) {
        ("GET", "/health") | ("GET", "/v1/health") => {
            Ok(serde_json::json!({"status": "ok", "mode": mode}).to_string())
        }
        ("POST", "/v1/ingest") | ("POST", "/ingest") => crate::package::mcp_call_json(
            &serde_json::json!({"tool": "nextral.memory.ingest", "payload_json": body.to_string()})
                .to_string(),
        )
        .map_err(|e| e.message),
        ("POST", "/v1/retrieve") | ("POST", "/retrieve") => crate::package::mcp_call_json(
            &serde_json::json!({"tool": "nextral.memory.retrieve", "payload_json": body.to_string()})
                .to_string(),
        )
        .map_err(|e| e.message),
        ("POST", "/v1/forget") | ("POST", "/forget") => crate::package::mcp_call_json(
            &serde_json::json!({"tool": "nextral.memory.forget", "payload_json": body.to_string()})
                .to_string(),
        )
        .map_err(|e| e.message),
        ("POST", "/v1/graph/query") | ("POST", "/graph/query") => crate::package::mcp_call_json(
            &serde_json::json!({"tool": "nextral.graph.query", "payload_json": body.to_string()})
                .to_string(),
        )
        .map_err(|e| e.message),
        ("POST", "/v1/reminders/due") | ("POST", "/reminders/due") => {
            crate::package::mcp_call_json(
                &serde_json::json!({"tool": "nextral.reminders.due", "payload_json": body.to_string()})
                    .to_string(),
            )
            .map_err(|e| e.message)
        }
        ("POST", "/v1/batch/ingest") | ("POST", "/batch/ingest") => {
            crate::package::mcp_call_json(
                &serde_json::json!({"tool": "nextral.batch.ingest", "payload_json": body.to_string()})
                    .to_string(),
            )
            .map_err(|e| e.message)
        }
        ("POST", "/v1/batch/retrieve") | ("POST", "/batch/retrieve") => {
            crate::package::mcp_call_json(
                &serde_json::json!({"tool": "nextral.batch.retrieve", "payload_json": body.to_string()})
                    .to_string(),
            )
            .map_err(|e| e.message)
        }
        ("POST", "/v1/batch/forget") | ("POST", "/batch/forget") => {
            crate::package::mcp_call_json(
                &serde_json::json!({"tool": "nextral.batch.forget", "payload_json": body.to_string()})
                    .to_string(),
            )
            .map_err(|e| e.message)
        }
        _ => return ((404, "Not Found"), serde_json::json!({"error": format!("{} {} not found", method, path)}).to_string()),
    };
    match result {
        Ok(body) => ((200, "OK"), body),
        Err(error) => (
            (500, "Internal Server Error"),
            serde_json::json!({"error": error}).to_string(),
        ),
    }
}
