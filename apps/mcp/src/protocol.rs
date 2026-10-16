use crate::{catalog, validate_arguments};
use nextral::package::mcp_call_json;
use serde_json::{json, Value};
use std::io::{self, BufRead, Write};

const VERSIONS: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

#[derive(Default, PartialEq, Eq)]
enum State {
    #[default]
    New,
    Initializing,
    Ready,
}

#[derive(Default)]
pub struct Server {
    state: State,
}

fn error(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message.into()}})
}

fn result(id: Value, value: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"result":value})
}

impl Server {
    pub fn handle(&mut self, line: &[u8]) -> Option<Value> {
        let message: Value = match serde_json::from_slice(line) {
            Ok(value) => value,
            Err(_) => return Some(error(Value::Null, -32700, "Parse error")),
        };
        let Some(object) = message.as_object() else {
            return Some(error(
                Value::Null,
                -32600,
                "Expected a JSON-RPC object; batches are not supported",
            ));
        };
        let valid_id = message
            .get("id")
            .is_some_and(|id| id.is_string() || id.as_i64().is_some() || id.as_u64().is_some());
        let id = if valid_id {
            message["id"].clone()
        } else {
            Value::Null
        };
        if message["jsonrpc"] != "2.0" || (object.contains_key("id") && !valid_id) {
            return Some(error(id, -32600, "Invalid JSON-RPC version or request ID"));
        }
        // No server-to-client requests are issued, so client responses need no reply.
        if !object.contains_key("method")
            && valid_id
            && (object.contains_key("result") || object.contains_key("error"))
        {
            return None;
        }
        let Some(method) = message["method"].as_str() else {
            return Some(error(id, -32600, "Request method must be a string"));
        };
        let params = message.get("params");
        if params.is_some_and(|params| !params.is_object()) {
            return valid_id.then(|| error(id, -32602, "params must be an object"));
        }
        if !valid_id {
            if method == "notifications/initialized" && self.state == State::Initializing {
                self.state = State::Ready;
            }
            return None;
        }
        if method == "ping" {
            return Some(result(id, json!({})));
        }
        if method == "initialize" {
            if self.state != State::New {
                return Some(error(id, -32600, "Session is already initialized"));
            }
            let Some(params) = params else {
                return Some(error(
                    id,
                    -32602,
                    "initialize requires protocolVersion, capabilities and clientInfo",
                ));
            };
            let Some(requested) = params["protocolVersion"].as_str() else {
                return Some(error(id, -32602, "protocolVersion must be a string"));
            };
            if !params["capabilities"].is_object()
                || !params["clientInfo"]["name"].is_string()
                || !params["clientInfo"]["version"].is_string()
            {
                return Some(error(
                    id,
                    -32602,
                    "capabilities and clientInfo.name/version are required",
                ));
            }
            let version = if VERSIONS.contains(&requested) {
                requested
            } else {
                VERSIONS[0]
            };
            self.state = State::Initializing;
            return Some(result(
                id,
                json!({
                    "protocolVersion":version,"capabilities":{"tools":{"listChanged":false}},
                    "serverInfo":{"name":"nextral","version":env!("CARGO_PKG_VERSION")},
                    "instructions":"Scoped memory tools use the configured Nextral backend. Production embedding vectors and extraction output are supplied by the host."
                }),
            ));
        }
        if self.state != State::Ready {
            return Some(error(
                id,
                -32002,
                "Initialize the session and send notifications/initialized before using tools",
            ));
        }
        match method {
            "tools/list" => {
                if params
                    .and_then(|params| params.get("cursor"))
                    .is_some_and(|cursor| !cursor.is_null())
                {
                    Some(error(
                        id,
                        -32602,
                        "This tool list is not paginated; omit cursor",
                    ))
                } else {
                    Some(result(id, json!({"tools":catalog::tools()})))
                }
            }
            "tools/call" => {
                let Some(name) = params.and_then(|params| params["name"].as_str()) else {
                    return Some(error(id, -32602, "tools/call requires a tool name"));
                };
                let empty = json!({});
                let arguments = params
                    .and_then(|params| params.get("arguments"))
                    .unwrap_or(&empty);
                if let Err(failure) = validate_arguments(name, arguments) {
                    let code = if failure.code == "internal" {
                        -32603
                    } else {
                        -32602
                    };
                    return Some(error(id, code, failure.message));
                }
                let request = json!({"tool":name,"payload_json":arguments.to_string()});
                match mcp_call_json(&request.to_string()) {
                    Ok(text) => match serde_json::from_str::<Value>(&text) {
                        Ok(value) => {
                            let structured = if value.is_object() {
                                value
                            } else {
                                json!({"result":value})
                            };
                            Some(result(
                                id,
                                json!({"content":[{"type":"text","text":text}],"structuredContent":structured,"isError":false}),
                            ))
                        }
                        Err(_) => Some(error(id, -32603, "Package returned invalid JSON")),
                    },
                    Err(failure) => {
                        let structured =
                            json!({"error":{"code":failure.code,"message":failure.message}});
                        Some(result(
                            id,
                            json!({"content":[{"type":"text","text":structured.to_string()}],"structuredContent":structured,"isError":true}),
                        ))
                    }
                }
            }
            _ => Some(error(id, -32601, format!("Method not found: {method}"))),
        }
    }
}

/// Process newline-delimited UTF-8 JSON-RPC until the client closes stdin.
pub fn serve(reader: &mut impl BufRead, writer: &mut impl Write) -> io::Result<()> {
    let mut server = Server::default();
    let mut line = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            return Ok(());
        }
        if let Some(response) = server.handle(&line) {
            serde_json::to_writer(&mut *writer, &response)?;
            writer.write_all(b"\n")?;
            writer.flush()?;
        }
    }
}
