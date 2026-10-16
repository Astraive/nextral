use serde_json::{json, Value};
use std::{
    io::Write,
    process::{Command, Stdio},
};

fn exchange(messages: &[Value]) -> Vec<Value> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_nextral-mcp"))
        .args([
            "--config",
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../examples/config_example.json"
            ),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    for message in messages {
        writeln!(stdin, "{message}").unwrap();
    }
    drop(stdin);
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).expect("stdout must contain only JSON-RPC messages"))
        .collect()
}

fn initialize() -> Value {
    json!({"jsonrpc":"2.0","id":"initialize","method":"initialize","params":{
        "protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"regression","version":"1"}
    }})
}

fn initialized() -> Value {
    json!({"jsonrpc":"2.0","method":"notifications/initialized"})
}

fn call(id: Value, name: &str, arguments: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":name,"arguments":arguments}})
}

#[test]
fn stdio_discovers_and_executes_memory_workflow_then_exits_on_eof() {
    let ingest = json!({
        "id":"stdio-memory","tenant_id":"stdio-tenant","user_id":"stdio-user", "content":"Use PostgreSQL for Atlas",
        "content_type":"fact","memory_type":"semantic","source_type":"manual", "source_message_ids":[],
        "importance_score":0.8,"confidence_score":0.9,"entities":["Atlas"],"tags":["database"],
        "privacy_level":"private","graph_hints":[],"policy":{"min_importance_score":0.0,"min_confidence_score":0.0}
    });
    let replies = exchange(&[
        initialize(),
        initialized(),
        json!({"jsonrpc":"2.0","id":0,"method":"ping"}),
        json!({"jsonrpc":"2.0","id":"discovery","method":"tools/list"}),
        call(json!(1), "nextral.memory.ingest", ingest),
        call(
            json!("retrieve"),
            "nextral.memory.retrieve",
            json!({
                "tenant_id":"stdio-tenant","user_id":"stdio-user","query_text":"PostgreSQL","entities":[],
                "token_budget":1800,"privacy_scope":["private"],"top_k_vector":12,"max_graph_hops":2
            }),
        ),
        call(
            json!(3),
            "nextral.memory.forget",
            json!({
                "tenant_id":"stdio-tenant","user_id":"stdio-user","memory_id":"stdio-memory",
                "actor":"stdio-user","reason":"regression cleanup","redact":true
            }),
        ),
        call(
            json!(4),
            "nextral.memory.list",
            json!({"tenant_id":"stdio-tenant","user_id":"stdio-user"}),
        ),
    ]);
    assert_eq!(
        replies.iter().map(|reply| &reply["id"]).collect::<Vec<_>>(),
        vec![
            &json!("initialize"),
            &json!(0),
            &json!("discovery"),
            &json!(1),
            &json!("retrieve"),
            &json!(3),
            &json!(4)
        ]
    );
    assert_eq!(replies[0]["result"]["protocolVersion"], "2025-11-25");
    let tools = replies[2]["result"]["tools"].as_array().unwrap();
    let ingest_tool = tools
        .iter()
        .find(|tool| tool["name"] == "nextral.memory.ingest")
        .unwrap();
    assert_eq!(
        ingest_tool["inputSchema"]["properties"]["content"]["type"],
        "string"
    );
    assert_eq!(ingest_tool["annotations"]["readOnlyHint"], false);
    assert_eq!(
        replies[3]["result"]["structuredContent"]["record_id"],
        "stdio-memory"
    );
    assert_eq!(
        replies[4]["result"]["structuredContent"]["items"][0]["memory_id"],
        "stdio-memory"
    );
    assert_eq!(replies[5]["result"]["isError"], false);
    assert_eq!(
        replies[6]["result"]["structuredContent"]["result"],
        json!([])
    );
}

#[test]
fn protocol_and_tool_errors_preserve_ids_without_responding_to_notifications() {
    let replies = exchange(&[
        json!({"jsonrpc":"2.0","id":"early","method":"tools/list"}),
        initialize(),
        initialized(),
        json!({"jsonrpc":"2.0","method":"unrecognized/notification"}),
        json!({"jsonrpc":"2.0","id":8,"method":"unknown/method"}),
        call(json!("unknown-tool"), "missing", json!({})),
        call(json!("invalid-args"), "nextral.memory.ingest", json!({})),
        call(
            json!("missing-memory"),
            "nextral.memory.forget",
            json!({
                "tenant_id":"t","user_id":"u","memory_id":"absent","actor":"u","reason":"test","redact":false
            }),
        ),
    ]);
    assert_eq!(replies.len(), 6);
    assert_eq!(replies[0]["error"]["code"], -32002);
    assert_eq!(replies[2]["id"], 8);
    assert_eq!(replies[2]["error"]["code"], -32601);
    assert_eq!(replies[3]["id"], "unknown-tool");
    assert_eq!(replies[3]["error"]["code"], -32602);
    assert_eq!(replies[4]["error"]["code"], -32602);
    assert_eq!(replies[5]["id"], "missing-memory");
    assert_eq!(replies[5]["result"]["isError"], true);
    assert_eq!(
        replies[5]["result"]["structuredContent"]["error"]["code"],
        "not_found"
    );
}

#[test]
fn malformed_json_returns_parse_error_and_next_line_remains_usable() {
    let mut output = Vec::new();
    nextral_mcp::protocol::serve(
        &mut &b"not-json\n{\"jsonrpc\":\"2.0\",\"id\":0,\"method\":\"ping\"}\n"[..],
        &mut output,
    )
    .unwrap();
    let replies: Vec<Value> = String::from_utf8(output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(replies[0]["id"], Value::Null);
    assert_eq!(replies[0]["error"]["code"], -32700);
    assert_eq!(replies[1], json!({"jsonrpc":"2.0","id":0,"result":{}}));
}

#[test]
fn version_negotiation_and_initialized_notification_gate_tool_access() {
    let mut request = initialize();
    request["params"]["protocolVersion"] = json!("unsupported-version");
    let replies = exchange(&[
        request,
        json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}),
        initialized(),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
        initialize(),
    ]);
    assert_eq!(replies[0]["result"]["protocolVersion"], "2025-11-25");
    assert_eq!(replies[1]["error"]["code"], -32002);
    assert!(replies[2]["result"]["tools"].is_array());
    assert_eq!(replies[3]["error"]["code"], -32600);
}

#[test]
fn malformed_request_ids_and_batches_are_invalid_requests() {
    let mut server = nextral_mcp::protocol::Server::default();
    for request in [
        json!([]),
        json!({"jsonrpc":"2.0","id":null,"method":"ping"}),
        json!({"jsonrpc":"2.0","id":false,"method":"ping"}),
        json!({"jsonrpc":"1.0","id":1,"method":"ping"}),
    ] {
        let reply = server.handle(request.to_string().as_bytes()).unwrap();
        assert_eq!(reply["error"]["code"], -32600);
    }
}
