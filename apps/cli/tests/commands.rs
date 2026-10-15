use serde_json::{json, Value};
use std::process::Command;

#[test]
fn binary_ingest_executes_request_and_prints_a_receipt() {
    let request = json!({
        "id":"cli-memory","tenant_id":"cli-tenant","user_id":"cli-user","content":"The CLI executes ingestion",
        "content_type":"fact","memory_type":"semantic","source_type":"manual","source_message_ids":[],
        "importance_score":0.8,"confidence_score":0.9,"entities":[],"tags":[],"privacy_level":"private","graph_hints":[],
        "policy":{"min_importance_score":0.0,"min_confidence_score":0.0}
    });
    let output = Command::new(env!("CARGO_BIN_EXE_nextral"))
        .args([
            "--config",
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../examples/config_example.json"
            ),
            "memory",
            "ingest",
            &request.to_string(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["record_id"], "cli-memory");
    assert_eq!(receipt["status"], "accepted");
}

#[test]
fn invalid_commands_exit_nonzero_and_leave_stdout_machine_clean() {
    for arguments in [
        vec!["unknown"],
        vec!["memory", "ingest"],
        vec!["health", "extra"],
        vec!["call", "missing", "{}"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_nextral"))
            .args(arguments)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        let error: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(error["error"]["code"], "invalid_input");
    }
}

#[test]
fn schema_help_and_version_are_machine_readable_without_a_backend() {
    for arguments in [
        vec!["schema", "nextral.memory.ingest"],
        vec!["--help"],
        vec!["--version"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_nextral"))
            .args(arguments)
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(serde_json::from_slice::<Value>(&output.stdout)
            .unwrap()
            .is_object());
    }
}
