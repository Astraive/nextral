use nextral::package::{e2e_smoke_json, ingest_request_schema_json, mcp_call_json};
use std::{env, process};

fn main() {
    tracing_subscriber::fmt::init();
    if let Err(error) = run() {
        tracing::error!(error = %error, "MCP call failed");
        eprintln!("{error}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    match env::args().nth(1).as_deref() {
        Some("tools") => {
            println!(
                "{}",
                serde_json::json!({
                    "tools": [
                        "nextral.memory.ingest",
                        "nextral.memory.retrieve",
                        "nextral.memory.forget",
                        "nextral.memory.list",
                        "nextral.graph.query",
                        "nextral.graph.graphify",
                        "nextral.reminders.due",
                        "nextral.reminders.schedule",
                        "nextral.session.append",
                        "nextral.session.context",
                        "nextral.consolidation.run",
                        "nextral.reembed.plan",
                        "nextral.health",
                        "nextral.runtime.scored_search",
                        "nextral.memory.update",
                        "nextral.memory.stats",
                        "nextral.memory.search_by_tag",
                        "nextral.memory.search_by_entity",
                        "nextral.batch.ingest",
                        "nextral.batch.retrieve",
                        "nextral.batch.forget",
                        "experiments.create",
                        "experiments.promote",
                        "experiments.rollback",
                        "experiments.status",
                        "safety.policy.get",
                        "safety.policy.set"
                    ]
                })
            );
        }
        Some("schema") => println!("{}", ingest_request_schema_json()),
        Some("smoke") => println!("{}", e2e_smoke_json().map_err(|error| error.message)?),
        Some("call") => {
            let request_json = env::args().nth(2).ok_or("missing MCP call request JSON")?;
            println!(
                "{}",
                mcp_call_json(&request_json).map_err(|error| error.message)?
            );
        }
        _ => println!("usage: nextral-mcp tools | schema | smoke | call '<json>'"),
    }
    Ok(())
}
