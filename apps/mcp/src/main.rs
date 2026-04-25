use nextral::package::{e2e_smoke_json, ingest_request_schema_json};
use std::{env, process};

fn main() {
    if let Err(error) = run() {
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
                        "nextral.graph.query",
                        "nextral.reminders.due"
                    ]
                })
            );
        }
        Some("schema") => println!("{}", ingest_request_schema_json()),
        Some("smoke") => println!("{}", e2e_smoke_json().map_err(|error| error.message)?),
        _ => println!("usage: nextral-mcp tools | schema | smoke"),
    }
    Ok(())
}
