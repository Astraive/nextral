use nextral::{config::validate_config_json, package};
use std::{env, fs, process};

fn main() {
    tracing_subscriber::fmt::init();
    if let Err(error) = run() {
        tracing::error!(error = %error, "command failed");
        eprintln!("{error}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let command = args.next();
    let subcommand = args.next();
    match (command.as_deref(), subcommand.as_deref()) {
        (Some("config"), Some("validate")) => {
            let path = args.next().ok_or("missing config path")?;
            let json = read_json_file(path)?;
            println!(
                "{}",
                validate_config_json(&json).map_err(|error| error.to_string())?
            );
        }
        (Some("memory"), Some("smoke")) => {
            println!(
                "{}",
                package::e2e_smoke_json().map_err(|error| error.message)?
            );
        }
        (Some("adapters"), Some("smoke")) => {
            let path = args.next().ok_or("missing adapter smoke request path")?;
            let json = read_json_file(path)?;
            println!(
                "{}",
                package::adapter_smoke_json(&json).map_err(|error| error.message)?
            );
        }
        (Some("memory"), Some("ingest")) => {
            println!("{}", package::ingest_request_schema_json());
        }
        (Some("jobs"), Some("reembed-plan")) => {
            let path = args.next().ok_or("missing reembed plan path")?;
            let json = read_json_file(path)?;
            println!(
                "{}",
                package::reembed_plan_json(&json).map_err(|error| error.message)?
            );
        }
        (Some("memory"), Some("retrieve")) => {
            let input = args.next().ok_or("missing retrieve request json")?;
            let json = read_json_arg_or_file(&input)?;
            let request = build_mcp_request("nextral.memory.retrieve", &json);
            println!(
                "{}",
                package::mcp_call_json(&request).map_err(|error| error.message)?
            );
        }
        (Some("memory"), Some("forget")) => {
            let input = args.next().ok_or("missing forget request json")?;
            let json = read_json_arg_or_file(&input)?;
            let request = build_mcp_request("nextral.memory.forget", &json);
            println!(
                "{}",
                package::mcp_call_json(&request).map_err(|error| error.message)?
            );
        }
        (Some("memory"), Some("list")) => {
            let input = args.next().ok_or("missing list request json")?;
            let json = read_json_arg_or_file(&input)?;
            let request = build_mcp_request("nextral.memory.list", &json);
            println!(
                "{}",
                package::mcp_call_json(&request).map_err(|error| error.message)?
            );
        }
        (Some("session"), Some("append")) => {
            let input = args.next().ok_or("missing append request json")?;
            let json = read_json_arg_or_file(&input)?;
            let request = build_mcp_request("nextral.session.append", &json);
            println!(
                "{}",
                package::mcp_call_json(&request).map_err(|error| error.message)?
            );
        }
        (Some("session"), Some("context")) => {
            let input = args.next().ok_or("missing context request json")?;
            let json = read_json_arg_or_file(&input)?;
            let request = build_mcp_request("nextral.session.context", &json);
            println!(
                "{}",
                package::mcp_call_json(&request).map_err(|error| error.message)?
            );
        }
        (Some("reminders"), Some("due")) => {
            let input = args.next().ok_or("missing due request json")?;
            let json = read_json_arg_or_file(&input)?;
            let request = build_mcp_request("nextral.reminders.due", &json);
            println!(
                "{}",
                package::mcp_call_json(&request).map_err(|error| error.message)?
            );
        }
        (Some("reminders"), Some("schedule")) => {
            let input = args.next().ok_or("missing schedule request json")?;
            let json = read_json_arg_or_file(&input)?;
            let request = build_mcp_request("nextral.reminders.schedule", &json);
            println!(
                "{}",
                package::mcp_call_json(&request).map_err(|error| error.message)?
            );
        }
        (Some("consolidation"), Some("run")) => {
            let input = args.next().ok_or("missing consolidation request json")?;
            let json = read_json_arg_or_file(&input)?;
            let request = build_mcp_request("nextral.consolidation.run", &json);
            println!(
                "{}",
                package::mcp_call_json(&request).map_err(|error| error.message)?
            );
        }
        (Some("health"), _) => {
            println!("{}", serde_json::json!({"status": "ok"}));
        }
        (Some("batch"), Some("ingest")) => {
            let input = args.next().ok_or("missing batch ingest request json")?;
            let json = read_json_arg_or_file(&input)?;
            let request = build_mcp_request("nextral.batch.ingest", &json);
            println!(
                "{}",
                package::mcp_call_json(&request).map_err(|error| error.message)?
            );
        }
        (Some("batch"), Some("retrieve")) => {
            let input = args.next().ok_or("missing batch retrieve request json")?;
            let json = read_json_arg_or_file(&input)?;
            let request = build_mcp_request("nextral.batch.retrieve", &json);
            println!(
                "{}",
                package::mcp_call_json(&request).map_err(|error| error.message)?
            );
        }
        (Some("batch"), Some("forget")) => {
            let input = args.next().ok_or("missing batch forget request json")?;
            let json = read_json_arg_or_file(&input)?;
            let request = build_mcp_request("nextral.batch.forget", &json);
            println!(
                "{}",
                package::mcp_call_json(&request).map_err(|error| error.message)?
            );
        }
        (Some("mcp"), Some("call")) => {
            let request = args.next().ok_or("missing mcp call request json")?;
            println!(
                "{}",
                package::mcp_call_json(&request).map_err(|error| error.message)?
            );
        }
        _ => {
            println!("usage: nextral config validate <config.json> | memory ingest | memory smoke | memory retrieve <json> | memory forget <json> | memory list <json> | session append <json> | session context <json> | reminders due <json> | reminders schedule <json> | consolidation run <json> | health | batch ingest <json> | batch retrieve <json> | batch forget <json> | jobs reembed-plan <request.json> | adapters smoke <request.json> | mcp call '<json>'");
        }
    }
    Ok(())
}

fn read_json_file(path: String) -> Result<String, String> {
    let json = fs::read_to_string(path).map_err(|error| error.to_string())?;
    Ok(json.trim_start_matches('\u{feff}').to_string())
}

fn read_json_arg_or_file(input: &str) -> Result<String, String> {
    if input.starts_with('{') || input.starts_with('[') {
        return Ok(input.to_string());
    }
    read_json_file(input.to_string())
}

fn build_mcp_request(tool: &str, payload_json: &str) -> String {
    serde_json::json!({
        "tool": tool,
        "payload_json": payload_json
    })
    .to_string()
}
