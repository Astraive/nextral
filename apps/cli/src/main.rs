use nextral::{config::validate_config_json, package};
use std::{env, fs, process};

fn main() {
    if let Err(error) = run() {
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
        (Some("mcp"), Some("call")) => {
            let request = args.next().ok_or("missing mcp call request json")?;
            println!(
                "{}",
                package::mcp_call_json(&request).map_err(|error| error.message)?
            );
        }
        _ => {
            println!("usage: nextral config validate <config.json> | adapters smoke <request.json> | memory ingest | memory smoke | jobs reembed-plan <request.json> | mcp call '<json>'");
        }
    }
    Ok(())
}

fn read_json_file(path: String) -> Result<String, String> {
    let json = fs::read_to_string(path).map_err(|error| error.to_string())?;
    Ok(json.trim_start_matches('\u{feff}').to_string())
}
