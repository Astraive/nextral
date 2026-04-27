use nextral::{
    api::{run_service_hosts, startup_plan, startup_readiness_matrix, ServiceMode},
    config::validate_config_json,
    package::mcp_call_json,
};
use std::{env, fs, process};

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let command = args.next().unwrap_or_else(|| "help".to_string());
    match command.as_str() {
        "validate-config" => {
            let path = args.next().ok_or("missing config path")?;
            let json = fs::read_to_string(path).map_err(|error| error.to_string())?;
            println!(
                "{}",
                validate_config_json(&json).map_err(|error| error.to_string())?
            );
        }
        "plan" => {
            let mode = match args.next().unwrap_or_else(|| "all".to_string()).as_str() {
                "http" => ServiceMode::Http,
                "grpc" => ServiceMode::Grpc,
                "graphql" => ServiceMode::Graphql,
                "all" => ServiceMode::All,
                other => return Err(format!("unknown service mode: {other}")),
            };
            let path = args.next().ok_or("missing config path")?;
            let json = fs::read_to_string(path).map_err(|error| error.to_string())?;
            let config: nextral::config::NextralConfig =
                serde_json::from_str(&json).map_err(|error| error.to_string())?;
            let plan = startup_plan(&config, mode).map_err(|error| error.to_string())?;
            println!(
                "{}",
                serde_json::to_string_pretty(&plan).map_err(|error| error.to_string())?
            );
        }
        "execute" => {
            let request = args.next().ok_or("missing execute request json")?;
            println!(
                "{}",
                mcp_call_json(&request).map_err(|error| error.message)?
            );
        }
        "serve" => {
            let mode = match args.next().unwrap_or_else(|| "all".to_string()).as_str() {
                "http" => ServiceMode::Http,
                "grpc" => ServiceMode::Grpc,
                "graphql" => ServiceMode::Graphql,
                "all" => ServiceMode::All,
                other => return Err(format!("unknown service mode: {other}")),
            };
            let path = args.next().ok_or("missing config path")?;
            let json = fs::read_to_string(path).map_err(|error| error.to_string())?;
            let config: nextral::config::NextralConfig =
                serde_json::from_str(&json).map_err(|error| error.to_string())?;
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &run_service_hosts(&config, mode).map_err(|error| error.to_string())?
                )
                .map_err(|error| error.to_string())?
            );
        }
        "readiness" => {
            let path = args.next().ok_or("missing config path")?;
            let json = fs::read_to_string(path).map_err(|error| error.to_string())?;
            let config: nextral::config::NextralConfig =
                serde_json::from_str(&json).map_err(|error| error.to_string())?;
            let matrix = startup_readiness_matrix(&config).map_err(|error| error.to_string())?;
            println!(
                "{}",
                serde_json::to_string_pretty(&matrix).map_err(|error| error.to_string())?
            );
        }
        _ => {
            println!("usage: nextral-api validate-config <config.json> | plan <http|grpc|graphql|all> <config.json> | execute '<json>' | serve <http|grpc|graphql|all> <config.json> | readiness <config.json>");
        }
    }
    Ok(())
}
