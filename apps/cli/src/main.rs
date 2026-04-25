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
            let json = fs::read_to_string(path).map_err(|error| error.to_string())?;
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
        (Some("memory"), Some("ingest")) => {
            println!("{}", package::ingest_request_schema_json());
        }
        (Some("jobs"), Some("reembed-plan")) => {
            let path = args.next().ok_or("missing reembed plan path")?;
            let json = fs::read_to_string(path).map_err(|error| error.to_string())?;
            println!(
                "{}",
                package::reembed_plan_json(&json).map_err(|error| error.message)?
            );
        }
        _ => {
            println!("usage: nextral config validate <config.json> | memory ingest | memory smoke | jobs reembed-plan <request.json>");
        }
    }
    Ok(())
}
