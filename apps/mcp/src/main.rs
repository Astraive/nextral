use nextral::package::PackageError;
use nextral_mcp::{command_arguments, create_client, input_error, protocol};
use std::{env, io, process::ExitCode};

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_ansi(false)
        .init();
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!(
                "{}",
                serde_json::json!({"error":{"code":error.code,"message":error.message}})
            );
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), PackageError> {
    let (config, args) = command_arguments(env::args().skip(1).collect())?;
    match args.as_slice() {
        [flag] if flag == "--help" || flag == "-h" => {
            eprintln!("nextral-mcp [--config FILE] [stdio]\nNewline-delimited MCP JSON-RPC over stdin/stdout. Configure with --config or NEXTRAL_CONFIG.\n--help, -h    Show help\n--version, -V Show version");
            return Ok(());
        }
        [flag] if flag == "--version" || flag == "-V" => {
            eprintln!("nextral-mcp {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        [] => {}
        [command] if command == "stdio" => {}
        _ => {
            return Err(input_error(
                "usage: nextral-mcp [--config FILE] [stdio] | --help | --version",
            ))
        }
    }
    let _client = create_client(config.as_deref())?;
    protocol::serve(&mut io::stdin().lock(), &mut io::stdout().lock()).map_err(|error| {
        PackageError {
            code: "io".into(),
            message: error.to_string(),
            execution_status: None,
        }
    })
}
