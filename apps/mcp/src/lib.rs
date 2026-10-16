#![recursion_limit = "512"]

pub mod catalog;
mod promote_schema;
pub mod protocol;

use nextral::package::{self, PackageError};
use serde_json::{json, Value};
use std::{
    env, fs,
    io::{self, Read},
    sync::LazyLock,
};

/// Configure the process explicitly, preferring --config over NEXTRAL_CONFIG.
/// With neither present the package resolves its own explicit runtime environment.
pub fn configure_runtime(path: Option<&str>) -> Result<(), PackageError> {
    let environment = env::var("NEXTRAL_CONFIG").ok();
    if let Some(path) = path.or(environment.as_deref()) {
        let config = read_file(path)?;
        package::configure_runtime_json(&config)?;
    }
    Ok(())
}

pub fn input_error(message: impl Into<String>) -> PackageError {
    PackageError {
        code: "invalid_input".into(),
        message: message.into(),
    }
}

pub fn read_file(path: &str) -> Result<String, PackageError> {
    fs::read_to_string(path)
        .map(|mut text| {
            if text.starts_with('\u{feff}') {
                text.replace_range(..'\u{feff}'.len_utf8(), "");
            }
            text
        })
        .map_err(|error| PackageError {
            code: "io".into(),
            message: format!("cannot read {path}: {error}"),
        })
}

/// Accept inline JSON, a file path (optionally prefixed with @), or - for stdin.
pub fn read_json_input(input: &str) -> Result<Value, PackageError> {
    let text = if input == "-" {
        let mut text = String::new();
        io::stdin()
            .read_to_string(&mut text)
            .map_err(|error| PackageError {
                code: "io".into(),
                message: error.to_string(),
            })?;
        std::borrow::Cow::Owned(text)
    } else if input.trim_start().starts_with(['{', '[']) {
        std::borrow::Cow::Borrowed(input)
    } else {
        std::borrow::Cow::Owned(read_file(input.strip_prefix('@').unwrap_or(input))?)
    };
    serde_json::from_str(text.trim_start_matches('\u{feff}'))
        .map_err(|error| input_error(format!("invalid JSON: {error}")))
}

pub fn validate_arguments(name: &str, arguments: &Value) -> Result<(), PackageError> {
    static VALIDATORS: LazyLock<Result<Vec<jsonschema::Validator>, String>> = LazyLock::new(|| {
        catalog::tools()
            .iter()
            .map(|tool| {
                jsonschema::validator_for(&tool["inputSchema"]).map_err(|error| error.to_string())
            })
            .collect()
    });
    let index = catalog::tools()
        .iter()
        .position(|tool| tool["name"] == name)
        .ok_or_else(|| input_error(format!("unknown tool: {name}")))?;
    let validators = VALIDATORS.as_ref().map_err(|message| PackageError {
        code: "internal".into(),
        message: format!("invalid tool schema: {message}"),
    })?;
    validators[index]
        .validate(arguments)
        .map_err(|error| input_error(format!("{name}: {error} at {}", error.instance_path)))
}

pub fn call_tool(
    name: &str,
    arguments: &Value,
    config_path: Option<&str>,
) -> Result<String, PackageError> {
    validate_arguments(name, arguments)?;
    configure_runtime(config_path)?;
    package::mcp_call_json(&json!({"tool":name,"payload_json":arguments.to_string()}).to_string())
}

/// Extract one leading --config option; commands retain strict positional parsing.
pub fn command_arguments(
    mut arguments: Vec<String>,
) -> Result<(Option<String>, Vec<String>), PackageError> {
    if arguments
        .first()
        .is_some_and(|argument| argument == "--config")
    {
        if arguments.len() < 2 || arguments[1].starts_with('-') {
            return Err(input_error("--config requires a configuration file path"));
        }
        let path = arguments.remove(1);
        arguments.remove(0);
        Ok((Some(path), arguments))
    } else {
        Ok((None, arguments))
    }
}
