#![recursion_limit = "512"]

pub mod catalog;
mod promote_schema;
pub mod protocol;

use nextral::package::PackageError;
use serde_json::Value;
use std::{
    env, fs,
    io::{self, Read},
    sync::LazyLock,
};

pub fn create_client(path: Option<&str>) -> Result<nextral::package::RuntimeClient, PackageError> {
    let environment = env::var("NEXTRAL_CONFIG").ok();
    let config = if let Some(path) = path.or(environment.as_deref()) {
        let text = read_file(path)?;
        nextral::config::load_config(&text).map_err(PackageError::from)?
    } else {
        let mut cfg = nextral::config::NextralConfig::default();
        cfg.backend = nextral::config::RuntimeBackend::TestMemory;
        cfg
    };
    nextral::package::RuntimeClient::from_config(config)
}

pub fn input_error(message: impl Into<String>) -> PackageError {
    PackageError {
        code: "invalid_input".into(),
        message: message.into(),
        execution_status: None,
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
            execution_status: None,
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
                execution_status: None,
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
        execution_status: None,
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
    let client = create_client(config_path)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| PackageError {
            code: "io".into(),
            message: e.to_string(),
            execution_status: None,
        })?;
    rt.block_on(client.call_json(
        name,
        arguments.to_string(),
        nextral::package::CallOptions::default(),
    ))
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
