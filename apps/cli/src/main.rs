use nextral::{
    config::validate_config_json,
    package::{self, PackageError},
};
use nextral_mcp::{call_tool, catalog, command_arguments, input_error, read_file, read_json_input};
use serde_json::{json, Value};
use std::{env, io, process::ExitCode};

const OPERATIONS: &[(&str, &str, &str)] = &[
    ("memory", "ingest", "nextral.memory.ingest"),
    ("memory", "retrieve", "nextral.memory.retrieve"),
    ("memory", "forget", "nextral.memory.forget"),
    ("memory", "get", "nextral.memory.get"),
    ("memory", "list", "nextral.memory.list"),
    ("memory", "query", "nextral.memory.list"),
    ("memory", "update", "nextral.memory.update"),
    ("memory", "stats", "nextral.memory.stats"),
    ("memory", "search-by-tag", "nextral.memory.search_by_tag"),
    (
        "memory",
        "search-by-entity",
        "nextral.memory.search_by_entity",
    ),
    ("session", "append", "nextral.session.append"),
    ("session", "eligible", "nextral.session.eligible"),
    ("session", "context", "nextral.session.context"),
    ("policy", "upsert", "nextral.policy.upsert"),
    ("policy", "list", "nextral.policy.list"),
    ("policy", "remove", "nextral.policy.remove"),
    ("graph", "query", "nextral.graph.query"),
    ("graph", "graphify", "nextral.graph.graphify"),
    ("reminders", "schedule", "nextral.reminders.schedule"),
    ("reminders", "due", "nextral.reminders.due"),
    ("reminders", "list", "nextral.reminders.list"),
    ("consolidation", "run", "nextral.consolidation.run"),
    ("batch", "ingest", "nextral.batch.ingest"),
    ("batch", "retrieve", "nextral.batch.retrieve"),
    ("batch", "forget", "nextral.batch.forget"),
    ("jobs", "reembed-plan", "nextral.reembed.plan"),
    ("jobs", "reembed-execute", "nextral.jobs.reembed.execute"),
    ("jobs", "enqueue", "nextral.jobs.enqueue"),
    ("jobs", "run", "nextral.jobs.run"),
    ("jobs", "list", "nextral.jobs.list"),
    ("jobs", "get", "nextral.jobs.get"),
    ("outbox", "run", "nextral.outbox.run"),
    ("database", "migrate", "nextral.database.migrate"),
    ("runtime", "scored-search", "nextral.runtime.scored_search"),
    ("experiments", "create", "experiments.create"),
    ("experiments", "promote", "experiments.promote"),
    ("experiments", "rollback", "experiments.rollback"),
    ("experiments", "status", "experiments.status"),
    ("safety", "get", "safety.policy.get"),
    ("safety", "set", "safety.policy.set"),
    ("index", "repair", "nextral.index.repair"),
];

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_ansi(false)
        .init();
    match run(env::args().skip(1).collect()) {
        Ok(output) => {
            println!("{output}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!(
                "{}",
                json!({"error":{"code":error.code,"message":error.message}})
            );
            ExitCode::FAILURE
        }
    }
}

fn help(group: Option<&str>) -> Value {
    let commands: Vec<_> = OPERATIONS.iter()
        .filter(|(name, _, _)| group.is_none_or(|group| group == *name))
        .map(|(group, operation, tool)| json!({"command":format!("{group} {operation} <JSON|FILE|@FILE|->"),"tool":tool}))
        .collect();
    json!({
        "name":"nextral","version":env!("CARGO_PKG_VERSION"),
        "usage":"nextral [--config FILE] <command>",
        "configuration":"--config FILE overrides NEXTRAL_CONFIG. Production calls use the configured durable backend; no implicit test backend.",
        "input":"Pass an inline JSON object, a JSON file path, @FILE, or - to read stdin.",
        "commands":commands,
        "other_commands":["help [GROUP]","--help","--version","config validate FILE","schema [TOOL]","tools","call TOOL <JSON|FILE|@FILE|->","health","memory smoke","adapters smoke <JSON|FILE|@FILE|->"]
    })
}

fn run(arguments: Vec<String>) -> Result<String, PackageError> {
    let (config, args) = command_arguments(arguments)?;
    match args.as_slice() {
        [flag] if flag == "--help" || flag == "-h" || flag == "help" => {
            return Ok(help(None).to_string())
        }
        [flag] if flag == "--version" || flag == "-V" => {
            return Ok(json!({"name":"nextral","version":env!("CARGO_PKG_VERSION")}).to_string())
        }
        [command, group]
            if command == "help"
                && OPERATIONS
                    .iter()
                    .any(|(name, _, _)| *name == group.as_str()) =>
        {
            return Ok(help(Some(group)).to_string())
        }
        [group, flag]
            if (flag == "--help" || flag == "-h")
                && OPERATIONS
                    .iter()
                    .any(|(name, _, _)| *name == group.as_str()) =>
        {
            return Ok(help(Some(group)).to_string())
        }
        [command] if command == "tools" => return Ok(json!({"tools":catalog::tools()}).to_string()),
        [command] if command == "schema" => {
            return Ok(json!({"tools":catalog::tools()}).to_string())
        }
        [command, name] if command == "schema" => {
            return catalog::tool(name)
                .map(|tool| tool["inputSchema"].to_string())
                .ok_or_else(|| input_error(format!("unknown tool: {name}")))
        }
        [command, operation, path] if command == "config" && operation == "validate" => {
            return validate_config_json(&read_file(path)?)
                .map_err(|error| input_error(error.to_string()));
        }
        [command, operation] if command == "memory" && operation == "smoke" => {
            return package::e2e_smoke_json();
        }
        [command, operation, input] if command == "adapters" && operation == "smoke" => {
            return package::adapter_smoke_json(&read_json_input(input)?.to_string());
        }
        [command] if command == "health" => {
            return call_tool("nextral.health", &json!({}), config.as_deref());
        }
        [command, tool, input] if command == "call" => {
            let arguments = read_json_input(input)?;
            return call_tool(tool, &arguments, config.as_deref());
        }
        [command, action, rest @ ..] if command == "worker" && action == "run" => {
            let mut worker_path = None;
            let mut config_path = config.clone();
            let mut i = 0;
            while i < rest.len() {
                if rest[i] == "--worker" && i + 1 < rest.len() {
                    worker_path = Some(rest[i + 1].clone());
                    i += 2;
                } else if rest[i] == "--config" && i + 1 < rest.len() {
                    config_path = Some(rest[i + 1].clone());
                    i += 2;
                } else {
                    return Err(input_error("usage: nextral worker run --config PATH --worker PATH"));
                }
            }
            let config_file = config_path.ok_or_else(|| input_error("--config PATH is required"))?;
            let worker_file = worker_path.ok_or_else(|| input_error("--worker PATH is required"))?;
            let cfg_text = read_file(&config_file)?;
            let wrk_text = read_file(&worker_file)?;
            let nextral_cfg = nextral::config::load_config(&cfg_text).map_err(PackageError::from)?;
            let worker_cfg: nextral::runtime::prospective::WorkerConfig = serde_json::from_str(&wrk_text).map_err(|e| input_error(e.to_string()))?;

            let client = package::RuntimeClient::from_config(nextral_cfg)?;
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| input_error(e.to_string()))?;
            rt.block_on(async {
                let handle = client.start_worker(worker_cfg).await?;
                let _ = tokio::signal::ctrl_c().await;
                handle.shutdown(std::time::Duration::from_secs(30)).await?;
                client.close(std::time::Duration::from_secs(30)).await?;
                Ok::<_, PackageError>(())
            })?;
            return Ok(json!({"status":"stopped","worker":"drained"}).to_string());
        }
        [group, operation] => {
            let tool = OPERATIONS
                .iter()
                .find(|(name, command, _)| {
                    *name == group.as_str() && *command == operation.as_str()
                })
                .map(|(_, _, tool)| *tool)
                .ok_or_else(|| {
                    input_error(format!("unknown command: {group} {operation}; use --help"))
                })?;
            return call_tool(tool, &json!({}), config.as_deref());
        }
        [group, operation, input] => {
            let tool = OPERATIONS
                .iter()
                .find(|(name, command, _)| {
                    *name == group.as_str() && *command == operation.as_str()
                })
                .map(|(_, _, tool)| *tool)
                .ok_or_else(|| {
                    input_error(format!("unknown command: {group} {operation}; use --help"))
                })?;
            let arguments = read_json_input(input)?;
            return call_tool(tool, &arguments, config.as_deref());
        }
        _ => {}
    }
    Err(input_error(
        "invalid command or arguments; use nextral --help",
    ))
}

#[cfg(test)]
mod tests {
    use super::run;

    #[test]
    fn ingest_requires_a_request_instead_of_printing_a_schema() {
        assert!(run(vec!["memory".into(), "ingest".into()]).is_err());
    }

    #[test]
    fn unknown_commands_and_extra_arguments_are_errors() {
        for arguments in [
            vec!["unknown"],
            vec!["health", "extra"],
            vec!["--version", "extra"],
        ] {
            assert!(run(arguments.into_iter().map(str::to_owned).collect()).is_err());
        }
    }

    #[test]
    fn invalid_ingest_arguments_are_rejected_before_runtime_configuration() {
        assert!(run(vec!["memory".into(), "ingest".into(), "{}".into()]).is_err());
    }
}
