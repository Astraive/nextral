"""Installed command-line bridge to the canonical Rust package runtime."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence

from . import core
from ._version import __version__
from .errors import InvalidInputError, NextralError, SerializationError, StorageError


_COMMANDS = {
    "memory": {
        "ingest": "nextral.memory.ingest",
        "get": "nextral.memory.get",
        "retrieve": "nextral.memory.retrieve",
        "forget": "nextral.memory.forget",
        "list": "nextral.memory.list",
        "update": "nextral.memory.update",
        "stats": "nextral.memory.stats",
        "search-by-tag": "nextral.memory.search_by_tag",
        "search-by-entity": "nextral.memory.search_by_entity",
    },
    "session": {
        "append": "nextral.session.append",
        "eligible": "nextral.session.eligible",
        "context": "nextral.session.context",
    },
    "graph": {"query": "nextral.graph.query", "graphify": "nextral.graph.graphify"},
    "reminders": {
        "due": "nextral.reminders.due",
        "schedule": "nextral.reminders.schedule",
        "list": "nextral.reminders.list",
    },
    "consolidation": {"run": "nextral.consolidation.run"},
    "batch": {
        "ingest": "nextral.batch.ingest",
        "retrieve": "nextral.batch.retrieve",
        "forget": "nextral.batch.forget",
    },
    "runtime": {"scored-search": "nextral.runtime.scored_search"},
    "outbox": {"run": "nextral.outbox.run"},
    "policy": {
        "upsert": "nextral.policy.upsert",
        "list": "nextral.policy.list",
        "remove": "nextral.policy.remove",
    },
}


def _load_json(source: str) -> dict[str, Any]:
    try:
        if source == "-":
            text = sys.stdin.read()
        elif source.lstrip().startswith(("{", "[")):
            text = source
        else:
            text = Path(source).read_text(encoding="utf-8")
        value = json.loads(text)
    except (OSError, UnicodeError) as error:
        raise StorageError("io", str(error)) from error
    except ValueError as error:
        raise SerializationError("serialization", str(error)) from error
    if not isinstance(value, dict):
        raise InvalidInputError("invalid_input", "JSON input must be an object")
    return value


def _runtime_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config", dest="runtime_config", default=argparse.SUPPRESS,
        help="Configure the runtime from JSON (otherwise Rust reads NEXTRAL_CONFIG).",
    )


def _request_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("request", help="Inline JSON, JSON file, or '-' for stdin.")
    _runtime_option(parser)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="nextral", description=__doc__)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    _runtime_option(parser)
    commands = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    about = commands.add_parser("about", help="Show package metadata.")
    about.set_defaults(action="about")

    config = commands.add_parser("config", help="Configuration operations.")
    config_commands = config.add_subparsers(required=True)
    validate = config_commands.add_parser("validate", help="Validate runtime configuration.")
    validate.add_argument("request", help="JSON configuration file, inline JSON, or '-'.")
    validate.set_defaults(action="validate")

    for group_name, operations in _COMMANDS.items():
        group = commands.add_parser(group_name, help=f"{group_name.capitalize()} operations.")
        subcommands = group.add_subparsers(dest="operation", required=True)
        for operation, tool in operations.items():
            command = subcommands.add_parser(operation, help=f"Execute {tool}.")
            _request_option(command)
            command.set_defaults(action="tool", tool=tool)
        if group_name == "memory":
            schema = subcommands.add_parser("ingest-schema", help="Print the ingest request template.")
            schema.set_defaults(action="schema")
            smoke = subcommands.add_parser("smoke", help="Execute an isolated native testkit flow.")
            smoke.set_defaults(action="smoke")

    jobs = commands.add_parser("jobs", help="Native runtime job operations.")
    job_commands = jobs.add_subparsers(required=True)
    plan = job_commands.add_parser("reembed-plan", help="Plan vector re-embedding.")
    _request_option(plan)
    plan.set_defaults(action="reembed_plan")
    execute = job_commands.add_parser("reembed-execute", help="Execute host-vector re-embedding.")
    _request_option(execute)
    execute.set_defaults(action="tool", tool="nextral.jobs.reembed.execute")
    for operation in ("enqueue", "run", "list", "get"):
        command = job_commands.add_parser(operation, help=f"Execute nextral.jobs.{operation}.")
        _request_option(command)
        command.set_defaults(action="tool", tool=f"nextral.jobs.{operation}")

    database = commands.add_parser("db", help="Database migrations.")
    database_commands = database.add_subparsers(required=True)
    migrate = database_commands.add_parser("migrate", help="Apply native database migrations.")
    migrate.add_argument("request", help="Runtime configuration JSON file, inline JSON, or '-'.")
    migrate.set_defaults(action="db_migrate")

    adapters = commands.add_parser("adapters", help="Production adapter operations.")
    adapter_commands = adapters.add_subparsers(required=True)
    smoke = adapter_commands.add_parser("smoke", help="Exercise real production adapters.")
    _request_option(smoke)
    smoke.set_defaults(action="adapter_smoke")

    health = commands.add_parser("health", help="Report native package health.")
    _runtime_option(health)
    health.set_defaults(action="health")

    mcp = commands.add_parser("mcp", help="Raw native tool dispatch.")
    mcp_commands = mcp.add_subparsers(required=True)
    call = mcp_commands.add_parser("call", help="Dispatch a native MCP call envelope.")
    _request_option(call)
    call.set_defaults(action="mcp")

    serve = commands.add_parser("serve", help="Run optional native service hosts.")
    modes = serve.add_subparsers(required=True)
    for mode in ("http", "grpc", "graphql", "all"):
        host = modes.add_parser(mode, help=f"Run {mode} until shutdown.")
        host.add_argument("request", help="Runtime configuration JSON file, inline JSON, or '-'.")
        host.set_defaults(action="serve", mode=mode)
    return parser


def _client_for(args: argparse.Namespace) -> core.Client:
    config_path = getattr(args, "runtime_config", None)
    if config_path is not None:
        return core.Client(_load_json(config_path))
    import os
    env_config = os.environ.get("NEXTRAL_CONFIG")
    if env_config is not None:
        return core.Client(_load_json(env_config))
    return core.Client({})


def _execute(args: argparse.Namespace) -> Any:
    if args.action == "about":
        return {"name": "nextral", "version": __version__, "runtime": "rust"}
    if args.action == "validate":
        return core.validate_config(_load_json(args.request))
    if args.action == "schema":
        return core.ingest_request_schema()
    if args.action == "smoke":
        return core.e2e_smoke()
    if args.action == "health":
        with _client_for(args) as client:
            return client.health()
    if args.action == "db_migrate":
        with core.Client(_load_json(args.request)) as client:
            return client.migrate_database()
    if args.action == "serve":
        import signal
        import time
        client = core.Client(_load_json(args.request))
        handle = client.serve()
        stop = False
        def on_stop(*_):
            nonlocal stop
            stop = True
        try:
            signal.signal(signal.SIGINT, on_stop)
            signal.signal(signal.SIGTERM, on_stop)
        except (ValueError, AttributeError):
            pass
        try:
            while not stop:
                time.sleep(0.05)
        finally:
            handle.shutdown()
            client.close()
        return {"status": "stopped", "binds": handle.binds}
    request = _load_json(args.request)
    if args.action == "tool":
        with _client_for(args) as client:
            return client.call(args.tool, request)
    if args.action == "reembed_plan":
        return core.reembed_plan(request)
    if args.action == "adapter_smoke":
        return core.adapter_smoke(request)
    if args.action == "mcp":
        tool = request.get("tool")
        payload = request.get("payload_json")
        if not isinstance(tool, str) or not isinstance(payload, str):
            raise InvalidInputError(
                "invalid_input", "MCP envelope requires string tool and payload_json fields"
            )
        try:
            decoded = json.loads(payload)
        except ValueError as error:
            raise SerializationError("serialization", str(error)) from error
        if not isinstance(decoded, dict):
            raise InvalidInputError("invalid_input", "MCP payload_json must encode an object")
        with _client_for(args) as client:
            return client.call(tool, decoded)
    raise InvalidInputError("invalid_input", f"unknown command action: {args.action}")


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = _execute(args)
        print(json.dumps(result, ensure_ascii=False, allow_nan=False))
        return 0
    except NextralError as error:
        print(json.dumps({"error": error.as_dict()}), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
    except BrokenPipeError:
        return 1
