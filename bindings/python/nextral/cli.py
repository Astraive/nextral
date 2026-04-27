"""CLI for nextral."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from ._version import __version__
from .core import e2e_smoke, ingest_request_schema, mcp_call, reembed_plan, validate_config


def _load_json(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _handle_about(_: argparse.Namespace) -> int:
    print(f"nextral {__version__}")
    print("Status: package-first production runtime with configured storage and model providers.")
    print("Docs:")
    print("  - docs\\README.md")
    print("  - docs\\architecture\\project-structure.md")
    print("  - docs\\memory\\README.md")
    return 0


def _handle_config_validate(args: argparse.Namespace) -> int:
    result = validate_config(_load_json(args.config))
    print(json.dumps(result, indent=2))
    return 0


def _handle_e2e_smoke(_: argparse.Namespace) -> int:
    print(json.dumps(e2e_smoke(), indent=2))
    return 0


def _handle_ingest_schema(_: argparse.Namespace) -> int:
    print(json.dumps(ingest_request_schema(), indent=2))
    return 0


def _handle_reembed_plan(args: argparse.Namespace) -> int:
    print(json.dumps(reembed_plan(_load_json(args.request)), indent=2))
    return 0


def _handle_service_plan(args: argparse.Namespace) -> int:
    config = _load_json(args.config)
    validate_config(config)
    print(
        json.dumps(
            {
                "mode": args.mode,
                "status": "configured",
                "config": args.config,
            },
            indent=2,
        )
    )
    return 0


def _handle_graph_query(args: argparse.Namespace) -> int:
    print(
        json.dumps(
            mcp_call(
                "nextral.graph.query",
                {
                    "tenant_id": args.tenant_id,
                    "user_id": args.user_id,
                    "session_id": None,
                    "query_text": args.query_text,
                    "entities": args.entities,
                    "intent_topic": args.intent_topic,
                    "token_budget": args.token_budget,
                    "privacy_scope": args.privacy_scope,
                    "top_k_vector": args.top_k_vector,
                    "max_graph_hops": args.max_graph_hops,
                },
            ),
            indent=2,
        )
    )
    return 0


def _handle_due_reminders(args: argparse.Namespace) -> int:
    print(
        json.dumps(
            mcp_call(
                "nextral.reminders.due",
                {
                    "tenant_id": args.tenant_id,
                    "user_id": args.user_id,
                    "due_at_or_before": args.due_at_or_before,
                    "actor": args.actor,
                    "retry_delay_seconds": args.retry_delay_seconds,
                },
            ),
            indent=2,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nextral",
        description="Nextral CLI - package-first memory runtime backed by configured production stores.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")
    about_parser = subparsers.add_parser("about", help="Show package status and documentation entry points.")
    about_parser.set_defaults(handler=_handle_about)

    config_parser = subparsers.add_parser("config", help="Configuration commands.")
    config_subparsers = config_parser.add_subparsers(dest="config_command", metavar="SUBCOMMAND")
    validate_parser = config_subparsers.add_parser("validate", help="Validate a Nextral runtime config file.")
    validate_parser.add_argument("config", help="Path to JSON config.")
    validate_parser.set_defaults(handler=_handle_config_validate)

    memory_parser = subparsers.add_parser("memory", help="Memory ingestion, search, and governance commands.")
    memory_subparsers = memory_parser.add_subparsers(dest="memory_command", metavar="SUBCOMMAND")
    ingest_parser = memory_subparsers.add_parser("ingest", help="Print the JSON request shape for memory ingestion.")
    ingest_parser.set_defaults(handler=_handle_ingest_schema)
    smoke_parser = memory_subparsers.add_parser("smoke", help="Run a testkit E2E memory flow.")
    smoke_parser.set_defaults(handler=_handle_e2e_smoke)

    jobs_parser = subparsers.add_parser("jobs", help="Runtime job planning commands.")
    jobs_subparsers = jobs_parser.add_subparsers(dest="jobs_command", metavar="SUBCOMMAND")
    reembed_parser = jobs_subparsers.add_parser("reembed-plan", help="Plan a re-embed operation from JSON.")
    reembed_parser.add_argument("request", help="Path to re-embed plan JSON.")
    reembed_parser.set_defaults(handler=_handle_reembed_plan)

    serve_parser = subparsers.add_parser("serve", help="Run optional HTTP, gRPC, or GraphQL service modes.")
    serve_subparsers = serve_parser.add_subparsers(dest="serve_command", metavar="SUBCOMMAND")
    for mode in ["http", "grpc", "graphql", "all"]:
        mode_parser = serve_subparsers.add_parser(mode, help=f"Show {mode} service startup plan.")
        mode_parser.add_argument("config", help="Path to JSON config.")
        mode_parser.set_defaults(handler=_handle_service_plan, mode=mode)

    db_parser = subparsers.add_parser("db", help="Database migration and provisioning commands.")
    db_parser.add_argument("config", help="Path to JSON config.")
    db_parser.set_defaults(handler=_handle_config_validate)

    session_parser = subparsers.add_parser("session", help="Session append and lifecycle commands.")
    session_parser.set_defaults(handler=_handle_e2e_smoke)

    graph_parser = subparsers.add_parser("graph", help="Graph query commands.")
    graph_parser.add_argument("tenant_id")
    graph_parser.add_argument("user_id")
    graph_parser.add_argument("query_text")
    graph_parser.add_argument("--entities", nargs="*", default=[])
    graph_parser.add_argument("--intent-topic", default=None)
    graph_parser.add_argument("--token-budget", type=int, default=1800)
    graph_parser.add_argument("--privacy-scope", nargs="*", default=["private", "sensitive", "shared"])
    graph_parser.add_argument("--top-k-vector", type=int, default=12)
    graph_parser.add_argument("--max-graph-hops", type=int, default=2)
    graph_parser.set_defaults(handler=_handle_graph_query)

    reminders_parser = subparsers.add_parser("reminders", help="Prospective memory commands.")
    reminders_parser.add_argument("tenant_id")
    reminders_parser.add_argument("user_id")
    reminders_parser.add_argument("due_at_or_before")
    reminders_parser.add_argument("--actor", default="system")
    reminders_parser.add_argument("--retry-delay-seconds", type=int, default=300)
    reminders_parser.set_defaults(handler=_handle_due_reminders)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 0
    return handler(args)
