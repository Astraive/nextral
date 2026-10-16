"""Thin JSON bridges to the configured, process-wide Rust runtime.

Requests retain their native field names, including ``embedding``,
``query_vector`` and host-provided extraction results. Python does not select
providers, synthesize vectors, or implement memory policies.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from . import _nextral as _native
from .errors import SerializationError, from_core_error
from .types import EligibleSession, EligibleSessionsRequest

JsonObject = dict[str, Any]
Request = Mapping[str, Any]


def _encode(value: Any) -> str:
    try:
        return json.dumps(value, allow_nan=False)
    except (TypeError, ValueError) as error:
        raise SerializationError("serialization", str(error)) from error


def _invoke(function: Callable[..., Any], *args: Any) -> Any:
    try:
        return function(*args)
    except _native.NativeError as error:
        code, message = error.args
        raise from_core_error(code, message) from error


def _json_call(function: Callable[..., str], *args: str) -> Any:
    return json.loads(_invoke(function, *args))


def lexical_score(text: str, query: str) -> float:
    return _invoke(_native.lexical_score, text, query)


def validate_config(config: Request) -> JsonObject:
    return _json_call(_native.validate_config, _encode(config))


def configure_runtime(config: Request) -> JsonObject:
    """Validate and install an explicit Rust runtime configuration.

    The backend is never implicitly changed to the test backend. Without an
    explicit call, Rust resolves the configuration from ``NEXTRAL_CONFIG``.
    """
    return _json_call(_native.configure_runtime, _encode(config))


def serve(config: Request, modes: Sequence[str] = ("http",)) -> JsonObject:
    """Run real service hosts, blocking until native service shutdown.

    ``modes`` contains ``http``, ``grpc``, ``graphql`` or ``all``. Bind/startup
    errors propagate; the return value reports stopped hosts, not a startup plan.
    The native call releases Python's GIL while the hosts run.
    """
    return _json_call(_native.serve, _encode(config), _encode(list(modes)))


def stop_serving() -> None:
    """Signal active native service hosts to shut down gracefully."""
    _invoke(_native.stop_serving)


def e2e_smoke() -> JsonObject:
    """Execute the explicit, isolated Rust testkit smoke workflow."""
    return _json_call(_native.e2e_smoke)


def reembed_plan(request: Request) -> JsonObject:
    return _json_call(_native.reembed_plan, _encode(request))


def ingest_request_schema() -> JsonObject:
    """Return the canonical ingest request template without executing ingestion."""
    return _json_call(_native.ingest_request_schema)


def adapter_smoke(request: Request) -> JsonObject:
    return _json_call(_native.adapter_smoke, _encode(request))


def mcp_call(tool: str, payload: Request) -> Any:
    """Dispatch a native tool; preserve its complete response and error code."""
    return _json_call(
        _native.mcp_call, _encode({"tool": tool, "payload_json": _encode(payload)})
    )


def ingest(request: Request) -> JsonObject:
    return mcp_call("nextral.memory.ingest", request)


def retrieve(request: Request) -> JsonObject:
    return mcp_call("nextral.memory.retrieve", request)


def forget(request: Request) -> JsonObject:
    return mcp_call("nextral.memory.forget", request)


def update_memory(request: Request) -> JsonObject:
    return mcp_call("nextral.memory.update", request)


def list_memories(request: Request) -> list[JsonObject]:
    return mcp_call("nextral.memory.list", request)


def memory_stats(request: Request) -> JsonObject:
    return mcp_call("nextral.memory.stats", request)


def search_by_tag(request: Request) -> list[JsonObject]:
    return mcp_call("nextral.memory.search_by_tag", request)


def search_by_entity(request: Request) -> list[JsonObject]:
    return mcp_call("nextral.memory.search_by_entity", request)


def append_session(request: Request) -> JsonObject:
    return mcp_call("nextral.session.append", request)


def eligible_sessions(request: EligibleSessionsRequest) -> list[EligibleSession]:
    """List scoped inactive sessions, oldest first; limit defaults to 100 (1–1000)."""
    return mcp_call("nextral.session.eligible", request)


def session_context(request: Request) -> JsonObject:
    return mcp_call("nextral.session.context", request)


def graph_query(request: Request) -> JsonObject:
    return mcp_call("nextral.graph.query", request)


def graphify(request: Request) -> JsonObject:
    return mcp_call("nextral.graph.graphify", request)


def schedule_reminder(request: Request) -> JsonObject:
    return mcp_call("nextral.reminders.schedule", request)


def due_reminders(request: Request) -> JsonObject:
    return mcp_call("nextral.reminders.due", request)


def consolidate(request: Request) -> JsonObject:
    return mcp_call("nextral.consolidation.run", request)


def batch_ingest(request: Request) -> JsonObject:
    return mcp_call("nextral.batch.ingest", request)


def batch_retrieve(request: Request) -> JsonObject:
    return mcp_call("nextral.batch.retrieve", request)


def batch_forget(request: Request) -> JsonObject:
    return mcp_call("nextral.batch.forget", request)


def scored_search(request: Request) -> JsonObject:
    return mcp_call("nextral.runtime.scored_search", request)


def health() -> JsonObject:
    return mcp_call("nextral.health", {})


def execute_reembed(request: Request) -> JsonObject:
    return mcp_call("nextral.jobs.reembed.execute", request)


def create_experiment(request: Request) -> JsonObject:
    return mcp_call("experiments.create", request)


def promote_experiment(request: Request) -> JsonObject:
    return mcp_call("experiments.promote", request)


def rollback_experiment(request: Request) -> JsonObject:
    return mcp_call("experiments.rollback", request)


def experiment_status(request: Request) -> JsonObject:
    return mcp_call("experiments.status", request)


def safety_policy() -> JsonObject:
    return mcp_call("safety.policy.get", {})


def set_safety_policy(request: Request) -> JsonObject:
    return mcp_call("safety.policy.set", request)


def migrate_database() -> JsonObject:
    return mcp_call("nextral.database.migrate", {})


def get_memory(request: Request) -> JsonObject | None:
    return mcp_call("nextral.memory.get", request)


def list_reminders(request: Request) -> list[JsonObject]:
    return mcp_call("nextral.reminders.list", request)


def enqueue_job(request: Request) -> JsonObject:
    return mcp_call("nextral.jobs.enqueue", request)


def run_jobs(request: Request) -> list[JsonObject]:
    return mcp_call("nextral.jobs.run", request)


def list_jobs(request: Request) -> list[JsonObject]:
    return mcp_call("nextral.jobs.list", request)


def get_job(request: Request) -> JsonObject | None:
    return mcp_call("nextral.jobs.get", request)


def run_outbox(request: Request) -> JsonObject:
    return mcp_call("nextral.outbox.run", request)


def upsert_policy(request: Request) -> JsonObject:
    return mcp_call("nextral.policy.upsert", request)


def list_policies(request: Request) -> list[JsonObject]:
    return mcp_call("nextral.policy.list", request)


def remove_policy(request: Request) -> JsonObject:
    return mcp_call("nextral.policy.remove", request)
