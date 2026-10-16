"""Client-owned Python bridge for Nextral's configured Rust memory runtime.

Requests retain their native field names, including ``embedding``,
``query_vector`` and host-provided extraction results. Python does not select
providers, synthesize vectors, or implement memory policies.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
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


async def _invoke_async(coro: Any) -> Any:
    try:
        return await coro
    except _native.NativeError as error:
        code, message = error.args
        raise from_core_error(code, message) from error


def _json_call(function: Callable[..., str], *args: Any) -> Any:
    return json.loads(_invoke(function, *args))


def lexical_score(text: str, query: str) -> float:
    return _invoke(_native.lexical_score, text, query)


def validate_config(config: Request) -> JsonObject:
    return _json_call(_native.validate_config, _encode(config))


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


class ServiceHandle:
    def __init__(self, native_handle: Any) -> None:
        self._native = native_handle

    def shutdown(self, timeout_ms: int | None = None) -> None:
        _invoke(self._native.shutdown, timeout_ms)

    async def shutdown_async(self, timeout_ms: int | None = None) -> None:
        await _invoke_async(self._native.shutdown_async(timeout_ms))

    @property
    def binds(self) -> list[str]:
        return self._native.binds()


class WorkerHandle:
    def __init__(self, native_handle: Any) -> None:
        self._native = native_handle

    def tick(self) -> JsonObject:
        raw = _invoke(self._native.tick)
        return json.loads(raw)

    async def tick_async(self) -> JsonObject:
        raw = await _invoke_async(self._native.tick_async())
        return json.loads(raw)

    def shutdown(self, timeout_ms: int | None = None) -> None:
        _invoke(self._native.shutdown, timeout_ms)

    async def shutdown_async(self, timeout_ms: int | None = None) -> None:
        await _invoke_async(self._native.shutdown_async(timeout_ms))

class Client:
    """Synchronous owned client for the Nextral memory runtime."""

    def __init__(self, config: Request | str) -> None:
        config_json = config if isinstance(config, str) else _encode(config)
        self._native = _invoke(_native.Client, config_json)

    def __enter__(self) -> Client:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def call(self, tool: str, payload: Request, timeout_ms: int | None = None) -> Any:
        raw = _invoke(self._native.call_json, tool, _encode(payload), timeout_ms)
        return json.loads(raw)

    def close(self, timeout_ms: int | None = None) -> JsonObject:
        raw = _invoke(self._native.close, timeout_ms)
        return json.loads(raw)

    def serve(self) -> ServiceHandle:
        native_handle = _invoke(self._native.serve)
        return ServiceHandle(native_handle)

    def start_worker(self, config: Request | str) -> WorkerHandle:
        config_json = config if isinstance(config, str) else _encode(config)
        native_handle = _invoke(self._native.start_worker, config_json)
        return WorkerHandle(native_handle)

    def health(self, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.health", {}, timeout_ms)

    def ingest(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.memory.ingest", request, timeout_ms)

    def retrieve(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.memory.retrieve", request, timeout_ms)

    def forget(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.memory.forget", request, timeout_ms)

    def get_memory(
        self,
        request: Request | None = None,
        *,
        tenant_id: str | None = None,
        user_id: str | None = None,
        memory_id: str | None = None,
        timeout_ms: int | None = None,
    ) -> JsonObject | None:
        payload = request if isinstance(request, dict) else {"tenant_id": tenant_id, "user_id": user_id, "memory_id": memory_id}
        return self.call("nextral.memory.get", payload, timeout_ms)

    def list_memories(self, request: Request, timeout_ms: int | None = None) -> list[JsonObject]:
        res = self.call("nextral.memory.list", request, timeout_ms)
        return res if isinstance(res, list) else (res.get("items", []) if isinstance(res, dict) else [])

    def query_memories(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.memory.list", request, timeout_ms)

    def repair_index(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.index.repair", request, timeout_ms)
    def update_memory(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.memory.update", request, timeout_ms)

    def memory_stats(
        self,
        request: Request | None = None,
        *,
        tenant_id: str | None = None,
        user_id: str | None = None,
        timeout_ms: int | None = None,
    ) -> JsonObject:
        payload = request if isinstance(request, dict) else {"tenant_id": tenant_id, "user_id": user_id}
        return self.call("nextral.memory.stats", payload, timeout_ms)

    def search_by_tag(self, request: Request, timeout_ms: int | None = None) -> list[JsonObject]:
        return self.call("nextral.memory.search_by_tag", request, timeout_ms)

    def search_by_entity(self, request: Request, timeout_ms: int | None = None) -> list[JsonObject]:
        return self.call("nextral.memory.search_by_entity", request, timeout_ms)

    def scored_search(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.runtime.scored_search", request, timeout_ms)

    def append_session(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.session.append", request, timeout_ms)

    def session_context(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.session.context", request, timeout_ms)

    def session_tail(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.session.tail", request, timeout_ms)

    def eligible_sessions(self, request: Request | EligibleSessionsRequest, timeout_ms: int | None = None) -> list[EligibleSession]:
        raw_request = (
            request.to_dict() if hasattr(request, "to_dict") else request
        )
        return [
            EligibleSession(**item)
            for item in self.call("nextral.session.eligible", raw_request, timeout_ms)
        ]

    def graph_query(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.graph.query", request, timeout_ms)

    def graphify(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.graph.graphify", request, timeout_ms)

    def consolidate(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.consolidation.run", request, timeout_ms)

    def schedule_reminder(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.reminders.schedule", request, timeout_ms)

    def list_reminders(
        self,
        request: Request | None = None,
        *,
        tenant_id: str | None = None,
        user_id: str | None = None,
        timeout_ms: int | None = None,
    ) -> list[JsonObject]:
        payload = request if isinstance(request, dict) else {"tenant_id": tenant_id, "user_id": user_id}
        return self.call("nextral.reminders.list", payload, timeout_ms)

    def due_reminders(self, request: Request, timeout_ms: int | None = None) -> list[JsonObject]:
        return self.call("nextral.reminders.due", request, timeout_ms)

    def upsert_policy(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.policy.upsert", request, timeout_ms)

    def list_policies(
        self,
        request: Request | None = None,
        *,
        tenant_id: str | None = None,
        user_id: str | None = None,
        timeout_ms: int | None = None,
    ) -> list[JsonObject]:
        payload = request if isinstance(request, dict) else {"tenant_id": tenant_id, "user_id": user_id}
        return self.call("nextral.policy.list", payload, timeout_ms)

    def remove_policy(
        self,
        request: Request | None = None,
        *,
        tenant_id: str | None = None,
        user_id: str | None = None,
        policy_id: str | None = None,
        timeout_ms: int | None = None,
    ) -> JsonObject:
        payload = request if isinstance(request, dict) else {"tenant_id": tenant_id, "user_id": user_id, "policy_id": policy_id}
        return self.call("nextral.policy.remove", payload, timeout_ms)

    def execute_reembed(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.jobs.reembed.execute", request, timeout_ms)

    def enqueue_job(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.jobs.enqueue", request, timeout_ms)

    def get_job(
        self,
        request: Request | None = None,
        *,
        tenant_id: str | None = None,
        user_id: str | None = None,
        job_id: str | None = None,
        timeout_ms: int | None = None,
    ) -> JsonObject | None:
        payload = request if isinstance(request, dict) else {"tenant_id": tenant_id, "user_id": user_id, "job_id": job_id}
        return self.call("nextral.jobs.get", payload, timeout_ms)

    def list_jobs(
        self,
        request: Request | None = None,
        *,
        tenant_id: str | None = None,
        user_id: str | None = None,
        timeout_ms: int | None = None,
    ) -> list[JsonObject]:
        payload = request if isinstance(request, dict) else {"tenant_id": tenant_id, "user_id": user_id}
        return self.call("nextral.jobs.list", payload, timeout_ms)

    def run_jobs(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.jobs.run", request, timeout_ms)

    def run_outbox(self, tenant_id: str, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.outbox.run", {"tenant_id": tenant_id}, timeout_ms)

    def migrate_database(self, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.database.migrate", {}, timeout_ms)

    def create_experiment(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("experiments.create", request, timeout_ms)

    def promote_experiment(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("experiments.promote", request, timeout_ms)

    def rollback_experiment(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("experiments.rollback", request, timeout_ms)

    def experiment_status(self, experiment_id: str | None = None, timeout_ms: int | None = None) -> JsonObject:
        payload = {"experiment_id": experiment_id} if experiment_id else {}
        return self.call("experiments.status", payload, timeout_ms)

    def safety_policy(self, timeout_ms: int | None = None) -> JsonObject:
        return self.call("safety.policy.get", {}, timeout_ms)

    def set_safety_policy(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("safety.policy.set", request, timeout_ms)

    def batch_ingest(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.batch.ingest", request, timeout_ms)

    def batch_retrieve(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.batch.retrieve", request, timeout_ms)

    def batch_forget(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return self.call("nextral.batch.forget", request, timeout_ms)


class AsyncClient:
    """Asynchronous owned client for the Nextral memory runtime."""

    def __init__(self, native_client: Any) -> None:
        self._native = native_client

    @classmethod
    async def create(cls, config: Request | str) -> AsyncClient:
        config_json = config if isinstance(config, str) else _encode(config)
        native = _invoke(_native.Client, config_json)
        return cls(native)

    async def __aenter__(self) -> AsyncClient:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    async def call(self, tool: str, payload: Request, timeout_ms: int | None = None) -> Any:
        raw = await _invoke_async(self._native.call_json_async(tool, _encode(payload), timeout_ms))
        return json.loads(raw)

    async def close(self, timeout_ms: int | None = None) -> JsonObject:
        raw = await _invoke_async(self._native.close_async(timeout_ms))
        return json.loads(raw)

    async def serve(self) -> ServiceHandle:
        native_handle = await _invoke_async(self._native.serve_async())
        return ServiceHandle(native_handle)

    async def start_worker(self, config: Request | str) -> WorkerHandle:
        config_json = config if isinstance(config, str) else _encode(config)
        native_handle = await _invoke_async(self._native.start_worker_async(config_json))
        return WorkerHandle(native_handle)

    async def health(self, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.health", {}, timeout_ms)

    async def ingest(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.memory.ingest", request, timeout_ms)

    async def retrieve(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.memory.retrieve", request, timeout_ms)

    async def forget(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.memory.forget", request, timeout_ms)

    async def get_memory(
        self,
        request: Request | None = None,
        *,
        tenant_id: str | None = None,
        user_id: str | None = None,
        memory_id: str | None = None,
        timeout_ms: int | None = None,
    ) -> JsonObject | None:
        payload = request if isinstance(request, dict) else {"tenant_id": tenant_id, "user_id": user_id, "memory_id": memory_id}
        return await self.call("nextral.memory.get", payload, timeout_ms)

    async def list_memories(self, request: Request, timeout_ms: int | None = None) -> list[JsonObject]:
        res = await self.call("nextral.memory.list", request, timeout_ms)
        return res if isinstance(res, list) else (res.get("items", []) if isinstance(res, dict) else [])

    async def query_memories(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.memory.list", request, timeout_ms)

    async def repair_index(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.index.repair", request, timeout_ms)
    async def update_memory(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.memory.update", request, timeout_ms)

    async def memory_stats(
        self,
        request: Request | None = None,
        *,
        tenant_id: str | None = None,
        user_id: str | None = None,
        timeout_ms: int | None = None,
    ) -> JsonObject:
        payload = request if isinstance(request, dict) else {"tenant_id": tenant_id, "user_id": user_id}
        return await self.call("nextral.memory.stats", payload, timeout_ms)

    async def search_by_tag(self, request: Request, timeout_ms: int | None = None) -> list[JsonObject]:
        return await self.call("nextral.memory.search_by_tag", request, timeout_ms)

    async def search_by_entity(self, request: Request, timeout_ms: int | None = None) -> list[JsonObject]:
        return await self.call("nextral.memory.search_by_entity", request, timeout_ms)

    async def scored_search(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.runtime.scored_search", request, timeout_ms)

    async def append_session(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.session.append", request, timeout_ms)

    async def session_context(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.session.context", request, timeout_ms)

    async def session_tail(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.session.tail", request, timeout_ms)

    async def eligible_sessions(self, request: Request | EligibleSessionsRequest, timeout_ms: int | None = None) -> list[EligibleSession]:
        raw_request = (
            request.to_dict() if hasattr(request, "to_dict") else request
        )
        return [
            EligibleSession(**item)
            for item in await self.call("nextral.session.eligible", raw_request, timeout_ms)
        ]

    async def graph_query(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.graph.query", request, timeout_ms)

    async def graphify(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.graph.graphify", request, timeout_ms)

    async def consolidate(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.consolidation.run", request, timeout_ms)

    async def schedule_reminder(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.reminders.schedule", request, timeout_ms)

    async def list_reminders(
        self,
        request: Request | None = None,
        *,
        tenant_id: str | None = None,
        user_id: str | None = None,
        timeout_ms: int | None = None,
    ) -> list[JsonObject]:
        payload = request if isinstance(request, dict) else {"tenant_id": tenant_id, "user_id": user_id}
        return await self.call("nextral.reminders.list", payload, timeout_ms)

    async def due_reminders(self, request: Request, timeout_ms: int | None = None) -> list[JsonObject]:
        return await self.call("nextral.reminders.due", request, timeout_ms)

    async def upsert_policy(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.policy.upsert", request, timeout_ms)

    async def list_policies(
        self,
        request: Request | None = None,
        *,
        tenant_id: str | None = None,
        user_id: str | None = None,
        timeout_ms: int | None = None,
    ) -> list[JsonObject]:
        payload = request if isinstance(request, dict) else {"tenant_id": tenant_id, "user_id": user_id}
        return await self.call("nextral.policy.list", payload, timeout_ms)

    async def remove_policy(
        self,
        request: Request | None = None,
        *,
        tenant_id: str | None = None,
        user_id: str | None = None,
        policy_id: str | None = None,
        timeout_ms: int | None = None,
    ) -> JsonObject:
        payload = request if isinstance(request, dict) else {"tenant_id": tenant_id, "user_id": user_id, "policy_id": policy_id}
        return await self.call("nextral.policy.remove", payload, timeout_ms)

    async def execute_reembed(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.jobs.reembed.execute", request, timeout_ms)

    async def enqueue_job(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.jobs.enqueue", request, timeout_ms)

    async def get_job(
        self,
        request: Request | None = None,
        *,
        tenant_id: str | None = None,
        user_id: str | None = None,
        job_id: str | None = None,
        timeout_ms: int | None = None,
    ) -> JsonObject | None:
        payload = request if isinstance(request, dict) else {"tenant_id": tenant_id, "user_id": user_id, "job_id": job_id}
        return await self.call("nextral.jobs.get", payload, timeout_ms)

    async def list_jobs(
        self,
        request: Request | None = None,
        *,
        tenant_id: str | None = None,
        user_id: str | None = None,
        timeout_ms: int | None = None,
    ) -> list[JsonObject]:
        payload = request if isinstance(request, dict) else {"tenant_id": tenant_id, "user_id": user_id}
        return await self.call("nextral.jobs.list", payload, timeout_ms)

    async def run_jobs(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.jobs.run", request, timeout_ms)

    async def run_outbox(self, tenant_id: str, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.outbox.run", {"tenant_id": tenant_id}, timeout_ms)

    async def migrate_database(self, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.database.migrate", {}, timeout_ms)

    async def create_experiment(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("experiments.create", request, timeout_ms)

    async def promote_experiment(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("experiments.promote", request, timeout_ms)

    async def rollback_experiment(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("experiments.rollback", request, timeout_ms)

    async def experiment_status(self, experiment_id: str | None = None, timeout_ms: int | None = None) -> JsonObject:
        payload = {"experiment_id": experiment_id} if experiment_id else {}
        return await self.call("experiments.status", payload, timeout_ms)

    async def safety_policy(self, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("safety.policy.get", {}, timeout_ms)

    async def set_safety_policy(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("safety.policy.set", request, timeout_ms)

    async def batch_ingest(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.batch.ingest", request, timeout_ms)

    async def batch_retrieve(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.batch.retrieve", request, timeout_ms)

    async def batch_forget(self, request: Request, timeout_ms: int | None = None) -> JsonObject:
        return await self.call("nextral.batch.forget", request, timeout_ms)
