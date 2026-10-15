"""JSON request types for the model-agnostic host input seam.

These annotations do not validate or transform requests; Rust owns validation.
Other optional native request fields may be supplied through the dictionary API.
"""

from __future__ import annotations

from typing import Any, Literal, TypedDict


class IngestionPolicy(TypedDict):
    min_importance_score: float
    min_confidence_score: float


class _IngestFields(TypedDict):
    tenant_id: str
    user_id: str
    content: str
    content_type: str
    memory_type: str
    source_type: str
    source_message_ids: list[str]
    importance_score: float
    entities: list[str]
    tags: list[str]
    privacy_level: str
    graph_hints: list[dict[str, Any]]
    policy: IngestionPolicy


class IngestRequest(_IngestFields, total=False):
    id: str | None
    session_id: str | None
    confidence_score: float | None
    trace_id: str | None
    embedding: list[float] | None


class _RetrievalFields(TypedDict):
    tenant_id: str
    user_id: str
    query_text: str
    entities: list[str]
    token_budget: int
    privacy_scope: list[str]
    top_k_vector: int
    max_graph_hops: int


class RetrievalRequest(_RetrievalFields, total=False):
    session_id: str | None
    intent_topic: str | None
    lane: str | None
    policy_version: str | None
    trace_id: str | None
    scoring_weights: dict[str, float] | None
    query_vector: list[float] | None


class _ConsolidationFields(TypedDict):
    tenant_id: str
    user_id: str
    session_id: str
    lane: Literal["fast", "deep"]
    policy: IngestionPolicy


class ConsolidationRequest(_ConsolidationFields, total=False):
    version: str
    summary: str | None
    candidates: list[IngestRequest]
    chunk_messages: int


class ReembedVector(TypedDict):
    memory_id: str
    vector: list[float]


class _ReembedExecutionFields(TypedDict):
    plan: dict[str, Any]
    user_id: str
    vectors: list[ReembedVector]
    target_dimension: int


class ReembedExecutionRequest(_ReembedExecutionFields, total=False):
    cutover: bool
    rollback: bool


class _LifecycleJobFields(TypedDict):
    tenant_id: str
    user_id: str
    session_id: str
    trigger: Literal["idle", "close", "deep", "retention"]
    run_at: str


class LifecycleJobRequest(_LifecycleJobFields, total=False):
    idle_seconds: int | None
    consolidation: ConsolidationRequest | None
    memory_ids: list[str]
    redact: bool
    max_attempts: int


class _RunLifecycleJobFields(TypedDict):
    tenant_id: str
    user_id: str
    now: str


class RunLifecycleJobsRequest(_RunLifecycleJobFields, total=False):
    limit: int
    lease_seconds: int
