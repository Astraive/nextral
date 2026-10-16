"""Typed Python bridge for Nextral's configured Rust memory runtime."""

from ._version import __version__
from .core import (
    AsyncClient,
    Client,
    ServiceHandle,
    adapter_smoke,
    e2e_smoke,
    ingest_request_schema,
    lexical_score,
    reembed_plan,
    validate_config,
)
from .errors import (
    ConflictError,
    InvalidInputError,
    NextralError,
    NotFoundError,
    SerializationError,
    StorageError,
)
from .types import (
    ConsolidationRequest,
    EligibleSession,
    EligibleSessionsRequest,
    IngestRequest,
    IngestionPolicy,
    LifecycleJobRequest,
    ReembedExecutionRequest,
    ReembedVector,
    RetrievalRequest,
    RunLifecycleJobsRequest,
)

__all__ = [
    "__version__",
    "Client",
    "AsyncClient",
    "ServiceHandle",
    "lexical_score",
    "validate_config",
    "ingest_request_schema",
    "e2e_smoke",
    "reembed_plan",
    "adapter_smoke",
    "NextralError",
    "InvalidInputError",
    "NotFoundError",
    "ConflictError",
    "StorageError",
    "SerializationError",
    "ConsolidationRequest",
    "IngestRequest",
    "IngestionPolicy",
    "ReembedExecutionRequest",
    "ReembedVector",
    "RetrievalRequest",
    "LifecycleJobRequest",
    "RunLifecycleJobsRequest",
    "EligibleSession",
    "EligibleSessionsRequest",
]
