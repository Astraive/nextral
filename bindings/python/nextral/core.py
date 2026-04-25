"""High-level Python wrappers over the native extension."""

from __future__ import annotations

import json
from typing import Any

from ._nextral import lexical_score as _lexical_score
from ._nextral import e2e_smoke as _e2e_smoke
from ._nextral import ingest_request_schema as _ingest_request_schema
from ._nextral import reembed_plan as _reembed_plan
from ._nextral import validate_config as _validate_config


def lexical_score(text: str, query: str) -> float:
    return float(_lexical_score(text, query))


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    return json.loads(_validate_config(json.dumps(config)))


def e2e_smoke() -> dict[str, Any]:
    return json.loads(_e2e_smoke())


def reembed_plan(request: dict[str, Any]) -> dict[str, Any]:
    return json.loads(_reembed_plan(json.dumps(request)))


def ingest_request_schema() -> dict[str, Any]:
    return json.loads(_ingest_request_schema())
