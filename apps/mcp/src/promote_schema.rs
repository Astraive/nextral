pub(crate) fn schema() -> serde_json::Value {
    serde_json::json!(
    {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "object", "additionalProperties": false,
      "required": ["experiment_id", "evidence"],
      "properties": {
        "experiment_id": {"type": "string", "minLength": 1},
        "evidence": {"type": "object", "additionalProperties": false, "required": ["baseline", "candidate"],
          "properties": {"baseline": {"$ref": "#/$defs/report"}, "candidate": {"$ref": "#/$defs/report"}}}
      },
      "$defs": {
        "label": {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[A-Za-z0-9._-]+$"},
        "counter": {"type": "integer", "minimum": 0, "maximum": u64::MAX},
        "outcome": {"type": "string", "enum": ["completed", "retry_scheduled", "cancelled", "failed"]},
        "run": {"type": "object", "additionalProperties": false,
          "required": ["run_id", "implementation_revision", "policy_version", "host_id", "latency_scope"],
          "properties": {
            "run_id": {"$ref": "#/$defs/label"}, "implementation_revision": {"$ref": "#/$defs/label"},
            "policy_version": {"$ref": "#/$defs/label"}, "host_id": {"$ref": "#/$defs/label"}, "latency_scope": {"$ref": "#/$defs/label"}
          }},
        "metadata": {"type": "object", "additionalProperties": false,
          "required": ["schema_version", "suite_id", "suite_revision", "suite_sha256", "run", "started_at_unix_ms", "build_profile", "target_os", "target_arch"],
          "properties": {
            "schema_version": {"type": "integer", "const": 1}, "suite_id": {"$ref": "#/$defs/label"}, "suite_revision": {"$ref": "#/$defs/label"},
            "suite_sha256": {"type": "string", "pattern": "^[a-fA-F0-9]{64}$"}, "run": {"$ref": "#/$defs/run"},
            "started_at_unix_ms": {"type": "integer", "minimum": 1, "maximum": u64::MAX},
            "build_profile": {"type": "string", "const": "release"}, "target_os": {"$ref": "#/$defs/label"}, "target_arch": {"$ref": "#/$defs/label"}
          }},
        "observed_outcome": {"oneOf": [
          {"type": "object", "additionalProperties": false, "required": ["kind", "expected", "matched", "returned", "forbidden"], "properties": {
            "kind": {"const": "retrieval"}, "expected": {"type": "integer", "minimum": 1, "maximum": u64::MAX},
            "matched": {"$ref": "#/$defs/counter"}, "returned": {"$ref": "#/$defs/counter"}, "forbidden": {"$ref": "#/$defs/counter"}}},
          {"type": "object", "additionalProperties": false, "required": ["kind", "expected", "detected"], "properties": {
            "kind": {"const": "contradiction"}, "expected": {"type": "boolean"}, "detected": {"type": "boolean"}}},
          {"type": "object", "additionalProperties": false, "required": ["kind", "expected", "observed"], "properties": {
            "kind": {"const": "reminder"}, "expected": {"$ref": "#/$defs/outcome"}, "observed": {"$ref": "#/$defs/outcome"}}}
        ]},
        "observation": {"type": "object", "additionalProperties": false,
          "required": ["case_id", "elapsed_ns", "destructive_events", "outcome"],
          "properties": {"case_id": {"$ref": "#/$defs/label"}, "elapsed_ns": {"$ref": "#/$defs/counter"},
            "destructive_events": {"$ref": "#/$defs/counter"}, "outcome": {"$ref": "#/$defs/observed_outcome"}}},
        "latency": {"type": "object", "additionalProperties": false, "required": ["samples", "mean_ns", "p95_ns", "max_ns"],
          "properties": {"samples": {"type": "integer", "minimum": 1, "maximum": u64::MAX},
            "mean_ns": {"type": "number", "minimum": 0}, "p95_ns": {"$ref": "#/$defs/counter"}, "max_ns": {"$ref": "#/$defs/counter"}}},
        "score": {"type": "number", "minimum": 0, "maximum": 1},
        "report": {"type": "object", "additionalProperties": false,
          "required": ["metadata", "observations", "golden_recall_score", "contradiction_score", "reminder_outcome_score", "latency_budget_ns", "latency", "latency_slo_passed", "destructive_events"],
          "properties": {"metadata": {"$ref": "#/$defs/metadata"}, "observations": {"type": "array", "minItems": 3, "items": {"$ref": "#/$defs/observation"}},
            "golden_recall_score": {"$ref": "#/$defs/score"}, "contradiction_score": {"$ref": "#/$defs/score"}, "reminder_outcome_score": {"$ref": "#/$defs/score"},
            "latency_budget_ns": {"type": "integer", "minimum": 1, "maximum": u64::MAX}, "latency": {"$ref": "#/$defs/latency"},
            "latency_slo_passed": {"type": "boolean"}, "destructive_events": {"$ref": "#/$defs/counter"}}}
      }
    }
    )
}
