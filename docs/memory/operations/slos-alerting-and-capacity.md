# Deployment SLO targets, telemetry and replay research

## Targets are not measurements

These are example deployment objectives, not measured Nextral guarantees:

| Indicator | Example target to validate locally |
| --- | --- |
| Retrieval memory-layer latency | p95 ≤ 40 ms |
| Retrieval success | ≥ 99.5% |
| Fast consolidation completion | ≥ 99.0% |
| Forget completion | ≥ 99.9% within the deployment policy window |

Define workload, enabled paths, candidate/hop limits, concurrency, dataset size, hardware/network, measurement window and degradation policy before adopting a target. Count full required-store completion rather than request acceptance. Cold process/setup cost and steady-state request latency are different measurements. No source-level constant or passing smoke test establishes these targets.

## Optional real telemetry

Enable `observability.enabled` with a configured `prometheus_bind` and/or `otlp_endpoint`. Nextral exports Prometheus metrics and OTLP/HTTP JSON metrics/spans using real transports. Disabled telemetry needs no collector; enabled telemetry does not imply a bundled monitoring backend. Protect telemetry listeners and collector endpoints.

Prometheus is served at `/metrics` on the configured bind. `otlp_endpoint` is a collector base HTTP(S) URL; exports use `/v1/metrics` and `/v1/traces`, not gRPC. Periodic exports run every 10 seconds and shutdown flushes pending data. Keep the process alive for collection or shut down gracefully; successful configuration alone is not evidence of collector acknowledgement.

Record request/stage duration, outcomes, candidate/selection counts, worker retries and backlog using available runtime telemetry and deployment instrumentation. Do not claim a desired dashboard panel is an implemented exporter metric. Never put raw memory text or tenant/user IDs in metric labels. `scripts/telemetry-smoke.py` validates received metric/span wire data with official OpenTelemetry protobuf classes and graceful Python shutdown; it is a validation tool, not a latency benchmark.

## Measured replay research

The representative suite is `fixtures/evaluation/representative.json`. `examples/research_host.rs` executes fixture setup, real runtime operations, controlled HTTP delivery outcomes and audited event counting; `examples/research_evaluation.rs` runs a real host process per case and compares observed outcomes to expectations. The runner requires release mode:

Set `NEXTRAL_CONFIG` in the environment to a migrated `production_stores` configuration with reachable stores and required credentials. The reference host rejects `test_memory`; it uses isolated fixture scopes within the configured real stores and fixture-provided embeddings, not an internal model. Use disposable research infrastructure.

```sh
cargo build --release --example research_host
cargo run --release --example research_evaluation -- representative HOST_EXECUTABLE REVISION POLICY RUN_ID HOST_ID LATENCY_BUDGET_NS
```

Replace uppercase arguments with the actual executable path, revision/policy/run/host metadata and positive nanosecond budget. `HOST_EXECUTABLE` is under Cargo's configured release examples directory (`research_host.exe` on Windows); it is not assumed to be on `PATH`. An optional final baseline-report path enables the comparison gate.

Reports measure real outcome agreement and process-roundtrip timing, including host startup/setup. Real-store replay is not a sustained production load test or proof of the example deployment SLOs. Runtime severity is not a fabricated hallucination/misuse/privacy metric. Promotion consumes actual baseline/candidate evidence; do not replace reports with invented scores. Retain report metadata, failures and scope alongside any published measurement.

Promotion requires distinct baseline/candidate run IDs with matching suite ID/revision/hash, host ID, latency scope, OS/architecture, release profile, latency budget and observation count. The gate rejects regression in observed recall/contradiction/reminder outcomes, destructive-event counts, p95/max timing and per-case outcomes; passing a threshold alone does not override a regression against baseline.

The integrated five-store runner builds both release examples and retains authentic baseline/candidate reports, source/executable identity, and installed Python/Node control results under Cargo's `integration-research/<run-id>` directory. `--skip-build` requires `--wheel-dir` and `--npm-package`; it skips package/debug builds, not these research builds. `--research-latency-budget-ns` sets the replay budget, not a production SLO. Independent same-implementation timing can fail the zero-regression gate: the runner records rejection and exits nonzero if promotion/rollback is not verified. It never retries measurements to manufacture passing evidence.

## Alerts and capacity

Choose operational thresholds from observed baselines: all-path retrieval failure, deletion still incomplete, durable job/outbox age, retry exhaustion, lease churn, drift in active canonical/index records and archive errors. Watch Redis memory/evictions, PostgreSQL growth/locks/indexes, Qdrant memory/disk/search latency, Neo4j traversal/index health, and private S3 object growth/error rate. Provisioning partitions, dashboards and alert delivery remains deployment work, not a package guarantee.
