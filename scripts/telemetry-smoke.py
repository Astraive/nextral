#!/usr/bin/env python3
"""Verify installed Python service shutdown, Prometheus and OTLP/HTTP against real operations."""
from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import sys
import threading
import time
from urllib.error import URLError
from urllib.request import urlopen
import uuid

from google.protobuf.json_format import ParseDict, ParseError
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
import nextral


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def protobuf_document(value):
    if isinstance(value, dict):
        for key, item in value.items():
            if key in ("traceId", "spanId", "parentSpanId"):
                decoded = bytes.fromhex(item)
                expected = 16 if key == "traceId" else 8
                if len(decoded) != expected or not any(decoded):
                    raise ValueError("invalid OTLP trace/span identifier")
                value[key] = base64.b64encode(decoded).decode("ascii")
            else:
                protobuf_document(item)
    elif isinstance(value, list):
        for item in value:
            protobuf_document(item)
    return value


class Collector:
    def __init__(self):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(handler):
                message = {"/v1/metrics": ExportMetricsServiceRequest,
                           "/v1/traces": ExportTraceServiceRequest}.get(handler.path)
                size = int(handler.headers.get("Content-Length", "0"))
                if message is None or not 0 < size <= 1024 * 1024:
                    handler.send_error(400)
                    return
                raw = handler.rfile.read(size)
                try:
                    if handler.headers.get_content_type() != "application/json":
                        raise ValueError("OTLP/JSON requires application/json")
                    parsed = ParseDict(protobuf_document(json.loads(raw)), message())
                except (ParseError, ValueError, TypeError) as error:
                    handler.server.errors.append(str(error))
                    handler.send_error(400)
                    return
                handler.server.messages.append((handler.path, raw, parsed))
                handler.send_response(200)
                handler.send_header("Content-Type", "application/json")
                handler.send_header("Content-Length", "2")
                handler.end_headers()
                handler.wfile.write(b"{}")

            def log_message(self, *_args):
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.messages = []
        self.server.errors = []
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def main():
    config = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    ports = {name: free_port() for name in ("http", "prometheus")}
    config["service"]["http_bind"] = f"127.0.0.1:{ports['http']}"
    scope = {"tenant_id": "otel_" + uuid.uuid4().hex, "user_id": "private-user-" + uuid.uuid4().hex}
    memory_id = "private-memory-" + uuid.uuid4().hex
    sentinel = "private-telemetry-sentinel-" + uuid.uuid4().hex
    vector = [1.0] + [0.0] * (config["embedding"]["dimension"] - 1)
    request = nextral.ingest_request_schema()
    request.update(scope, id=memory_id, content=sentinel, importance_score=1.0,
                   confidence_score=1.0, privacy_level="private", embedding=vector,
                   policy=config["ingestion_policy"], entities=[], tags=[], graph_hints=[])
    query = dict(scope, query_text="telemetry", query_vector=vector, entities=[],
                 privacy_scope=["private"], token_budget=config["retrieval_policy"]["token_budget"],
                 top_k_vector=config["retrieval_policy"]["top_k_vector"],
                 max_graph_hops=config["retrieval_policy"]["max_graph_hops"])
    expected = {"nextral_ingest_accepted_total": 1, "nextral_retrieval_total": 2,
                "nextral_forget_completed_total": 1}
    with Collector() as collector, ThreadPoolExecutor(max_workers=1) as executor:
        config["observability"] = {
            "enabled": True, "prometheus_bind": f"127.0.0.1:{ports['prometheus']}",
            "otlp_endpoint": f"http://127.0.0.1:{collector.server.server_port}",
        }
        serving = executor.submit(nextral.serve, config, ["http"])
        try:
            deadline = time.monotonic() + 30
            while True:
                if serving.done():
                    raise RuntimeError(f"service stopped before readiness: {serving.result()}")
                try:
                    with urlopen(f"http://127.0.0.1:{ports['http']}/healthz", timeout=1) as response:
                        if json.load(response) != {"status": "alive"}:
                            raise AssertionError("unexpected health response")
                    break
                except (URLError, TimeoutError, OSError):
                    if time.monotonic() >= deadline:
                        raise RuntimeError("native Python service never became healthy")
                    time.sleep(0.05)
            operation_start = time.time_ns()
            accepted = nextral.ingest(request)
            if accepted["status"] != "accepted" or accepted["record_id"] != memory_id:
                raise AssertionError(f"telemetry fixture was not accepted: {accepted}")
            if memory_id not in {item["memory_id"] for item in nextral.retrieve(query)["items"]}:
                raise AssertionError("telemetry fixture was not recalled")
            forgotten = nextral.forget(dict(scope, memory_id=memory_id, actor=scope["user_id"],
                                            reason="telemetry smoke cleanup", redact=True))
            if forgotten["status"] != "completed" or nextral.retrieve(query)["items"]:
                raise AssertionError("telemetry fixture did not complete actual redaction")
            operation_end = time.time_ns()
            with urlopen(f"http://127.0.0.1:{ports['prometheus']}/metrics", timeout=10) as response:
                prometheus = response.read().decode()
            samples = dict(line.split() for line in prometheus.splitlines()
                           if line and not line.startswith("#"))
            if any(int(samples[name]) != count for name, count in expected.items()):
                raise AssertionError("Prometheus counters disagree with executed memory operations")
        finally:
            nextral.stop_serving()
            stopped = serving.result(timeout=45)
        if stopped["status"] != "stopped":
            raise AssertionError("native Python service did not acknowledge shutdown")
        with socket.socket() as connection:
            if connection.connect_ex(("127.0.0.1", ports["http"])) == 0:
                raise AssertionError("service listener remained open after graceful shutdown")
        if collector.server.errors:
            raise AssertionError(f"collector rejected OTLP: {collector.server.errors}")
        metrics = [message for path, _, message in collector.server.messages if path == "/v1/metrics"]
        traces = [message for path, _, message in collector.server.messages if path == "/v1/traces"]
        if not metrics or not traces:
            raise AssertionError("shutdown did not flush both telemetry signals")
        latest = {metric.name: metric for resource in metrics[-1].resource_metrics
                  for scope_metric in resource.scope_metrics for metric in scope_metric.metrics}
        for name, count in expected.items():
            if latest[name].sum.data_points[0].as_int != count:
                raise AssertionError(f"OTLP counter disagrees with executed operations: {name}")
        for point in latest["nextral_retrieval_stage_seconds"].histogram.data_points:
            if point.count != 2 or sum(point.bucket_counts) != 2 or point.sum < 0:
                raise AssertionError("OTLP stage histogram is not based on the two retrievals")
        spans = [span for message in traces for resource in message.resource_spans
                 for scope_span in resource.scope_spans for span in scope_span.spans]
        if not {"nextral.ingest", "nextral.retrieve"}.issubset({span.name for span in spans}):
            raise AssertionError("actual ingestion/retrieval operation spans were not exported")
        clock_slack = max(1_000_000, int(time.get_clock_info("time").resolution * 1_000_000_000) + 1) * 2
        if any(not operation_start - clock_slack <= span.start_time_unix_nano <= span.end_time_unix_nano <= operation_end + clock_slack
               for span in spans):
            raise AssertionError("operation spans do not reflect the exercised wall-clock interval")
        exported = prometheus + "".join(raw.decode() for _, raw, _ in collector.server.messages)
        if any(secret in exported for secret in (sentinel, memory_id, *scope.values())):
            raise AssertionError("telemetry exposed private content or scope identities")
        print(json.dumps({"status": "ok", "backend": config["backend"], "counters": expected,
                          "otlp_metrics_requests": len(metrics), "otlp_trace_requests": len(traces),
                          "span_names": sorted({span.name for span in spans}), "graceful_shutdown": True}))


if __name__ == "__main__":
    main()
