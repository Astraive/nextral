#!/usr/bin/env python3
"""Exercise installed packages and real protocols against the local integration stack."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
POLICY = {"min_importance_score": 0.35, "min_confidence_score": 0.6}


def run(args, *, cwd=ROOT, env=None, input=None, timeout=300, expected=0):
    command = [str(arg) for arg in args]
    command[0] = shutil.which(command[0], path=(env or os.environ).get("PATH")) or command[0]
    result = subprocess.run(command, cwd=cwd, env=env, input=input,
                            text=True, capture_output=True, timeout=timeout)
    if result.returncode != expected:
        raise RuntimeError(f"{args[0]} returned {result.returncode}, expected {expected}\n{result.stdout}\n{result.stderr}")
    if result.stderr:
        print(result.stderr, file=sys.stderr, end="" if result.stderr.endswith("\n") else "\n")
    return result.stdout


def json_run(args, **kwargs):
    return json.loads(run(args, **kwargs))


def http(url, payload=None, *, token=None, method=None, expected=200):
    headers = {"Accept": "application/json"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, None if payload is None else json.dumps(payload).encode(),
                                     headers=headers, method=method)
    try:
        response = urllib.request.urlopen(request, timeout=15)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        body = response.read().decode()
        if response.status != expected:
            raise AssertionError(f"{request.method} {url}: expected {expected}, got {response.status}: {body}")
        return json.loads(body) if body and "json" in response.headers.get("Content-Type", "") else body


def wait_ready(name, check, *, process=None):
    deadline = time.monotonic() + 120
    last_error = None
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"{name} exited before readiness ({process.returncode})")
        try:
            check()
            print(f"ready: {name}", file=sys.stderr)
            return
        except (RuntimeError, OSError, AssertionError, urllib.error.URLError) as error:
            last_error = error
            time.sleep(1)
    raise RuntimeError(f"{name} did not become ready: {last_error}")


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def fixture_config(ports):
    return {
        "backend": "production_stores",
        "stores": {
            "postgres_url": "postgres://nextral:nextral@127.0.0.1:5432/nextral",
            "redis_url": "redis://127.0.0.1:6379/0", "qdrant_url": "http://127.0.0.1:6333",
            "qdrant_collection": "nextral_integration", "neo4j_url": "http://127.0.0.1:7474",
            "s3_endpoint": "http://127.0.0.1:9000", "s3_bucket": "nextral-memory",
            "s3_region": "us-east-1", "s3_access_key_env": "NEXTRAL_S3_ACCESS_KEY",
            "s3_secret_key_env": "NEXTRAL_S3_SECRET_KEY", "enforce_tls": False,
            "transport_profile": "baseline",
        },
        "embedding": {"kind": "external_callback", "model": "integration-host-vectors", "dimension": 4,
                      "endpoint": None, "api_key_env": None},
        "extraction": {"kind": "external_callback", "model": "integration-host-extraction",
                       "endpoint": None, "api_key_env": None},
        "reranker": {"kind": "none", "model": None, "endpoint": None, "api_key_env": None},
        "ingestion_policy": POLICY,
        "retrieval_policy": {"privacy_scope": ["private"], "token_budget": 1800, "top_k_vector": 12,
                             "max_graph_hops": 2, "scoring_weights": {"semantic_similarity": 0.5,
                             "recency": 0.2, "importance": 0.2, "access": 0.1}},
        "cache": {"key_prefix": "nextral:integration", "session_ttl_seconds": 7200,
                  "retrieval_ttl_seconds": 120, "policy_ttl_seconds": 300},
        "service": {"http_bind": f"127.0.0.1:{ports['http']}", "grpc_bind": f"127.0.0.1:{ports['grpc']}",
                    "graphql_bind": f"127.0.0.1:{ports['graphql']}"},
        "auth": {"issuer": None, "audience": None, "jwks_url": None, "service_token_env": "NEXTRAL_INTEGRATION_TOKEN"},
        "observability": {"enabled": False, "otlp_endpoint": None, "prometheus_bind": None},
        "rate_limit": {"enabled": False, "max_requests_per_second": 100, "burst_size": 200},
        "batch": {"max_batch_size": 100, "max_concurrent_batches": 4},
    }


def ingest(scope, memory_id, memory_type="semantic", *, private=True, embedding=None):
    return dict(scope, id=memory_id, session_id="session_integration", content="Aster uses PostgreSQL for durable knowledge.",
                content_type="fact", memory_type=memory_type, source_type="manual", source_message_ids=[],
                importance_score=0.9, confidence_score=0.95, entities=["Aster", "PostgreSQL"], tags=["integration"],
                privacy_level="private" if private else "sensitive", graph_hints=[{
                    "from_label": "Project", "from_name": "Aster", "relationship_type": "USES",
                    "to_label": "Technology", "to_name": "PostgreSQL", "confidence": 0.95}],
                policy=POLICY, trace_id=None, embedding=embedding or [1.0, 0.0, 0.0, 0.0])


def retrieval(scope):
    return dict(scope, session_id="session_integration", query_text="preferred persistence engine", entities=["Aster"],
                intent_topic="decision_recall", token_budget=1800, privacy_scope=["private"], top_k_vector=12,
                max_graph_hops=2, lane=None, policy_version=None, trace_id=None, scoring_weights=None,
                query_vector=[1.0, 0.0, 0.0, 0.0])


def assert_recalled(response, memory_id):
    if response.get("status") not in ("ok", "degraded"):
        raise AssertionError(f"retrieval unavailable: {response}")
    if memory_id not in {item["memory_id"] for item in response["items"]}:
        raise AssertionError(f"semantic memory not recalled: {response}")
    if response["telemetry"]["token_estimate"] > 1800:
        raise AssertionError("retrieval exceeded hard token budget")


def stop_process(process):
    if process.poll() is not None:
        return
    if os.name != "nt":
        process.send_signal(signal.SIGINT)
    else:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


class DeliveryServer:
    def __init__(self):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(handler):
                size = int(handler.headers.get("Content-Length", "0"))
                if size > 1024 * 1024:
                    handler.send_error(413)
                    return
                payload = json.loads(handler.rfile.read(size))
                handler.server.deliveries.append((handler.headers.get("Idempotency-Key"), payload))
                handler.send_response(handler.server.delivery_status)
                handler.send_header("Content-Length", "0")
                handler.end_headers()

            def log_message(self, *_args):
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.deliveries = []
        self.server.delivery_status = 503
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def exercise_lifecycles(py_call, node_call, scope, ids, query, compose, env):
    messages = []
    for index in range(21):
        content = f"Aster migration checkpoint {index}"
        request = dict(scope, session_id="session_integration", role="user", content=content,
                       idempotency_key=f"message-{index}", privacy_level="private")
        response = py_call("nextral.session.append", request)
        messages.append((response["message_id"], content))
    py_call("nextral.session.append", request)
    sql = f"SELECT count(*) FROM nextral_session_messages WHERE tenant_id='{scope['tenant_id']}' AND user_id='{scope['user_id']}'"
    warm_count = run(compose + ["exec", "-T", "postgres", "psql", "-U", "nextral", "-d", "nextral", "-Atc", sql], env=env).strip()
    if warm_count != "21":
        raise AssertionError(f"session append/replay produced {warm_count} durable messages, expected 21")
    hot = []
    redis = compose + ["exec", "-T", "redis", "redis-cli"]
    for key in run(redis + ["--scan"], env=env).splitlines():
        if run(redis + ["TYPE", key], env=env).strip() == "list":
            for encoded in json_run(redis + ["--json", "LRANGE", key, "0", "-1"], env=env):
                message = json.loads(encoded)
                if message.get("tenant_id") == scope["tenant_id"] and message.get("user_id") == scope["user_id"]:
                    hot.append(message["id"])
    if len(hot) != 20 or len(set(hot)) != 20 or messages[0][0] in hot or messages[-1][0] not in hot:
        raise AssertionError("Redis hot tail did not preserve the latest 20 unique messages")
    policy = py_call("nextral.policy.upsert", dict(scope, name="response-format",
                     body="Use concise engineering updates", privacy_level="private"))["policy"]
    peer = dict(scope, user_id="peer-user")
    hidden = py_call("nextral.policy.upsert", dict(peer, name="private-format",
                     body="Another user's private instructions", privacy_level="private"))["policy"]
    context = node_call("workingContext", query)
    if policy["id"] not in context["procedural_policy_ids"] or hidden["id"] in context["procedural_policy_ids"]:
        raise AssertionError("working context did not load isolated procedural policy")
    if len(context["session_tail"]) != 20 or context["token_estimate"] > query["token_budget"]:
        raise AssertionError(f"working context hot-tail/budget contract failed: {context}")
    candidate = ingest(scope, "host-fast-output")
    candidate.update(content="Track Aster migration checkpoints", source_message_ids=[messages[-1][0]])
    fast = dict(scope, session_id="session_integration", lane="fast", policy=POLICY,
                version="integration-fast-v1", summary="Aster migration checkpoints are being tracked",
                candidates=[candidate], chunk_messages=100)
    first = py_call("nextral.consolidation.run", fast)
    replay = py_call("nextral.consolidation.run", fast)
    if first["job"]["status"] != "completed" or replay != first:
        raise AssertionError("fast consolidation replay changed completed outputs")
    deep_candidate = ingest(scope, "host-deep-output", "episodic")
    deep_candidate.update(content="\n".join("user: " + text for _, text in messages),
                          source_message_ids=[message_id for message_id, _ in messages])
    deep = dict(scope, session_id="session_integration", lane="deep", policy=POLICY,
                version="integration-deep-v1", summary=None, candidates=[deep_candidate], chunk_messages=100)
    eligible_request = dict(scope, inactive_at_or_before=datetime.now(timezone.utc).isoformat(), limit=100)
    eligible = py_call("nextral.session.eligible", eligible_request)
    selected = next((item for item in eligible if item["session_id"] == deep["session_id"]), None)
    if selected is None or selected["message_count"] != len(messages):
        raise AssertionError(f"host could not select its durable session for deep work: {eligible}")
    queued_deep = py_call("nextral.jobs.enqueue", dict(scope, session_id=selected["session_id"],
                         trigger="deep", run_at=str(int(time.time())), consolidation=deep))
    jobs = node_call("runJobs", dict(scope, now=str(int(time.time()) + 1), limit=100, lease_seconds=30))
    if not any(job["id"] == queued_deep["id"] and job["status"] == "completed" for job in jobs):
        raise AssertionError(f"host-selected deep job did not complete: {jobs}")
    archived = py_call("nextral.consolidation.run", deep)
    archive_receipts = [receipt for receipt in archived["receipts"]
                        if receipt["operation"] == "archive_transcript" and receipt["status"] == "ok"]
    if archived["job"]["status"] != "completed" or len(archive_receipts) != 1:
        raise AssertionError("deep consolidation lacks an archive acknowledgement")
    archive_key = archive_receipts[0]["target_id"]
    archive = json_run(compose + ["run", "--rm", "--no-deps", "-T",
                        "-e", "NEXTRAL_S3_ACCESS_KEY", "-e", "NEXTRAL_S3_SECRET_KEY",
                        "--entrypoint", "/bin/sh", "mc", "-c",
                        'mc alias set local http://minio:9000 "$NEXTRAL_S3_ACCESS_KEY" "$NEXTRAL_S3_SECRET_KEY" >/dev/null && mc cat "local/nextral-memory/$1"',
                        "nextral-archive-check", archive_key], env=env)
    if (archive["tenant_id"] != scope["tenant_id"] or archive["user_id"] != scope["user_id"]
            or [(message["id"], message["content"]) for message in archive["messages"]] != messages):
        raise AssertionError("stored S3 archive differs from the complete scoped transcript")
    if py_call("nextral.consolidation.run", deep) != archived:
        raise AssertionError("deep consolidation was not replay-safe")
    py_call("nextral.policy.remove", dict(scope, policy_id=policy["id"]))
    if policy["id"] in node_call("workingContext", query)["procedural_policy_ids"]:
        raise AssertionError("removed policy leaked through stale policy cache")
    now = int(time.time())
    due_at = datetime.fromtimestamp(now, timezone.utc).isoformat().replace("+00:00", "Z")
    with DeliveryServer() as receiver:
        env["NEXTRAL_REMINDER_WEBHOOK_URL"] = f"http://127.0.0.1:{receiver.server.server_port}/"
        scheduled = py_call("nextral.reminders.schedule", dict(scope, source_memory_id=ids["prospective"],
                            kind="task", title="fail text must not simulate dispatch failure",
                            due_at=due_at, timezone="UTC"))["reminder"]
        execution = dict(scope, due_at_or_before=str(now), actor="integration-worker",
                         retry_delay_seconds=60, max_retries=2, privacy_scope=["private"])
        failed = py_call("nextral.reminders.due", execution)
        if failed["results"][0]["status"] != "retry_scheduled" or len(receiver.server.deliveries) != 1:
            raise AssertionError("real HTTP 503 did not persist a retry outcome")
        py_call("nextral.reminders.due", execution)
        if len(receiver.server.deliveries) != 1:
            raise AssertionError("reminder retried before next_attempt_at")
        receiver.server.delivery_status = 204
        execution["due_at_or_before"] = str(now + 61)
        completed = py_call("nextral.reminders.due", execution)
        if completed["results"][0]["status"] != "completed" or len(receiver.server.deliveries) != 2:
            raise AssertionError("acknowledged HTTP dispatch did not complete")
        if receiver.server.deliveries[0][0] != scheduled["dedupe_key"] or receiver.server.deliveries[1][0] != scheduled["dedupe_key"]:
            raise AssertionError("delivery retries did not preserve the idempotency key")
        py_call("nextral.reminders.due", execution)
        if len(receiver.server.deliveries) != 2:
            raise AssertionError("completed reminder was dispatched twice")
    env.pop("NEXTRAL_REMINDER_WEBHOOK_URL", None)
    return {"fast_memory_id": first["accepted"][0]["record_id"],
            "archive_memory_id": archived["accepted"][0]["record_id"], "archive_key": archive_key,
            "hot_messages": len(hot), "warm_messages": len(messages), "callback_attempts": 2}


def exercise_scheduled_jobs(py_call, node_call, scope, compose, env):
    owner = dict(scope, user_id="retention-worker")
    memory_id = scope["tenant_id"] + "_retention"
    source = py_call("nextral.session.append", dict(owner, session_id="retention-session",
                     role="user", content="Preserve this source until real deep archival",
                     idempotency_key="retention-source", privacy_level="private"))
    candidate = ingest(owner, memory_id, "episodic")
    candidate.update(session_id="retention-session", source_message_ids=[source["message_id"]])
    py_call("nextral.memory.ingest", candidate)
    now = int(time.time())
    request = dict(owner, session_id="retention-session", trigger="retention",
                   run_at=str(now + 120), memory_ids=[memory_id], redact=True, max_attempts=2)
    queued = py_call("nextral.jobs.enqueue", request)
    if py_call("nextral.jobs.enqueue", request)["id"] != queued["id"]:
        raise AssertionError("replayed scheduling duplicated a durable job")
    persisted = node_call("getJob", dict(owner, job_id=queued["id"]))
    if persisted["status"] != "queued":
        raise AssertionError("new process did not observe the queued retention job")
    if node_call("runJobs", dict(owner, now=str(now), limit=10, lease_seconds=30)):
        raise AssertionError("retention executed before its durable due time")
    execution = dict(owner, now=str(now + 121), limit=10, lease_seconds=30)
    blocked = node_call("runJobs", execution)
    if not any(job["id"] == queued["id"] and job["status"] == "retry_pending"
               and job["attempts"] == 1 and job["last_error"] for job in blocked):
        raise AssertionError(f"unarchived source retention did not defer: {blocked}")
    before_archive = node_call("listMemories", dict(owner, include_inactive=True))
    preserved = next(record for record in before_archive if record["id"] == memory_id)
    if (preserved["status"] != "active" or preserved["content"] != candidate["content"]
            or preserved["embedding"] != candidate["embedding"]):
        raise AssertionError("scheduled retention changed canonical source before deep archive")
    context_query = dict(retrieval(owner), session_id="retention-session")
    tail = node_call("workingContext", context_query)["session_tail"]
    if [(message["id"], message["content"]) for message in tail] != [
            (source["message_id"], "Preserve this source until real deep archival")]:
        raise AssertionError("scheduled retention changed the unarchived transcript")
    deep_candidate = dict(candidate, content="user: Preserve this source until real deep archival")
    deep = dict(owner, session_id="retention-session", lane="deep", policy=POLICY,
                version="retention-verified-deep", candidates=[deep_candidate], chunk_messages=100)
    archived = py_call("nextral.consolidation.run", deep)
    if archived["job"]["status"] != "completed" or not any(
            receipt["operation"] == "archive_transcript" and receipt["status"] == "ok"
            for receipt in archived["receipts"]):
        raise AssertionError(f"real deep archiving did not acknowledge storage: {archived}")
    archive_key = next(receipt["target_id"] for receipt in archived["receipts"]
                       if receipt["operation"] == "archive_transcript" and receipt["status"] == "ok")
    archive = json_run(compose + ["run", "--rm", "--no-deps", "-T",
                       "-e", "NEXTRAL_S3_ACCESS_KEY", "-e", "NEXTRAL_S3_SECRET_KEY",
                       "--entrypoint", "/bin/sh", "mc", "-c",
                       'mc alias set local http://minio:9000 "$NEXTRAL_S3_ACCESS_KEY" "$NEXTRAL_S3_SECRET_KEY" >/dev/null && mc cat "local/nextral-memory/$1"',
                       "nextral-retention-archive-check", archive_key], env=env)
    if (archive["tenant_id"] != owner["tenant_id"] or archive["user_id"] != owner["user_id"]
            or archive["session_id"] != "retention-session"
            or [(message["id"], message["content"]) for message in archive["messages"]]
            != [(source["message_id"], "Preserve this source until real deep archival")]):
        raise AssertionError("retention deep archive did not preserve its actual scoped source")
    if node_call("runJobs", execution):
        raise AssertionError("retention retried before its durable retry deadline")
    execution["now"] = str(now + 182)
    completed = node_call("runJobs", execution)
    if not any(job["id"] == queued["id"] and job["status"] == "completed"
               and job["attempts"] == 2 for job in completed):
        raise AssertionError(f"archived scheduled retention did not complete on retry: {completed}")
    records = node_call("listMemories", dict(owner, include_inactive=True))
    record = next(record for record in records if record["id"] == memory_id)
    if record["status"] != "redacted" or record["embedding"] is not None:
        raise AssertionError("retention did not redact canonical content and embedding")
    if node_call("retrieve", retrieval(owner))["items"] or node_call("runJobs", execution):
        raise AssertionError("retained memory remained recallable or the completed job repeated")
    missing = py_call("nextral.jobs.enqueue", dict(request, memory_ids=["absent-retention-source"],
                      run_at=str(now), max_attempts=1))
    failed = node_call("runJobs", execution)
    if not any(job["id"] == missing["id"] and job["status"] == "terminal_failed"
               and job["last_error"] for job in failed):
        raise AssertionError("terminal retention failure did not persist its cause")
    standalone_id = memory_id + "_standalone"
    standalone = ingest(owner, standalone_id, "episodic")
    standalone["session_id"] = None
    py_call("nextral.memory.ingest", standalone)
    standalone_job = py_call("nextral.jobs.enqueue", dict(request, memory_ids=[standalone_id],
                            run_at=str(now), max_attempts=1))
    if not any(job["id"] == standalone_job["id"] and job["status"] == "completed"
               for job in node_call("runJobs", execution)):
        raise AssertionError("standalone memory retention incorrectly required a transcript archive")
    return {"completed_job_id": queued["id"], "terminal_job_id": missing["id"]}


def exercise_reembed(py_call, node_call, scope, query, collection, recall_network, expected_id):
    peer = dict(scope, user_id="peer-user")
    peer_id = scope["tenant_id"] + "_peer"
    py_call("nextral.memory.ingest", ingest(peer, peer_id))
    records = node_call("listMemories", scope)
    vectors = [{"memory_id": record["id"], "vector": [1.0, 0.0, 0.0]} for record in records]
    plan = py_call("nextral.reembed.plan", {
        "tenant_id": scope["tenant_id"], "source_collection": collection,
        "shadow_collection": "nextral_shadow_" + uuid.uuid4().hex,
        "target_embedding_provider": "external_callback", "target_embedding_model": "integration-host-v2",
    })
    execution = {"plan": plan, "user_id": scope["user_id"], "target_dimension": 3, "cutover": False}
    first = py_call("nextral.jobs.reembed.execute", dict(execution, vectors=vectors[:1]))
    if first["status"] != "awaiting_vectors" or first["embedded_count"] != 1 or first["cutover_applied"]:
        raise AssertionError("partial backfill did not retain a non-cut-over checkpoint")
    completed = py_call("nextral.jobs.reembed.execute", dict(execution, vectors=vectors[1:], cutover=True))
    if (completed["status"] != "completed" or not completed["cutover_applied"]
            or completed["source_count"] != len(records) or completed["validated_count"] != len(records)):
        raise AssertionError(f"measured backfill/cutover failed: {completed}")
    migrated_query = dict(query, query_vector=[1.0, 0.0, 0.0])
    assert_recalled(node_call("retrieve", migrated_query), expected_id)
    recall_network(migrated_query, expected_id)
    peer_query = retrieval(peer)
    assert_recalled(node_call("retrieve", peer_query), peer_id)
    recall_network(peer_query, peer_id)
    rolled_back = py_call("nextral.jobs.reembed.execute", dict(execution, vectors=[], rollback=True))
    if rolled_back["status"] != "rolled_back" or rolled_back["cutover_applied"]:
        raise AssertionError("rollback did not restore the scoped source route")
    assert_recalled(node_call("retrieve", query), expected_id)
    recall_network(query, expected_id)
    return {"plan_id": plan["id"], "source_count": len(records),
            "validated_count": completed["validated_count"], "dimension": 3, "rolled_back": True}


def collect_research_evidence(py_call, target, env, latency_budget_ns):
    # Always build these release examples so revision metadata describes this source,
    # including when installed package artifacts were supplied with --skip-build.
    run(["cargo", "build", "--release", "--locked", "--example", "research_host",
         "--example", "research_evaluation"], env=env, timeout=1800)
    suffix = ".exe" if os.name == "nt" else ""
    host = target / "release/examples" / f"research_host{suffix}"
    evaluation = target / "release/examples" / f"research_evaluation{suffix}"
    revision = run(["git", "rev-parse", "HEAD"], env=env).strip()
    source_diff = run(["git", "diff", "--binary", "HEAD"], env=env)
    if source_diff:
        revision += "-dirty-" + hashlib.sha256(source_diff.encode()).hexdigest()
    host_identity = {
        "hostname": socket.gethostname(), "os": platform.system(), "architecture": platform.machine(),
        "research_host_sha256": hashlib.sha256(host.read_bytes()).hexdigest(),
        "research_evaluation_sha256": hashlib.sha256(evaluation.read_bytes()).hexdigest(),
    }
    host_id = "integration-" + hashlib.sha256(json.dumps(host_identity, sort_keys=True).encode()).hexdigest()
    run_id = uuid.uuid4().hex
    directory = target / "integration-research" / run_id
    directory.mkdir(parents=True)
    print(f"measured research artifacts: {directory}", file=sys.stderr)
    prior = py_call("experiments.status", {})
    policies = {"baseline": prior["active_policy_version"], "candidate": "integration-" + run_id}
    manifest = dict(host_identity, implementation_revision=revision, host_id=host_id,
                    policy_versions=policies, prior_control=prior,
                    latency_budget_ns=latency_budget_ns,
                    comparison="Two independent same-implementation release replays; no policy algorithm change.")
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    reports = {}
    for label, policy_version in policies.items():
        report = run([evaluation, "representative", host, revision, policy_version,
                      label + "-" + run_id, host_id, str(latency_budget_ns)],
                     env=env, timeout=900)
        (directory / f"{label}.json").write_text(report, encoding="utf-8")
        reports[label] = json.loads(report)
    evidence = directory / "evidence.json"
    evidence.write_text(json.dumps(reports, indent=2), encoding="utf-8")
    return directory, evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-start", action="store_true")
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--skip-build", action="store_true",
                        help="Use prebuilt packages/debug binaries; research examples still build in release.")
    parser.add_argument("--wheel-dir", type=Path, help="Install a compatible wheel from this artifact directory.")
    parser.add_argument("--npm-package", type=Path, help="Install this prebuilt npm tarball.")
    parser.add_argument("--research-latency-budget-ns", type=int, default=30_000_000_000,
                        help="Process-roundtrip replay budget including setup (default: 30 seconds; not a production SLO).")
    args = parser.parse_args()
    if args.research_latency_budget_ns <= 0:
        parser.error("--research-latency-budget-ns must be positive")
    if args.skip_build and (args.wheel_dir is None or args.npm_package is None):
        parser.error("--skip-build requires --wheel-dir and --npm-package")
    if args.wheel_dir is not None:
        args.wheel_dir = args.wheel_dir.resolve()
        if not args.wheel_dir.is_dir():
            parser.error("--wheel-dir must name an existing directory")
    if args.npm_package is not None:
        args.npm_package = args.npm_package.resolve()
        if not args.npm_package.is_file():
            parser.error("--npm-package must name an existing tarball")
    env = os.environ.copy()
    for name in ("PYTHONPATH", "PYTHONHOME", "AWS_SESSION_TOKEN", "NEXTRAL_REMINDER_WEBHOOK_TOKEN",
                 "NEXTRAL_QDRANT_API_KEY", "NEXTRAL_NEO4J_API_KEY", "NEXTRAL_RESEARCH_EVIDENCE",
                 "NEXTRAL_CONTROL_RESULT", "NEXTRAL_PYTHON_PRODUCTION_CONFIG", "NEXTRAL_NODE_PRODUCTION_CONFIG"):
        env.pop(name, None)
    env.update(NEXTRAL_S3_ACCESS_KEY="nextral", NEXTRAL_S3_SECRET_KEY="nextraldev",
               NEXTRAL_POSTGRES_DB="nextral", NEXTRAL_POSTGRES_USER="nextral", NEXTRAL_POSTGRES_PASSWORD="nextral",
               NEXTRAL_MINIO_USER="nextral", NEXTRAL_MINIO_PASSWORD="nextraldev", NEXTRAL_NEO4J_AUTH="neo4j/nextraldev",
               NEXTRAL_NEO4J_USER="neo4j", NEXTRAL_NEO4J_PASSWORD="nextraldev",
               NEXTRAL_INTEGRATION_TOKEN=uuid.uuid4().hex)
    compose = ["docker", "compose", "-p", env.get("COMPOSE_PROJECT_NAME", "nextral"), "-f", ROOT / "docker-compose.integration.yml"]
    started = not args.skip_start and not run(compose + ["ps", "--quiet"], env=env).strip()
    metadata = json_run(["cargo", "metadata", "--no-deps", "--format-version", "1"], env=env)
    target = Path(metadata["target_directory"])
    core_version = next(package["version"] for package in metadata["packages"] if package["name"] == "nextral")
    target.mkdir(parents=True, exist_ok=True)
    scratch = target / "tmp"
    scratch.mkdir(parents=True, exist_ok=True)
    env.update(TEMP=str(scratch), TMP=str(scratch), TMPDIR=str(scratch),
               CARGO_TARGET_DIR=str(target), PIP_CACHE_DIR=str(target / "pip-cache"))
    suffix = ".exe" if os.name == "nt" else ""
    cli, api, mcp = [target / "debug" / f"{name}{suffix}" for name in ("nextral", "nextral-api", "nextral-mcp")]
    try:
        if not args.skip_start:
            run(compose + ["up", "-d"], env=env, timeout=600)
        wait_ready("PostgreSQL", lambda: run(compose + ["exec", "-T", "postgres", "pg_isready", "-U", "nextral", "-d", "nextral"], env=env))
        wait_ready("Redis", lambda: run(compose + ["exec", "-T", "redis", "redis-cli", "PING"], env=env))
        wait_ready("Qdrant", lambda: http("http://127.0.0.1:6333/readyz"))
        wait_ready("Neo4j", lambda: run(compose + ["exec", "-T", "neo4j", "cypher-shell", "-u", "neo4j", "-p", "nextraldev", "RETURN 1;"], env=env))
        wait_ready("MinIO", lambda: http("http://127.0.0.1:9000/minio/health/ready"))
        run(compose + ["run", "--rm", "--no-deps", "-T", "-e", "NEXTRAL_S3_ACCESS_KEY", "-e", "NEXTRAL_S3_SECRET_KEY",
             "--entrypoint", "/bin/sh", "mc", "-c",
             'mc alias set local http://minio:9000 "$NEXTRAL_S3_ACCESS_KEY" "$NEXTRAL_S3_SECRET_KEY" >/dev/null && mc mb --ignore-existing local/nextral-memory'], env=env)
        run(compose + ["exec", "-T", "neo4j", "cypher-shell", "-u", "neo4j", "-p", "nextraldev"],
            input=(ROOT / "migrations/neo4j/0001_graph_schema.cypher").read_text(), env=env)
        if not args.skip_build:
            run(["cargo", "build", "--workspace", "--locked"], env=env, timeout=1800)
            if args.npm_package is None:
                run(["npm", "ci"], env=env)
                run(["npm", "--workspace", "bindings/node", "run", "build:debug"], env=env, timeout=1800)
        with tempfile.TemporaryDirectory(prefix="nextral-integration-", dir=target) as directory:
            work = Path(directory)
            ports = {name: free_port() for name in ("http", "grpc", "graphql")}
            config = fixture_config(ports)
            config_path = work / "config.json"
            config_path.write_text(json.dumps(config))
            env["NEXTRAL_CONFIG"] = str(config_path)
            stores = config["stores"]
            env.update(NEXTRAL_QDRANT_URL=stores["qdrant_url"], NEXTRAL_REDIS_URL=stores["redis_url"],
                       NEXTRAL_NEO4J_URL=stores["neo4j_url"], NEXTRAL_S3_ENDPOINT=stores["s3_endpoint"],
                       NEXTRAL_S3_BUCKET=stores["s3_bucket"], NEXTRAL_S3_REGION=stores["s3_region"])
            print(run(["cargo", "test", "--workspace", "--all-features", "--locked", "--", "--ignored"],
                      env=env, timeout=900), file=sys.stderr)
            wheel_dir = args.wheel_dir or work / "wheels"
            if args.wheel_dir is None:
                run([sys.executable, "-m", "maturin", "build", "--manifest-path", ROOT / "bindings/python/Cargo.toml", "--locked", "--target-dir", target / "python-native", "--out", wheel_dir], env=env, timeout=1800)
            run([sys.executable, "-m", "venv", work / "venv"], env=env)
            python = work / "venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
            run([python, "-m", "pip", "install", "--no-index", "--find-links", wheel_dir,
                 "--no-deps", f"nextral=={core_version}"], env=env, timeout=600)
            run([python, "-m", "pip", "install", "grpcio==1.82.1", "grpcio-tools==1.82.1",
                 "opentelemetry-proto==1.45.1"], env=env, timeout=600)
            package_path = args.npm_package
            if package_path is None:
                packed = json_run(["npm", "pack", "--json", "--pack-destination", work],
                                  cwd=ROOT / "bindings/node", env=env)
                package_path = work / packed[0]["filename"]
            consumer = work / "node-consumer"
            consumer.mkdir()
            (consumer / "package.json").write_text('{"private":true}')
            run(["npm", "install", "--ignore-scripts", "--no-audit", "--no-fund", package_path], cwd=consumer, env=env)
            python_code = "import json,sys,nextral; client=nextral.Client(json.load(open(sys.argv[1]))); print(json.dumps(client.call(sys.argv[2],json.loads(sys.argv[3])))); client.close()"
            node_code = "const fs=require('node:fs'),n=require('nextral');(async()=>{const c=await n.Client.create(JSON.parse(fs.readFileSync(process.argv[1],'utf8')));console.log(JSON.stringify(await c[process.argv[2]](JSON.parse(process.argv[3]))));await c.close();})().catch(e=>{console.error(e);process.exit(1);});"
            def py_call(tool, payload):
                return json_run([python, "-I", "-c", python_code, config_path, tool, json.dumps(payload)], cwd=work, env=env)
            def node_call(method, payload):
                return json_run(["node", "-e", node_code, config_path, method, json.dumps(payload)], cwd=consumer, env=env)
            def cli_call(tool, payload):
                return json_run([cli, "call", tool, json.dumps(payload)], cwd=work, env=env)
            py_call("nextral.database.migrate", {})
            run(compose + ["exec", "-T", "postgres", "psql", "-U", "nextral", "-d", "nextral", "-v", "ON_ERROR_STOP=1"], env=env, input="""
DO $$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='nextral_integration_runtime') THEN
  CREATE ROLE nextral_integration_runtime LOGIN PASSWORD 'integration_runtime' NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE;
 END IF;
END $$;
GRANT USAGE ON SCHEMA public TO nextral_integration_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO nextral_integration_runtime;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO nextral_integration_runtime;
REVOKE INSERT, UPDATE, DELETE ON nextral_schema_migrations FROM nextral_integration_runtime;
""")
            config["stores"]["postgres_url"] = "postgres://nextral_integration_runtime:integration_runtime@127.0.0.1:5432/nextral"
            config_path.write_text(json.dumps(config))
            research_directory, research_evidence = collect_research_evidence(
                py_call, target, env, args.research_latency_budget_ns)
            python_control = research_directory / "python-control.json"
            python_env = dict(env, NEXTRAL_PYTHON_PRODUCTION_CONFIG=str(config_path),
                              NEXTRAL_RESEARCH_EVIDENCE=str(research_evidence),
                              NEXTRAL_CONTROL_RESULT=str(python_control))
            print(run([python, "-I", ROOT / "bindings/python/tests/test_consumer.py"],
                      cwd=work, env=python_env), file=sys.stderr)
            consumer_env = dict(env, NEXTRAL_PACKAGE=str(consumer / "node_modules/nextral"),
                                NEXTRAL_NODE_PRODUCTION_CONFIG=str(config_path),
                                NEXTRAL_RESEARCH_EVIDENCE=str(research_evidence),
                                NEXTRAL_CONTROL_RESULT=str(research_directory / "node-control.json"))
            print(run(["node", "--test", ROOT / "bindings/node/test/consumer.test.cjs"],
                      cwd=consumer, env=consumer_env), file=sys.stderr)
            research = {"evidence_directory": str(research_directory),
                        "python": json.loads(python_control.read_text(encoding="utf-8")),
                        "node": json.loads((research_directory / "node-control.json").read_text(encoding="utf-8"))}
            runtime_psql = compose + ["exec", "-T", "-e", "PGPASSWORD=integration_runtime", "postgres", "psql",
                                     "-h", "127.0.0.1", "-U", "nextral_integration_runtime", "-d", "nextral", "-qAt",
                                     "-v", "ON_ERROR_STOP=1"]
            run(runtime_psql + ["-c", "CREATE TABLE public.nextral_forbidden_ddl(id integer)"], env=env, expected=1)
            scope = {"tenant_id": "integration_" + uuid.uuid4().hex, "user_id": "user_integration"}
            ids = {}
            for memory_type in ("session", "episodic", "semantic", "relational", "procedural", "prospective"):
                memory_id = f"{scope['tenant_id']}_{memory_type}"
                response = py_call("nextral.memory.ingest", ingest(scope, memory_id, memory_type))
                if response.get("status") != "accepted" or response.get("record_id") != memory_id:
                    raise AssertionError(f"ingestion failed: {response}")
                ids[memory_type] = memory_id
            unscoped = run(runtime_psql + ["-c", "SELECT count(*) FROM nextral_memories"], env=env).strip()
            if unscoped != "0":
                raise AssertionError(f"database RLS exposed {unscoped} rows without tenant/user context")
            query = retrieval(scope)
            for caller in (lambda: py_call("nextral.memory.retrieve", query), lambda: node_call("retrieve", query), lambda: cli_call("nextral.memory.retrieve", query)):
                assert_recalled(caller(), ids["semantic"])
            lifecycle = exercise_lifecycles(py_call, node_call, scope, ids, query, compose, env)
            scheduled_jobs = exercise_scheduled_jobs(py_call, node_call, scope, compose, env)
            messages = [
                {"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"nextral-integration","version":"1"}}},
                {"jsonrpc":"2.0","method":"notifications/initialized"},
                {"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}},
                {"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"nextral.memory.retrieve","arguments":query}},
            ]
            responses = [json.loads(line) for line in run([mcp], cwd=work, env=env, input="".join(json.dumps(message)+"\n" for message in messages)).splitlines()]
            by_id = {response.get("id"): response for response in responses}
            if "error" in by_id[1] or not by_id[2]["result"]["tools"]:
                raise AssertionError(f"MCP lifecycle failed: {responses}")
            result = by_id[3]["result"]
            if result.get("isError"):
                raise AssertionError(f"MCP tool failed: {result}")
            assert_recalled(result.get("structuredContent") or json.loads(result["content"][0]["text"]), ids["semantic"])
            log_path = work / "api.log"
            with log_path.open("w+") as log:
                server = subprocess.Popen([str(api), "serve", "all", str(config_path)], cwd=work, env=env, stdout=log, stderr=log)
                try:
                    base = f"http://127.0.0.1:{ports['http']}"
                    token = env["NEXTRAL_INTEGRATION_TOKEN"]
                    wait_ready("API protocols", lambda: http(base + "/readyz", token=token), process=server)
                    http(base + "/v1/retrieval/query", query, expected=401)
                    generated = work / "grpc"
                    generated.mkdir()
                    run([python,"-m","grpc_tools.protoc","-I",ROOT / "contracts/grpc",f"--python_out={generated}",f"--grpc_python_out={generated}",ROOT / "contracts/grpc/nextral.proto"], cwd=work, env=env)
                    grpc_code = "import json,sys,grpc,nextral_pb2 as p,nextral_pb2_grpc as g; from google.protobuf.json_format import MessageToDict; channel=grpc.insecure_channel(sys.argv[1]); request=p.RetrieveMemoryRequest(tenant_id=sys.argv[2],user_id=sys.argv[3],query_text='preferred persistence engine',privacy_scope=['private'],token_budget=1800,top_k_vector=12,max_graph_hops=2,query_vector=json.loads(sys.argv[5])); response=g.MemoryServiceStub(channel).Retrieve(request,metadata=[('authorization','Bearer '+sys.argv[4])],timeout=15); print(json.dumps(MessageToDict(response,preserving_proto_field_name=True)))"
                    def recall_network(request, expected_id):
                        assert_recalled(http(base + "/v1/retrieval/query", request, token=token), expected_id)
                        gql = {"query":"query Recall($tenant:String!,$user:String!,$vector:[Float!]) { retrieve(tenantId:$tenant,userId:$user,queryText:\"preferred persistence engine\",privacyScope:[\"private\"],tokenBudget:1800,queryVector:$vector) { status items { memoryId content } } }",
                               "variables":{"tenant":request["tenant_id"],"user":request["user_id"],
                                            "vector":request["query_vector"]}}
                        graph = http(f"http://127.0.0.1:{ports['graphql']}/graphql", gql, token=token)
                        if graph.get("errors") or expected_id not in {item["memoryId"] for item in graph["data"]["retrieve"]["items"]}:
                            raise AssertionError(f"GraphQL retrieval failed: {graph}")
                        recalled = json_run([python,"-c",grpc_code,f"127.0.0.1:{ports['grpc']}",
                                             request["tenant_id"],request["user_id"],token,
                                             json.dumps(request["query_vector"])], cwd=generated, env=env)
                        assert_recalled(recalled, expected_id)
                    recall_network(query, ids["semantic"])
                    reembedding = exercise_reembed(py_call, node_call, scope, query,
                                                   config["stores"]["qdrant_collection"],
                                                   recall_network, ids["semantic"])
                    http(base + f"/v1/memories/{ids['semantic']}/forget", dict(scope, actor=scope["user_id"], reason="integration cleanup", redact=True), token=token, expected=202)
                    after = node_call("retrieve", query)
                    if ids["semantic"] in {item["memory_id"] for item in after["items"]}:
                        raise AssertionError("forgotten memory leaked through Node retrieval")
                except Exception:
                    log.flush()
                    log.seek(0)
                    print(log.read(), file=sys.stderr)
                    raise
                finally:
                    stop_process(server)
            graph_replay = json_run([python, "-I", ROOT / "scripts/graph-smoke.py",
                                     "--cli", cli, "--config", config_path], cwd=work, env=env, timeout=900)
            authorization = json_run([python, "-I", ROOT / "scripts/auth-smoke.py",
                                      "--api", api, "--config", config_path, "--scratch", target],
                                     cwd=work, env=env, timeout=900)
            telemetry = json_run([python, "-I", ROOT / "scripts/telemetry-smoke.py", config_path],
                                 cwd=work, env=env)
            research_blocked = any(research[consumer]["status"] != "promoted_and_rolled_back"
                                   for consumer in ("python", "node"))
            print(json.dumps({"status":"blocked" if research_blocked else "ok","backend":"production_stores","durable_memory_types":sorted(ids),"working_context":"ephemeral","surfaces":["installed-python","packed-node","cli","mcp-stdio","http","grpc","graphql"],"restart_persistence":True,"forget_recall_blocked":True,"lifecycle":lifecycle,"scheduled_jobs":scheduled_jobs,"reembedding":reembedding,"graph_replay":graph_replay,"authorization":authorization,"telemetry":telemetry,"research":research}))
            if research_blocked:
                raise RuntimeError(
                    "Measured promotion gate rejected genuine replay evidence; control rejection boundaries passed, "
                    "but live promotion/rollback remains unverified. Same-implementation process-roundtrip p95/max "
                    "must not regress; independent store/host timing can fail this gate. No retries or relaxation "
                    f"were applied. Inspect reports and native errors in {research_directory}.")
    finally:
        if started and not args.keep:
            run(compose + ["down", "--volumes"], env=env)


if __name__ == "__main__":
    main()
