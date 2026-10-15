#!/usr/bin/env python3
"""Exercise canonical graph replay against an already running five-store stack."""
from __future__ import annotations

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cli", type=Path, help="Built Nextral CLI; defaults to Cargo's target directory.")
    parser.add_argument("--config", type=Path, help="Existing production configuration; migrations/stores must already be ready")
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location("integration_smoke", ROOT / "scripts/integration-smoke.py")
    integration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(integration)
    target = Path(integration.json_run(["cargo", "metadata", "--no-deps", "--format-version", "1"])["target_directory"])
    cli = args.cli or target / "debug" / ("nextral.exe" if os.name == "nt" else "nextral")
    config = json.loads(args.config.read_text()) if args.config else integration.fixture_config({"http": 18180, "grpc": 18181, "graphql": 18182})
    if config["backend"] != "production_stores":
        raise ValueError("graph smoke requires production_stores")
    config["retrieval_policy"]["max_graph_hops"] = 10
    env = os.environ.copy()
    if not args.config:
        env.setdefault("NEXTRAL_NEO4J_USER", "neo4j")
        env.setdefault("NEXTRAL_NEO4J_PASSWORD", "nextraldev")
        env.setdefault("NEXTRAL_S3_ACCESS_KEY", "nextral")
        env.setdefault("NEXTRAL_S3_SECRET_KEY", "nextraldev")
    scratch = target / "tmp"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="nextral-graph-", dir=scratch) as directory:
        path = Path(directory) / "config.json"
        path.write_text(json.dumps(config))
        env["NEXTRAL_CONFIG"] = str(path)

        def call(tool, payload, *, failure=False, may_reject=False):
            result = subprocess.run([str(cli.resolve()), "call", tool, "-"], input=json.dumps(payload), text=True, capture_output=True, env=env, timeout=120)
            if failure:
                if result.returncode == 0:
                    raise AssertionError(f"{tool} unexpectedly accepted an unavailable scoped source")
                return
            if result.returncode and may_reject:
                error = json.loads(result.stderr).get("error", {})
                if error.get("code") in {"conflict", "not_found", "invalid_input"}:
                    return {"rejected": error["code"]}
            if result.returncode:
                raise RuntimeError(f"{tool} failed: {result.stderr}")
            return json.loads(result.stdout)

        def cypher(statement, scope):
            endpoint = config["stores"]["neo4j_url"].replace("neo4j+s://", "https://").replace("bolt+s://", "https://").replace("neo4j://", "http://").replace("bolt://", "http://").replace(":7687", ":7474").rstrip("/")
            credentials = (env.get("NEXTRAL_NEO4J_USER", "neo4j") + ":" + env["NEXTRAL_NEO4J_PASSWORD"]).encode()
            request = urllib.request.Request(endpoint + "/db/neo4j/tx/commit", json.dumps({"statements": [{"statement": statement, "parameters": scope}]}).encode(), headers={"Content-Type": "application/json", "Authorization": "Basic " + base64.b64encode(credentials).decode()})
            with urllib.request.urlopen(request, timeout=30) as response:
                body = json.load(response)
            if body["errors"]:
                raise AssertionError("Neo4j rejected graph smoke query")
            return [entry["row"] for entry in body["results"][0]["data"]]

        tenant = "graph-smoke-" + uuid.uuid4().hex
        owner = {"tenant_id": tenant, "user_id": "owner"}
        peer = dict(owner, user_id="peer")
        other = dict(owner, tenant_id=tenant + "-other")
        hint = {"from_label": "Project", "from_name": "Aster", "relationship_type": "USES", "to_label": "Technology", "to_name": "PostgreSQL", "confidence": 0.95}
        edge_query = "MATCH ()-[r:NEXTRAL_RELATES_TO {tenant_id:$tenant_id,user_id:$user_id}]->() RETURN r.from_key,r.relationship_type,r.to_key,r.source_memory_ids ORDER BY r.from_key,r.relationship_type,r.to_key"

        def edges(scope):
            return cypher(edge_query, scope)

        def source_edges(scope, memory_id):
            return [row for row in edges(scope) if memory_id in row[3]]

        def replay(scope, memory_id, hints):
            response = call("nextral.graph.graphify", dict(scope, memory_id=memory_id, hints=hints))
            if response["status"] != "accepted" or not any(receipt.startswith("outbox:") for receipt in response["write_receipts"]):
                raise AssertionError("graph replay did not acknowledge its durable transaction")
            return response

        def forget(scope, memory_id):
            result = call("nextral.memory.forget", dict(scope, memory_id=memory_id, actor=scope["user_id"], reason="graph smoke cleanup", redact=True))
            if result["status"] != "completed":
                raise AssertionError("graph forget did not receive all real-store acknowledgements")

        created = []
        try:
            for scope, memory_id in [(owner, "source"), (owner, "evidence-peer"), (peer, "peer-source"), (other, "source")]:
                request = integration.ingest(scope, memory_id, embedding=[1.0] + [0.0] * (config["embedding"]["dimension"] - 1))
                request["graph_hints"] = [hint]
                response = call("nextral.memory.ingest", request)
                if response["status"] != "accepted":
                    raise AssertionError("graph setup ingestion rejected")
                created.append((scope, memory_id))
            for index in range(10):
                memory_id = f"hop-{index}"
                request = integration.ingest(owner, memory_id, embedding=[1.0] + [0.0] * (config["embedding"]["dimension"] - 1))
                request.update(entities=[], graph_hints=[{
                    "from_label": "Entity", "from_name": f"Hop{index}", "relationship_type": "RELATED_TO",
                    "to_label": "Entity", "to_name": f"Hop{index + 1}", "confidence": 0.95,
                }])
                if call("nextral.memory.ingest", request)["status"] != "accepted":
                    raise AssertionError("traversal chain ingestion rejected")
                created.append((owner, memory_id))
            traversal = integration.retrieval(owner)
            traversal.update(entities=["Hop0"], query_text="terminal traversal", top_k_vector=0, max_graph_hops=9)
            if any(item["memory_id"] == "hop-9" for item in call("nextral.graph.query", traversal)["items"]):
                raise AssertionError("graph traversal crossed its requested nine-hop boundary")
            traversal["max_graph_hops"] = 10
            if not any(item["memory_id"] == "hop-9" for item in call("nextral.graph.query", traversal)["items"]):
                raise AssertionError("graph traversal failed to reach the configured tenth hop")
            initial = source_edges(owner, "source")
            if not initial or not any(set(row[3]) == {"source", "evidence-peer"} for row in initial):
                raise AssertionError("initial graph did not preserve both canonical sources")
            peer_before, other_before = edges(peer), edges(other)
            call("nextral.memory.update", dict(owner, memory_id="source", content="Aster now uses Redis.", remove_entities=["Aster", "PostgreSQL"], embedding=[1.0] + [0.0] * (config["embedding"]["dimension"] - 1)))
            if source_edges(owner, "source") or not source_edges(owner, "evidence-peer"):
                raise AssertionError("empty-hint canonical update left stale graph evidence or removed peer evidence")
            canonical_before = call("nextral.memory.get", dict(owner, memory_id="source"))
            updated_hint = dict(hint, to_name="Redis")
            replay(owner, "source", [updated_hint])
            first = edges(owner)
            duplicate_hint = dict(updated_hint, from_name="  ASTER  ")
            replay(owner, "source", [updated_hint, duplicate_hint])
            if edges(owner) != first:
                raise AssertionError("replay created duplicate identities or source contributions")
            canonical_after = call("nextral.memory.get", dict(owner, memory_id="source"))
            if canonical_after != canonical_before:
                raise AssertionError("graph replay rewrote canonical content or lifecycle metadata")
            count = cypher("MATCH (n:NextralEntity {tenant_id:$tenant_id,user_id:$user_id,label:'Project',canonical_name:'aster'}) RETURN count(n)", owner)
            if count != [[1]]:
                raise AssertionError("duplicate graph names did not share one scoped canonical identity")
            call("nextral.graph.graphify", dict(peer, memory_id="source", hints=[hint]), failure=True)
            call("nextral.graph.graphify", dict(owner, memory_id="missing", hints=[hint]), failure=True)
            call("nextral.graph.graphify", dict(owner, memory_id="source", hints=[hint] * 257), failure=True)
            if edges(peer) != peer_before or edges(other) != other_before:
                raise AssertionError("graph replay changed another user or tenant")
            call("nextral.memory.update", dict(owner, memory_id="source", privacy_level="sensitive"))
            if source_edges(owner, "source"):
                raise AssertionError("privacy update retained old host-hint source evidence")
            replay(owner, "source", [updated_hint])
            query = integration.retrieval(owner)
            query.update(query_text="Redis", entities=["Redis"], top_k_vector=0)
            response = call("nextral.memory.retrieve", query)
            if any(item["memory_id"] == "source" for item in response["items"]):
                raise AssertionError("private graph retrieval exposed a sensitive canonical source")
            call("nextral.graph.graphify", dict(owner, memory_id="source", hints=[updated_hint], record=canonical_after), failure=True)
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(call, "nextral.graph.graphify",
                    dict(owner, memory_id="source", hints=[updated_hint]), may_reject=True) for _ in range(4)]
                futures.append(executor.submit(call, "nextral.memory.forget",
                    dict(owner, memory_id="source", actor="owner", reason="concurrent graph replay", redact=True),
                    may_reject=True))
                race_results = [future.result() for future in futures]
            forget(owner, "source")
            call("nextral.graph.graphify", dict(owner, memory_id="source", hints=[updated_hint]), failure=True)
            # Claimed work that lost the privacy lease retries after thirty seconds.
            time.sleep(31)
            for _ in range(2):
                drained = call("nextral.outbox.run", {"tenant_id": tenant})
                if drained["failed"]:
                    raise AssertionError("graph replay left failed durable work")
            if source_edges(owner, "source") or not source_edges(owner, "evidence-peer"):
                raise AssertionError("outbox replay resurrected forgotten graph evidence or erased a peer")
            if edges(peer) != peer_before or edges(other) != other_before:
                raise AssertionError("forget changed graph evidence in another scope")
            print(json.dumps({"status": "ok", "backend": "production_stores", "max_graph_hops_verified": 10, "initial_graph": True, "canonical_change_invalidated": True, "privacy_change_invalidated": True, "canonical_replay_no_rewrite": True, "scoped_idempotent_replay": True, "duplicate_names_merged": True, "cross_user_and_tenant_isolation": True, "forget_replay_no_resurrection": True, "concurrent_replay_forget_results": race_results}))
        finally:
            for scope, memory_id in reversed(created):
                forget(scope, memory_id)


if __name__ == "__main__":
    main()
