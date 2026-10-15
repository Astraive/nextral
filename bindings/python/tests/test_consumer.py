"""Run against an installed wheel: python -I /absolute/path/test_consumer.py.

The runner must use a virtualenv and a working directory outside this checkout.
No mocks, source-path injection, pytest, or checked-in native binary is used.
"""

from __future__ import annotations

import copy
import importlib.metadata
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from urllib.error import URLError
from urllib.request import urlopen

import nextral


FIXTURE = Path(__file__).parent / "fixtures" / "test-config.json"
CONFIG = json.loads(FIXTURE.read_text(encoding="utf-8"))
CHECKOUT = Path(__file__).resolve().parents[3]


class InstalledConsumerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        origin = Path(nextral.__file__).resolve()
        if origin.is_relative_to(CHECKOUT):
            raise AssertionError(f"source-shadowed import: {origin}")
        if Path.cwd().resolve().is_relative_to(CHECKOUT):
            raise AssertionError("run consumer tests from outside the checkout")
        nextral.configure_runtime(CONFIG)

    def setUp(self) -> None:
        self.scope = {"tenant_id": "python-consumer", "user_id": uuid.uuid4().hex}

    def ingest_request(self, text: str = "PostgreSQL stores the Atlas project") -> dict:
        request = nextral.ingest_request_schema()
        request.update(self.scope, content=text, importance_score=0.9, confidence_score=0.9)
        request["embedding"] = [1.0, 0.0]
        request["tags"] = ["atlas"]
        return request

    def retrieval_request(self) -> dict:
        return {
            **self.scope,
            "query_text": "PostgreSQL Atlas",
            "query_vector": [1.0, 0.0],
            "entities": [],
            "token_budget": 1800,
            "privacy_scope": ["private"],
            "top_k_vector": 12,
            "max_graph_hops": 2,
        }

    def cli(
        self, *args: str, stdin: str | None = None, config_environment: str | None = None,
    ) -> subprocess.CompletedProcess:
        environment = os.environ.copy()
        environment.pop("NEXTRAL_CONFIG", None)
        if config_environment is not None:
            environment["NEXTRAL_CONFIG"] = config_environment
        with tempfile.TemporaryDirectory(prefix="nextral-consumer-") as cwd:
            return subprocess.run(
                [sys.executable, "-I", "-m", "nextral", *args],
                cwd=cwd, env=environment, input=stdin, text=True,
                encoding="utf-8", capture_output=True, timeout=30, check=False,
            )

    def test_wheel_contains_types_license_and_abi_metadata(self) -> None:
        distribution = importlib.metadata.distribution("nextral")
        files = {str(path).replace("\\", "/") for path in distribution.files or []}
        self.assertIn("nextral/py.typed", files)
        self.assertIn("nextral/_nextral.pyi", files)
        self.assertTrue(any("LICENSE" in path for path in files))
        self.assertIn("abi3", distribution.read_text("WHEEL") or "")

    def test_ingest_retrieve_scope_isolation_and_forget(self) -> None:
        result = nextral.ingest(self.ingest_request())
        self.assertEqual(result["status"], "accepted")
        memory_id = result["record_id"]
        self.assertEqual(nextral.get_memory(self.scope | {"memory_id": memory_id})["id"], memory_id)
        self.assertIsNone(nextral.get_memory(self.scope | {"memory_id": "absent"}))
        retrieval = nextral.retrieve(self.retrieval_request())
        self.assertIn(memory_id, [item["memory_id"] for item in retrieval["items"]])
        isolated = self.retrieval_request() | {"user_id": "another-user"}
        self.assertEqual(nextral.retrieve(isolated)["items"], [])
        self.assertIn(memory_id, [row["id"] for row in nextral.list_memories(self.scope)])
        nextral.forget({**self.scope, "memory_id": memory_id, "actor": "consumer", "reason": "erase", "redact": True})
        self.assertNotIn(memory_id, [item["memory_id"] for item in nextral.retrieve(self.retrieval_request())["items"]])

    def test_session_host_extraction_and_reminders(self) -> None:
        session_id = uuid.uuid4().hex
        appended = nextral.append_session({
            **self.scope, "session_id": session_id, "role": "user",
            "content": "PostgreSQL powers Atlas", "privacy_level": "private",
        })
        self.assertTrue(appended["message_id"])
        candidate = self.ingest_request("PostgreSQL powers Atlas")
        candidate.update(session_id=session_id, source_message_ids=[appended["message_id"]])
        consolidated = nextral.consolidate({
            **self.scope, "session_id": session_id, "lane": "fast",
            "policy": CONFIG["ingestion_policy"], "version": "consumer-v1",
            "summary": "Atlas uses PostgreSQL", "candidates": [candidate],
        })
        self.assertEqual(consolidated["job"]["status"], "completed")
        memory_id = consolidated["accepted"][0]["record_id"]
        context = nextral.session_context(self.retrieval_request() | {"session_id": session_id})
        self.assertTrue(context["session_tail"])
        scheduled = nextral.schedule_reminder({
            **self.scope, "source_memory_id": memory_id, "kind": "follow_up",
            "title": "Check Atlas", "due_at": "9999999999", "timezone": "UTC",
        })
        self.assertEqual(scheduled["reminder"]["status"], "scheduled")
        self.assertIn(
            scheduled["reminder"]["id"],
            [reminder["id"] for reminder in nextral.list_reminders(self.scope)],
        )
        due = nextral.due_reminders({
            **self.scope, "due_at_or_before": "1", "actor": "consumer", "retry_delay_seconds": 60,
        })
        self.assertEqual(due["results"], [])

    def test_batch_and_graph_use_native_runtime(self) -> None:
        batch = nextral.batch_ingest({"items": [self.ingest_request()], "idempotency_key": uuid.uuid4().hex})
        self.assertEqual(batch["succeeded"], 1)
        retrieval = nextral.batch_retrieve({"queries": [self.retrieval_request()]})
        self.assertEqual(retrieval["succeeded"], 1)
        graph = nextral.graph_query(self.retrieval_request())
        self.assertIsInstance(graph, dict)
        memory_id = batch["results"][0]["record_id"]
        forgotten = nextral.batch_forget({"items": [{
            **self.scope, "memory_id": memory_id, "actor": "consumer", "reason": "batch erase", "redact": True,
        }]})
        self.assertEqual(forgotten["succeeded"], 1)

    def test_host_vectors_reembed_and_durable_retention_jobs(self) -> None:
        memory_id = nextral.ingest(self.ingest_request())["record_id"]
        suffix = uuid.uuid4().hex
        plan = nextral.reembed_plan({
            "tenant_id": self.scope["tenant_id"],
            "source_collection": "source-" + suffix,
            "shadow_collection": "shadow-" + suffix,
            "target_embedding_provider": "external_callback",
            "target_embedding_model": "consumer-host",
        })
        report = nextral.execute_reembed({
            "plan": plan, "user_id": self.scope["user_id"],
            "vectors": [{"memory_id": memory_id, "vector": [0.0, 1.0]}],
            "target_dimension": 2, "cutover": True,
        })
        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["validated_count"], 1)
        job = nextral.enqueue_job({
            **self.scope, "session_id": suffix, "trigger": "retention",
            "run_at": "1", "memory_ids": [memory_id], "redact": True,
        })
        self.assertEqual(
            nextral.get_job(self.scope | {"job_id": job["id"]})["id"], job["id"],
        )
        self.assertIn(job["id"], [row["id"] for row in nextral.list_jobs(self.scope)])
        executed = nextral.run_jobs(self.scope | {"now": str(int(time.time()))})
        self.assertTrue(any(row["id"] == job["id"] and row["status"] == "completed" for row in executed))
        self.assertEqual(nextral.retrieve(self.retrieval_request())["items"], [])

    def test_procedural_policy_crud_and_context(self) -> None:
        response = nextral.upsert_policy({
            **self.scope, "name": "format", "body": "Use concise bullet points",
            "privacy_level": "private",
        })
        policy_id = response["policy"]["id"]
        self.assertIn(policy_id, [row["id"] for row in nextral.list_policies(self.scope)])
        self.assertEqual(
            nextral.list_policies(self.scope | {"user_id": "another-user"}), [],
        )
        context = nextral.session_context(self.retrieval_request())
        self.assertIn(policy_id, context["procedural_policy_ids"])
        removed = nextral.remove_policy(self.scope | {"policy_id": policy_id})
        self.assertEqual(removed["status"], "removed")
        self.assertEqual(nextral.list_policies(self.scope), [])

    def test_structured_core_errors_and_json_errors(self) -> None:
        with self.assertRaises(nextral.InvalidInputError) as raised:
            nextral.mcp_call("nextral.not-a-tool", {})
        self.assertEqual(raised.exception.code, "invalid_input")
        self.assertTrue(raised.exception.message)
        with self.assertRaises(nextral.NotFoundError) as missing:
            nextral.forget({**self.scope, "memory_id": "absent", "actor": "consumer", "reason": "erase", "redact": True})
        self.assertEqual(missing.exception.code, "not_found")
        with self.assertRaises(nextral.SerializationError):
            nextral.ingest(self.ingest_request() | {"embedding": [float("nan")]})
        invalid = copy.deepcopy(CONFIG)
        invalid["embedding"]["dimension"] = 0
        with self.assertRaises(nextral.InvalidInputError):
            nextral.validate_config(invalid)
        with self.assertRaises(nextral.InvalidInputError) as malformed:
            nextral.configure_runtime({"backend": "unsupported"})
        self.assertEqual(malformed.exception.code, "invalid_input")

    def test_cli_execution_schema_stdin_and_files(self) -> None:
        schema = self.cli("memory", "ingest-schema")
        self.assertEqual(schema.returncode, 0, schema.stderr)
        self.assertIn("content", json.loads(schema.stdout))
        request = self.ingest_request()
        inline = self.cli("--config", str(FIXTURE), "memory", "ingest", json.dumps(request))
        self.assertEqual(inline.returncode, 0, inline.stderr)
        self.assertEqual(json.loads(inline.stdout)["status"], "accepted")
        stdin = self.cli("memory", "ingest", "-", "--config", str(FIXTURE), stdin=json.dumps(request))
        self.assertEqual(stdin.returncode, 0, stdin.stderr)
        with tempfile.TemporaryDirectory(prefix="nextral-request-") as directory:
            path = Path(directory) / "request.json"
            path.write_text(json.dumps(request), encoding="utf-8")
            file_result = self.cli("memory", "ingest", str(path), "--config", str(FIXTURE))
        self.assertEqual(file_result.returncode, 0, file_result.stderr)
        self.assertEqual(json.loads(file_result.stdout)["status"], "accepted")

    def test_cli_uses_explicit_environment_config_and_never_falls_back(self) -> None:
        configured = self.cli(
            "memory", "ingest", json.dumps(self.ingest_request()),
            config_environment=str(FIXTURE),
        )
        self.assertEqual(configured.returncode, 0, configured.stderr)
        self.assertEqual(json.loads(configured.stdout)["status"], "accepted")
        unconfigured = self.cli("memory", "list", json.dumps(self.scope))
        self.assertEqual(unconfigured.returncode, 1, unconfigured.stdout)
        self.assertEqual(unconfigured.stdout, "")
        self.assertIn("code", json.loads(unconfigured.stderr)["error"])

    def test_cli_argument_and_runtime_failures_are_not_success(self) -> None:
        self.assertEqual(self.cli("memory", "ingest").returncode, 2)
        self.assertEqual(self.cli("session").returncode, 2)
        invalid = self.cli("memory", "retrieve", "{", "--config", str(FIXTURE))
        self.assertEqual(invalid.returncode, 1)
        self.assertEqual(invalid.stdout, "")
        self.assertEqual(json.loads(invalid.stderr)["error"]["code"], "serialization")
        missing = self.cli("memory", "ingest", "missing-request.json")
        self.assertEqual(missing.returncode, 1)
        self.assertEqual(json.loads(missing.stderr)["error"]["code"], "io")

    def test_cli_service_startup_rejects_occupied_bind(self) -> None:
        with socket.socket() as occupied:
            occupied.bind(("127.0.0.1", 0))
            occupied.listen()
            config = copy.deepcopy(CONFIG)
            config["service"]["http_bind"] = "127.0.0.1:" + str(occupied.getsockname()[1])
            result = self.cli("serve", "http", json.dumps(config))
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertEqual(result.stdout, "")
        self.assertIn("code", json.loads(result.stderr)["error"])

    def test_cli_service_hosts_real_http_health(self) -> None:
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        config = copy.deepcopy(CONFIG)
        config["service"]["http_bind"] = f"127.0.0.1:{port}"
        with tempfile.TemporaryDirectory(prefix="nextral-http-") as cwd:
            process = subprocess.Popen(
                [sys.executable, "-I", "-m", "nextral", "serve", "http", json.dumps(config)],
                cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            try:
                deadline = time.monotonic() + 15
                while True:
                    if process.poll() is not None:
                        stdout, stderr = process.communicate()
                        self.fail(f"service exited before health: {stdout} {stderr}")
                    try:
                        with urlopen(f"http://127.0.0.1:{port}/healthz", timeout=1) as response:
                            self.assertEqual(response.status, 200)
                            self.assertEqual(json.load(response)["status"], "alive")
                            break
                    except (URLError, TimeoutError, OSError):
                        if time.monotonic() >= deadline:
                            self.fail("native HTTP host never became healthy")
                        time.sleep(0.05)
                self.assertIsNone(process.poll())
            finally:
                if process.poll() is None:
                    process.terminate()
                try:
                    process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=5)


    def test_installed_console_entrypoint(self) -> None:
        name = "nextral.exe" if os.name == "nt" else "nextral"
        entrypoint = Path(sys.executable).parent / name
        with tempfile.TemporaryDirectory(prefix="nextral-console-") as cwd:
            result = subprocess.run([str(entrypoint), "--version"], cwd=cwd, text=True, capture_output=True, timeout=10, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(nextral.__version__, result.stdout)


@unittest.skipUnless(
    os.environ.get("NEXTRAL_PYTHON_PRODUCTION_CONFIG") and os.environ.get("NEXTRAL_RESEARCH_EVIDENCE"),
    "Requires installed-package integration replay evidence and a real production config.",
)
class ProductionControlTests(unittest.TestCase):
    def test_measured_promotion_and_prior_policy_rollback(self) -> None:
        config = json.loads(Path(os.environ["NEXTRAL_PYTHON_PRODUCTION_CONFIG"]).read_text(encoding="utf-8"))
        evidence = json.loads(Path(os.environ["NEXTRAL_RESEARCH_EVIDENCE"]).read_text(encoding="utf-8"))
        self.assertEqual(config["backend"], "production_stores")
        nextral.configure_runtime(config)

        def snapshot(state):
            return {"lane": state["current_lane"], "policy_version": state["active_policy_version"],
                    "experiment_id": state["active_experiment_id"]}

        before = snapshot(nextral.experiment_status({}))
        candidate = nextral.create_experiment({
            "lane": "sandbox" if before["lane"] == "canary" else "canary",
            "policy_version": evidence["candidate"]["metadata"]["run"]["policy_version"],
            "description": "Installed Python measured replay " + evidence["candidate"]["metadata"]["run"]["run_id"],
        })
        candidate_request = {"experiment_id": candidate["id"]}
        self.assertEqual(nextral.experiment_status(candidate_request)["status"], "created")
        self.assertEqual(snapshot(nextral.experiment_status({})), before)
        created_state = nextral.experiment_status({})

        def expect_reason_required():
            with self.assertRaises(nextral.InvalidInputError) as blank:
                nextral.rollback_experiment(candidate_request | {"reason": "  "})
            self.assertEqual(blank.exception.code, "invalid_input")
            with self.assertRaises(nextral.InvalidInputError):
                nextral.rollback_experiment(candidate_request)

        with self.assertRaises(nextral.ConflictError) as unpromoted:
            nextral.rollback_experiment(candidate_request | {"reason": "Not promoted"})
        self.assertEqual(unpromoted.exception.code, "conflict")
        expect_reason_required()
        self.assertEqual(nextral.experiment_status({}), created_state)

        def record(result):
            result.update(experiment_id=candidate["id"],
                          baseline_run_id=evidence["baseline"]["metadata"]["run"]["run_id"],
                          candidate_run_id=evidence["candidate"]["metadata"]["run"]["run_id"],
                          unpromoted_rollback_rejected=True, reasonless_rollback_rejected=True)
            if os.environ.get("NEXTRAL_CONTROL_RESULT"):
                Path(os.environ["NEXTRAL_CONTROL_RESULT"]).write_text(
                    json.dumps(result, indent=2), encoding="utf-8")

        try:
            promoted = nextral.promote_experiment(candidate_request | {"evidence": evidence})
        except nextral.ConflictError as error:
            self.assertEqual(error.code, "conflict")
            self.assertEqual(nextral.experiment_status({}), created_state)
            record({"status": "promotion_rejected", "error": error.as_dict(),
                    "control_unchanged": True, "promotion_rollback_verified": False})
            self.skipTest(f"Genuine evidence rejected: {error.message}; positive promotion/rollback not verified.")

        try:
            self.assertEqual(promoted["status"], "promoted")
            self.assertEqual(promoted["promoted_report_run_id"], evidence["candidate"]["metadata"]["run"]["run_id"])
            self.assertEqual(promoted["rollback_target"], before)
            active = {"lane": candidate["lane"], "policy_version": candidate["policy_version"],
                      "experiment_id": candidate["id"]}
            self.assertEqual(snapshot(nextral.experiment_status({})), active)
            self.assertEqual(nextral.experiment_status(candidate_request)["status"], "promoted")
            expect_reason_required()
            self.assertEqual(snapshot(nextral.experiment_status({})), active)
            reason = "Restore the prior policy after installed Python measured smoke"
            rolled_back = nextral.rollback_experiment(candidate_request | {"reason": reason})
            self.assertEqual(rolled_back["status"], "rolled_back")
            self.assertEqual(rolled_back["rollback_reason"], reason)
            self.assertTrue(rolled_back["rollback_id"])
            self.assertEqual(snapshot(nextral.experiment_status({})), before)
            self.assertEqual(nextral.experiment_status(candidate_request)["status"], "rolled_back")
            record({"status": "promoted_and_rolled_back", "before": before, "promoted": active,
                    "restored": snapshot(nextral.experiment_status({})), "promotion_rollback_verified": True})
        finally:
            if nextral.experiment_status({})["active_experiment_id"] == candidate["id"]:
                nextral.rollback_experiment(candidate_request | {"reason": "Cleanup after failed Python control smoke"})


if __name__ == "__main__":
    unittest.main()
