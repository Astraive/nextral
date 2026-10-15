#!/usr/bin/env python3
"""Exercise JWT authorization on built HTTP/gRPC/GraphQL against production stores.

Run with an installed Python environment containing grpcio and grpcio-tools,
Node (built-in crypto only), OpenSSL, a built nextral-api, and an already migrated
production_stores config. Store credentials come from the invoking environment.
The fixture uses a fresh local HTTPS certificate, configured through the named
JWKS CA environment setting; it never disables TLS verification. Scratch files
live below --scratch (D:/target by default) and are removed on exit. API process
output is discarded to avoid credential disclosure. No build or install occurs.
Migration is not exposed by these network contracts: this script proves rejection
of attempted remote migration, not successful remote migration administration.
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = Path(__file__).resolve().parents[1]
CA_ENV = "NEXTRAL_AUTH_SMOKE_JWKS_CA"
TOKEN_ENV = "NEXTRAL_AUTH_SMOKE_SERVICE_TOKEN"
SIGNER = r"""
const crypto = require('node:crypto');
const options = JSON.parse(process.argv[1]);
const enc = value => Buffer.from(JSON.stringify(value)).toString('base64url');
const keys = {};
const jwks = [];
const tokens = {};
for (const alg of ['RS256', 'ES256']) {
  const pair = alg === 'RS256'
    ? crypto.generateKeyPairSync('rsa', {modulusLength: 2048})
    : crypto.generateKeyPairSync('ec', {namedCurve: 'prime256v1'});
  const kid = options.run + '-' + alg;
  keys[alg] = pair.privateKey;
  jwks.push({...pair.publicKey.export({format: 'jwk'}), kid, alg, use: 'sig'});
  const claims = {iss: options.issuer, aud: options.audience,
    tenant_id: options.tenant_id, sub: options.user_id,
    iat: options.now - 10, exp: options.now + 3600};
  function sign(patch = {}, headerPatch = {}, privateKey = pair.privateKey) {
    const header = {alg, typ: 'JWT', kid, ...headerPatch};
    if (header.kid === null) delete header.kid;
    const input = enc(header) + '.' + enc({...claims, ...patch});
    return input + '.' + crypto.sign('sha256', Buffer.from(input),
      {key: privateKey, dsaEncoding: 'ieee-p1363'}).toString('base64url');
  }
  const other = alg === 'RS256'
    ? crypto.generateKeyPairSync('rsa', {modulusLength: 2048})
    : crypto.generateKeyPairSync('ec', {namedCurve: 'prime256v1'});
  tokens[alg] = {
    valid: sign(), expired: sign({exp: options.now - 3600}),
    wrong_issuer: sign({iss: options.issuer + '/wrong'}),
    wrong_audience: sign({aud: options.audience + '-wrong'}),
    bad_signature: sign({}, {}, other.privateKey),
    unknown_kid: sign({}, {kid: 'not-in-jwks'}), missing_kid: sign({}, {kid: null}),
    missing_tenant: sign({tenant_id: ''}), missing_subject: sign({sub: ''}),
    missing_expiry: sign({exp: null})
  };
}
const base = {iss: options.issuer, aud: options.audience,
  tenant_id: options.tenant_id, sub: options.user_id, exp: options.now + 3600};
const unsigned = enc({alg: 'none', kid: jwks[0].kid}) + '.' + enc(base) + '.';
const hsInput = enc({alg: 'HS256', kid: jwks[0].kid}) + '.' + enc(base);
const hs = hsInput + '.' + crypto.createHmac('sha256', 'fixture-not-a-key')
  .update(hsInput).digest('base64url');
const rsInput = enc({alg: 'RS512', kid: jwks[0].kid}) + '.' + enc(base);
const rs512 = rsInput + '.' + crypto.sign('sha512', Buffer.from(rsInput), keys.RS256)
  .toString('base64url');
process.stdout.write(JSON.stringify({jwks: {keys: jwks}, tokens,
  unsupported: {none: unsigned, HS256: hs, RS512: rs512}}));
"""


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def command(args, *, env, cwd, label):
    result = subprocess.run([str(arg) for arg in args], cwd=cwd, env=env,
                            capture_output=True, text=True, timeout=60)
    if result.returncode:
        # Do not echo child output: an error may contain credentials or tokens.
        raise RuntimeError(f"{label} failed (exit {result.returncode}); child output withheld")
    return result.stdout


def free_ports(names):
    # Reserve all candidates together so no two services receive the same port.
    with contextlib.ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in names]
        for listener in sockets:
            listener.bind(("127.0.0.1", 0))
        return dict(zip(names, (listener.getsockname()[1] for listener in sockets)))


def http(url, payload=None, token=None):
    headers = {"Accept": "application/json"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    if token is not None:
        headers["Authorization"] = "Bearer " + token
    request = urllib.request.Request(url, headers=headers,
                                    data=None if payload is None else json.dumps(payload).encode())
    try:
        response = urllib.request.urlopen(request, timeout=45)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read()
        try:
            body = json.loads(raw) if raw else None
        except ValueError:
            body = None
        return response.status, body, response.headers


class JwksFixture:
    def __init__(self, port, cert, key, jwks):
        self.requests = 0
        self.body = json.dumps(jwks).encode()
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path != "/jwks.json":
                    self.send_error(404)
                    return
                fixture.requests += 1
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(fixture.body)))
                self.end_headers()
                self.wfile.write(fixture.body)

            def log_message(self, *_args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert, key)
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def load_grpc(work):
    from grpc_tools import protoc
    import grpc
    from google.protobuf.json_format import MessageToDict, ParseDict

    generated = work / "grpc"
    generated.mkdir()
    result = protoc.main(["grpc_tools.protoc", "-I" + str(ROOT / "contracts/grpc"),
                          "--python_out=" + str(generated), "--grpc_python_out=" + str(generated),
                          str(ROOT / "contracts/grpc/nextral.proto")])
    require(result == 0, "gRPC client generation failed")
    sys.path.insert(0, str(generated))
    import nextral_pb2
    import nextral_pb2_grpc
    return grpc, ParseDict, MessageToDict, nextral_pb2, nextral_pb2_grpc


class Protocols:
    def __init__(self, ports, modules, vector):
        self.http = f"http://127.0.0.1:{ports['http']}"
        self.graphql = f"http://127.0.0.1:{ports['graphql']}/graphql"
        self.grpc, self.parse, self.to_dict, self.pb, stub = modules
        self.channel = self.grpc.insecure_channel(f"127.0.0.1:{ports['grpc']}")
        self.stub = stub.MemoryServiceStub(self.channel)
        self.vector = vector
        self.evidence = []

    def record(self, protocol, case, outcome):
        self.evidence.append({"protocol": protocol, "case": case, "outcome": outcome})

    def gql(self, query, variables, token, *, error=None):
        status, response, _ = http(self.graphql, {"query": query, "variables": variables}, token)
        require(status == 200, f"GraphQL execution expected HTTP200, got HTTP{status}")
        require(isinstance(response, dict), "GraphQL returned a non-object")
        if error is not None:
            errors = response.get("errors", [])
            require(errors and all(item.get("extensions", {}).get("status") == error for item in errors),
                    f"GraphQL expected authorization status {error}")
            require(not response.get("data") or all(value is None for value in response["data"].values()),
                    "GraphQL error disclosed data")
            return response
        require(not response.get("errors"), "GraphQL returned execution errors")
        return response["data"]

    def rpc(self, method, payload, token, *, expected=None):
        message = getattr(self.pb, method + "MemoryRequest")()
        self.parse(payload, message)
        try:
            response = getattr(self.stub, method)(message, timeout=45,
                metadata=[] if token is None else [("authorization", "Bearer " + token)])
        except self.grpc.RpcError as error:
            require(expected is not None and error.code() == expected,
                    f"gRPC {method}: expected {expected}, got {error.code()}")
            return None
        require(expected is None, f"gRPC {method}: expected denial, got success")
        return self.to_dict(response, preserving_proto_field_name=True)

    def retrieval(self, scope):
        return dict(scope, query_text="auth fixture knowledge", privacy_scope=["private"],
                    token_budget=1800, top_k_vector=12, max_graph_hops=2,
                    query_vector=self.vector)

    def recall(self, protocol, scope, token, *, required=(), forbidden=(), denied=False):
        if protocol == "http":
            status, result, _ = http(self.http + "/v1/retrieval/query", self.retrieval(scope), token)
            require(status == (403 if denied else 200), f"HTTP scope/retrieval unexpected status {status}")
            if denied:
                return
        elif protocol == "grpc":
            result = self.rpc("Retrieve", self.retrieval(scope), token,
                              expected=self.grpc.StatusCode.PERMISSION_DENIED if denied else None)
            if denied:
                return
        else:
            query = """query($t:String!,$u:String!,$v:[Float!]) {
              retrieve(tenantId:$t,userId:$u,queryText:"auth fixture knowledge",
              privacyScope:["private"],tokenBudget:1800,topKVector:12,maxGraphHops:2,queryVector:$v)
              { status items { memoryId content } } }"""
            result = self.gql(query, {"t": scope["tenant_id"], "u": scope["user_id"], "v": self.vector},
                              token, error=403 if denied else None)
            if denied:
                return
            result = result["retrieve"]
        require(result.get("status") in ("ok", "degraded"), f"{protocol} retrieval unavailable")
        ids = {item.get("memory_id", item.get("memoryId")) for item in result.get("items", [])}
        require(set(required) <= ids, f"{protocol} did not recall owned records")
        require(not set(forbidden) & ids, f"{protocol} disclosed foreign record IDs")

    def invalid(self, scope, token, case):
        status, result, headers = http(self.http + "/v1/retrieval/query", self.retrieval(scope), token)
        require(status == 401 and result.get("error", {}).get("code") == "unauthenticated",
                f"HTTP accepted {case} or returned wrong denial")
        require(headers.get("WWW-Authenticate") == "Bearer", "HTTP401 lacks Bearer challenge")
        self.record("http", case, "401 unauthenticated")
        self.rpc("Retrieve", self.retrieval(scope), token, expected=self.grpc.StatusCode.UNAUTHENTICATED)
        self.record("grpc", case, "UNAUTHENTICATED")
        query = "query { __typename }"
        status, result, headers = http(self.graphql, {"query": query}, token)
        require(status == 401 and result.get("error", {}).get("code") == "unauthenticated",
                f"GraphQL accepted {case} or returned wrong denial")
        require(headers.get("WWW-Authenticate") == "Bearer", "GraphQL401 lacks Bearer challenge")
        self.record("graphql", case, "401 unauthenticated")

    def ingest(self, protocol, scope, token, label):
        payload = dict(scope, content="auth fixture knowledge " + label,
                       content_type="fact", memory_type="semantic", privacy_level="private",
                       importance_score=0.95, confidence_score=0.99, embedding=self.vector)
        if protocol == "http":
            status, result, _ = http(self.http + "/v1/memories", payload, token)
            require(status == 202, f"HTTP JWT ingest failed: status {status}")
            memory_id = result.get("memory_id")
        elif protocol == "grpc":
            result = self.rpc("Ingest", payload, token)
            memory_id = result.get("memory_id")
        else:
            query = """mutation($t:String!,$u:String!,$c:String!,$v:[Float!]) {
              ingestMemory(tenantId:$t,userId:$u,content:$c,contentType:"fact",memoryType:"semantic",
              privacyLevel:"private",importanceScore:0.95,confidenceScore:0.99,embedding:$v)
              { memoryId status validationErrors } }"""
            result = self.gql(query, {"t": scope["tenant_id"], "u": scope["user_id"],
                                     "c": payload["content"], "v": self.vector}, token)["ingestMemory"]
            memory_id = result.get("memoryId")
        require(memory_id and result.get("status") != "rejected", f"{protocol} ingest did not persist a record")
        self.record(protocol, label, "ingest accepted")
        return memory_id

    def forget(self, protocol, scope, memory_id, actor, token, *, denied=False):
        payload = dict(scope, memory_id=memory_id, actor_id=actor, reason="authentication fixture cleanup", redact=True)
        if protocol == "http":
            status, result, _ = http(self.http + f"/v1/memories/{memory_id}/forget", payload, token)
            require(status == (403 if denied else 202), f"HTTP forget unexpected status {status}")
            return result
        if protocol == "grpc":
            return self.rpc("Forget", payload, token,
                            expected=self.grpc.StatusCode.PERMISSION_DENIED if denied else None)
        query = """mutation($t:String!,$u:String!,$id:ID!,$a:String!) {
          forgetMemory(tenantId:$t,userId:$u,memoryId:$id,actorId:$a,
          reason:"authentication fixture cleanup",redact:true) { workflowId } }"""
        return self.gql(query, {"t": scope["tenant_id"], "u": scope["user_id"], "id": memory_id, "a": actor},
                        token, error=403 if denied else None)

    def migration_unexposed(self, token, label):
        for route in ("/v1/database/migrate", "/v1/tools/nextral.database.migrate"):
            status, _, _ = http(self.http + route, {}, token)
            require(status == 404, f"HTTP privileged migration unexpectedly exposed: {status}")
        self.record("http", label + " migration unavailable", "404 not_found")
        for method in ("/nextral.v1.DatabaseService/Migrate", "/nextral.v1.MemoryService/Migrate"):
            rpc = self.channel.unary_unary(method)
            try:
                rpc(b"", timeout=15, metadata=[("authorization", "Bearer " + token)])
            except self.grpc.RpcError as error:
                require(error.code() == self.grpc.StatusCode.UNIMPLEMENTED, "gRPC migration unexpectedly exposed")
            else:
                raise AssertionError("gRPC privileged migration unexpectedly succeeded")
        self.record("grpc", label + " migration unavailable", "UNIMPLEMENTED")
        status, result, _ = http(self.graphql, {"query": "mutation { databaseMigrate }"}, token)
        errors = result.get("errors", []) if isinstance(result, dict) else []
        require(status == 200 and errors and all("Unknown field" in item.get("message", "") for item in errors),
                "GraphQL privileged migration unexpectedly exposed")
        require(not result.get("data"), "GraphQL migration returned data")
        self.record("graphql", label + " migration unavailable", "unknown field")


def wait_ready(process, ports):
    deadline = time.monotonic() + 150
    while time.monotonic() < deadline:
        require(process.poll() is None, "API exited before readiness; server output withheld to avoid credential disclosure")
        try:
            if all(http(f"http://127.0.0.1:{ports[name]}/readyz")[0] == 200 for name in ("http", "graphql")):
                return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(1)
    raise RuntimeError("API production-store readiness timed out")


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def rejected_key_loading(api, config, env, work, protocols):
    for case in ("missing_ca_env", "malformed_ca_pem", "untrusted_local_ca", "insecure_jwks_url"):
        candidate = json.loads(json.dumps(config))
        candidate["service"] = {name + "_bind": "127.0.0.1:0" for name in ("http", "grpc", "graphql")}
        child_env = env.copy()
        if case == "missing_ca_env":
            child_env.pop(CA_ENV, None)
        elif case == "malformed_ca_pem":
            child_env[CA_ENV] = "not a PEM certificate"
        elif case == "untrusted_local_ca":
            candidate["auth"]["jwks_ca_pem_env"] = None
        else:
            candidate["auth"]["jwks_url"] = candidate["auth"]["jwks_url"].replace("https://", "http://", 1)
        path = work / (case + ".json")
        path.write_text(json.dumps(candidate), encoding="utf-8")
        child_env["NEXTRAL_CONFIG"] = str(path)
        try:
            result = subprocess.run([str(api), "serve", "all", str(path)], cwd=work, env=child_env,
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=25)
        except subprocess.TimeoutExpired:
            raise AssertionError(f"API did not reject JWKS loading case {case}") from None
        require(result.returncode != 0, f"API accepted JWKS loading case {case}")
        protocols.record("startup", case, "nonzero exit; no TLS bypass")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--api", type=Path, required=True, help="built nextral-api executable")
    parser.add_argument("--config", type=Path, required=True, help="already migrated production_stores JSON config")
    parser.add_argument("--scratch", type=Path, default=Path("D:/target"))
    parser.add_argument("--node", default="node")
    parser.add_argument("--openssl", default="openssl")
    parser.add_argument("--issuer", default="https://nextral-auth-fixture.invalid")
    parser.add_argument("--audience", default="nextral-auth-smoke")
    args = parser.parse_args()
    api = args.api.resolve()
    require(api.is_file(), "--api must name an existing built executable")
    config = json.loads(args.config.read_text(encoding="utf-8"))
    require(config.get("backend") == "production_stores", "auth evidence requires production_stores, not test memory")
    dimension = config.get("embedding", {}).get("dimension")
    require(isinstance(dimension, int) and dimension > 0, "config requires a positive embedding dimension")
    require("private" in config.get("retrieval_policy", {}).get("privacy_scope", []), "fixture requires private retrieval policy")
    node, openssl = shutil.which(args.node), shutil.which(args.openssl)
    require(node and openssl, "Node and OpenSSL must be available (or pass --node/--openssl)")
    args.scratch.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex
    scope = {"tenant_id": "auth-" + run_id, "user_id": "jwt-user"}
    peers = [dict(scope, user_id="foreign-user"), {"tenant_id": "foreign-" + run_id, "user_id": "jwt-user"}]
    env = os.environ.copy()
    env[TOKEN_ENV] = uuid.uuid4().hex + uuid.uuid4().hex
    with tempfile.TemporaryDirectory(prefix="nextral-auth-", dir=args.scratch) as directory:
        work = Path(directory).resolve()
        env.update(TMP=str(work), TEMP=str(work), TMPDIR=str(work))
        ports = free_ports(("http", "grpc", "graphql", "jwks"))
        fixture = json.loads(command([node, "-e", SIGNER, json.dumps(dict(scope, run=run_id,
            issuer=args.issuer, audience=args.audience, now=int(time.time())))], env=env, cwd=work, label="JWT signing"))
        cert, key = work / "jwks-cert.pem", work / "jwks-key.pem"
        ca, ca_key, csr = work / "ca.pem", work / "ca-key.pem", work / "jwks.csr"
        command([openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-sha256", "-days", "1",
                 "-keyout", ca_key, "-out", ca, "-subj", "/CN=Nextral auth fixture CA",
                 "-addext", "basicConstraints=critical,CA:TRUE",
                 "-addext", "keyUsage=critical,keyCertSign,cRLSign"],
                env=env, cwd=work, label="JWKS CA generation")
        command([openssl, "req", "-new", "-newkey", "rsa:2048", "-nodes", "-sha256",
                 "-keyout", key, "-out", csr, "-subj", "/CN=localhost"],
                env=env, cwd=work, label="JWKS certificate request")
        extensions = work / "jwks.ext"
        extensions.write_text("basicConstraints=critical,CA:FALSE\n"
                              "keyUsage=critical,digitalSignature,keyEncipherment\n"
                              "extendedKeyUsage=serverAuth\n"
                              "subjectAltName=DNS:localhost,IP:127.0.0.1\n", encoding="ascii")
        command([openssl, "x509", "-req", "-in", csr, "-CA", ca, "-CAkey", ca_key,
                 "-CAcreateserial", "-out", cert, "-days", "1", "-sha256", "-extfile", extensions],
                env=env, cwd=work, label="JWKS certificate signing")
        env[CA_ENV] = ca.read_text(encoding="ascii")
        config["auth"] = {"issuer": args.issuer, "audience": args.audience,
                          "jwks_url": f"https://127.0.0.1:{ports['jwks']}/jwks.json",
                          "jwks_ca_pem_env": CA_ENV, "service_token_env": TOKEN_ENV}
        config["service"] = {name + "_bind": f"127.0.0.1:{ports[name]}" for name in ("http", "grpc", "graphql")}
        config["cache"]["key_prefix"] = "nextral:auth:" + run_id
        config["observability"] = {"enabled": False, "otlp_endpoint": None, "prometheus_bind": None}
        config["rate_limit"] = {"enabled": False, "max_requests_per_second": 100, "burst_size": 200}
        config_path = work / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        env["NEXTRAL_CONFIG"] = str(config_path)
        modules = load_grpc(work)
        protocols = Protocols(ports, modules, [1.0] + [0.0] * (dimension - 1))
        with JwksFixture(ports["jwks"], cert, key, fixture["jwks"]) as jwks:
            with open(os.devnull, "w") as log:
                process = subprocess.Popen([str(api), "serve", "all", str(config_path)], cwd=work,
                                           env=env, stdout=log, stderr=log)
                records = []
                try:
                    wait_ready(process, ports)
                    modules[0].channel_ready_future(protocols.channel).result(timeout=30)
                    for alg, tokens in fixture["tokens"].items():
                        for protocol in ("http", "grpc", "graphql"):
                            memory_id = protocols.ingest(protocol, scope, tokens["valid"], alg + " valid JWT")
                            records.append((scope, memory_id))
                            protocols.recall(protocol, scope, tokens["valid"], required=[memory_id])
                            protocols.record(protocol, alg + " owned retrieval", "owned record recalled")
                        for case, token in tokens.items():
                            if case != "valid":
                                protocols.invalid(scope, token, alg + " " + case)
                    protocols.invalid(scope, None, "missing bearer")
                    protocols.invalid(scope, "not-a-jwt", "malformed bearer")
                    for alg, token in fixture["unsupported"].items():
                        protocols.invalid(scope, token, alg + " unsupported algorithm")
                    for index, peer in enumerate(peers):
                        foreign_id = protocols.ingest("http", peer, env[TOKEN_ENV], f"service cross-scope {index}")
                        records.append((peer, foreign_id))
                        for protocol in ("http", "grpc", "graphql"):
                            for alg, tokens in fixture["tokens"].items():
                                protocols.recall(protocol, peer, tokens["valid"], denied=True)
                                protocols.record(protocol, alg + f" scope override {index}", "permission denied")
                                protocols.recall(protocol, scope, tokens["valid"], forbidden=[foreign_id])
                                protocols.forget(protocol, peer, foreign_id, peer["user_id"], tokens["valid"], denied=True)
                                protocols.forget(protocol, scope, records[0][1], peer["user_id"] + "-forged", tokens["valid"], denied=True)
                                protocols.record(protocol, alg + f" foreign write and actor override {index}", "permission denied")
                            protocols.recall(protocol, peer, env[TOKEN_ENV], required=[foreign_id])
                            protocols.record(protocol, f"explicit service cross-scope {index}", "foreign scope recalled")
                        for alg, tokens in fixture["tokens"].items():
                            query = "query($t:String!,$u:String!,$id:ID!) { memory(tenantId:$t,userId:$u,id:$id) { id content } }"
                            hidden = protocols.gql(query, {"t": scope["tenant_id"], "u": scope["user_id"], "id": foreign_id}, tokens["valid"])
                            require(hidden["memory"] is None, "GraphQL direct foreign ID disclosed a record")
                            protocols.record("graphql", alg + f" foreign direct ID {index}", "null, no disclosure")
                        # Reference-by-ID write must not reveal a foreign record through error text.
                        # Compare the public denial with a nonexistent ID rather than accept a broad 5xx.
                        for protocol in ("http", "grpc", "graphql"):
                            token = fixture["tokens"]["RS256"]["valid"]
                            assert_hidden_reference(protocols, protocol, scope, foreign_id, token)
                    for alg, tokens in fixture["tokens"].items():
                        for protocol in ("http", "grpc", "graphql"):
                            memory_id = protocols.ingest(protocol, scope, tokens["valid"],
                                                         alg + " owned forget " + protocol)
                            records.append((scope, memory_id))
                            protocols.forget(protocol, scope, memory_id, scope["user_id"], tokens["valid"])
                            protocols.record(protocol, alg + " owned forget", "accepted with bound actor")
                        protocols.migration_unexposed(tokens["valid"], alg + " ordinary JWT")
                    protocols.migration_unexposed(env[TOKEN_ENV], "service token")
                    rejected_key_loading(api, config, env, work, protocols)
                    require(jwks.requests == 1, f"expected one startup JWKS fetch, observed {jwks.requests}")
                finally:
                    # Cleanup uses only the explicit service token, never a bypass or a user JWT.
                    try:
                        for owner, memory_id in records:
                            protocols.forget("http", owner, memory_id, owner["user_id"], env[TOKEN_ENV])
                    finally:
                        protocols.channel.close()
                        stop(process)
        print(json.dumps({"status": "ok", "backend": "production_stores", "algorithms": ["RS256", "ES256"],
                          "jwks": {"transport": "HTTPS with explicit fixture CA", "fetches": jwks.requests,
                                   "loading": "startup snapshot; no rotation/retry claim"},
                          "migration": "not exposed by HTTP, gRPC, or GraphQL, including for service token",
                          "checks": protocols.evidence}))


def assert_hidden_reference(protocols, protocol, scope, foreign_id, token):
    payload = lambda memory_id: dict(scope, memory_id=memory_id, actor_id=scope["user_id"],
                                    reason="foreign reference probe", redact=True)
    missing = "missing-" + uuid.uuid4().hex
    if protocol == "http":
        results = [http(protocols.http + f"/v1/memories/{item}/forget", payload(item), token)[:2]
                   for item in (foreign_id, missing)]
        require(all(status == 404 for status, _ in results), "HTTP foreign/nonexistent ID must both be 404")
        require(results[0][1] == results[1][1], "HTTP foreign ID existence leaked through error body")
    elif protocol == "grpc":
        errors = []
        for item in (foreign_id, missing):
            message = protocols.parse(payload(item), protocols.pb.ForgetMemoryRequest())
            try:
                protocols.stub.Forget(message, timeout=45, metadata=[("authorization", "Bearer " + token)])
            except protocols.grpc.RpcError as error:
                require(error.code() == protocols.grpc.StatusCode.NOT_FOUND, "gRPC foreign ID must be NOT_FOUND")
                errors.append(error.details())
            else:
                raise AssertionError("gRPC foreign/nonexistent ID forget succeeded")
        require(errors[0] == errors[1], "gRPC foreign ID existence leaked through error details")
    else:
        query = """mutation($t:String!,$u:String!,$id:ID!,$a:String!) {
          forgetMemory(tenantId:$t,userId:$u,memoryId:$id,actorId:$a,reason:"foreign reference probe",redact:true)
          { workflowId } }"""
        results = [protocols.gql(query, {"t": scope["tenant_id"], "u": scope["user_id"], "id": item,
                                       "a": scope["user_id"]}, token, error=404) for item in (foreign_id, missing)]
        require(results[0] == results[1], "GraphQL foreign ID existence leaked through error response")
    protocols.record(protocol, "foreign ID versus nonexistent ID", "identical not-found response")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # Exception strings are deliberately authored without request bodies or bearer values.
        print(json.dumps({"status": "failed", "error_type": type(error).__name__, "message": str(error)}), file=sys.stderr)
        sys.exit(1)
