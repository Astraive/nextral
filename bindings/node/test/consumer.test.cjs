'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const { randomUUID } = require('node:crypto');
const { readFileSync, writeFileSync } = require('node:fs');
const nextral = require(process.env.NEXTRAL_PACKAGE || '..');

const policy = { min_importance_score: 0, min_confidence_score: 0 };
const weights = { semantic_similarity: 1, recency: 0, importance: 0, access: 0 };
const testConfig = {
  backend: 'test_memory', stores: null,
  embedding: { kind: 'test', model: 'consumer-test', dimension: 3 },
  extraction: { kind: 'test', model: 'consumer-test' },
  ingestion_policy: policy,
  retrieval_policy: { privacy_scope: ['private'], token_budget: 1800, top_k_vector: 12, max_graph_hops: 2, scoring_weights: weights },
  cache: { key_prefix: 'node-consumer', session_ttl_seconds: 7200, retrieval_ttl_seconds: 120, policy_ttl_seconds: 300 },
  service: {}, auth: {}, observability: { enabled: false },
};
function scope() { return { tenant_id: `node-${randomUUID()}`, user_id: 'consumer' }; }
function memory(owner, content, overrides = {}) {
  return { ...owner, id: randomUUID(), content, content_type: 'fact', memory_type: 'semantic',
    source_type: 'manual', source_message_ids: [], importance_score: 1, confidence_score: 1,
    entities: [], tags: [], privacy_level: 'private', graph_hints: [], policy, ...overrides };
}
function query(owner, query_text, overrides = {}) {
  return { ...owner, query_text, entities: [], token_budget: 1800, privacy_scope: ['private'],
    top_k_vector: 12, max_graph_hops: 2, scoring_weights: weights, ...overrides };
}
function forget(owner, memory_id) {
  return nextral.forgetMemory({ ...owner, memory_id, actor: 'consumer', reason: 'consumer cleanup', redact: true });
}

test('installed consumer ingests, retrieves, isolates tenants, updates and redacts', () => {
  assert.deepEqual(nextral.configureRuntime(testConfig), { backend: 'test_memory', status: 'configured' });
  const owner = scope();
  const input = memory(owner, 'Atlas uses PostgreSQL for durable memory', { entities: ['Atlas'], tags: ['architecture'], embedding: [0.25, 0.5, 0.75] });
  const original = structuredClone(input);
  const accepted = nextral.ingestMemory(input);
  assert.equal(accepted.status, 'accepted');
  assert.equal(accepted.record_id, input.id);
  assert.deepEqual(input, original);
  assert.ok(nextral.retrieve(query(owner, 'Atlas PostgreSQL')).items.some(item => item.memory_id === input.id));
  assert.equal(nextral.retrieve(query(scope(), 'Atlas PostgreSQL')).items.length, 0);
  assert.equal(nextral.searchByTag({ ...owner, tag: 'architecture' })[0].id, input.id);
  assert.equal(nextral.searchByEntity({ ...owner, entity: 'Atlas' })[0].id, input.id);
  assert.equal(nextral.updateMemory({ ...owner, memory_id: input.id, add_tags: ['confirmed'] }).tags.includes('confirmed'), true);
  assert.equal(nextral.memoryStats(owner).active_count, 1);
  forget(owner, input.id);
  assert.equal(nextral.retrieve(query(owner, 'Atlas PostgreSQL')).items.length, 0);
  const redacted = nextral.listMemories({ ...owner, include_inactive: true }).find(item => item.id === input.id);
  assert.equal(redacted.status, 'redacted');
  assert.equal(redacted.content, '[redacted]');
});

test('native validation has machine-readable errors and policy rejects low confidence', () => {
  nextral.configureRuntime(testConfig);
  assert.throws(() => nextral.configureRuntime({ ...testConfig, backend: 'unsupported' }),
    error => error instanceof nextral.NextralError && error.code === 'invalid_input');
  assert.throws(() => nextral.retrieve(query({ tenant_id: '', user_id: 'consumer' }, 'Atlas')),
    error => error instanceof nextral.NextralError && error.code === 'invalid_input');
  assert.throws(() => nextral.ingestMemory(memory(scope(), 'invalid vector', { embedding: [NaN, 0, 1] })),
    error => error instanceof nextral.NextralError && error.code === 'invalid_input');
  nextral.configureRuntime({ ...testConfig, ingestion_policy: { min_importance_score: 0, min_confidence_score: 0.8 } });
  const rejected = nextral.ingestMemory(memory(scope(), 'uncertain claim', {
    confidence_score: 0.1, policy: { min_importance_score: 0, min_confidence_score: 0.8 },
  }));
  assert.equal(rejected.status, 'rejected');
  assert.equal(rejected.record_id, null);
});

test('session and batch operations expose real stored state', () => {
  nextral.configureRuntime(testConfig);
  const owner = scope();
  const session_id = randomUUID();
  const message = { ...owner, session_id, role: 'user', content: 'Keep the PostgreSQL choice', idempotency_key: randomUUID(), privacy_level: 'private' };
  const first = nextral.appendSessionMessage(message);
  assert.equal(nextral.appendSessionMessage(message).message_id, first.message_id);
  const context = nextral.workingContext(query(owner, 'PostgreSQL', { session_id }));
  assert.ok(context.session_tail.some(item => item.id === first.message_id && item.content === message.content));
  const items = [memory(owner, 'Aurora is the project codename'), memory(owner, 'Aurora deployment uses PostgreSQL')];
  const ingested = nextral.batchIngest({ items, idempotency_key: randomUUID() });
  assert.equal(ingested.succeeded, 2);
  assert.equal(ingested.failed, 0);
  const retrieved = nextral.batchRetrieve({ queries: [query(owner, 'Aurora')], idempotency_key: randomUUID() });
  assert.ok(retrieved.results[0].items.length > 0);
  const forgotten = nextral.batchForget({ items: items.map(item => ({ ...owner, memory_id: item.id, actor: 'consumer', reason: 'cleanup', redact: true })), idempotency_key: randomUUID() });
  assert.equal(forgotten.succeeded, 2);
  assert.equal(nextral.listMemories(owner).length, 0);
});


test('production host vectors determine retrieval independently of lexical content', {
  skip: !process.env.NEXTRAL_NODE_PRODUCTION_CONFIG && 'Set NEXTRAL_NODE_PRODUCTION_CONFIG to a real production config file.',
}, () => {
  const config = JSON.parse(readFileSync(process.env.NEXTRAL_NODE_PRODUCTION_CONFIG, 'utf8'));
  assert.equal(config.backend, 'production_stores');
  assert.ok(config.embedding.dimension >= 2);
  nextral.configureRuntime(config);
  const owner = scope();
  const vector = Array(config.embedding.dimension).fill(0);
  const other = vector.slice();
  vector[0] = 1; other[1] = 1;
  const target = memory(owner, 'The oceans contain salt', { embedding: vector });
  const distractor = memory(owner, 'The compiler emits machine code', { embedding: other });
  try {
    assert.equal(nextral.ingestMemory(target).status, 'accepted');
    assert.equal(nextral.ingestMemory(distractor).status, 'accepted');
    const request = query(owner, distractor.content, { query_vector: vector, max_graph_hops: 0, top_k_vector: 2 });
    const original = structuredClone(request);
    const response = nextral.retrieve(request);
    assert.equal(response.items[0].memory_id, target.id);
    assert.deepEqual(request, original);
    assert.throws(() => nextral.ingestMemory(memory(owner, 'wrong dimensions', { embedding: [1] })),
      error => error instanceof nextral.NextralError && error.code === 'invalid_input');
  } finally {
    for (const item of [target, distractor]) {
      if (nextral.listMemories(owner).some(record => record.id === item.id)) forget(owner, item.id);
    }
  }
});

test('host consolidation preserves source provenance and a durable summary', () => {
  nextral.configureRuntime(testConfig);
  const owner = scope();
  const session_id = randomUUID();
  const message = nextral.appendSessionMessage({ ...owner, session_id, role: 'user',
    content: 'Choose PostgreSQL for Atlas', idempotency_key: randomUUID() });
  const candidate = memory(owner, 'Atlas uses PostgreSQL', {
    source_message_ids: [message.message_id], embedding: [1, 0, 0],
    graph_hints: [{ from_label: 'Project', from_name: 'Atlas', relationship_type: 'USES',
      to_label: 'Technology', to_name: 'PostgreSQL', confidence: 1 }],
    entities: ['Atlas', 'PostgreSQL'],
  });
  const request = { ...owner, session_id, lane: 'fast', policy, version: randomUUID(),
    summary: 'The host extracted the Atlas database choice.', candidates: [candidate] };
  const consolidated = nextral.consolidate(request);
  assert.equal(consolidated.job.status, 'completed');
  assert.equal(consolidated.accepted.length, 1);
  assert.equal(nextral.consolidate(request).job.id, consolidated.job.id);
  const context = nextral.workingContext(query(owner, 'Atlas', { session_id, entities: ['Atlas'] }));
  assert.equal(context.session_summary.summary, request.summary);
  const record = nextral.listMemories(owner).find(item => item.id === consolidated.accepted[0].record_id);
  assert.deepEqual(record.source_message_ids, [message.message_id]);
  assert.throws(() => nextral.graphify({ ...owner, memory_id: record.id, record }),
    error => error instanceof nextral.NextralError && error.code === 'invalid_input');
  const graph = nextral.graphify({ ...owner, memory_id: record.id, hints: candidate.graph_hints });
  assert.equal(graph.status, 'accepted');
  assert.deepEqual(nextral.getMemory({ ...owner, memory_id: record.id }), record);
  assert.ok(nextral.queryGraph(query(owner, 'Atlas', { entities: ['Atlas'], top_k_vector: 0, max_graph_hops: 10 }))
    .items.some(item => item.memory_id === record.id));
  const reminder = nextral.scheduleReminder({ ...owner, source_memory_id: record.id, kind: 'task',
    title: 'Review the database choice', due_at: '1900000000', timezone: 'UTC' });
  assert.equal(reminder.reminder.source_memory_id, record.id);
  forget(owner, record.id);
  const afterForget = nextral.workingContext(query(owner, 'Atlas', { session_id }));
  assert.equal(afterForget.session_summary, null);
  assert.ok(!afterForget.session_tail.some(item => item.id === message.message_id));
});

test('durable retention jobs can be inspected and execute actual deletion', () => {
  nextral.configureRuntime(testConfig);
  const owner = scope();
  const item = memory(owner, 'Temporary deployment credential policy');
  nextral.ingestMemory(item);
  assert.equal(nextral.getMemory({ ...owner, memory_id: item.id }).content, item.content);
  const request = { ...owner, session_id: randomUUID(), trigger: 'retention',
    run_at: '0', memory_ids: [item.id], redact: true, max_attempts: 2 };
  const queued = nextral.enqueueJob(request);
  assert.equal(queued.status, 'queued');
  assert.equal(nextral.getJob({ ...owner, job_id: queued.id }).id, queued.id);
  assert.ok(nextral.listJobs(owner).some(job => job.id === queued.id));
  const executed = nextral.runJobs({ ...owner, now: String(Math.floor(Date.now() / 1000)), limit: 10, lease_seconds: 30 });
  assert.equal(executed.find(job => job.id === queued.id).status, 'completed');
  assert.equal(nextral.getMemory({ ...owner, memory_id: item.id }).status, 'redacted');
  assert.equal(nextral.listMemories(owner).length, 0);
  assert.equal(nextral.runJobs({ ...owner, now: String(Math.floor(Date.now() / 1000)) }).length, 0);
});

test('serving bridge exposes a real HTTP listener and graceful shutdown', { timeout: 30000 }, async () => {
  const { createServer } = require('node:net');
  const reservation = createServer();
  await new Promise((resolve, reject) => {
    reservation.once('error', reject);
    reservation.listen(0, '127.0.0.1', resolve);
  });
  const port = reservation.address().port;
  await new Promise((resolve, reject) => reservation.close(error => error ? reject(error) : resolve()));
  const serving = nextral.serve({ ...testConfig, service: { http_bind: `127.0.0.1:${port}` } }, ['http']);
  let startupError;
  void serving.catch(error => { startupError = error; });
  try {
    let health;
    const deadline = Date.now() + 15000;
    while (Date.now() < deadline && !health) {
      if (startupError) throw startupError;
      try {
        const response = await fetch(`http://127.0.0.1:${port}/healthz`, { signal: AbortSignal.timeout(1000) });
        if (response.ok) health = await response.json();
      } catch { /* Wait only for this native listener's startup. */ }
      if (!health) await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert.deepEqual(health, { status: 'alive' });
  } finally {
    nextral.stopServing();
    const stopped = await serving;
    assert.equal(stopped.status, 'stopped');
    assert.ok(stopped.binds.includes(`http://127.0.0.1:${port}`));
  }
});

test('procedural policies change real working context and remove cleanly', () => {
  nextral.configureRuntime(testConfig);
  const owner = scope();
  const upserted = nextral.upsertPolicy({ ...owner, name: 'Citations', body: 'Always cite primary sources.',
    privacy_level: 'private', enabled: true });
  assert.equal(upserted.policy.body, 'Always cite primary sources.');
  assert.ok(nextral.listPolicies(owner).some(policy => policy.id === upserted.policy.id));
  assert.ok(nextral.workingContext(query(owner, 'instructions')).procedural_policies.some(policy => policy.id === upserted.policy.id));
  assert.equal(nextral.listPolicies(scope()).length, 0);
  const removed = nextral.removePolicy({ ...owner, policy_id: upserted.policy.id });
  assert.equal(removed.status, 'removed');
  assert.equal(nextral.listPolicies(owner).length, 0);
  assert.ok(!nextral.workingContext(query(owner, 'instructions')).procedural_policies.some(policy => policy.id === upserted.policy.id));
});

test('forgetting short IDs preserves unrelated policies in both owner scopes', () => {
  nextral.configureRuntime(testConfig);
  const owner = scope();
  const peer = { ...owner, user_id: 'peer' };
  const item = memory(owner, 'A temporary fact', { id: 'm' });
  nextral.ingestMemory(item);
  const ownPolicy = nextral.upsertPolicy({ ...owner, name: 'Format', body: 'Use memory citations.',
    privacy_level: 'private' }).policy;
  const peerPolicy = nextral.upsertPolicy({ ...peer, name: 'Format', body: 'Keep my instructions.',
    privacy_level: 'private' }).policy;
  forget(owner, item.id);
  assert.ok(nextral.listPolicies(owner).some(policy => policy.id === ownPolicy.id));
  assert.ok(nextral.listPolicies(peer).some(policy => policy.id === peerPolicy.id));
});

test('batch read keys fence payloads without retaining stale private content', () => {
  nextral.configureRuntime(testConfig);
  const owner = scope();
  const item = memory(owner, 'Atlas uses PostgreSQL');
  nextral.ingestMemory(item);
  const request = { queries: [query(owner, 'Atlas')], idempotency_key: randomUUID() };
  assert.ok(nextral.batchRetrieve(request).results[0].items.some(result => result.memory_id === item.id));
  nextral.updateMemory({ ...owner, memory_id: item.id, content: 'Atlas uses SQLite' });
  const refreshed = nextral.batchRetrieve(request).results[0].items.find(result => result.memory_id === item.id);
  assert.equal(refreshed.content, 'Atlas uses SQLite');
  forget(owner, item.id);
  assert.deepEqual(nextral.batchRetrieve(request).results[0].items, []);
  assert.throws(() => nextral.batchRetrieve({ ...request, queries: [query(owner, 'different query')] }),
    error => error instanceof nextral.NextralError && error.code === 'conflict');
});

test('measured real-store evidence controls promotion and restores prior policy on rollback', {
  skip: (!process.env.NEXTRAL_NODE_PRODUCTION_CONFIG || !process.env.NEXTRAL_RESEARCH_EVIDENCE)
    && 'Requires installed-package integration replay evidence and a real production config.',
}, t => {
  const config = JSON.parse(readFileSync(process.env.NEXTRAL_NODE_PRODUCTION_CONFIG, 'utf8'));
  const evidence = JSON.parse(readFileSync(process.env.NEXTRAL_RESEARCH_EVIDENCE, 'utf8'));
  assert.equal(config.backend, 'production_stores');
  nextral.configureRuntime(config);
  const snapshot = state => ({
    lane: state.current_lane, policy_version: state.active_policy_version,
    experiment_id: state.active_experiment_id,
  });
  const before = snapshot(nextral.experimentStatus());
  const candidate = nextral.createExperiment({
    lane: before.lane === 'canary' ? 'sandbox' : 'canary',
    policy_version: evidence.candidate.metadata.run.policy_version,
    description: `Installed Node measured replay ${evidence.candidate.metadata.run.run_id}`,
  });
  assert.equal(nextral.experimentStatus({ experiment_id: candidate.id }).status, 'created');
  assert.deepEqual(snapshot(nextral.experimentStatus()), before);
  const createdState = nextral.experimentStatus();
  const expectReasonRequired = () => {
    assert.throws(() => nextral.rollbackExperiment({ experiment_id: candidate.id, reason: '  ' }),
      error => error instanceof nextral.NextralError && error.code === 'invalid_input');
    assert.throws(() => nextral.rollbackExperiment({ experiment_id: candidate.id }),
      error => error instanceof nextral.NextralError && error.code === 'invalid_input');
  };
  assert.throws(() => nextral.rollbackExperiment({ experiment_id: candidate.id, reason: 'Not promoted' }),
    error => error instanceof nextral.NextralError && error.code === 'conflict');
  expectReasonRequired();
  assert.deepEqual(nextral.experimentStatus(), createdState);
  const record = result => {
    if (process.env.NEXTRAL_CONTROL_RESULT) {
      writeFileSync(process.env.NEXTRAL_CONTROL_RESULT, JSON.stringify({
        ...result, experiment_id: candidate.id,
        baseline_run_id: evidence.baseline.metadata.run.run_id,
        candidate_run_id: evidence.candidate.metadata.run.run_id,
        unpromoted_rollback_rejected: true, reasonless_rollback_rejected: true,
      }, null, 2));
    }
  };
  let promoted;
  try {
    promoted = nextral.promoteExperiment({ experiment_id: candidate.id, evidence });
  } catch (error) {
    assert.ok(error instanceof nextral.NextralError);
    assert.equal(error.code, 'conflict');
    assert.deepEqual(nextral.experimentStatus(), createdState);
    record({ status: 'promotion_rejected', error: { code: error.code, message: error.message },
      control_unchanged: true, promotion_rollback_verified: false });
    t.skip(`Genuine evidence rejected: ${error.message}; positive promotion/rollback not verified.`);
    return;
  }
  try {
    assert.equal(promoted.status, 'promoted');
    assert.equal(promoted.promoted_report_run_id, evidence.candidate.metadata.run.run_id);
    assert.deepEqual(promoted.rollback_target, before);
    const active = { lane: candidate.lane, policy_version: candidate.policy_version, experiment_id: candidate.id };
    assert.deepEqual(snapshot(nextral.experimentStatus()), active);
    assert.equal(nextral.experimentStatus({ experiment_id: candidate.id }).status, 'promoted');
    expectReasonRequired();
    assert.deepEqual(snapshot(nextral.experimentStatus()), active);
    const reason = 'Restore the prior policy after installed Node measured smoke';
    const rolledBack = nextral.rollbackExperiment({ experiment_id: candidate.id, reason });
    assert.equal(rolledBack.status, 'rolled_back');
    assert.equal(rolledBack.rollback_reason, reason);
    assert.ok(rolledBack.rollback_id);
    assert.deepEqual(snapshot(nextral.experimentStatus()), before);
    assert.equal(nextral.experimentStatus({ experiment_id: candidate.id }).status, 'rolled_back');
    record({ status: 'promoted_and_rolled_back', before, promoted: active,
      restored: snapshot(nextral.experimentStatus()), promotion_rollback_verified: true });
  } finally {
    if (nextral.experimentStatus().active_experiment_id === candidate.id) {
      nextral.rollbackExperiment({ experiment_id: candidate.id, reason: 'Cleanup after failed Node control smoke' });
    }
  }
});
