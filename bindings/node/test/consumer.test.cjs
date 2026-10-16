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
async function forget(client, owner, memory_id) {
  return await client.forget({ ...owner, memory_id, actor: 'consumer', reason: 'consumer cleanup', redact: true });
}

test('installed consumer ingests, retrieves, isolates tenants, updates and redacts', async () => {
  const client = await nextral.Client.create(testConfig);
  const owner = scope();
  const input = memory(owner, 'Atlas uses PostgreSQL for durable memory', { entities: ['Atlas'], tags: ['architecture'], embedding: [0.25, 0.5, 0.75] });
  const original = structuredClone(input);
  const accepted = await client.ingest(input);
  assert.equal(accepted.status, 'accepted');
  assert.equal(accepted.record_id, input.id);
  assert.deepEqual(input, original);
  const ret1 = await client.retrieve(query(owner, 'Atlas PostgreSQL'));
  assert.ok(ret1.items.some(item => item.memory_id === input.id));
  const ret2 = await client.retrieve(query(scope(), 'Atlas PostgreSQL'));
  assert.equal(ret2.items.length, 0);
  const byTag = await client.searchByTag({ ...owner, tag: 'architecture' });
  assert.equal(byTag[0].id, input.id);
  const byEnt = await client.searchByEntity({ ...owner, entity: 'Atlas' });
  assert.equal(byEnt[0].id, input.id);
  const updated = await client.updateMemory({ ...owner, memory_id: input.id, add_tags: ['confirmed'] });
  assert.equal(updated.tags.includes('confirmed'), true);
  const stats = await client.memoryStats(owner);
  assert.equal(stats.active_count, 1);
  await forget(client, owner, input.id);
  const ret3 = await client.retrieve(query(owner, 'Atlas PostgreSQL'));
  assert.equal(ret3.items.length, 0);
  const memories = await client.listMemories({ ...owner, include_inactive: true });
  const redacted = memories.find(item => item.id === input.id);
  assert.equal(redacted.status, 'redacted');
  assert.equal(redacted.content, '[redacted]');
  await client.close();
});

test('native validation has machine-readable errors and policy rejects low confidence', async () => {
  await assert.rejects(async () => nextral.Client.create({ ...testConfig, backend: 'unsupported' }),
    error => error instanceof nextral.NextralError && error.code === 'invalid_input');
  const client = await nextral.Client.create(testConfig);
  await assert.rejects(async () => client.retrieve(query({ tenant_id: '', user_id: 'consumer' }, 'Atlas')),
    error => error instanceof nextral.NextralError && error.code === 'invalid_input');
  await assert.rejects(async () => client.ingest(memory(scope(), 'invalid vector', { embedding: [NaN, 0, 1] })),
    error => error instanceof nextral.NextralError && error.code === 'invalid_input');
  await client.close();
  const client2 = await nextral.Client.create({ ...testConfig, ingestion_policy: { min_importance_score: 0, min_confidence_score: 0.8 } });
  const rejected = await client2.ingest(memory(scope(), 'uncertain claim', {
    confidence_score: 0.1, policy: { min_importance_score: 0, min_confidence_score: 0.8 },
  }));
  assert.equal(rejected.status, 'rejected');
  assert.equal(rejected.record_id, null);
  await client2.close();
});

test('eligible sessions reflect native appends, counts, cutoff and scope', async () => {
  const client = await nextral.Client.create(testConfig);
  const owner = scope();
  for (const session_id of ['aged-a', 'aged-b']) for (let i = 0; i < 2; i++) {
    await client.appendSessionMessage({ ...owner, session_id, role: 'user', content: `message ${i}`,
      idempotency_key: `${session_id}-${i}` });
  }
  const request = { ...owner, inactive_at_or_before: '9999-12-31T23:59:59Z' };
  const rows = await client.eligibleSessions(request);
  assert.equal(rows.length, 2);
  assert.deepEqual(new Set(rows.map(row => row.session_id)), new Set(['aged-a', 'aged-b']));
  for (const row of rows) {
    assert.equal(row.message_count, 2);
    assert.ok(Number.isFinite(Date.parse(row.last_message_at)));
    assert.match(row.last_message_at, /(?:Z|\+00:00)$/);
  }
  assert.deepEqual(rows, [...rows].sort((a, b) =>
    Date.parse(a.last_message_at) - Date.parse(b.last_message_at) || a.session_id.localeCompare(b.session_id)));
  assert.deepEqual(await client.eligibleSessions({ ...request, limit: 1 }), rows.slice(0, 1));
  assert.deepEqual(await client.eligibleSessions({ ...request, user_id: 'other' }), []);
  assert.deepEqual(await client.eligibleSessions({ ...request, inactive_at_or_before: '1970-01-01T00:00:00Z' }), []);
  for (const limit of [0, 1001]) await assert.rejects(async () => client.eligibleSessions({ ...request, limit }),
    error => error.code === 'invalid_input');
  await client.close();
});

test('session and batch operations expose real stored state', async () => {
  const client = await nextral.Client.create(testConfig);
  const owner = scope();
  const session_id = randomUUID();
  const message = { ...owner, session_id, role: 'user', content: 'Keep the PostgreSQL choice', idempotency_key: randomUUID(), privacy_level: 'private' };
  const first = await client.appendSessionMessage(message);
  const second = await client.appendSessionMessage(message);
  assert.equal(second.message_id, first.message_id);
  const context = await client.workingContext(query(owner, 'PostgreSQL', { session_id }));
  assert.ok(context.session_tail.some(item => item.id === first.message_id && item.content === message.content));
  const items = [memory(owner, 'Aurora is the project codename'), memory(owner, 'Aurora deployment uses PostgreSQL')];
  const ingested = await client.batchIngest({ items, idempotency_key: randomUUID() });
  assert.equal(ingested.succeeded, 2);
  assert.equal(ingested.failed, 0);
  const retrieved = await client.batchRetrieve({ queries: [query(owner, 'Aurora')], idempotency_key: randomUUID() });
  assert.ok(retrieved.results[0].items.length > 0);
  const forgotten = await client.batchForget({ items: items.map(item => ({ ...owner, memory_id: item.id, actor: 'consumer', reason: 'cleanup', redact: true })), idempotency_key: randomUUID() });
  assert.equal(forgotten.succeeded, 2);
  const remaining = await client.listMemories(owner);
  assert.equal(remaining.length, 0);
  await client.close();
});

test('production host vectors determine retrieval independently of lexical content', {
  skip: !process.env.NEXTRAL_NODE_PRODUCTION_CONFIG && 'Set NEXTRAL_NODE_PRODUCTION_CONFIG to a real production config file.',
}, async () => {
  const config = JSON.parse(readFileSync(process.env.NEXTRAL_NODE_PRODUCTION_CONFIG, 'utf8'));
  assert.equal(config.backend, 'production_stores');
  assert.ok(config.embedding.dimension >= 2);
  const client = await nextral.Client.create(config);
  const owner = scope();
  const vector = Array(config.embedding.dimension).fill(0);
  const other = vector.slice();
  vector[0] = 1; other[1] = 1;
  const target = memory(owner, 'The oceans contain salt', { embedding: vector });
  const distractor = memory(owner, 'The compiler emits machine code', { embedding: other });
  try {
    assert.equal((await client.ingest(target)).status, 'accepted');
    assert.equal((await client.ingest(distractor)).status, 'accepted');
    const request = query(owner, distractor.content, { query_vector: vector, max_graph_hops: 0, top_k_vector: 2 });
    const original = structuredClone(request);
    const response = await client.retrieve(request);
    assert.equal(response.items[0].memory_id, target.id);
    assert.deepEqual(request, original);
    await assert.rejects(async () => client.ingest(memory(owner, 'wrong dimensions', { embedding: [1] })),
      error => error instanceof nextral.NextralError && error.code === 'invalid_input');
  } finally {
    const list = await client.listMemories(owner);
    for (const item of [target, distractor]) {
      if (list.some(record => record.id === item.id)) await forget(client, owner, item.id);
    }
    await client.close();
  }
});

test('host consolidation preserves source provenance and a durable summary', async () => {
  const client = await nextral.Client.create(testConfig);
  const owner = scope();
  const session_id = randomUUID();
  const message = await client.appendSessionMessage({ ...owner, session_id, role: 'user',
    content: 'Choose PostgreSQL for Atlas', idempotency_key: randomUUID() });
  const candidate = memory(owner, 'Atlas uses PostgreSQL', {
    source_message_ids: [message.message_id], embedding: [1, 0, 0],
    graph_hints: [{ from_label: 'Project', from_name: 'Atlas', relationship_type: 'USES',
      to_label: 'Technology', to_name: 'PostgreSQL', confidence: 1 }],
    entities: ['Atlas', 'PostgreSQL'],
  });
  const request = { ...owner, session_id, lane: 'fast', policy, version: randomUUID(),
    summary: 'The host extracted the Atlas database choice.', candidates: [candidate] };
  const consolidated = await client.consolidate(request);
  assert.equal(consolidated.job.status, 'completed');
  assert.equal(consolidated.accepted.length, 1);
  const consolidatedAgain = await client.consolidate(request);
  assert.equal(consolidatedAgain.job.id, consolidated.job.id);
  const context = await client.workingContext(query(owner, 'Atlas', { session_id, entities: ['Atlas'] }));
  assert.equal(context.session_summary.summary, request.summary);
  const memories = await client.listMemories(owner);
  const record = memories.find(item => item.id === consolidated.accepted[0].record_id);
  assert.deepEqual(record.source_message_ids, [message.message_id]);
  await assert.rejects(async () => client.graphify({ ...owner, memory_id: record.id, record }),
    error => error instanceof nextral.NextralError && error.code === 'invalid_input');
  const graph = await client.graphify({ ...owner, memory_id: record.id, hints: candidate.graph_hints });
  assert.equal(graph.status, 'accepted');
  const gotMemory = await client.getMemory({ ...owner, memory_id: record.id });
  assert.deepEqual(gotMemory, record);
  const qGraph = await client.queryGraph(query(owner, 'Atlas', { entities: ['Atlas'], top_k_vector: 0, max_graph_hops: 10 }));
  assert.ok(qGraph.items.some(item => item.memory_id === record.id));
  const reminder = await client.scheduleReminder({ ...owner, source_memory_id: record.id, kind: 'task',
    title: 'Review the database choice', due_at: '1900000000', timezone: 'UTC' });
  assert.equal(reminder.reminder.source_memory_id, record.id);
  await forget(client, owner, record.id);
  const afterForget = await client.workingContext(query(owner, 'Atlas', { session_id }));
  assert.equal(afterForget.session_summary, null);
  assert.ok(!afterForget.session_tail.some(item => item.id === message.message_id));
  await client.close();
});

test('durable retention jobs can be inspected and execute actual deletion', async () => {
  const client = await nextral.Client.create(testConfig);
  const owner = scope();
  const item = memory(owner, 'Temporary deployment credential policy');
  await client.ingest(item);
  const got = await client.getMemory({ ...owner, memory_id: item.id });
  assert.equal(got.content, item.content);
  const request = { ...owner, session_id: randomUUID(), trigger: 'retention',
    run_at: '0', memory_ids: [item.id], redact: true, max_attempts: 2 };
  const queued = await client.enqueueJob(request);
  assert.equal(queued.status, 'queued');
  const gotJob = await client.getJob({ ...owner, job_id: queued.id });
  assert.equal(gotJob.id, queued.id);
  const jobs = await client.listJobs(owner);
  assert.ok(jobs.some(job => job.id === queued.id));
  const executed = await client.runJobs({ ...owner, now: String(Math.floor(Date.now() / 1000)), limit: 10, lease_seconds: 30 });
  assert.equal(executed.find(job => job.id === queued.id).status, 'completed');
  const gotAfter = await client.getMemory({ ...owner, memory_id: item.id });
  assert.equal(gotAfter.status, 'redacted');
  const mems = await client.listMemories(owner);
  assert.equal(mems.length, 0);
  const ranAgain = await client.runJobs({ ...owner, now: String(Math.floor(Date.now() / 1000)) });
  assert.equal(ranAgain.length, 0);
  await client.close();
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
  const client = await nextral.Client.create({ ...testConfig, service: { http_bind: `127.0.0.1:${port}` } });
  const handle = await client.serve();
  assert.ok(handle.binds.some(b => b.includes(String(port))));
  try {
    let health;
    const deadline = Date.now() + 15000;
    while (Date.now() < deadline && !health) {
      try {
        const response = await fetch(`http://127.0.0.1:${port}/healthz`, { signal: AbortSignal.timeout(1000) });
        if (response.ok) health = await response.json();
      } catch { /* Wait only for this native listener's startup. */ }
      if (!health) await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert.deepEqual(health, { status: 'alive' });
  } finally {
    await handle.shutdown(5000);
    await client.close();
  }
});

test('procedural policies change real working context and remove cleanly', async () => {
  const client = await nextral.Client.create(testConfig);
  const owner = scope();
  const upserted = await client.upsertPolicy({ ...owner, name: 'Citations', body: 'Always cite primary sources.',
    privacy_level: 'private', enabled: true });
  assert.equal(upserted.policy.body, 'Always cite primary sources.');
  const policies = await client.listPolicies(owner);
  assert.ok(policies.some(policy => policy.id === upserted.policy.id));
  const context = await client.workingContext(query(owner, 'instructions'));
  assert.ok(context.procedural_policies.some(policy => policy.id === upserted.policy.id));
  const otherPolicies = await client.listPolicies(scope());
  assert.equal(otherPolicies.length, 0);
  const removed = await client.removePolicy({ ...owner, policy_id: upserted.policy.id });
  assert.equal(removed.status, 'removed');
  const afterPolicies = await client.listPolicies(owner);
  assert.equal(afterPolicies.length, 0);
  const afterContext = await client.workingContext(query(owner, 'instructions'));
  assert.ok(!afterContext.procedural_policies.some(policy => policy.id === upserted.policy.id));
  await client.close();
});

test('forgetting short IDs preserves unrelated policies in both owner scopes', async () => {
  const client = await nextral.Client.create(testConfig);
  const owner = scope();
  const peer = { ...owner, user_id: 'peer' };
  const item = memory(owner, 'A temporary fact', { id: 'm' });
  await client.ingest(item);
  const ownPolicy = (await client.upsertPolicy({ ...owner, name: 'Format', body: 'Use memory citations.',
    privacy_level: 'private' })).policy;
  const peerPolicy = (await client.upsertPolicy({ ...peer, name: 'Format', body: 'Keep my instructions.',
    privacy_level: 'private' })).policy;
  await forget(client, owner, item.id);
  const ownerPolicies = await client.listPolicies(owner);
  assert.ok(ownerPolicies.some(policy => policy.id === ownPolicy.id));
  const peerPolicies = await client.listPolicies(peer);
  assert.ok(peerPolicies.some(policy => policy.id === peerPolicy.id));
  await client.close();
});

test('batch read keys fence payloads without retaining stale private content', async () => {
  const client = await nextral.Client.create(testConfig);
  const owner = scope();
  const item = memory(owner, 'Atlas uses PostgreSQL');
  await client.ingest(item);
  const request = { queries: [query(owner, 'Atlas')], idempotency_key: randomUUID() };
  const ret1 = await client.batchRetrieve(request);
  assert.ok(ret1.results[0].items.some(result => result.memory_id === item.id));
  await client.updateMemory({ ...owner, memory_id: item.id, content: 'Atlas uses SQLite' });
  const ret2 = await client.batchRetrieve(request);
  const refreshed = ret2.results[0].items.find(result => result.memory_id === item.id);
  assert.equal(refreshed.content, 'Atlas uses SQLite');
  await forget(client, owner, item.id);
  const ret3 = await client.batchRetrieve(request);
  assert.deepEqual(ret3.results[0].items, []);
  await assert.rejects(async () => client.batchRetrieve({ ...request, queries: [query(owner, 'different query')] }),
    error => error instanceof nextral.NextralError && error.code === 'conflict');
  await client.close();
});

test('measured real-store evidence controls promotion and restores prior policy on rollback', {
  skip: (!process.env.NEXTRAL_NODE_PRODUCTION_CONFIG || !process.env.NEXTRAL_RESEARCH_EVIDENCE)
    && 'Requires installed-package integration replay evidence and a real production config.',
}, async t => {
  const config = JSON.parse(readFileSync(process.env.NEXTRAL_NODE_PRODUCTION_CONFIG, 'utf8'));
  const evidence = JSON.parse(readFileSync(process.env.NEXTRAL_RESEARCH_EVIDENCE, 'utf8'));
  assert.equal(config.backend, 'production_stores');
  const client = await nextral.Client.create(config);
  const snapshot = state => ({
    lane: state.current_lane, policy_version: state.active_policy_version,
    experiment_id: state.active_experiment_id,
  });
  const before = snapshot(await client.experimentStatus());
  const candidate = await client.createExperiment({
    lane: before.lane === 'canary' ? 'sandbox' : 'canary',
    policy_version: evidence.candidate.metadata.run.policy_version,
    description: `Installed Node measured replay ${evidence.candidate.metadata.run.run_id}`,
  });
  const candStatus = await client.experimentStatus({ experiment_id: candidate.id });
  assert.equal(candStatus.status, 'created');
  assert.deepEqual(snapshot(await client.experimentStatus()), before);
  const createdState = await client.experimentStatus();
  const expectReasonRequired = async () => {
    await assert.rejects(async () => client.rollbackExperiment({ experiment_id: candidate.id, reason: '  ' }),
      error => error instanceof nextral.NextralError && error.code === 'invalid_input');
    await assert.rejects(async () => client.rollbackExperiment({ experiment_id: candidate.id }),
      error => error instanceof nextral.NextralError && error.code === 'invalid_input');
  };
  await assert.rejects(async () => client.rollbackExperiment({ experiment_id: candidate.id, reason: 'Not promoted' }),
    error => error instanceof nextral.NextralError && error.code === 'conflict');
  await expectReasonRequired();
  assert.deepEqual(await client.experimentStatus(), createdState);
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
    promoted = await client.promoteExperiment({ experiment_id: candidate.id, evidence });
  } catch (error) {
    assert.ok(error instanceof nextral.NextralError);
    assert.equal(error.code, 'conflict');
    assert.deepEqual(await client.experimentStatus(), createdState);
    record({ status: 'promotion_rejected', error: { code: error.code, message: error.message },
      control_unchanged: true, promotion_rollback_verified: false });
    t.skip(`Genuine evidence rejected: ${error.message}; positive promotion/rollback not verified.`);
    await client.close();
    return;
  }
  try {
    assert.equal(promoted.status, 'promoted');
    assert.equal(promoted.promoted_report_run_id, evidence.candidate.metadata.run.run_id);
    assert.deepEqual(promoted.rollback_target, before);
    const active = { lane: candidate.lane, policy_version: candidate.policy_version, experiment_id: candidate.id };
    assert.deepEqual(snapshot(await client.experimentStatus()), active);
    const candStatus2 = await client.experimentStatus({ experiment_id: candidate.id });
    assert.equal(candStatus2.status, 'promoted');
    await expectReasonRequired();
    assert.deepEqual(snapshot(await client.experimentStatus()), active);
    const reason = 'Restore the prior policy after installed Node measured smoke';
    const rolledBack = await client.rollbackExperiment({ experiment_id: candidate.id, reason });
    assert.equal(rolledBack.status, 'rolled_back');
    assert.equal(rolledBack.rollback_reason, reason);
    assert.ok(rolledBack.rollback_id);
    assert.deepEqual(snapshot(await client.experimentStatus()), before);
    const candStatus3 = await client.experimentStatus({ experiment_id: candidate.id });
    assert.equal(candStatus3.status, 'rolled_back');
    record({ status: 'promoted_and_rolled_back', before, promoted: active,
      restored: snapshot(await client.experimentStatus()), promotion_rollback_verified: true });
  } finally {
    const curStatus = await client.experimentStatus();
    if (curStatus.active_experiment_id === candidate.id) {
      await client.rollbackExperiment({ experiment_id: candidate.id, reason: 'Cleanup after failed Node control smoke' });
    }
    await client.close();
  }
});

test('consumer queries memories with filters, keyset pagination, and repair', async () => {
  const client = await nextral.Client.create(testConfig);
  const owner = scope();

  const mem1 = memory(owner, 'First memory about quantum physics', { tags: ['physics', 'science'] });
  const mem2 = memory(owner, 'Second memory about classical mechanics', { tags: ['mechanics', 'science'] });
  await client.ingest(mem1);
  await client.ingest(mem2);

  const queryRes = await client.queryMemories({
    ...owner,
    filters: { tags_any: ['physics'] },
    limit: 10,
  });
  assert.ok(queryRes.items.length >= 1);
  assert.ok(queryRes.items.some(i => i.id === mem1.id));
  assert.ok(!queryRes.items.some(i => i.id === mem2.id));

  const repairRes = await client.repairIndex({ tenant_id: owner.tenant_id, limit: 10 });
  assert.equal(typeof repairRes.repaired, 'number');
  assert.equal(repairRes.complete, true);

  await client.close();
});

test('consumer manages owned worker handle and tick', async () => {
  const client = await nextral.Client.create(testConfig);
  const owner = scope();

  const worker = await client.startWorker({
    scopes: [owner],
    job_limit: 5,
    lease_seconds: 300,
    tick_interval_ms: 1000,
  });

  const report = await worker.tick();
  assert.equal(typeof report.jobs.processed, 'number');
  assert.equal(typeof report.reminders.processed, 'number');

  await worker.shutdown(2000);
  await client.close();
});
