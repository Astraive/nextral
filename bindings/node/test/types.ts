import {
  Client, NextralError, ServiceHandle,
  type PromotionEvidence,
  type RuntimeConfig, type IngestMemoryRequest, type RetrievalRequest,
  type EligibleSessionsRequest, type EligibleSession,
} from 'nextral';

const policy = { min_importance_score: 0, min_confidence_score: 0 };
const config: RuntimeConfig = {
  backend: 'test_memory',
  embedding: { kind: 'test', model: 'test-model', dimension: 3 },
  extraction: { kind: 'test', model: 'test-model' },
  ingestion_policy: policy,
  retrieval_policy: {
    privacy_scope: ['private'],
    token_budget: 1800,
    top_k_vector: 12,
    max_graph_hops: 2,
    scoring_weights: { semantic_similarity: 1, recency: 0, importance: 0, access: 0 },
  },
  cache: { key_prefix: 'types', session_ttl_seconds: 60, retrieval_ttl_seconds: 60, policy_ttl_seconds: 60 },
  service: {},
  auth: {},
  observability: { enabled: false },
};
declare const evidence: PromotionEvidence;

async function testTypings() {
  const client = await Client.create(config);
  const scope = { tenant_id: 'types', user_id: 'consumer' };
  const eligibility: EligibleSessionsRequest = { ...scope, inactive_at_or_before: '2026-01-01T00:00:00Z', limit: 100 };
  const eligible: EligibleSession[] = await client.eligibleSessions(eligibility);
  const lastActivity: string | undefined = eligible[0]?.last_message_at;
  void lastActivity;
  const messageCount: number | undefined = eligible[0]?.message_count;
  void messageCount;

  const input: IngestMemoryRequest = {
    ...scope,
    content: 'Atlas uses PostgreSQL',
    content_type: 'decision',
    memory_type: 'semantic',
    source_type: 'manual',
    source_message_ids: [],
    importance_score: 1,
    entities: ['Atlas'],
    tags: [],
    privacy_level: 'private',
    graph_hints: [],
    policy,
    embedding: [1, 0, 0],
  };

  const query: RetrievalRequest = {
    ...scope,
    query_text: 'Atlas',
    entities: ['Atlas'],
    token_budget: 1800,
    privacy_scope: ['private'],
    top_k_vector: 12,
    max_graph_hops: 2,
    query_vector: [1, 0, 0],
  };

  const accepted = await client.ingest(input);
  const id: string = accepted.record_id!;
  const retrieveRes = await client.retrieve(query);
  const result: string = retrieveRes.items[0].content;
  const forgotten = { ...scope, memory_id: id, actor: 'consumer', reason: 'cleanup', redact: true };
  await client.forget(forgotten);

  const appendRes = await client.appendSessionMessage({
    ...scope,
    session_id: 'session',
    role: 'user',
    content: result,
    privacy_level: 'private',
  });
  void appendRes.degraded_reasons;

  const context = await client.workingContext(query);
  const degradedReasons: string[] = context.degraded_reasons;
  void degradedReasons;

  await client.consolidate({
    ...scope,
    session_id: 'session',
    lane: 'fast',
    policy,
    version: '1',
    summary: result,
    candidates: [input],
    chunk_messages: 100,
  });

  await client.scheduleReminder({
    ...scope,
    source_memory_id: id,
    kind: 'task',
    title: 'Review',
    due_at: '1800000000',
    timezone: 'UTC',
    channel: 'webhook',
  });

  await client.executeDueReminders({
    ...scope,
    due_at_or_before: '1800000000',
    actor: 'consumer',
    retry_delay_seconds: 30,
    privacy_scope: ['private'],
    lease_seconds: 300,
  });

  const batchIngestRes = await client.batchIngest({ items: [input] });
  void batchIngestRes.results[0].record_id;

  const batchRetRes = await client.batchRetrieve({ queries: [query] });
  void batchRetRes.results[0].items;

  const batchForRes = await client.batchForget({ items: [forgotten] });
  void batchForRes.results[0].transition;

  const memories = await client.listMemories(scope);
  const record = memories[0];

  const graphifyRes = await client.graphify({ ...scope, memory_id: record.id, hints: [] });
  void graphifyRes.write_receipts;

  const queryGraphRes = await client.queryGraph(query);
  void queryGraphRes.items;

  await client.updateMemory({
    ...scope,
    memory_id: id,
    content: 'Atlas uses a new persistence engine',
    embedding: [0, 1, 0],
    add_tags: ['confirmed'],
  });

  const stats = await client.memoryStats(scope);
  void stats.active_count;

  await client.searchByEntity({ ...scope, entity: 'Atlas' });
  await client.searchByTag({ ...scope, tag: 'confirmed' });

  const plan = await client.planReembed({
    tenant_id: scope.tenant_id,
    source_collection: 'old',
    shadow_collection: 'new',
    target_embedding_provider: 'host',
    target_embedding_model: 'new',
  });

  const reembedded = await client.executeReembed({
    plan,
    user_id: scope.user_id,
    vectors: [{ memory_id: id, vector: [1, 0, 0] }],
    target_dimension: 3,
  });
  const cutoverApplied: boolean = reembedded.cutover_applied;
  void cutoverApplied;

  const dbMigrateRes = await client.migrateDatabase();
  void dbMigrateRes.backend;

  const mem = await client.getMemory({ ...scope, memory_id: id });
  void mem?.status;

  const reminders = await client.listReminders(scope);
  void reminders[0].priority;

  const job = await client.enqueueJob({
    ...scope,
    session_id: 'session',
    trigger: 'retention',
    run_at: '1800000000',
    memory_ids: [id],
    redact: true,
  });

  const fetchedJob = await client.getJob({ ...scope, job_id: job.id });
  void fetchedJob?.status;

  const jobs = await client.listJobs(scope);
  void jobs[0].request.trigger;

  const ranJobs = await client.runJobs({ ...scope, now: '1800000000' });
  void ranJobs[0].completed_memory_ids;

  const outboxRes = await client.runOutbox({ tenant_id: scope.tenant_id });
  void outboxRes.completed;

  const handle: ServiceHandle = await client.serve();
  await handle.shutdown(5000);

  const code: string = new NextralError('invalid_input', 'bad input').code;
  void code;

  // @ts-expect-error Host vectors are numeric arrays, not pre-encoded JSON.
  await client.ingest({ ...input, embedding: '[1, 0, 0]' });

  const procedural = await client.upsertPolicy({
    ...scope,
    name: 'Style',
    body: 'Use citations',
    privacy_level: 'private',
  });

  const policies = await client.listPolicies(scope);
  void policies[0].body;

  const removeRes = await client.removePolicy({ ...scope, policy_id: procedural.policy.id });
  void removeRes.receipts;

  const experiment = await client.createExperiment({
    lane: 'canary',
    policy_version: 'measured-candidate',
    description: 'Measured replay',
  });

  const promoteRes = await client.promoteExperiment({ experiment_id: experiment.id, evidence });
  void promoteRes.promoted_report_run_id;

  const status1 = await client.experimentStatus({ experiment_id: experiment.id });
  void status1?.status;

  const status2 = await client.experimentStatus();
  void status2.active_policy_version;

  const rollbackRes = await client.rollbackExperiment({
    experiment_id: experiment.id,
    reason: 'Restore the prior measured policy',
  });
  void rollbackRes.rollback_id;

  const safety = await client.safetyPolicy();
  void safety.actions.warning;

  await client.setSafetyPolicy({ severity: 'warning', action: 'constrain' });

  const scored = await client.scoredSearch(query);
  void scored.decision_action;

  await client.close();
}

void testTypings;
