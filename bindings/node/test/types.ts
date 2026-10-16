import {
  configureRuntime, ingestMemory, retrieve, forgetMemory, appendSessionMessage,
  workingContext, consolidate, scheduleReminder, executeDueReminders, batchIngest,
  batchRetrieve, batchForget, graphify, queryGraph, listMemories, updateMemory,
  memoryStats, searchByEntity, searchByTag, planReembed, serve, NextralError,
  migrateDatabase, getMemory, listReminders, executeReembed, enqueueJob, runJobs,
  listJobs, getJob, runOutbox, stopServing,
  upsertPolicy, listPolicies, removePolicy,
  createExperiment, promoteExperiment, rollbackExperiment, experimentStatus,
  getSafetyPolicy, setSafetyPolicy, scoredSearch, type PromotionEvidence,
  type RuntimeConfig, type IngestMemoryRequest, type RetrievalRequest,
  eligibleSessions, type EligibleSessionsRequest, type EligibleSession,
} from 'nextral';

const policy = { min_importance_score: 0, min_confidence_score: 0 };
const config: RuntimeConfig = {
  backend: 'test_memory', embedding: { kind: 'test', model: 'types', dimension: 3 },
  extraction: { kind: 'test', model: 'types' }, ingestion_policy: policy,
  retrieval_policy: { privacy_scope: ['private'], token_budget: 1800, top_k_vector: 12,
    max_graph_hops: 2, scoring_weights: { semantic_similarity: 1, recency: 0, importance: 0, access: 0 } },
  cache: { key_prefix: 'types', session_ttl_seconds: 7200, retrieval_ttl_seconds: 120, policy_ttl_seconds: 300 },
  service: {}, auth: {}, observability: { enabled: false },
};
configureRuntime(config);
configureRuntime({
  ...config, backend: 'production_stores',
  embedding: { kind: 'external_callback', model: 'host-vectors', dimension: 3 },
  extraction: { kind: 'external_callback', model: 'host-extraction' },
  stores: {
    postgres_url: 'postgres://127.0.0.1/nextral', redis_url: 'redis://127.0.0.1',
    qdrant_url: 'http://127.0.0.1:6333', qdrant_collection: 'consumer-vectors',
    neo4j_url: 'http://127.0.0.1:7474', s3_endpoint: 'http://127.0.0.1:9000',
    s3_bucket: 'consumer', s3_region: 'us-east-1',
    s3_access_key_env: 'HOST_ACCESS_KEY', s3_secret_key_env: 'HOST_SECRET_KEY',
  },
});
const scope = { tenant_id: 'types', user_id: 'consumer' };
const eligibility: EligibleSessionsRequest = { ...scope, inactive_at_or_before: '2026-01-01T00:00:00Z', limit: 100 };
const eligible: EligibleSession[] = eligibleSessions(eligibility);
const lastActivity: string | undefined = eligible[0]?.last_message_at;
const messageCount: number | undefined = eligible[0]?.message_count;
const input: IngestMemoryRequest = { ...scope, content: 'Atlas uses PostgreSQL', content_type: 'decision',
  memory_type: 'semantic', source_type: 'manual', source_message_ids: [], importance_score: 1,
  entities: ['Atlas'], tags: [], privacy_level: 'private', graph_hints: [], policy, embedding: [1, 0, 0] };
const query: RetrievalRequest = { ...scope, query_text: 'Atlas', entities: ['Atlas'], token_budget: 1800,
  privacy_scope: ['private'], top_k_vector: 12, max_graph_hops: 2, query_vector: [1, 0, 0] };
const accepted = ingestMemory(input);
const id: string = accepted.record_id!;
const result: string = retrieve(query).items[0].content;
const forgotten = { ...scope, memory_id: id, actor: 'consumer', reason: 'cleanup', redact: true };
forgetMemory(forgotten);
appendSessionMessage({ ...scope, session_id: 'session', role: 'user', content: result, privacy_level: 'private' }).degraded_reasons;
workingContext(query).session_tail;
const degradedReasons: string[] = workingContext(query).degraded_reasons;
consolidate({ ...scope, session_id: 'session', lane: 'fast', policy, version: '1', summary: result, candidates: [input], chunk_messages: 100 });
scheduleReminder({ ...scope, source_memory_id: id, kind: 'task', title: 'Review', due_at: '1800000000', timezone: 'UTC', channel: 'webhook' });
executeDueReminders({ ...scope, due_at_or_before: '1800000000', actor: 'consumer', retry_delay_seconds: 30, privacy_scope: ['private'], lease_seconds: 300 });
batchIngest({ items: [input] }).results[0].record_id;
batchRetrieve({ queries: [query] }).results[0].items;
batchForget({ items: [forgotten] }).results[0].transition;
const record = listMemories(scope)[0];
graphify({ ...scope, memory_id: record.id, hints: [] }).write_receipts;
queryGraph(query).items;
updateMemory({ ...scope, memory_id: id, content: 'Atlas uses a new persistence engine', embedding: [0, 1, 0], add_tags: ['confirmed'] });
memoryStats(scope).active_count;
searchByEntity({ ...scope, entity: 'Atlas' });
searchByTag({ ...scope, tag: 'confirmed' });
const plan = planReembed({ tenant_id: scope.tenant_id, source_collection: 'old', shadow_collection: 'new', target_embedding_provider: 'host', target_embedding_model: 'new' });
const reembedded = executeReembed({ plan, user_id: scope.user_id, vectors: [{ memory_id: id, vector: [1, 0, 0] }], target_dimension: 3 });
const cutoverApplied: boolean = reembedded.cutover_applied;
void cutoverApplied;
migrateDatabase().backend;
getMemory({ ...scope, memory_id: id })?.status;
listReminders(scope)[0].priority;
const job = enqueueJob({ ...scope, session_id: 'session', trigger: 'retention', run_at: '1800000000', memory_ids: [id], redact: true });
getJob({ ...scope, job_id: job.id })?.status;
listJobs(scope)[0].request.trigger;
runJobs({ ...scope, now: '1800000000' })[0].completed_memory_ids;
runOutbox({ tenant_id: scope.tenant_id }).completed;
const hosts: Promise<{ status: 'stopped'; binds: string[] }> = serve(config, ['http']);
void hosts;
stopServing();
const code: string = new NextralError('invalid_input', 'bad input').code;
void code;
// @ts-expect-error Native JSON transport is deliberately not public.
import { mcpCall } from 'nextral';
void mcpCall;
// @ts-expect-error Host vectors are numeric arrays, not pre-encoded JSON.
ingestMemory({ ...input, embedding: '[1, 0, 0]' });
const procedural = upsertPolicy({ ...scope, name: 'Style', body: 'Use citations', privacy_level: 'private' });
listPolicies(scope)[0].body;
removePolicy({ ...scope, policy_id: procedural.policy.id }).receipts;
declare const evidence: PromotionEvidence;
const experiment = createExperiment({ lane: 'canary', policy_version: 'measured-candidate', description: 'Measured replay' });
promoteExperiment({ experiment_id: experiment.id, evidence }).promoted_report_run_id;
experimentStatus({ experiment_id: experiment.id })?.status;
experimentStatus().active_policy_version;
rollbackExperiment({ experiment_id: experiment.id, reason: 'Restore the prior measured policy' }).rollback_id;
getSafetyPolicy().actions.warning;
setSafetyPolicy({ severity: 'warning', action: 'constrain' });
scoredSearch(query).decision_action;
// @ts-expect-error Graph replay accepts a canonical ID, not a caller-supplied record.
graphify({ ...scope, memory_id: record.id, record });
