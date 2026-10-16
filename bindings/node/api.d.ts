export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type PrivacyLevel = 'private' | 'sensitive' | 'shared' | 'restricted';
export type ContentType = 'decision' | 'preference' | 'goal' | 'fact' | 'task' | 'event' | 'note' | 'commitment' | 'pattern';
export type MemoryType = 'working' | 'session' | 'episodic' | 'semantic' | 'relational' | 'procedural' | 'prospective';
export type SourceType = 'realtime' | 'fast_lane' | 'deep_lane' | 'import' | 'manual';
export type MemoryStatus = 'active' | 'soft_deleted' | 'redacted' | 'archived';
export type RuntimeLane = 'sandbox' | 'canary' | 'stable';
export type Severity = 'info' | 'success' | 'warning' | 'destructive';
export type DecisionAction = 'continue' | 'record_improvement' | 'constrain' | 'auto_rollback_and_quarantine';
export interface Scope { tenant_id: string; user_id: string }
export interface IngestionPolicy { min_importance_score: number; min_confidence_score: number }
export interface ScoringWeights { semantic_similarity: number; recency: number; importance: number; access: number }
export interface RetrievalPolicy {
  privacy_scope: PrivacyLevel[]; token_budget: number; top_k_vector: number;
  max_graph_hops: number; scoring_weights: ScoringWeights;
}
export type ProviderKind = 'open_ai_compatible' | 'http' | 'external_callback' | 'test';
export interface ProviderConfig { kind: ProviderKind; model: string; endpoint?: string | null; api_key_env?: string | null }
export interface StoreConfig {
  postgres_url: string; redis_url: string; qdrant_url: string; neo4j_url: string;
  qdrant_collection?: string | null;
  s3_endpoint: string; s3_bucket: string; s3_region: string;
  s3_access_key_env: string; s3_secret_key_env: string;
  transport_profile?: string | null; enforce_tls?: boolean | null;
}
export interface ExecutionConfig {
  max_in_flight?: number;
  max_pending?: number;
  max_pending_bytes?: number;
  request_timeout_ms?: number;
  shutdown_timeout_ms?: number;
  lease_seconds?: number;
}
export interface RuntimeConfig {
  backend: 'production_stores' | 'test_memory'; stores?: StoreConfig | null;
  embedding: ProviderConfig & { dimension: number };
  extraction: ProviderConfig;
  reranker?: { kind: ProviderKind | 'none'; model?: string | null; endpoint?: string | null; api_key_env?: string | null } | null;
  ingestion_policy: IngestionPolicy; retrieval_policy: RetrievalPolicy;
  cache: { key_prefix: string; session_ttl_seconds: number; retrieval_ttl_seconds: number; policy_ttl_seconds: number };
  service: { http_bind?: string | null; grpc_bind?: string | null; graphql_bind?: string | null };
  auth: { issuer?: string | null; audience?: string | null; jwks_url?: string | null; service_token_env?: string | null; jwks_ca_pem_env?: string | null };
  observability: { enabled: boolean; otlp_endpoint?: string | null; prometheus_bind?: string | null };
  rate_limit?: { enabled: boolean; max_requests_per_second: number; burst_size: number };
  batch?: { max_batch_size: number; max_concurrent_batches: number };
  execution?: ExecutionConfig;
  control_namespace?: string;
  base_policy_version?: string;
}
export interface GraphHint {
  from_label: string; from_name: string; relationship_type: string;
  to_label: string; to_name: string; confidence: number;
}
export interface IngestMemoryRequest extends Scope {
  id?: string | null; session_id?: string | null; content: string; content_type: ContentType;
  memory_type: MemoryType; source_type: SourceType; source_message_ids: string[];
  importance_score: number; confidence_score?: number | null; entities: string[]; tags: string[];
  privacy_level: PrivacyLevel; graph_hints: GraphHint[]; policy: IngestionPolicy;
  trace_id?: string | null; embedding?: number[] | null;
}
export interface IngestMemoryResponse {
  status: 'accepted' | 'rejected'; trace_id: string; record_id: string | null;
  validation_errors: string[]; write_receipts: string[];
}
export interface MemoryRecord extends Scope {
  id: string; session_id: string | null; content: string; content_type: ContentType;
  memory_type: MemoryType; source_type: SourceType; source_message_ids: string[];
  importance_score: number; confidence_score: number | null;
  embedding_provider: string | null; embedding_model: string | null; embedding_dim: number | null;
  embedding: number[] | null;
  extraction_provider: string | null; extraction_model: string | null;
  entities: string[]; tags: string[]; privacy_level: PrivacyLevel;
  created_at: string; updated_at: string; last_accessed_at: string | null;
  access_count: number; status: MemoryStatus; schema_version: string;
}
export interface RetrievalRequest extends Scope {
  session_id?: string | null; query_text: string; entities: string[]; intent_topic?: string | null;
  token_budget: number; privacy_scope: PrivacyLevel[]; top_k_vector: number; max_graph_hops: number;
  lane?: RuntimeLane | null; policy_version?: string | null; trace_id?: string | null;
  scoring_weights?: ScoringWeights | null; query_vector?: number[] | null;
}
export interface RetrievedItem {
  id: string; memory_id: string; content: string; score: number; component_scores: { vector: number; graph: number; recency: number; access: number };
  source_path: 'vector' | 'graph' | 'both'; privacy_level: PrivacyLevel;
}
export interface RetrievalTelemetry {
  vector_candidates: number; graph_candidates: number;
  selected_candidates: number; token_estimate: number; token_utilization: number;
  vector_ms: number; graph_ms: number; merge_ms: number; dedupe_ratio: number; degraded_reasons: string[];
}
export interface RetrievalResponse {
  retrieval_id: string; trace_id: string; status: 'ok' | 'degraded' | 'error'; lane: RuntimeLane;
  policy_version: string; quality_score: number; severity: Severity; decision_action: DecisionAction;
  rollback_id: string | null; telemetry: RetrievalTelemetry; items: RetrievedItem[];
}
export interface StoreReceipt { backend: string; operation: string; target_id: string | null; status: string }
export interface TransitionMetadata { memory_id: string; from: MemoryStatus; to: MemoryStatus; actor: string; reason: string; changed_at: string }
export interface ForgetMemoryRequest extends Scope { memory_id: string; actor: string; reason: string; redact: boolean }
export interface ForgetMemoryResponse { transition: TransitionMetadata; redaction_transition: TransitionMetadata | null; receipts: StoreReceipt[]; status: string; attempts: number; last_error: string | null }
export interface UpdateMemoryRequest extends Scope {
  memory_id: string; content?: string; content_type?: ContentType; importance_score?: number;
  confidence_score?: number | null; privacy_level?: PrivacyLevel;
  add_tags?: string[]; remove_tags?: string[]; add_entities?: string[]; remove_entities?: string[];
  embedding?: number[] | null;
}
export interface ListMemoriesRequest extends Scope { include_inactive?: boolean; privacy_scope?: PrivacyLevel[] }
export interface MemoryStats extends Scope {
  active_count: number; total_count: number; entity_count: number; tag_count: number; avg_importance: number;
  by_memory_type: Record<string, number>; by_content_type: Record<string, number>;
}
export interface AppendSessionMessageRequest extends Scope {
  session_id: string; role: string; content: string; idempotency_key?: string | null; privacy_level?: PrivacyLevel;
}
export interface EligibleSessionsRequest extends Scope {
  inactive_at_or_before: string; limit?: number;
}
export interface EligibleSession {
  session_id: string; last_message_at: string; message_count: number;
}
export interface SessionMessage extends Scope {
  id: string; session_id: string; role: string; content: string; idempotency_key: string;
  created_at: string; source_metadata: Json;
}
export interface WorkingContext extends Scope {
  session_id: string | null; session_tail: SessionMessage[]; retrieved_memory_ids: string[];
  procedural_policy_ids: string[];
  procedural_policies: ProceduralPolicy[]; session_summary: SessionSummary | null;
  retrieved_memories: RetrievedItem[]; token_estimate: number;
  degraded_reasons: string[];
}
export interface SessionSummary extends Scope { id: string; session_id: string; summary: string; source_message_ids: string[]; created_at: string }
export interface ProceduralPolicy extends Scope { id: string; name: string; body: string; privacy_level: PrivacyLevel; enabled: boolean; created_at: string; updated_at: string }
export interface ConsolidationRequest extends Scope {
  session_id: string; lane: 'fast' | 'deep'; policy: IngestionPolicy;
  version?: string; summary?: string | null; candidates?: IngestMemoryRequest[]; chunk_messages?: number;
}
export interface RuntimeJob { id: string; tenant_id: string; job_type: string; target_id: string; status: string; attempts: number; last_error: string | null; created_at: string; updated_at: string }
export interface ConsolidationResponse { job: RuntimeJob; accepted: IngestMemoryResponse[]; receipts: StoreReceipt[] }
export interface ScheduleReminderRequest extends Scope {
  source_memory_id: string; kind: 'task' | 'follow_up' | 'commitment'; title: string;
  due_at: string; timezone: string; trace_id?: string | null; channel?: string; allowed_channels?: string[];
  quiet_hours?: { start_hour: number; end_hour: number } | null; expires_at?: string | null;
  priority?: 'low' | 'normal' | 'high' | null;
}
export interface ReminderDeliveryAttempt {
  attempt: number; attempted_at: string; outcome_code: string;
  http_status: number | null; receipt: StoreReceipt;
}
export interface ReminderRecord extends Scope {
  id: string; source_memory_id: string; kind: 'task' | 'follow_up' | 'commitment';
  title: string; details: string; due_at: string; timezone: string; priority: 'low' | 'normal' | 'high';
  status: 'draft' | 'scheduled' | 'due' | 'dispatched' | 'completed' | 'failed' | 'retry_scheduled' | 'cancelled' | 'expired';
  attempt_count: number; last_attempt_at: string | null; next_attempt_at: string | null;
  dedupe_key: string; created_at: string; updated_at: string; channel: string; allowed_channels: string[];
  quiet_hours: { start_hour: number; end_hour: number } | null; expires_at: string | null;
  privacy_level: PrivacyLevel;
  terminal_reason: 'delivered' | 'retry_exhausted' | 'source_unavailable' | 'privacy_denied' | 'channel_denied' | 'expired' | null;
  last_error: string | null; delivery_attempts: ReminderDeliveryAttempt[];
}
export interface ExecuteDueRemindersRequest extends Scope {
  due_at_or_before: string; actor: string; retry_delay_seconds: number; max_retries?: number | null;
  dispatch_policy_version?: string | null; retry_strategy_id?: string | null; trace_id?: string | null;
  privacy_scope?: PrivacyLevel[]; lease_seconds?: number;
}
export interface ReminderDispatchResult {
  reminder_id: string; dedupe_key: string; dispatched: boolean; status: string; reason: string;
  dispatch_policy_version: string; retry_strategy_id: string; outcome_severity: string;
}
export interface GraphNode extends Scope { label: string; name: string; canonical_name: string; key: string; confidence: number; created_at: string }
export interface GraphEdge extends Scope { from_key: string; relationship_type: string; to_key: string; confidence: number; source_memory_ids: string[]; created_at: string; last_confirmed_at: string }
export interface GraphifyResponse { trace_id: string; nodes: GraphNode[]; relationships: GraphEdge[]; contradictions: string[]; confidence_calibration_version: string; contradiction_class: string; evidence_count: number; status: 'accepted'; write_receipts: string[] }
export interface ReembedPlanRequest {
  tenant_id: string; source_collection: string; shadow_collection: string;
  target_embedding_provider: string; target_embedding_model: string;
}
export interface ReembedPlan extends ReembedPlanRequest { id: string; status: string }
export interface BatchResponse<T> { total: number; succeeded: number; failed: number; errors: string[]; results: T[] }
export interface LifecycleJobRequest extends Scope {
  session_id: string; trigger: 'idle' | 'close' | 'deep' | 'retention'; run_at: string;
  idle_seconds?: number | null; consolidation?: ConsolidationRequest | null;
  memory_ids?: string[]; redact?: boolean; max_attempts?: number;
}
export interface RunLifecycleJobsRequest extends Scope { now: string; limit?: number; lease_seconds?: number }
export interface LifecycleScheduledJob {
  id: string; request: LifecycleJobRequest; status: string; attempts: number; next_attempt_at: string;
  created_at: string; updated_at: string; completed_memory_ids: string[]; last_error: string | null;
}
export interface ReembedExecutionRequest {
  plan: ReembedPlan; user_id: string; vectors?: { memory_id: string; vector: number[] }[];
  target_dimension: number; cutover?: boolean; rollback?: boolean;
}
export interface ReembedExecutionReport {
  plan_id: string; status: string; source_count: number; embedded_count: number; failed_count: number;
  duration_ms: number; checkpoint_memory_ids: string[]; validated_count: number;
  cutover_applied: boolean;
  receipts: StoreReceipt[]; last_error: string | null;
}
export interface UpsertPolicyRequest extends Scope {
  policy_id?: string; name: string; body: string; privacy_level: PrivacyLevel; enabled?: boolean;
}
export type ExperimentStatus = 'created' | 'promoted' | 'rolled_back' | 'quarantined';
export interface RuntimeSnapshot { lane: RuntimeLane; policy_version: string; experiment_id: string | null }
export interface ExperimentCandidate {
  id: string; lane: RuntimeLane; policy_version: string; description: string;
  confidence: number; status: ExperimentStatus; rollback_id: string | null;
  last_severity: Severity; promoted_report_run_id: string | null;
  rollback_target: RuntimeSnapshot | null; rollback_reason: string | null;
}
export interface ExperimentRegistryStatus {
  current_lane: RuntimeLane; active_policy_version: string;
  active_experiment_id: string | null;
  experiments: Record<string, ExperimentCandidate>;
}
export type ReminderOutcome = 'completed' | 'retry_scheduled' | 'cancelled' | 'failed';
export type OutcomeObservation =
  | { kind: 'retrieval'; expected: number; matched: number; returned: number; forbidden: number }
  | { kind: 'contradiction'; expected: boolean; detected: boolean }
  | { kind: 'reminder'; expected: ReminderOutcome; observed: ReminderOutcome };
export interface CaseObservation {
  case_id: string; elapsed_ns: number; destructive_events: number; outcome: OutcomeObservation;
}
export interface RunMetadata {
  run_id: string; implementation_revision: string; policy_version: string;
  host_id: string; latency_scope: string;
}
export interface ReportMetadata {
  schema_version: number; suite_id: string; suite_revision: string; suite_sha256: string;
  run: RunMetadata; started_at_unix_ms: number; build_profile: string;
  target_os: string; target_arch: string;
}
export interface EvaluationReport {
  metadata: ReportMetadata; observations: CaseObservation[];
  golden_recall_score: number; contradiction_score: number; reminder_outcome_score: number;
  latency_budget_ns: number;
  latency: { samples: number; mean_ns: number; p95_ns: number; max_ns: number };
  latency_slo_passed: boolean; destructive_events: number;
}
export interface PromotionEvidence { baseline: EvaluationReport; candidate: EvaluationReport }
export interface SafetyPolicy { actions: Partial<Record<Severity, DecisionAction>> }

export class NextralError extends Error {
  readonly code: string;
  readonly executionStatus?: string;
  constructor(code: string, message: string, options?: ErrorOptions & { executionStatus?: string });
}

export interface CallOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
}

export class ServiceHandle {
  shutdown(timeoutMs?: number): Promise<void>;
  readonly binds: string[];
}
export interface WorkerScope {
  tenant_id: string;
  user_id: string;
}

export interface WorkerConfig {
  scopes: WorkerScope[];
  actor: string;
  poll_interval_ms?: number;
  job_limit?: number;
  reminder_limit?: number;
  outbox_limit?: number;
  lease_seconds?: number;
}

export interface KindStats {
  processed: number;
  succeeded: number;
  retried: number;
  failed: number;
}

export interface WorkerTickReport {
  jobs: KindStats;
  reminders: KindStats;
  outbox: KindStats;
  lease_lost: number;
  errors: number;
}

export class WorkerHandle {
  tick(): Promise<WorkerTickReport>;
  shutdown(timeoutMs?: number): Promise<void>;
}


export class Client {
  static create(config: RuntimeConfig): Promise<Client>;
  call(tool: string, payload: unknown, options?: CallOptions): Promise<unknown>;
  close(timeoutMs?: number): Promise<{ drained: boolean; in_flight_remaining: number }>;
  serve(): Promise<ServiceHandle>;
  startWorker(config: WorkerConfig): Promise<WorkerHandle>;
  health(options?: CallOptions): Promise<{ fail_fast: boolean; backends: { backend: string; status: 'ready' | 'unavailable'; degraded_signature: string | null }[] }>;
  ingest(request: IngestMemoryRequest, options?: CallOptions): Promise<IngestMemoryResponse>;
  retrieve(request: RetrievalRequest, options?: CallOptions): Promise<RetrievalResponse>;
  forget(request: ForgetMemoryRequest, options?: CallOptions): Promise<ForgetMemoryResponse>;
  getMemory(request: Scope & { memory_id: string }, options?: CallOptions): Promise<MemoryRecord | null>;
  listMemories(request: ListMemoriesRequest, options?: CallOptions): Promise<MemoryRecord[]>;
  queryMemories(request: unknown, options?: CallOptions): Promise<unknown>;
  repairIndex(request: { tenant_id: string; limit?: number }, options?: CallOptions): Promise<{ repaired: number; remaining: number; complete: boolean }>;
  updateMemory(request: UpdateMemoryRequest, options?: CallOptions): Promise<MemoryRecord>;
  memoryStats(request: Scope, options?: CallOptions): Promise<MemoryStats>;
  searchByTag(request: Scope & { tag: string }, options?: CallOptions): Promise<MemoryRecord[]>;
  searchByEntity(request: Scope & { entity: string }, options?: CallOptions): Promise<MemoryRecord[]>;
  scoredSearch(request: RetrievalRequest, options?: CallOptions): Promise<RetrievalResponse>;
  appendSessionMessage(request: AppendSessionMessageRequest, options?: CallOptions): Promise<{ message_id: string; hot_tail_count: number; degraded_reasons: string[] }>;
  eligibleSessions(request: EligibleSessionsRequest, options?: CallOptions): Promise<EligibleSession[]>;
  workingContext(request: RetrievalRequest, options?: CallOptions): Promise<WorkingContext>;
  queryGraph(request: RetrievalRequest, options?: CallOptions): Promise<{ items: RetrievedItem[] }>;
  graphify(request: Scope & { memory_id: string; hints?: GraphHint[] }, options?: CallOptions): Promise<GraphifyResponse>;
  consolidate(request: ConsolidationRequest, options?: CallOptions): Promise<ConsolidationResponse>;
  scheduleReminder(request: ScheduleReminderRequest, options?: CallOptions): Promise<{ trace_id: string; reminder: ReminderRecord; receipts: StoreReceipt[] }>;
  listReminders(request: Scope, options?: CallOptions): Promise<ReminderRecord[]>;
  executeDueReminders(request: ExecuteDueRemindersRequest, options?: CallOptions): Promise<{ trace_id: string; results: ReminderDispatchResult[]; receipts: StoreReceipt[] }>;
  planReembed(request: ReembedPlanRequest, options?: CallOptions): Promise<ReembedPlan>;
  executeReembed(request: ReembedExecutionRequest, options?: CallOptions): Promise<ReembedExecutionReport>;
  enqueueJob(request: LifecycleJobRequest, options?: CallOptions): Promise<LifecycleScheduledJob>;
  getJob(request: Scope & { job_id: string }, options?: CallOptions): Promise<LifecycleScheduledJob | null>;
  listJobs(request: Scope, options?: CallOptions): Promise<LifecycleScheduledJob[]>;
  runJobs(request: RunLifecycleJobsRequest, options?: CallOptions): Promise<LifecycleScheduledJob[]>;
  runOutbox(request: { tenant_id: string }, options?: CallOptions): Promise<{ completed: number; failed: number }>;
  migrateDatabase(options?: CallOptions): Promise<{ status: string; backend: string }>;
  upsertPolicy(request: UpsertPolicyRequest, options?: CallOptions): Promise<{ policy: ProceduralPolicy; receipts: StoreReceipt[] }>;
  listPolicies(request: Scope & { privacy_scope?: PrivacyLevel[]; include_disabled?: boolean }, options?: CallOptions): Promise<ProceduralPolicy[]>;
  removePolicy(request: Scope & { policy_id: string }, options?: CallOptions): Promise<{ policy_id: string; status: 'removed'; receipts: StoreReceipt[] }>;
  createExperiment(request: { lane: RuntimeLane; policy_version: string; description: string }, options?: CallOptions): Promise<ExperimentCandidate>;
  promoteExperiment(request: { experiment_id: string; evidence: PromotionEvidence }, options?: CallOptions): Promise<ExperimentCandidate>;
  rollbackExperiment(request: { experiment_id: string; reason: string }, options?: CallOptions): Promise<ExperimentCandidate>;
  experimentStatus(options?: CallOptions): Promise<ExperimentRegistryStatus>;
  experimentStatus(request: { experiment_id: string }, options?: CallOptions): Promise<ExperimentCandidate | null>;
  safetyPolicy(options?: CallOptions): Promise<SafetyPolicy>;
  setSafetyPolicy(request: { severity: Severity; action: DecisionAction }, options?: CallOptions): Promise<SafetyPolicy>;
  batchIngest(request: { items: IngestMemoryRequest[]; idempotency_key?: string | null }, options?: CallOptions): Promise<BatchResponse<IngestMemoryResponse>>;
  batchRetrieve(request: { queries: RetrievalRequest[]; idempotency_key?: string | null }, options?: CallOptions): Promise<BatchResponse<RetrievalResponse>>;
  batchForget(request: { items: ForgetMemoryRequest[]; idempotency_key?: string | null }, options?: CallOptions): Promise<BatchResponse<ForgetMemoryResponse>>;
}

export function validateConfig(config: RuntimeConfig): { status: 'ok' };
export function lexicalScore(text: string, query: string): number;
