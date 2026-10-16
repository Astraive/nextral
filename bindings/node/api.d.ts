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
/** Runtime is process-wide, shared with every native package operation. */
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
}
export interface GraphHint {
  from_label: string; from_name: string; relationship_type: string;
  to_label: string; to_name: string; confidence: number;
}
/** Host extraction and vectors are serialized unchanged; Rust validates business rules. */
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
  memory_id: string; content: string; source_path: 'vector' | 'graph' | 'both'; retrieval_score: number;
  score_breakdown: ScoringWeights; token_estimate: number;
}
export interface RetrievalTelemetry {
  vector_candidates: number; graph_candidates: number; merged_candidates: number;
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
/** All native errors preserve Rust's machine-readable code. */
export class NextralError extends Error {
  readonly code: string;
  constructor(code: string, message: string, options?: ErrorOptions);
}
export function configureRuntime(config: RuntimeConfig): { backend: RuntimeConfig['backend']; status: 'configured' };
export function validateConfig(config: RuntimeConfig): { status: 'ok' };
export function lexicalScore(text: string, query: string): number;
export function health(): { fail_fast: boolean; backends: { backend: string; status: 'ready' | 'unavailable'; degraded_signature: string | null }[] };
/** Runs real service listeners off the Node event loop until native shutdown. */
export function serve(config: RuntimeConfig, modes: ('http' | 'grpc' | 'graphql' | 'all')[]): Promise<{ status: 'stopped'; binds: string[] }>;
export function ingestMemory(request: IngestMemoryRequest): IngestMemoryResponse;
export function retrieve(request: RetrievalRequest): RetrievalResponse;
export function forgetMemory(request: ForgetMemoryRequest): ForgetMemoryResponse;
export function updateMemory(request: UpdateMemoryRequest): MemoryRecord;
export function listMemories(request: ListMemoriesRequest): MemoryRecord[];
export function memoryStats(request: Scope): MemoryStats;
export function searchByTag(request: Scope & { tag: string }): MemoryRecord[];
export function searchByEntity(request: Scope & { entity: string }): MemoryRecord[];
export function appendSessionMessage(request: AppendSessionMessageRequest): { message_id: string; hot_tail_count: number; degraded_reasons: string[] };
/** Scoped sessions inactive at the cutoff, ordered by oldest activity then session ID. Limit defaults to 100 (1–1000). */
export function eligibleSessions(request: EligibleSessionsRequest): EligibleSession[];
export function workingContext(request: RetrievalRequest): WorkingContext;
export function queryGraph(request: RetrievalRequest): { items: RetrievedItem[] };
export function graphify(request: Scope & { memory_id: string; hints?: GraphHint[] }): GraphifyResponse;
export function consolidate(request: ConsolidationRequest): ConsolidationResponse;
export function scheduleReminder(request: ScheduleReminderRequest): { trace_id: string; reminder: ReminderRecord; receipts: StoreReceipt[] };
export function executeDueReminders(request: ExecuteDueRemindersRequest): { trace_id: string; results: ReminderDispatchResult[]; receipts: StoreReceipt[] };
export function planReembed(request: ReembedPlanRequest): ReembedPlan;
export function batchIngest(request: { items: IngestMemoryRequest[]; idempotency_key?: string | null }): BatchResponse<IngestMemoryResponse>;
export function batchRetrieve(request: { queries: RetrievalRequest[]; idempotency_key?: string | null }): BatchResponse<RetrievalResponse>;
export function batchForget(request: { items: ForgetMemoryRequest[]; idempotency_key?: string | null }): BatchResponse<ForgetMemoryResponse>;
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
export function migrateDatabase(): { status: string; backend: string };
export function getMemory(request: Scope & { memory_id: string }): MemoryRecord | null;
export function listReminders(request: Scope): ReminderRecord[];
export function executeReembed(request: ReembedExecutionRequest): ReembedExecutionReport;
export function enqueueJob(request: LifecycleJobRequest): LifecycleScheduledJob;
export function runJobs(request: RunLifecycleJobsRequest): LifecycleScheduledJob[];
export function listJobs(request: Scope): LifecycleScheduledJob[];
export function getJob(request: Scope & { job_id: string }): LifecycleScheduledJob | null;
export function runOutbox(request: { tenant_id: string }): { completed: number; failed: number };
/** Requests graceful shutdown of service hosts started in this process. */
export function stopServing(): void;
export interface UpsertPolicyRequest extends Scope {
  policy_id?: string; name: string; body: string; privacy_level: PrivacyLevel; enabled?: boolean;
}
export function upsertPolicy(request: UpsertPolicyRequest): { policy: ProceduralPolicy; receipts: StoreReceipt[] };
export function listPolicies(request: Scope & { privacy_scope?: PrivacyLevel[]; include_disabled?: boolean }): ProceduralPolicy[];
export function removePolicy(request: Scope & { policy_id: string }): { policy_id: string; status: 'removed'; receipts: StoreReceipt[] };
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
export function createExperiment(request: { lane: RuntimeLane; policy_version: string; description: string }): ExperimentCandidate;
export function promoteExperiment(request: { experiment_id: string; evidence: PromotionEvidence }): ExperimentCandidate;
export function rollbackExperiment(request: { experiment_id: string; reason: string }): ExperimentCandidate;
export function experimentStatus(): ExperimentRegistryStatus;
export function experimentStatus(request: { experiment_id: string }): ExperimentCandidate | null;
export function getSafetyPolicy(): SafetyPolicy;
export function setSafetyPolicy(request: { severity: Severity; action: DecisionAction }): SafetyPolicy;
export function scoredSearch(request: RetrievalRequest): RetrievalResponse;
