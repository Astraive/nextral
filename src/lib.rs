//! # Nextral
//!
//! Canonical Rust core for runtime-neutral memory and retrieval.
//!
//! Nextral provides a seven-type memory architecture (Working, Session, Episodic,
//! Semantic, Relational, Procedural, Prospective) with production adapters for
//! PostgreSQL, Redis, Qdrant, Neo4j, and S3-compatible object storage.
//!
//! ## Architecture
//!
//! - **domain** — Core data types: `MemoryRecord`, `GraphNode`, `GraphEdge`, `ReminderRecord`
//! - **config** — `NextralConfig` with validation for all providers, stores, and policies
//! - **runtime** — Ingestion, retrieval, consolidation, governance, reminders, and graph operations
//! - **adapters** — Production store implementations (Postgres, Redis, Qdrant, Neo4j, S3)
//! - **ports** — Trait definitions for store and provider interfaces
//! - **testkit** — `TestMemoryStore` for in-memory testing without external dependencies
//! - **scoring** — Lexical and multi-factor retrieval scoring
//! - **topology** — Memory type to store role mapping and operation plans

pub mod adapters;
pub mod api;
pub mod config;
pub mod contracts;
pub mod domain;
pub mod graph;
pub mod ingestion;
pub mod memory;
pub mod package;
pub mod planner;
pub mod ports;
pub mod prospective;
pub mod providers;
pub mod retrieval;
pub mod runtime;
pub mod scoring;
pub mod store;
pub mod testkit;
pub mod topology;

pub use contracts::{CoreError, CoreResult};
pub use runtime::load_runner;

#[cfg(test)]
mod tests {
    use crate::{
        config::{
            AuthConfig, CacheConfig, EmbeddingProviderConfig, EmbeddingProviderKind,
            ExtractionProviderConfig, ExtractionProviderKind, IngestionPolicy, NextralConfig,
            ObservabilityConfig, RerankerProviderConfig, RerankerProviderKind, RetrievalPolicy,
            RuntimeBackend, ScoringWeights, ServiceConfig, StoreConfig,
        },
        ingestion::{ingest_memory, IngestMemoryRequest, IngestStatus},
        memory::{ContentType, MemoryRecord, MemoryStatus, MemoryType, PrivacyLevel, SourceType},
        planner::{all_operation_plans, operation_plan, MemoryOperation},
        prospective::{ReminderRecord, ReminderStatus},
        providers::{Clock, IdGenerator, TokenEstimator},
        retrieval::{retrieve, RetrievalRequest, SourcePath},
        runtime::{
            self,
            consolidation::{consolidate_session, ConsolidationLane, ConsolidationRequest},
            governance::{forget_memory, ForgetMemoryRequest},
            intelligence::{ExperimentStatus, RuntimeLane, Severity},
            reembed::plan_reembed,
            reminders::{
                execute_due_reminders, schedule_reminder, ExecuteDueRemindersRequest,
                ScheduleReminderRequest,
            },
            session::{
                append_session_message, assemble_working_context, AppendSessionMessageRequest,
            },
        },
        store::{GraphStore, MemoryIndexStore, ReminderStore, SessionStore, TestMemoryStore},
        topology::{all_profiles, requires_store, StoreRole},
        CoreError,
    };
    fn test_client() -> crate::package::RuntimeClient {
        let mut config = crate::config::NextralConfig::default();
        config.backend = crate::config::RuntimeBackend::TestMemory;
        crate::package::RuntimeClient::from_config(config).unwrap()
    }

    fn mcp_call_json(request_json: &str) -> Result<String, crate::package::PackageError> {
        let value: serde_json::Value = serde_json::from_str(request_json).unwrap();
        let tool = value["tool"].as_str().unwrap();
        let payload_json = value.get("payload_json").and_then(|v| v.as_str()).unwrap_or("{}");
        static CLIENT: std::sync::OnceLock<crate::package::RuntimeClient> = std::sync::OnceLock::new();
        let client = CLIENT.get_or_init(test_client);
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(client.call_json(tool, payload_json.to_string(), crate::package::CallOptions::default()))
    }

    #[tokio::test]
    async fn scored_search_returns_matches() {
        let records = vec![
            MemoryRecord::new(
                "1",
                "tenant_1",
                "usr_1",
                "tokio runtime boundary",
                ContentType::Note,
                MemoryType::Semantic,
                SourceType::Manual,
            ),
            MemoryRecord::new(
                "2",
                "tenant_1",
                "usr_1",
                "graph traversal",
                ContentType::Note,
                MemoryType::Semantic,
                SourceType::Manual,
            ),
        ];

        let scored = runtime::scored_keyword_search(&records, "tokio")
            .await
            .expect("search should succeed");
        assert_eq!(scored.len(), 1);
        assert_eq!(scored[0].id, "1");
    }

    #[test]
    fn memory_contract_validation_and_transition_work() {
        let mut record = MemoryRecord::new(
            "mem_1",
            "tenant_1",
            "usr_1",
            "Use PostgreSQL for Atlas",
            ContentType::Decision,
            MemoryType::Semantic,
            SourceType::Realtime,
        );
        assert!(record.validate().is_ok());
        let transition = record
            .transition(MemoryStatus::SoftDeleted, "usr_1", "forget requested")
            .expect("transition should be valid");
        assert_eq!(transition.from, MemoryStatus::Active);
        assert_eq!(record.status, MemoryStatus::SoftDeleted);
        assert!(record
            .transition(MemoryStatus::Archived, "usr_1", "bad")
            .is_err());
    }

    #[test]
    fn local_store_filters_by_user_privacy_and_status() {
        let mut store = TestMemoryStore::new();
        let mut private = MemoryRecord::new(
            "mem_1",
            "tenant_1",
            "usr_1",
            "private fact",
            ContentType::Fact,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        private.privacy_level = PrivacyLevel::Private;
        store.upsert_memory(private).unwrap();
        let mut shared = MemoryRecord::new(
            "mem_2",
            "tenant_1",
            "usr_2",
            "other user fact",
            ContentType::Fact,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        shared.privacy_level = PrivacyLevel::Shared;
        store.upsert_memory(shared).unwrap();
        let records = store
            .list_memories("tenant_1", "usr_1", &[PrivacyLevel::Private], false)
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].id, "mem_1");
    }

    #[test]
    fn ingestion_retrieval_graph_and_reminders_work() {
        let mut store = TestMemoryStore::new();
        let mut request = IngestMemoryRequest::new(
            "tenant_1",
            "usr_1",
            "Rajan leads backend for Project Atlas using PostgreSQL",
            ContentType::Fact,
            MemoryType::Semantic,
            SourceType::Manual,
            IngestionPolicy {
                min_importance_score: 0.2,
                min_confidence_score: 0.2,
            },
        );
        request.entities = vec!["Project Atlas".to_string(), "PostgreSQL".to_string()];
        request.importance_score = 0.9;
        request.confidence_score = Some(0.8);

        let response = ingest_memory(&mut store, request).unwrap();
        assert_eq!(response.status, IngestStatus::Accepted);

        let retrieval = retrieve(
            &mut store,
            RetrievalRequest::test("tenant_1", "usr_1", "PostgreSQL"),
        )
        .unwrap();
        assert_eq!(retrieval.items.len(), 1);
        assert!(matches!(
            retrieval.items[0].source_path,
            SourcePath::Vector | SourcePath::Both
        ));

        let memory_id = response.record_id.unwrap();
        let mut reminder = ReminderRecord::new(
            "tenant_1",
            "usr_1",
            memory_id,
            crate::prospective::ReminderKind::FollowUp,
            "Check migration",
            "9999999999",
            "Asia/Kolkata",
        )
        .unwrap();
        reminder
            .transition(ReminderStatus::Due, "system", "window reached")
            .unwrap();
        store.upsert_reminder(reminder).unwrap();
        assert_eq!(
            store
                .list_due_reminders("tenant_1", "usr_1", "9999999999")
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn ingest_rejects_same_tenant_user_ownership_collision() {
        let mut store = TestMemoryStore::new();
        let policy = IngestionPolicy {
            min_importance_score: 0.0,
            min_confidence_score: 0.0,
        };

        let mut tenant_a = IngestMemoryRequest::new(
            "tenant_a",
            "usr_1",
            "Tenant A fact",
            ContentType::Fact,
            MemoryType::Semantic,
            SourceType::Manual,
            policy.clone(),
        );
        tenant_a.id = Some("shared-id".to_string());
        tenant_a.importance_score = 1.0;
        assert_eq!(
            ingest_memory(&mut store, tenant_a).unwrap().status,
            IngestStatus::Accepted
        );

        let mut tenant_b = IngestMemoryRequest::new(
            "tenant_a",
            "usr_2",
            "Different user overwrite attempt",
            ContentType::Fact,
            MemoryType::Semantic,
            SourceType::Manual,
            policy,
        );
        tenant_b.id = Some("shared-id".to_string());
        tenant_b.importance_score = 1.0;

        let error = ingest_memory(&mut store, tenant_b).unwrap_err();
        assert!(matches!(error, CoreError::Conflict(_)));
        let original = store.get_memory("tenant_a", "usr_1", "shared-id").unwrap();
        assert_eq!(original.unwrap().content, "Tenant A fact");
    }

    #[test]
    fn all_seven_memory_types_have_runtime_topology() {
        let profiles = all_profiles();
        assert_eq!(profiles.len(), 7);
        assert!(requires_store(&MemoryType::Semantic, StoreRole::Qdrant));
        assert!(requires_store(&MemoryType::Relational, StoreRole::Neo4j));
        assert!(requires_store(&MemoryType::Prospective, StoreRole::Redis));
        assert!(requires_store(&MemoryType::Episodic, StoreRole::S3));
        assert!(
            !profiles
                .iter()
                .find(|profile| profile.memory_type == MemoryType::Working)
                .unwrap()
                .durable
        );
    }

    #[test]
    fn all_memory_types_have_operation_plans_in_every_direction() {
        let plans = all_operation_plans();
        assert_eq!(plans.len(), 49);

        let prospective_schedule =
            operation_plan(&MemoryType::Prospective, MemoryOperation::Schedule);
        assert!(prospective_schedule
            .steps
            .iter()
            .any(|step| step.name == "enqueue due reminder"));

        let relational_retrieve =
            operation_plan(&MemoryType::Relational, MemoryOperation::Retrieve);
        assert!(relational_retrieve
            .steps
            .iter()
            .any(|step| step.name == "run graph traversal"));

        let episodic_archive = operation_plan(&MemoryType::Episodic, MemoryOperation::Archive);
        assert!(episodic_archive
            .steps
            .iter()
            .any(|step| step.name == "write archive object"));

        let procedural_retrieve =
            operation_plan(&MemoryType::Procedural, MemoryOperation::Retrieve);
        assert!(procedural_retrieve
            .steps
            .iter()
            .any(|step| step.name == "load procedural policy"));
    }

    #[test]
    fn production_config_requires_all_store_and_provider_settings() {
        let config = NextralConfig {
            backend: RuntimeBackend::ProductionStores,
            stores: Some(StoreConfig {
                qdrant_collection: Some("test_memories".into()),
                postgres_url: "postgres://nextral".to_string(),
                redis_url: "redis://nextral".to_string(),
                qdrant_url: "http://qdrant:6334".to_string(),
                neo4j_url: "neo4j://neo4j:7687".to_string(),
                s3_endpoint: "http://minio:9000".to_string(),
                s3_bucket: "nextral".to_string(),
                s3_region: "us-east-1".to_string(),
                s3_access_key_env: "NEXTRAL_S3_ACCESS_KEY".to_string(),
                s3_secret_key_env: "NEXTRAL_S3_SECRET_KEY".to_string(),
                transport_profile: Some("baseline".to_string()),
                enforce_tls: Some(false),
            }),
            embedding: EmbeddingProviderConfig {
                kind: EmbeddingProviderKind::OpenAiCompatible,
                model: "configured-by-user".to_string(),
                dimension: 1536,
                endpoint: Some("https://example.invalid/v1/embeddings".to_string()),
                api_key_env: Some("NEXTRAL_EMBEDDING_API_KEY".to_string()),
            },
            extraction: ExtractionProviderConfig {
                kind: ExtractionProviderKind::Http,
                model: "configured-by-user".to_string(),
                endpoint: Some("https://example.invalid/extract".to_string()),
                api_key_env: Some("NEXTRAL_EXTRACTION_API_KEY".to_string()),
            },
            reranker: Some(RerankerProviderConfig {
                kind: RerankerProviderKind::None,
                model: None,
                endpoint: None,
                api_key_env: None,
            }),
            ingestion_policy: IngestionPolicy {
                min_importance_score: 0.4,
                min_confidence_score: 0.6,
            },
            retrieval_policy: RetrievalPolicy {
                privacy_scope: vec![PrivacyLevel::Private],
                token_budget: 1200,
                top_k_vector: 12,
                max_graph_hops: 2,
                scoring_weights: ScoringWeights {
                    semantic_similarity: 0.5,
                    recency: 0.2,
                    importance: 0.2,
                    access: 0.1,
                },
            },
            cache: CacheConfig {
                key_prefix: "nextral".to_string(),
                session_ttl_seconds: 7200,
                retrieval_ttl_seconds: 120,
                policy_ttl_seconds: 300,
            },
            service: ServiceConfig {
                http_bind: Some("127.0.0.1:8080".to_string()),
                grpc_bind: None,
                graphql_bind: None,
            },
            auth: AuthConfig {
                issuer: None,
                audience: None,
                jwks_url: None,
                jwks_ca_pem_env: None,
                service_token_env: Some("NEXTRAL_SERVICE_TOKEN".to_string()),
            },
            observability: ObservabilityConfig {
                enabled: false,
                otlp_endpoint: None,
                prometheus_bind: None,
            },
            rate_limit: crate::config::RateLimitConfig::default(),
            batch: crate::config::BatchConfig::default(),
            ..Default::default()
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn session_consolidation_reminder_forget_and_reembed_paths_work() {
        let mut store = TestMemoryStore::new();
        let appended = append_session_message(
            &mut store,
            AppendSessionMessageRequest {
                privacy_level: PrivacyLevel::Private,
                tenant_id: "tenant_1".to_string(),
                user_id: "usr_1".to_string(),
                session_id: "sess_1".to_string(),
                role: "user".to_string(),
                content: "Atlas should use PostgreSQL and remind me Friday".to_string(),
                idempotency_key: None,
            },
            20,
        )
        .unwrap();
        assert_eq!(appended.hot_tail_count, 1);
        let mut candidate = IngestMemoryRequest::new(
            "tenant_1",
            "usr_1",
            "Atlas should use PostgreSQL",
            ContentType::Fact,
            MemoryType::Semantic,
            SourceType::Manual,
            IngestionPolicy {
                min_importance_score: 0.1,
                min_confidence_score: 0.1,
            },
        );
        candidate.source_message_ids = vec![appended.message_id.clone()];
        candidate.importance_score = 0.9;
        candidate.confidence_score = Some(0.9);

        let consolidated = consolidate_session(
            &mut store,
            ConsolidationRequest {
                version: "v1".into(),
                summary: Some("Atlas uses PostgreSQL".into()),
                candidates: vec![candidate],
                chunk_messages: 100,
                tenant_id: "tenant_1".to_string(),
                user_id: "usr_1".to_string(),
                session_id: "sess_1".to_string(),
                lane: ConsolidationLane::Fast,
                policy: IngestionPolicy {
                    min_importance_score: 0.1,
                    min_confidence_score: 0.1,
                },
            },
        )
        .unwrap();
        assert_eq!(consolidated.job.status, "completed");

        let memory_id = consolidated.accepted[0].record_id.clone().unwrap();
        let scheduled = schedule_reminder(
            &mut store,
            ScheduleReminderRequest {
                channel: "webhook".into(),
                allowed_channels: vec!["webhook".into()],
                quiet_hours: None,
                expires_at: None,
                priority: None,
                tenant_id: "tenant_1".to_string(),
                user_id: "usr_1".to_string(),
                source_memory_id: memory_id.clone(),
                kind: crate::prospective::ReminderKind::FollowUp,
                title: "Check Atlas migration".to_string(),
                due_at: "9999999999".to_string(),
                timezone: "America/New_York".to_string(),
                trace_id: None,
            },
        )
        .unwrap();

        let mut retrieval_request = RetrievalRequest::test("tenant_1", "usr_1", "PostgreSQL");
        retrieval_request.session_id = Some("sess_1".to_string());
        let context = assemble_working_context(&mut store, retrieval_request, 20).unwrap();
        assert_eq!(context.session_tail.len(), 1);
        assert_eq!(context.retrieved_memory_ids.len(), 1);

        let forgotten = forget_memory(
            &mut store,
            ForgetMemoryRequest {
                tenant_id: "tenant_1".to_string(),
                user_id: "usr_1".to_string(),
                memory_id: memory_id.clone(),
                actor: "usr_1".to_string(),
                reason: "user requested removal".to_string(),
                redact: true,
            },
        )
        .unwrap();
        assert!(forgotten.redaction_transition.is_some());
        assert_eq!(
            store
                .reminders
                .iter()
                .find(|reminder| reminder.id == scheduled.reminder.id)
                .unwrap()
                .status,
            crate::domain::ReminderStatus::Cancelled
        );
        assert!(forgotten.receipts.len() >= 2);

        let plan = plan_reembed(
            "tenant_1",
            "memories_v1",
            "memories_v2_shadow",
            "configured-provider",
            "configured-model",
        )
        .unwrap();
        assert_eq!(plan.status, "planned");
    }

    #[test]
    fn retrieval_telemetry_contains_contract_fields() {
        let mut store = TestMemoryStore::new();
        let record = MemoryRecord::new(
            "mem_telemetry",
            "tenant_1",
            "usr_1",
            "Atlas uses PostgreSQL",
            ContentType::Fact,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        store.upsert_memory(record).unwrap();
        let response = retrieve(
            &mut store,
            RetrievalRequest::test("tenant_1", "usr_1", "PostgreSQL"),
        )
        .unwrap();
        assert!(response.telemetry.vector_candidates >= 1);
        assert!(
            response.telemetry.vector_ms < 10000,
            "vector search took too long: {}ms",
            response.telemetry.vector_ms
        );
        assert!(response.telemetry.token_utilization >= 0.0);
        assert!(response.telemetry.dedupe_ratio >= 0.0);
    }

    #[test]
    fn experiment_control_and_safety_policy_workflows_operate() {
        let create = mcp_call_json(
            &serde_json::json!({
                "tool": "experiments.create",
                "payload_json": serde_json::json!({
                    "lane": "canary",
                    "policy_version": "policy-v2",
                    "description": "ranker tune"
                }).to_string()
            })
            .to_string(),
        )
        .unwrap();
        let created: serde_json::Value = serde_json::from_str(&create).unwrap();
        let experiment_id = created["id"].as_str().unwrap().to_string();

        let blocked = mcp_call_json(
            &serde_json::json!({
                "tool": "experiments.promote",
                "payload_json": serde_json::json!({
                    "experiment_id": experiment_id,
                    "severity": "destructive"
                }).to_string()
            })
            .to_string(),
        );
        assert!(blocked.is_err());

        let policy = mcp_call_json(
            &serde_json::json!({
                "tool": "safety.policy.set",
                "payload_json": serde_json::json!({
                    "severity": "warning",
                    "action": "constrain"
                }).to_string()
            })
            .to_string(),
        )
        .unwrap();
        let policy_value: serde_json::Value = serde_json::from_str(&policy).unwrap();
        assert_eq!(policy_value["actions"]["warning"], "constrain");
    }

    // === DOMAIN: MemoryRecord validation ===

    #[test]
    fn memory_record_validate_empty_id_fails() {
        let record = MemoryRecord::new(
            "",
            "t",
            "u",
            "content",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        assert!(record.validate().is_err());
    }

    #[test]
    fn memory_record_validate_empty_content_fails() {
        let record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "  ",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        assert!(record.validate().is_err());
    }

    #[test]
    fn memory_record_validate_bad_importance_fails() {
        let mut record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "content",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        record.importance_score = 1.5;
        assert!(record.validate().is_err());
    }

    #[test]
    fn memory_record_validate_bad_confidence_fails() {
        let mut record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "content",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        record.confidence_score = Some(-0.1);
        assert!(record.validate().is_err());
    }

    #[test]
    fn memory_record_validate_wrong_schema_version_fails() {
        let mut record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "content",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        record.schema_version = "999.0.0".to_string();
        assert!(record.validate().is_err());
    }

    #[test]
    fn memory_record_validate_working_non_realtime_fails() {
        let record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "content",
            ContentType::Note,
            MemoryType::Working,
            SourceType::Manual,
        );
        assert!(record.validate().is_err());
    }

    #[test]
    fn memory_record_mark_accessed_increments() {
        let mut record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "content",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        assert_eq!(record.access_count, 0);
        assert!(record.last_accessed_at.is_none());
        record.mark_accessed();
        assert_eq!(record.access_count, 1);
        assert!(record.last_accessed_at.is_some());
    }

    #[test]
    fn memory_record_redacted_clears_content() {
        let mut record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "secret data",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        record.entities = vec!["entity".to_string()];
        record.tags = vec!["tag".to_string()];
        record
            .transition(MemoryStatus::SoftDeleted, "actor", "reason")
            .unwrap();
        record
            .transition(MemoryStatus::Redacted, "actor", "reason")
            .unwrap();
        assert_eq!(record.content, "[redacted]");
        assert!(record.entities.is_empty());
        assert!(record.tags.is_empty());
    }

    #[test]
    fn memory_record_invalid_transition_fails() {
        let mut record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "content",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        assert!(record
            .transition(MemoryStatus::Redacted, "actor", "reason")
            .is_err());
        assert!(record
            .transition(MemoryStatus::Active, "actor", "reason")
            .is_err());
    }

    // === DOMAIN: ReminderRecord ===

    #[test]
    fn reminder_new_validates_empty_fields() {
        assert!(crate::prospective::ReminderRecord::new(
            "",
            "u",
            "mem",
            crate::prospective::ReminderKind::FollowUp,
            "title",
            "100",
            "UTC"
        )
        .is_err());
        assert!(crate::prospective::ReminderRecord::new(
            "t",
            "",
            "mem",
            crate::prospective::ReminderKind::FollowUp,
            "title",
            "100",
            "UTC"
        )
        .is_err());
        assert!(crate::prospective::ReminderRecord::new(
            "t",
            "u",
            "",
            crate::prospective::ReminderKind::FollowUp,
            "title",
            "100",
            "UTC"
        )
        .is_err());
        assert!(crate::prospective::ReminderRecord::new(
            "t",
            "u",
            "mem",
            crate::prospective::ReminderKind::FollowUp,
            "",
            "100",
            "UTC"
        )
        .is_err());
    }

    #[test]
    fn reminder_new_validates_due_at() {
        assert!(crate::prospective::ReminderRecord::new(
            "t",
            "u",
            "mem",
            crate::prospective::ReminderKind::FollowUp,
            "title",
            "not_a_number",
            "UTC"
        )
        .is_err());
    }

    #[test]
    fn reminder_new_validates_timezone() {
        assert!(crate::prospective::ReminderRecord::new(
            "t",
            "u",
            "mem",
            crate::prospective::ReminderKind::FollowUp,
            "title",
            "100",
            ""
        )
        .is_err());
    }

    #[test]
    fn reminder_transition_valid_paths() {
        let mut r = crate::prospective::ReminderRecord::new(
            "t",
            "u",
            "mem",
            crate::prospective::ReminderKind::FollowUp,
            "title",
            "100",
            "UTC",
        )
        .unwrap();
        r.transition(crate::prospective::ReminderStatus::Due, "sys", "window")
            .unwrap();
        r.transition(
            crate::prospective::ReminderStatus::Dispatched,
            "sys",
            "start",
        )
        .unwrap();
        r.transition(crate::prospective::ReminderStatus::Completed, "sys", "done")
            .unwrap();
    }

    #[test]
    fn reminder_transition_invalid_fails() {
        let mut r = crate::prospective::ReminderRecord::new(
            "t",
            "u",
            "mem",
            crate::prospective::ReminderKind::FollowUp,
            "title",
            "100",
            "UTC",
        )
        .unwrap();
        assert!(r
            .transition(crate::prospective::ReminderStatus::Completed, "sys", "bad")
            .is_err());
    }

    #[test]
    fn reminder_is_due_visible() {
        let mut r = crate::prospective::ReminderRecord::new(
            "t",
            "u",
            "mem",
            crate::prospective::ReminderKind::FollowUp,
            "title",
            "100",
            "UTC",
        )
        .unwrap();
        assert!(r.is_due_visible());
        r.transition(crate::prospective::ReminderStatus::Due, "sys", "w")
            .unwrap();
        assert!(r.is_due_visible());
        r.transition(crate::prospective::ReminderStatus::Dispatched, "sys", "s")
            .unwrap();
        assert!(!r.is_due_visible());
    }

    // === DOMAIN: Graph ===

    #[test]
    fn graph_node_new_validates_confidence() {
        assert!(crate::graph::GraphNode::new("t", "u", "Entity", "name", 0.8).is_ok());
        assert!(crate::graph::GraphNode::new("t", "u", "Entity", "name", 1.5).is_err());
    }

    #[test]
    fn graph_edge_new_validates_confidence() {
        assert!(crate::graph::GraphEdge::new("t", "u", "a:X", "USES", "b:Y", 0.5, "mem1").is_ok());
        assert!(
            crate::graph::GraphEdge::new("t", "u", "a:X", "USES", "b:Y", -0.1, "mem1").is_err()
        );
    }

    #[test]
    fn graphify_record_with_entities_and_hints() {
        let mut record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "Atlas uses PostgreSQL",
            ContentType::Fact,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        record.entities = vec!["Atlas".to_string(), "PostgreSQL".to_string()];
        let output = crate::graph::graphify_record(&record, &[]).unwrap();
        assert!(!output.nodes.is_empty());
        assert!(!output.relationships.is_empty());
    }

    #[test]
    fn graphify_record_empty_entities() {
        let record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "some content",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        let output = crate::graph::graphify_record(&record, &[]).unwrap();
        assert!(output.nodes.is_empty());
        assert!(output.relationships.is_empty());
    }

    #[test]
    fn graph_dedup_nodes() {
        let mut store = crate::store::TestMemoryStore::new();
        let node1 = crate::graph::GraphNode::new("t", "u", "Entity", "Atlas", 0.8).unwrap();
        let node2 = crate::graph::GraphNode::new("t", "u", "Entity", "Atlas", 0.9).unwrap();
        store.merge_node(node1).unwrap();
        store.merge_node(node2).unwrap();
        assert_eq!(store.graph_nodes.len(), 1);
    }

    // === DOMAIN: ProceduralPolicy ===

    #[test]
    fn procedural_policy_new_validates() {
        assert!(crate::domain::policy::ProceduralPolicy::new(
            "",
            "u",
            "n",
            "body",
            crate::memory::PrivacyLevel::Private
        )
        .is_err());
        assert!(crate::domain::policy::ProceduralPolicy::new(
            "t",
            "",
            "n",
            "body",
            crate::memory::PrivacyLevel::Private
        )
        .is_err());
        assert!(crate::domain::policy::ProceduralPolicy::new(
            "t",
            "u",
            "",
            "body",
            crate::memory::PrivacyLevel::Private
        )
        .is_err());
        assert!(crate::domain::policy::ProceduralPolicy::new(
            "t",
            "u",
            "n",
            " ",
            crate::memory::PrivacyLevel::Private
        )
        .is_err());
    }

    // === SCORING ===

    #[test]
    fn lexical_score_empty_query_returns_zero() {
        assert_eq!(crate::scoring::lexical_score("hello world", ""), 0.0);
    }

    #[test]
    fn lexical_score_empty_text_returns_zero() {
        assert_eq!(crate::scoring::lexical_score("", "hello"), 0.0);
    }

    #[test]
    fn lexical_score_full_match() {
        assert_eq!(
            crate::scoring::lexical_score("hello world", "hello world"),
            1.0
        );
    }

    #[test]
    fn lexical_score_partial_match() {
        let score = crate::scoring::lexical_score("hello world", "hello universe");
        assert!(score > 0.0 && score < 1.0);
    }

    #[test]
    fn try_lexical_score_empty_query_errors() {
        assert!(crate::scoring::try_lexical_score("text", "").is_err());
    }

    #[test]
    fn retrieval_score_uses_weights() {
        let weights = crate::config::ScoringWeights {
            semantic_similarity: 0.5,
            recency: 0.2,
            importance: 0.2,
            access: 0.1,
        };
        let score = crate::scoring::retrieval_score(1.0, 1.0, 1.0, 1.0, &weights);
        assert!((score - 1.0).abs() < 0.001);
    }

    #[test]
    fn rank_records_orders_by_score() {
        let records = vec![
            MemoryRecord::new(
                "1",
                "t",
                "u",
                "tokio runtime",
                ContentType::Note,
                MemoryType::Semantic,
                SourceType::Manual,
            ),
            MemoryRecord::new(
                "2",
                "t",
                "u",
                "graph traversal",
                ContentType::Note,
                MemoryType::Semantic,
                SourceType::Manual,
            ),
        ];
        let ranked = crate::scoring::rank_records(&records, "tokio").unwrap();
        assert_eq!(ranked[0].id, "1");
    }

    // === CONFIG VALIDATION ===

    #[test]
    fn config_rejects_empty_privacy_scope() {
        let config = valid_test_config();
        let mut config = config;
        config.retrieval_policy.privacy_scope = vec![];
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_zero_token_budget() {
        let mut config = valid_test_config();
        config.retrieval_policy.token_budget = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_weights_not_summing_to_one() {
        let mut config = valid_test_config();
        config.retrieval_policy.scoring_weights.semantic_similarity = 0.9;
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_empty_embedding_model() {
        let mut config = valid_test_config();
        config.embedding.model = "".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_zero_embedding_dimension() {
        let mut config = valid_test_config();
        config.embedding.dimension = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_missing_endpoint_for_openai() {
        let mut config = valid_test_config();
        config.embedding.kind = EmbeddingProviderKind::OpenAiCompatible;
        config.embedding.endpoint = None;
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_empty_extraction_model() {
        let mut config = valid_test_config();
        config.extraction.model = "".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_empty_cache_prefix() {
        let mut config = valid_test_config();
        config.cache.key_prefix = "".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_zero_cache_ttl() {
        let mut config = valid_test_config();
        config.cache.session_ttl_seconds = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_empty_store_fields() {
        let mut config = valid_test_config();
        config.backend = RuntimeBackend::ProductionStores;
        config.stores.as_mut().unwrap().postgres_url = "".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_invalid_transport_profile() {
        let mut config = valid_test_config();
        config.backend = RuntimeBackend::ProductionStores;
        config.stores.as_mut().unwrap().transport_profile = Some("invalid".to_string());
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_enforce_tls_with_non_tls_redis() {
        let mut config = valid_test_config();
        config.backend = RuntimeBackend::ProductionStores;
        config.stores.as_mut().unwrap().enforce_tls = Some(true);
        config.stores.as_mut().unwrap().redis_url = "redis://localhost:6379".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_production_without_stores() {
        let mut config = valid_test_config();
        config.backend = RuntimeBackend::ProductionStores;
        config.stores = None;
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_test_backend_with_non_test_providers() {
        let mut config = valid_test_config();
        config.backend = RuntimeBackend::TestMemory;
        config.embedding.kind = EmbeddingProviderKind::OpenAiCompatible;
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_bad_importance_score() {
        let mut config = valid_test_config();
        config.ingestion_policy.min_importance_score = 1.5;
        assert!(config.validate().is_err());
    }

    fn valid_test_config() -> NextralConfig {
        NextralConfig {
            backend: RuntimeBackend::TestMemory,
            stores: Some(StoreConfig {
                qdrant_collection: Some("test_memories".into()),
                postgres_url: "postgres://test".to_string(),
                redis_url: "redis://test".to_string(),
                qdrant_url: "http://qdrant:6334".to_string(),
                neo4j_url: "neo4j://neo4j:7687".to_string(),
                s3_endpoint: "http://minio:9000".to_string(),
                s3_bucket: "test".to_string(),
                s3_region: "us-east-1".to_string(),
                s3_access_key_env: "KEY".to_string(),
                s3_secret_key_env: "SECRET".to_string(),
                transport_profile: Some("baseline".to_string()),
                enforce_tls: Some(false),
            }),
            embedding: EmbeddingProviderConfig {
                kind: EmbeddingProviderKind::Test,
                model: "test-model".to_string(),
                dimension: 128,
                endpoint: None,
                api_key_env: None,
            },
            extraction: ExtractionProviderConfig {
                kind: ExtractionProviderKind::Test,
                model: "test-model".to_string(),
                endpoint: None,
                api_key_env: None,
            },
            reranker: Some(RerankerProviderConfig {
                kind: RerankerProviderKind::None,
                model: None,
                endpoint: None,
                api_key_env: None,
            }),
            ingestion_policy: IngestionPolicy {
                min_importance_score: 0.0,
                min_confidence_score: 0.0,
            },
            retrieval_policy: RetrievalPolicy {
                privacy_scope: vec![PrivacyLevel::Private],
                token_budget: 1800,
                top_k_vector: 12,
                max_graph_hops: 2,
                scoring_weights: ScoringWeights {
                    semantic_similarity: 0.5,
                    recency: 0.2,
                    importance: 0.2,
                    access: 0.1,
                },
            },
            cache: CacheConfig {
                key_prefix: "test".to_string(),
                session_ttl_seconds: 7200,
                retrieval_ttl_seconds: 120,
                policy_ttl_seconds: 300,
            },
            service: ServiceConfig {
                http_bind: Some("127.0.0.1:8080".to_string()),
                grpc_bind: None,
                graphql_bind: None,
            },
            auth: AuthConfig {
                issuer: None,
                audience: None,
                jwks_url: None,
                jwks_ca_pem_env: None,
                service_token_env: None,
            },
            observability: ObservabilityConfig {
                enabled: false,
                otlp_endpoint: None,
                prometheus_bind: None,
            },
            rate_limit: crate::config::RateLimitConfig::default(),
            batch: crate::config::BatchConfig::default(),
            ..Default::default()
        }
    }

    // === TRANSPORT ===

    #[test]
    fn transport_strict_requires_tls() {
        let profile = crate::adapters::transport::TransportHardeningProfile::strict(None);
        assert!(profile.require_tls);
        assert!(
            crate::adapters::transport::validate_transport_url("https://example.com", true).is_ok()
        );
        assert!(
            crate::adapters::transport::validate_transport_url("http://example.com", true).is_err()
        );
    }

    #[test]
    fn transport_baseline_allows_http() {
        let profile = crate::adapters::transport::TransportHardeningProfile::baseline(None);
        assert!(!profile.require_tls);
        assert!(
            crate::adapters::transport::validate_transport_url("http://example.com", false).is_ok()
        );
    }

    // === API ===

    #[test]
    fn startup_plan_expands_all_mode() {
        let config = valid_test_config();
        let plan = crate::api::startup_plan(&config, crate::api::ServiceMode::All).unwrap();
        assert_eq!(plan.modes.len(), 3);
    }

    #[test]
    fn startup_plan_single_mode() {
        let config = valid_test_config();
        let plan = crate::api::startup_plan(&config, crate::api::ServiceMode::Http).unwrap();
        assert_eq!(plan.modes.len(), 1);
    }

    #[test]
    fn readiness_matrix_all_configured() {
        let config = valid_test_config();
        let matrix = crate::api::startup_readiness_matrix(&config).unwrap();
        assert!(!matrix.fail_fast);
        assert!(matrix.backends.iter().all(|b| b.status == "ready"));
    }

    #[test]
    fn readiness_matrix_missing_stores() {
        let mut config = valid_test_config();
        config.stores = None;
        let matrix = crate::api::startup_readiness_matrix(&config).unwrap();
        assert_eq!(matrix.backends[0].backend, "test_memory");
    }

    // === MCP TOOLS ===

    #[test]
    fn mcp_unknown_tool_returns_error() {
        let result = mcp_call_json(
            &serde_json::json!({
                "tool": "unknown.tool",
                "payload_json": "{}"
            })
            .to_string(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn mcp_health_tool() {
        let result = mcp_call_json(
            &serde_json::json!({
                "tool": "nextral.health",
                "payload_json": "{}"
            })
            .to_string(),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(v["fail_fast"], false);
    }

    #[test]
    fn mcp_session_append_tool() {
        let result = mcp_call_json(
            &serde_json::json!({
                "tool": "nextral.session.append",
                "payload_json": serde_json::json!({
                    "tenant_id": "t", "user_id": "u", "session_id": "s",
                    "role": "user", "content": "hello", "idempotency_key": null
                }).to_string()
            })
            .to_string(),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(v["message_id"].is_string());
    }

    #[test]
    fn mcp_session_context_tool() {
        let result = mcp_call_json(
            &serde_json::json!({
                "tool": "nextral.session.context",
                "payload_json": serde_json::json!({
                    "tenant_id": "t", "user_id": "u", "query_text": "test",
                    "entities": [], "token_budget": 1800,
                    "privacy_scope": ["private"], "top_k_vector": 12, "max_graph_hops": 2
                }).to_string()
            })
            .to_string(),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(v["retrieved_memory_ids"].is_array());
    }

    #[test]
    fn mcp_memory_list_tool() {
        let result = mcp_call_json(
            &serde_json::json!({
                "tool": "nextral.memory.list",
                "payload_json": serde_json::json!({"tenant_id": "t", "user_id": "u"}).to_string()
            })
            .to_string(),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(v["items"].is_array());
    }

    // === TESTMEMORYSTORE ===

    #[test]
    fn store_get_memory_not_found() {
        let store = crate::store::TestMemoryStore::new();
        let result = store.get_memory("t", "u", "nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn store_update_memory_not_found() {
        let mut store = crate::store::TestMemoryStore::new();
        let record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "content",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        assert!(store.update_memory(record).is_err());
    }

    #[test]
    fn store_list_memories_include_inactive() {
        let mut store = crate::store::TestMemoryStore::new();
        let mut record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "content",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        record.status = MemoryStatus::SoftDeleted;
        store.upsert_memory(record).unwrap();
        let active = store
            .list_memories("t", "u", &[PrivacyLevel::Private], false)
            .unwrap();
        assert!(active.is_empty());
        let all = store
            .list_memories("t", "u", &[PrivacyLevel::Private], true)
            .unwrap();
        assert_eq!(all.len(), 1);
    }

    #[test]
    fn store_upsert_overwrites_existing() {
        let mut store = crate::store::TestMemoryStore::new();
        let r1 = MemoryRecord::new(
            "id",
            "t",
            "u",
            "old content",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        store.upsert_memory(r1).unwrap();
        let r2 = MemoryRecord::new(
            "id",
            "t",
            "u",
            "new content",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        store.upsert_memory(r2).unwrap();
        let loaded = store.get_memory("t", "u", "id").unwrap().unwrap();
        assert_eq!(loaded.content, "new content");
    }

    #[test]
    fn store_graph_memory_ids_empty_query() {
        let store = crate::store::TestMemoryStore::new();
        let ids = store
            .graph_memory_ids("t", "u", "", 2, &[PrivacyLevel::Private])
            .unwrap();
        assert!(ids.is_empty());
    }

    #[test]
    fn store_merge_edge_deduplicates_source_ids() {
        let mut store = crate::store::TestMemoryStore::new();
        let e1 = crate::graph::GraphEdge::new("t", "u", "a:X", "USES", "b:Y", 0.5, "mem1").unwrap();
        let e2 = crate::graph::GraphEdge::new("t", "u", "a:X", "USES", "b:Y", 0.8, "mem1").unwrap();
        store.merge_edge(e1).unwrap();
        store.merge_edge(e2).unwrap();
        assert_eq!(store.graph_edges.len(), 1);
        assert_eq!(store.graph_edges[0].source_memory_ids.len(), 1);
    }

    #[test]
    fn store_merge_edge_adds_new_source_ids() {
        let mut store = crate::store::TestMemoryStore::new();
        let e1 = crate::graph::GraphEdge::new("t", "u", "a:X", "USES", "b:Y", 0.5, "mem1").unwrap();
        let e2 = crate::graph::GraphEdge::new("t", "u", "a:X", "USES", "b:Y", 0.8, "mem2").unwrap();
        store.merge_edge(e1).unwrap();
        store.merge_edge(e2).unwrap();
        assert_eq!(store.graph_edges.len(), 1);
        assert_eq!(store.graph_edges[0].source_memory_ids.len(), 2);
    }

    #[test]
    fn store_session_tail_limit() {
        let mut store = crate::store::TestMemoryStore::new();
        for i in 0..10 {
            let msg = crate::domain::SessionMessage::new(
                "t",
                "u",
                "s",
                "user",
                format!("msg {i}"),
                format!("key_{i}"),
            );
            store.append_session_message(msg).unwrap();
        }
        let tail = store.session_tail("t", "u", "s", 3).unwrap();
        assert_eq!(tail.len(), 3);
    }

    #[test]
    fn store_session_idempotency() {
        let mut store = crate::store::TestMemoryStore::new();
        let msg1 = crate::domain::SessionMessage::new("t", "u", "s", "user", "hello", "key1");
        let msg2 = crate::domain::SessionMessage::new("t", "u", "s", "user", "hello", "key1");
        store.append_session_message(msg1).unwrap();
        store.append_session_message(msg2).unwrap();
        let tail = store.session_tail("t", "u", "s", 100).unwrap();
        assert_eq!(tail.len(), 1);
    }

    #[test]
    fn store_reminder_dedupe_key_conflict() {
        let mut store = crate::store::TestMemoryStore::new();
        let r1 = crate::prospective::ReminderRecord::new(
            "t",
            "u",
            "mem1",
            crate::prospective::ReminderKind::FollowUp,
            "title1",
            "100",
            "UTC",
        )
        .unwrap();
        let r2 = crate::prospective::ReminderRecord::new(
            "t",
            "u",
            "mem2",
            crate::prospective::ReminderKind::FollowUp,
            "title2",
            "200",
            "UTC",
        )
        .unwrap();
        // r1 and r2 have different dedupe keys since they have different inputs
        store.upsert_reminder(r1).unwrap();
        store.upsert_reminder(r2).unwrap();
        assert_eq!(store.reminders.len(), 2);
    }

    // === EVALUATION ===

    // === INTELLIGENCE ===

    #[test]
    fn classify_severity_all_branches() {
        let d = crate::runtime::intelligence::classify_severity(0.1, 0.9, true);
        assert_eq!(d, Severity::Destructive);
        let w = crate::runtime::intelligence::classify_severity(0.6, 0.1, false);
        assert!(matches!(w, Severity::Warning | Severity::Info));
        let s = crate::runtime::intelligence::classify_severity(0.9, 0.0, false);
        assert_eq!(s, Severity::Success);
    }

    #[test]
    fn experiment_registry_lifecycle() {
        let mut reg = crate::runtime::intelligence::ExperimentRegistry::default();
        let exp = reg.create(
            RuntimeLane::Canary,
            "policy-v2".to_string(),
            "test".to_string(),
        );
        assert_eq!(exp.status, ExperimentStatus::Created);
        let promoted = reg
            .promote(
                &exp.id,
                &crate::runtime::evaluation::deterministic_test_evidence(),
            )
            .unwrap();
        assert_eq!(promoted.status, ExperimentStatus::Promoted);
    }

    #[test]
    fn experiment_not_found_returns_none() {
        let mut reg = crate::runtime::intelligence::ExperimentRegistry::default();
        assert!(reg
            .promote(
                "nonexistent",
                &crate::runtime::evaluation::deterministic_test_evidence()
            )
            .is_err());
    }

    // === DOMAIN: deterministic_id ===

    #[test]
    fn deterministic_id_is_deterministic() {
        let a = crate::memory::deterministic_id(&["hello", "world"]);
        let b = crate::memory::deterministic_id(&["hello", "world"]);
        assert_eq!(a, b);
    }

    #[test]
    fn deterministic_id_different_inputs() {
        let a = crate::memory::deterministic_id(&["hello", "world"]);
        let b = crate::memory::deterministic_id(&["hello", "world2"]);
        assert_ne!(a, b);
    }

    // === DOMAIN: RuntimePolicy ===

    #[test]
    fn runtime_policy_allows_memory_type() {
        let policy = crate::domain::RuntimePolicy::default();
        assert!(policy.allows_memory_type(&MemoryType::Semantic));
    }

    // === WORKING CONTEXT ===

    #[test]
    fn working_context_assembly_empty_session() {
        let mut store = crate::store::TestMemoryStore::new();
        let request = RetrievalRequest::test("t", "u", "query");
        let ctx =
            crate::runtime::session::assemble_working_context(&mut store, request, 20).unwrap();
        assert!(ctx.session_tail.is_empty());
    }

    // === CONSOLIDATION ===

    #[test]
    fn consolidation_empty_session_fails() {
        let mut store = crate::store::TestMemoryStore::new();
        let result = crate::runtime::consolidation::consolidate_session(
            &mut store,
            crate::runtime::consolidation::ConsolidationRequest {
                version: "v1".into(),
                summary: None,
                candidates: Vec::new(),
                chunk_messages: 100,
                tenant_id: "t".to_string(),
                user_id: "u".to_string(),
                session_id: "s".to_string(),
                lane: crate::runtime::consolidation::ConsolidationLane::Fast,
                policy: IngestionPolicy {
                    min_importance_score: 0.0,
                    min_confidence_score: 0.0,
                },
            },
        );
        assert!(result.is_err());
    }

    // === GOVERNANCE ===

    #[test]
    fn forget_nonexistent_memory_fails() {
        let mut store = crate::store::TestMemoryStore::new();
        let result = crate::runtime::governance::forget_memory(
            &mut store,
            crate::runtime::governance::ForgetMemoryRequest {
                tenant_id: "t".to_string(),
                user_id: "u".to_string(),
                memory_id: "nonexistent".to_string(),
                actor: "a".to_string(),
                reason: "r".to_string(),
                redact: false,
            },
        );
        assert!(result.is_err());
    }

    // === REEMBED ===

    #[test]
    fn reembed_plan_validates_inputs() {
        assert!(crate::runtime::reembed::plan_reembed("", "s", "d", "p", "m").is_err());
        assert!(crate::runtime::reembed::plan_reembed("t", "", "d", "p", "m").is_err());
    }

    #[test]
    fn reembed_plan_creates_plan() {
        let plan = crate::runtime::reembed::plan_reembed("t", "s", "d", "p", "m").unwrap();
        assert_eq!(plan.status, "planned");
        assert_eq!(plan.tenant_id, "t");
    }

    // === TOPLOGY ===

    #[test]
    fn topology_all_types_have_profiles() {
        for mt in [
            MemoryType::Working,
            MemoryType::Session,
            MemoryType::Episodic,
            MemoryType::Semantic,
            MemoryType::Relational,
            MemoryType::Procedural,
            MemoryType::Prospective,
        ] {
            let profile = crate::topology::profile(&mt);
            assert_eq!(profile.memory_type, mt);
        }
    }

    // === CONTRACTS ===

    #[test]
    fn core_error_display() {
        let e = crate::CoreError::InvalidInput("test".to_string());
        assert!(e.to_string().contains("test"));
        let e = crate::CoreError::NotFound("missing".to_string());
        assert!(e.to_string().contains("missing"));
    }

    #[test]
    fn core_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "not found");
        let core_err: crate::CoreError = io_err.into();
        assert!(matches!(core_err, crate::CoreError::Io(_)));
    }

    #[test]
    fn core_error_from_json() {
        let json_err = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
        let core_err: crate::CoreError = json_err.into();
        assert!(matches!(core_err, crate::CoreError::Serialization(_)));
    }

    // === PROVIDERS ===

    #[test]
    fn deterministic_ids_memory_id() {
        let gen = crate::providers::DeterministicIds;
        let id = gen.memory_id("t", "u", "content").unwrap();
        assert!(!id.is_empty());
    }

    #[test]
    fn system_clock_returns_timestamp() {
        let clock = crate::providers::SystemClock;
        let ts = clock.now_timestamp().unwrap();
        assert!(!ts.is_empty());
        assert!(ts.parse::<u64>().is_ok());
    }

    #[test]
    fn whitespace_token_estimator() {
        let est = crate::providers::WhitespaceTokenEstimator;
        assert_eq!(est.estimate_tokens("hello world").unwrap(), 2);
        assert_eq!(est.estimate_tokens("").unwrap(), 1);
    }

    // === BATCH OPERATIONS ===

    #[test]
    fn mcp_batch_ingest_tool() {
        let result = mcp_call_json(&serde_json::json!({
            "tool": "nextral.batch.ingest",
            "payload_json": serde_json::json!({
                "items": [
                    {
                        "tenant_id": "t", "user_id": "u", "content": "batch item 1",
                        "content_type": "note", "memory_type": "semantic", "source_type": "manual",
                        "source_message_ids": [], "importance_score": 0.5, "entities": [], "tags": [],
                        "privacy_level": "private", "graph_hints": [],
                        "policy": {"min_importance_score": 0.0, "min_confidence_score": 0.0}
                    },
                    {
                        "tenant_id": "t", "user_id": "u", "content": "batch item 2",
                        "content_type": "note", "memory_type": "semantic", "source_type": "manual",
                        "source_message_ids": [], "importance_score": 0.5, "entities": [], "tags": [],
                        "privacy_level": "private", "graph_hints": [],
                        "policy": {"min_importance_score": 0.0, "min_confidence_score": 0.0}
                    }
                ],
                "idempotency_key": "batch_test_1"
            }).to_string()
        }).to_string()).unwrap();
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(v["total"], 2);
        assert_eq!(v["succeeded"], 2);
        assert_eq!(v["failed"], 0);
    }

    #[test]
    fn mcp_batch_ingest_idempotency() {
        let mut payload = serde_json::json!({
            "items": [{
                "tenant_id": "t", "user_id": "u", "content": "idempotent item",
                "content_type": "note", "memory_type": "semantic", "source_type": "manual",
                "source_message_ids": [], "importance_score": 0.5, "entities": [], "tags": [],
                "privacy_level": "private", "graph_hints": [],
                "policy": {"min_importance_score": 0.0, "min_confidence_score": 0.0}
            }],
            "idempotency_key": "idempotent_batch_key_1"
        });
        let call = |payload: &serde_json::Value| {
            mcp_call_json(
                &serde_json::json!({
                    "tool": "nextral.batch.ingest",
                    "payload_json": payload.to_string()
                })
                .to_string(),
            )
        };
        let first = call(&payload).unwrap();
        assert_eq!(call(&payload).unwrap(), first);
        payload["items"][0]["content"] = serde_json::json!("different content");
        assert_eq!(call(&payload).unwrap_err().code, "conflict");
    }

    #[test]
    fn mcp_batch_retrieve_tool() {
        let result = mcp_call_json(
            &serde_json::json!({
                "tool": "nextral.batch.retrieve",
                "payload_json": serde_json::json!({
                    "queries": [
                        {
                            "tenant_id": "t", "user_id": "u", "query_text": "test",
                            "entities": [], "token_budget": 1800, "privacy_scope": ["private"],
                            "top_k_vector": 12, "max_graph_hops": 2
                        }
                    ]
                }).to_string()
            })
            .to_string(),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(v["total"], 1);
        assert_eq!(v["succeeded"], 1);
    }

    #[test]
    fn mcp_batch_forget_tool() {
        let result = mcp_call_json(
            &serde_json::json!({
                "tool": "nextral.batch.forget",
                "payload_json": serde_json::json!({
                    "items": [{
                        "tenant_id": "t", "user_id": "u", "memory_id": "nonexistent",
                        "actor": "a", "reason": "test", "redact": false
                    }]
                }).to_string()
            })
            .to_string(),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(v["total"], 1);
        assert_eq!(v["failed"], 1);
    }

    // === CONFIG: RATE LIMIT AND BATCH ===

    #[test]
    fn config_with_rate_limit_and_batch() {
        let mut config = valid_test_config();
        config.rate_limit = crate::config::RateLimitConfig {
            enabled: true,
            max_requests_per_second: 50,
            burst_size: 100,
        };
        config.batch = crate::config::BatchConfig {
            max_batch_size: 50,
            max_concurrent_batches: 2,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn rate_limit_config_defaults() {
        let config = crate::config::RateLimitConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.max_requests_per_second, 100);
    }

    #[test]
    fn batch_config_defaults() {
        let config = crate::config::BatchConfig::default();
        assert_eq!(config.max_batch_size, 100);
    }

    // === API: BATCH ENDPOINTS ===

    // === WORKING CONTEXT WITH RETRIEVED MEMORIES ===

    #[test]
    fn working_context_populates_retrieved_ids() {
        let mut store = crate::store::TestMemoryStore::new();
        let record = MemoryRecord::new(
            "mem1",
            "t",
            "u",
            "Atlas uses PostgreSQL",
            ContentType::Fact,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        store.upsert_memory(record).unwrap();
        let request = RetrievalRequest::test("t", "u", "PostgreSQL");
        let ctx =
            crate::runtime::session::assemble_working_context(&mut store, request, 20).unwrap();
        assert!(!ctx.retrieved_memory_ids.is_empty());
    }

    // === GOVERNANCE: FORGET WITH REDACT ===

    #[test]
    fn forget_with_redact_clears_content() {
        let mut store = crate::store::TestMemoryStore::new();
        let record = MemoryRecord::new(
            "mem_forget",
            "t",
            "u",
            "secret data",
            ContentType::Fact,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        store.upsert_memory(record).unwrap();
        let result = forget_memory(
            &mut store,
            ForgetMemoryRequest {
                tenant_id: "t".to_string(),
                user_id: "u".to_string(),
                memory_id: "mem_forget".to_string(),
                actor: "a".to_string(),
                reason: "test".to_string(),
                redact: true,
            },
        )
        .unwrap();
        assert!(result.redaction_transition.is_some());
        let loaded = store.get_memory("t", "u", "mem_forget").unwrap().unwrap();
        assert_eq!(loaded.content, "[redacted]");
    }

    // === REMINDERS: FULL LIFECYCLE ===

    #[test]
    fn reminder_full_lifecycle_scheduled_to_completed() {
        let mut store = crate::store::TestMemoryStore::new();
        store
            .upsert_memory(MemoryRecord::new(
                "mem",
                "t",
                "u",
                "Check migration",
                ContentType::Task,
                MemoryType::Prospective,
                SourceType::Manual,
            ))
            .unwrap();
        let scheduled = schedule_reminder(
            &mut store,
            ScheduleReminderRequest {
                channel: "webhook".into(),
                allowed_channels: vec!["webhook".into()],
                quiet_hours: None,
                expires_at: None,
                priority: None,
                tenant_id: "t".to_string(),
                user_id: "u".to_string(),
                source_memory_id: "mem".to_string(),
                kind: crate::prospective::ReminderKind::FollowUp,
                title: "Check migration".to_string(),
                due_at: "9999999999".to_string(),
                timezone: "UTC".to_string(),
                trace_id: None,
            },
        )
        .unwrap();
        assert_eq!(scheduled.reminder.status, ReminderStatus::Scheduled);
        let due = execute_due_reminders(
            &mut store,
            ExecuteDueRemindersRequest {
                privacy_scope: vec![PrivacyLevel::Private],
                lease_seconds: 300,
                tenant_id: "t".to_string(),
                user_id: "u".to_string(),
                due_at_or_before: "9999999999".to_string(),
                actor: "system".to_string(),
                retry_delay_seconds: 60,
                max_retries: Some(3),
                dispatch_policy_version: None,
                retry_strategy_id: None,
                trace_id: None,
            },
        )
        .unwrap();
        assert_eq!(due.results.len(), 1);
        assert_eq!(due.results[0].status, ReminderStatus::Completed);
    }

    // === HARD QA: HTTP STATUS CODES ===

    // === HARD QA: CONFIG UPPER BOUNDS ===

    #[test]
    fn config_rejects_huge_token_budget() {
        let mut config = valid_test_config();
        config.retrieval_policy.token_budget = 200_000;
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_huge_top_k() {
        let mut config = valid_test_config();
        config.retrieval_policy.top_k_vector = 5000;
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_huge_graph_hops() {
        let mut config = valid_test_config();
        config.retrieval_policy.max_graph_hops = 20;
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_huge_embedding_dimension() {
        let mut config = valid_test_config();
        config.embedding.dimension = 100_000;
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_long_model_name() {
        let mut config = valid_test_config();
        config.embedding.model = "x".repeat(300);
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_huge_batch_size() {
        let mut config = valid_test_config();
        config.batch.max_batch_size = 100_000;
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_zero_batch_size() {
        let mut config = valid_test_config();
        config.batch.max_batch_size = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_huge_concurrent_batches() {
        let mut config = valid_test_config();
        config.batch.max_concurrent_batches = 100;
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_rate_limit_zero_rps_when_enabled() {
        let mut config = valid_test_config();
        config.rate_limit.enabled = true;
        config.rate_limit.max_requests_per_second = 0;
        assert!(config.validate().is_err());
    }

    // === HARD QA: DOMAIN INPUT LIMITS ===

    #[test]
    fn memory_rejects_huge_content() {
        let mut record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "x",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        record.content = "a".repeat(2_000_000);
        assert!(record.validate().is_err());
    }

    #[test]
    fn memory_rejects_long_id() {
        let long_id = "x".repeat(300);
        let record = MemoryRecord::new(
            long_id,
            "t",
            "u",
            "content",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        assert!(record.validate().is_err());
    }

    #[test]
    fn memory_rejects_too_many_entities() {
        let mut record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "content",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        record.entities = (0..2000).map(|i| format!("entity_{i}")).collect();
        assert!(record.validate().is_err());
    }

    #[test]
    fn memory_rejects_too_many_tags() {
        let mut record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "content",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        record.tags = (0..2000).map(|i| format!("tag_{i}")).collect();
        assert!(record.validate().is_err());
    }

    #[test]
    fn memory_accepts_max_content() {
        let mut record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "x",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        record.content = "a".repeat(1_000_000);
        assert!(record.validate().is_ok());
    }

    // === HARD QA: BATCH EDGE CASES ===

    #[test]
    fn batch_ingest_empty_items() {
        let result = mcp_call_json(&serde_json::json!({
            "tool": "nextral.batch.ingest",
            "payload_json": serde_json::json!({"items": [], "idempotency_key": "empty_batch"}).to_string()
        }).to_string()).unwrap();
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(v["total"], 0);
        assert_eq!(v["succeeded"], 0);
    }

    #[test]
    fn batch_retrieve_empty_queries() {
        let result = mcp_call_json(
            &serde_json::json!({
                "tool": "nextral.batch.retrieve",
                "payload_json": serde_json::json!({"queries": []}).to_string()
            })
            .to_string(),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(v["total"], 0);
    }

    #[test]
    fn batch_forget_empty_items() {
        let result = mcp_call_json(
            &serde_json::json!({
                "tool": "nextral.batch.forget",
                "payload_json": serde_json::json!({"items": []}).to_string()
            })
            .to_string(),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(v["total"], 0);
    }

    // === HARD QA: IDEMPOTENCY ===

    #[test]
    fn ingest_idempotency_by_memory_id() {
        let r1 = mcp_call_json(
            &serde_json::json!({
                "tool": "nextral.memory.ingest",
                "payload_json": serde_json::json!({
                    "id": "idem_test_1", "tenant_id": "t", "user_id": "u",
                    "content": "first content", "content_type": "note",
                    "memory_type": "semantic", "source_type": "manual",
                    "source_message_ids": [], "importance_score": 0.5,
                    "entities": [], "tags": [], "privacy_level": "private",
                    "graph_hints": [],
                    "policy": {"min_importance_score": 0.0, "min_confidence_score": 0.0}
                }).to_string()
            })
            .to_string(),
        )
        .unwrap();
        let r2 = mcp_call_json(
            &serde_json::json!({
                "tool": "nextral.memory.ingest",
                "payload_json": serde_json::json!({
                    "id": "idem_test_1", "tenant_id": "t", "user_id": "u",
                    "content": "different content", "content_type": "note",
                    "memory_type": "semantic", "source_type": "manual",
                    "source_message_ids": [], "importance_score": 0.5,
                    "entities": [], "tags": [], "privacy_level": "private",
                    "graph_hints": [],
                    "policy": {"min_importance_score": 0.0, "min_confidence_score": 0.0}
                }).to_string()
            })
            .to_string(),
        )
        .unwrap();
        let v1: serde_json::Value = serde_json::from_str(&r1).unwrap();
        let v2: serde_json::Value = serde_json::from_str(&r2).unwrap();
        assert_eq!(v1["record_id"], v2["record_id"]);
    }

    // === HARD QA: RETRIEVAL EDGE CASES ===

    #[test]
    fn retrieval_with_zero_budget_returns_nothing() {
        let mut store = crate::store::TestMemoryStore::new();
        let record = MemoryRecord::new(
            "m1",
            "t",
            "u",
            "Atlas uses PostgreSQL",
            ContentType::Fact,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        store.upsert_memory(record).unwrap();
        let mut req = RetrievalRequest::test("t", "u", "PostgreSQL");
        req.token_budget = 0;
        let response = retrieve(&mut store, req).unwrap();
        assert!(response.items.is_empty());
    }

    #[test]
    fn retrieval_with_large_top_k() {
        let mut store = crate::store::TestMemoryStore::new();
        for i in 0..5 {
            let record = MemoryRecord::new(
                format!("m{i}"),
                "t",
                "u",
                format!("item {i} about PostgreSQL"),
                ContentType::Note,
                MemoryType::Semantic,
                SourceType::Manual,
            );
            store.upsert_memory(record).unwrap();
        }
        let mut req = RetrievalRequest::test("t", "u", "PostgreSQL");
        req.top_k_vector = 100;
        let response = retrieve(&mut store, req).unwrap();
        assert!(response.items.len() <= 5);
    }

    // === HARD QA: FORGET REDACT VERIFICATION ===

    #[test]
    fn forget_redact_clears_entities_and_tags() {
        let mut store = crate::store::TestMemoryStore::new();
        let mut record = MemoryRecord::new(
            "mem_redact",
            "t",
            "u",
            "sensitive data",
            ContentType::Fact,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        record.entities = vec!["entity1".to_string(), "entity2".to_string()];
        record.tags = vec!["tag1".to_string(), "tag2".to_string()];
        store.upsert_memory(record).unwrap();
        forget_memory(
            &mut store,
            ForgetMemoryRequest {
                tenant_id: "t".to_string(),
                user_id: "u".to_string(),
                memory_id: "mem_redact".to_string(),
                actor: "a".to_string(),
                reason: "gdpr".to_string(),
                redact: true,
            },
        )
        .unwrap();
        let loaded = store.get_memory("t", "u", "mem_redact").unwrap().unwrap();
        assert_eq!(loaded.content, "[redacted]");
        assert!(loaded.entities.is_empty());
        assert!(loaded.tags.is_empty());
        assert_eq!(loaded.status, MemoryStatus::Redacted);
    }

    #[test]
    fn forget_soft_delete_preserves_content() {
        let mut store = crate::store::TestMemoryStore::new();
        let record = MemoryRecord::new(
            "mem_soft",
            "t",
            "u",
            "keep this",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        store.upsert_memory(record).unwrap();
        forget_memory(
            &mut store,
            ForgetMemoryRequest {
                tenant_id: "t".to_string(),
                user_id: "u".to_string(),
                memory_id: "mem_soft".to_string(),
                actor: "a".to_string(),
                reason: "user request".to_string(),
                redact: false,
            },
        )
        .unwrap();
        let loaded = store.get_memory("t", "u", "mem_soft").unwrap().unwrap();
        assert_eq!(loaded.content, "keep this");
        assert_eq!(loaded.status, MemoryStatus::SoftDeleted);
    }

    // === HARD QA: SCORING BOUNDARY VALUES ===

    #[test]
    fn lexical_score_single_char_query() {
        let score = crate::scoring::lexical_score("hello world", "h");
        assert!(score > 0.0);
    }

    #[test]
    fn lexical_score_case_insensitive() {
        let s1 = crate::scoring::lexical_score("Hello World", "hello");
        let s2 = crate::scoring::lexical_score("hello world", "HELLO");
        assert!((s1 - s2).abs() < 0.001);
    }

    #[test]
    fn retrieval_score_zero_weights() {
        let weights = crate::config::ScoringWeights {
            semantic_similarity: 0.0,
            recency: 0.0,
            importance: 0.0,
            access: 0.0,
        };
        let score = crate::scoring::retrieval_score(1.0, 1.0, 1.0, 1.0, &weights);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn retrieval_score_max_weights() {
        let weights = crate::config::ScoringWeights {
            semantic_similarity: 1.0,
            recency: 1.0,
            importance: 1.0,
            access: 1.0,
        };
        let score = crate::scoring::retrieval_score(1.0, 1.0, 1.0, 1.0, &weights);
        assert!((score - 4.0).abs() < 0.001);
    }

    // === HARD QA: TOPOLOGY COMPLETENESS ===

    #[test]
    fn every_memory_type_has_required_stores() {
        use crate::topology::{profile, StoreRole};
        let p_sem = profile(&MemoryType::Semantic);
        assert!(p_sem.required_stores.contains(&StoreRole::Qdrant));
        let p_rel = profile(&MemoryType::Relational);
        assert!(p_rel.required_stores.contains(&StoreRole::Neo4j));
        let p_pro = profile(&MemoryType::Prospective);
        assert!(p_pro.required_stores.contains(&StoreRole::Redis));
        let p_ep = profile(&MemoryType::Episodic);
        assert!(p_ep.required_stores.contains(&StoreRole::S3));
    }

    // === HARD QA: MCP TOOL COVERAGE ===

    // === HARD QA: ERROR CONVERSION ===

    #[test]
    fn package_error_from_all_core_error_variants() {
        let cases = vec![
            crate::CoreError::InvalidInput("test".to_string()),
            crate::CoreError::NotFound("test".to_string()),
            crate::CoreError::Conflict("test".to_string()),
            crate::CoreError::Io("test".to_string()),
            crate::CoreError::Serialization("test".to_string()),
        ];
        for err in cases {
            let pkg: crate::package::PackageError = err.into();
            assert!(!pkg.code.is_empty());
            assert!(!pkg.message.is_empty());
        }
    }

    // === HARD QA: SESSION MESSAGE BOUNDARIES ===

    #[test]
    fn session_message_truncates_long_content() {
        let long_content = "x".repeat(200_000);
        let msg = crate::domain::SessionMessage::new("t", "u", "s", "user", long_content, "key");
        assert!(msg.content.len() <= 100_000);
    }

    #[test]
    fn session_message_accepts_normal_content() {
        let msg = crate::domain::SessionMessage::new("t", "u", "s", "user", "hello world", "key");
        assert_eq!(msg.content, "hello world");
    }

    // === HARD QA: DETERMINISTIC ID COLLISION ===

    #[test]
    fn deterministic_id_no_collision_across_types() {
        let id1 = crate::memory::deterministic_id(&["tenant", "user", "content"]);
        let id2 = crate::memory::deterministic_id(&["tenant", "user", "content", "extra"]);
        assert_ne!(id1, id2);
    }

    // === HARD QA: GRAPH EDGE MERGE ===

    #[test]
    fn graph_merge_edge_increases_confidence() {
        let mut store = crate::store::TestMemoryStore::new();
        let e1 = crate::graph::GraphEdge::new("t", "u", "a:X", "USES", "b:Y", 0.3, "mem1").unwrap();
        let e2 = crate::graph::GraphEdge::new("t", "u", "a:X", "USES", "b:Y", 0.9, "mem2").unwrap();
        store.merge_edge(e1).unwrap();
        store.merge_edge(e2).unwrap();
        assert_eq!(store.graph_edges[0].confidence, 0.9);
        assert_eq!(store.graph_edges[0].source_memory_ids.len(), 2);
    }

    // === HARD QA: PROSPECTIVE REMINDER TIMESTAMPS ===

    #[test]
    fn reminder_numeric_timestamp_comparison() {
        let mut store = crate::store::TestMemoryStore::new();
        let r1 = crate::prospective::ReminderRecord::new(
            "t",
            "u",
            "mem1",
            crate::prospective::ReminderKind::FollowUp,
            "early",
            "100",
            "UTC",
        )
        .unwrap();
        let r2 = crate::prospective::ReminderRecord::new(
            "t",
            "u",
            "mem2",
            crate::prospective::ReminderKind::FollowUp,
            "late",
            "999",
            "UTC",
        )
        .unwrap();
        store.upsert_reminder(r1).unwrap();
        store.upsert_reminder(r2).unwrap();
        let due_150 = store.list_due_reminders("t", "u", "150").unwrap();
        assert_eq!(due_150.len(), 1);
        assert_eq!(due_150[0].title, "early");
        let due_all = store.list_due_reminders("t", "u", "9999").unwrap();
        assert_eq!(due_all.len(), 2);
    }

    // === BUG FIX VERIFICATION ===

    #[test]
    fn graph_dedup_cross_tenant() {
        let mut store = crate::store::TestMemoryStore::new();
        let n1 = crate::graph::GraphNode::new("t", "u1", "Entity", "Atlas", 0.8).unwrap();
        let n2 = crate::graph::GraphNode::new("t", "u2", "Entity", "Atlas", 0.9).unwrap();
        let n3 = crate::graph::GraphNode::new("t", "u1", "Entity", "Atlas", 0.7).unwrap();
        store.merge_node(n1).unwrap();
        store.merge_node(n2).unwrap();
        store.merge_node(n3).unwrap();
        assert_eq!(store.graph_nodes.len(), 2);
    }

    #[test]
    fn experiment_ids_are_unique() {
        let mut reg = crate::runtime::intelligence::ExperimentRegistry::default();
        let e1 = reg.create(RuntimeLane::Canary, "v1".to_string(), "test".to_string());
        let e2 = reg.create(RuntimeLane::Canary, "v1".to_string(), "test".to_string());
        assert_ne!(e1.id, e2.id);
    }

    #[test]
    fn config_rejects_negative_weights() {
        let mut config = valid_test_config();
        config.retrieval_policy.scoring_weights.semantic_similarity = -0.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_huge_cache_ttl() {
        let mut config = valid_test_config();
        config.cache.session_ttl_seconds = u64::MAX;
        assert!(config.validate().is_err());
    }

    #[test]
    fn scoring_weights_reject_negative() {
        let weights = crate::config::ScoringWeights {
            semantic_similarity: -0.1,
            recency: 0.5,
            importance: 0.3,
            access: 0.3,
        };
        let result = crate::config::validate_scoring_weights(&weights);
        assert!(result.is_err());
    }

    #[test]
    fn scoring_weights_accept_zero() {
        let weights = crate::config::ScoringWeights {
            semantic_similarity: 0.0,
            recency: 0.0,
            importance: 0.0,
            access: 0.0,
        };
        assert!(crate::config::validate_scoring_weights(&weights).is_ok());
    }

    #[test]
    fn mcp_memory_list_default_privacy_scope() {
        let result = mcp_call_json(
            &serde_json::json!({
                "tool": "nextral.memory.list",
                "payload_json": serde_json::json!({"tenant_id": "t", "user_id": "u"}).to_string()
            })
            .to_string(),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(v["items"].is_array());
    }

    #[test]
    fn mcp_memory_list_with_custom_privacy_scope() {
        let result = mcp_call_json(
            &serde_json::json!({
                "tool": "nextral.memory.list",
                "payload_json": serde_json::json!({
                    "tenant_id": "t", "user_id": "u",
                    "privacy_scope": ["private"],
                    "include_inactive": true
                }).to_string()
            })
            .to_string(),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(v["items"].is_array());
    }

    #[test]
    fn mcp_memory_list_include_inactive() {
        let mut input = IngestMemoryRequest::new(
            "inactive_tenant",
            "inactive_user",
            "inactive memory",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
            IngestionPolicy {
                min_importance_score: 0.0,
                min_confidence_score: 0.0,
            },
        );
        input.id = Some("mem_inactive".into());
        mcp_call_json(&serde_json::json!({"tool":"nextral.memory.ingest","payload_json":serde_json::to_string(&input).unwrap()}).to_string()).unwrap();
        mcp_call_json(&serde_json::json!({"tool":"nextral.memory.forget","payload_json":serde_json::json!({"tenant_id":"inactive_tenant","user_id":"inactive_user","memory_id":"mem_inactive","actor":"inactive_user","reason":"test inactive listing","redact":false}).to_string()}).to_string()).unwrap();
        let result = mcp_call_json(
            &serde_json::json!({
                "tool": "nextral.memory.list",
                "payload_json": serde_json::json!({
                    "tenant_id": "inactive_tenant", "user_id": "inactive_user",
                    "filters": { "statuses": ["active", "soft_deleted"] }
                }).to_string()
            })
            .to_string(),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(!v["items"].as_array().unwrap().is_empty());
    }

    // === ENV VAR SUBSTITUTION ===

    #[test]
    fn env_var_substitution_resolves() {
        std::env::set_var("NEXTRAL_TEST_VAR", "resolved_value");
        let input = "prefix_${NEXTRAL_TEST_VAR}_suffix";
        let result = crate::config::resolve_env_vars(input);
        assert_eq!(result, "prefix_resolved_value_suffix");
        std::env::remove_var("NEXTRAL_TEST_VAR");
    }

    #[test]
    fn env_var_substitution_missing_var_left_as_is() {
        let input = "prefix_${NEXTRAL_NONEXISTENT_VAR_12345}_suffix";
        let result = crate::config::resolve_env_vars(input);
        assert_eq!(result, "prefix_${NEXTRAL_NONEXISTENT_VAR_12345}_suffix");
    }

    #[test]
    fn env_var_substitution_no_vars_unchanged() {
        let input = "no variables here";
        let result = crate::config::resolve_env_vars(input);
        assert_eq!(result, "no variables here");
    }

    #[test]
    fn env_var_substitution_multiple_vars() {
        std::env::set_var("NEXTRAL_A", "alpha");
        std::env::set_var("NEXTRAL_B", "beta");
        let input = "${NEXTRAL_A}-${NEXTRAL_B}";
        let result = crate::config::resolve_env_vars(input);
        assert_eq!(result, "alpha-beta");
        std::env::remove_var("NEXTRAL_A");
        std::env::remove_var("NEXTRAL_B");
    }

    #[test]
    fn load_config_resolves_env_vars() {
        std::env::set_var("NEXTRAL_TEST_MODEL", "test-model");
        let json = r#"{
            "backend": "test_memory",
            "embedding": {"kind": "test", "model": "${NEXTRAL_TEST_MODEL}", "dimension": 128},
            "extraction": {"kind": "test", "model": "test-model"},
            "ingestion_policy": {"min_importance_score": 0.0, "min_confidence_score": 0.0},
            "retrieval_policy": {"privacy_scope": ["private"], "token_budget": 1800, "top_k_vector": 12, "max_graph_hops": 2, "scoring_weights": {"semantic_similarity": 0.5, "recency": 0.2, "importance": 0.2, "access": 0.1}},
            "cache": {"key_prefix": "test", "session_ttl_seconds": 7200, "retrieval_ttl_seconds": 120, "policy_ttl_seconds": 300},
            "service": {},
            "auth": {},
            "observability": {"enabled": false}
        }"#;
        let config = crate::config::load_config(json).unwrap();
        assert_eq!(config.embedding.model, "test-model");
        std::env::remove_var("NEXTRAL_TEST_MODEL");
    }

    // === NEW FEATURES: TAG/ENTITY HELPERS ===

    #[test]
    fn memory_add_remove_tag() {
        let mut record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "content",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        record.add_tag("important");
        assert!(record.has_tag("important"));
        assert!(record.remove_tag("important"));
        assert!(!record.has_tag("important"));
        assert!(!record.remove_tag("nonexistent"));
    }

    #[test]
    fn memory_add_remove_entity() {
        let mut record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "content",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        record.add_entity("PostgreSQL");
        assert!(record.has_entity("PostgreSQL"));
        assert!(record.remove_entity("PostgreSQL"));
        assert!(!record.has_entity("PostgreSQL"));
    }

    #[test]
    fn memory_patch_updates_fields() {
        let mut record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "old content",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        record.importance_score = 0.3;
        record
            .patch(
                Some("new content".to_string()),
                Some(ContentType::Decision),
                Some(0.9),
                None,
                None,
            )
            .unwrap();
        assert_eq!(record.content, "new content");
        assert_eq!(record.content_type, ContentType::Decision);
        assert_eq!(record.importance_score, 0.9);
    }

    #[test]
    fn memory_patch_rejects_empty_content() {
        let mut record = MemoryRecord::new(
            "id",
            "t",
            "u",
            "content",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        assert!(record
            .patch(Some("  ".to_string()), None, None, None, None)
            .is_err());
    }

    // === NEW FEATURES: ENTITY-BASED RETRIEVAL ===

    // === NEW FEATURES: STORE METHODS ===

    #[test]
    fn count_memories_works() {
        let mut store = crate::store::TestMemoryStore::new();
        store
            .upsert_memory(MemoryRecord::new(
                "m1",
                "t",
                "u",
                "c1",
                ContentType::Note,
                MemoryType::Semantic,
                SourceType::Manual,
            ))
            .unwrap();
        store
            .upsert_memory(MemoryRecord::new(
                "m2",
                "t",
                "u",
                "c2",
                ContentType::Note,
                MemoryType::Semantic,
                SourceType::Manual,
            ))
            .unwrap();
        assert_eq!(store.count_memories("t", "u", false).unwrap(), 2);
        assert_eq!(store.count_memories("t", "other", false).unwrap(), 0);
    }

    #[test]
    fn list_entities_and_tags() {
        let mut store = crate::store::TestMemoryStore::new();
        let mut r = MemoryRecord::new(
            "m1",
            "t",
            "u",
            "c",
            ContentType::Note,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        r.entities = vec!["Atlas".to_string(), "PostgreSQL".to_string()];
        r.tags = vec!["important".to_string(), "backend".to_string()];
        store.upsert_memory(r).unwrap();
        let entities = store.list_entities("t", "u").unwrap();
        assert_eq!(entities.len(), 2);
        assert!(entities.contains(&"Atlas".to_string()));
        let tags = store.list_tags("t", "u").unwrap();
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&"important".to_string()));
    }

    // === NEW FEATURES: GRAPH MAX_HOPS ===

    #[test]
    fn graph_max_hops_respected() {
        let mut store = crate::store::TestMemoryStore::new();
        store
            .upsert_memory(MemoryRecord::new(
                "m1",
                "t",
                "u",
                "Atlas uses PostgreSQL",
                ContentType::Fact,
                MemoryType::Semantic,
                SourceType::Manual,
            ))
            .unwrap();
        store
            .upsert_memory(MemoryRecord::new(
                "m2",
                "t",
                "u",
                "PostgreSQL uses Redis",
                ContentType::Fact,
                MemoryType::Semantic,
                SourceType::Manual,
            ))
            .unwrap();
        store
            .merge_node(crate::graph::GraphNode::new("t", "u", "Entity", "Atlas", 0.8).unwrap())
            .unwrap();
        store
            .merge_node(
                crate::graph::GraphNode::new("t", "u", "Entity", "PostgreSQL", 0.8).unwrap(),
            )
            .unwrap();
        store
            .merge_node(crate::graph::GraphNode::new("t", "u", "Entity", "Redis", 0.8).unwrap())
            .unwrap();
        store
            .merge_edge(
                crate::graph::GraphEdge::new(
                    "t",
                    "u",
                    "Entity:atlas",
                    "USES",
                    "Entity:postgresql",
                    0.9,
                    "m1",
                )
                .unwrap(),
            )
            .unwrap();
        store
            .merge_edge(
                crate::graph::GraphEdge::new(
                    "t",
                    "u",
                    "Entity:postgresql",
                    "USES",
                    "Entity:redis",
                    0.9,
                    "m2",
                )
                .unwrap(),
            )
            .unwrap();
        let hop1 = store
            .graph_memory_ids("t", "u", "Atlas", 1, &[PrivacyLevel::Private])
            .unwrap();
        assert!(
            hop1.contains(&"m1".to_string()),
            "hop1 should contain m1, got {:?}",
            hop1
        );
        assert!(!hop1.contains(&"m2".to_string()));
        let hop2 = store
            .graph_memory_ids("t", "u", "Atlas", 2, &[PrivacyLevel::Private])
            .unwrap();
        assert!(hop2.contains(&"m1".to_string()));
        assert!(hop2.contains(&"m2".to_string()));
    }

    // === NEW FEATURES: MCP TOOLS ===

    #[test]
    fn mcp_memory_update_tool() {
        let ingest_result = mcp_call_json(
            &serde_json::json!({
                "tool": "nextral.memory.ingest",
                "payload_json": serde_json::json!({
                    "id": "update_test_1", "tenant_id": "t", "user_id": "u",
                    "content": "original content", "content_type": "note",
                    "memory_type": "semantic", "source_type": "manual",
                    "source_message_ids": [], "importance_score": 0.5,
                    "entities": [], "tags": ["old_tag"], "privacy_level": "private",
                    "graph_hints": [],
                    "policy": {"min_importance_score": 0.0, "min_confidence_score": 0.0}
                }).to_string()
            })
            .to_string(),
        )
        .unwrap();
        assert!(ingest_result.contains("accepted"));

        let update_result = mcp_call_json(
            &serde_json::json!({
                "tool": "nextral.memory.update",
                "payload_json": serde_json::json!({
                    "tenant_id": "t", "user_id": "u", "memory_id": "update_test_1",
                    "content": "updated content",
                    "importance_score": 0.9,
                    "add_tags": ["new_tag"],
                    "remove_tags": ["old_tag"]
                }).to_string()
            })
            .to_string(),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&update_result).unwrap();
        assert_eq!(v["content"], "updated content");
    }

    #[test]
    fn mcp_memory_stats_tool() {
        let result = mcp_call_json(
            &serde_json::json!({
                "tool": "nextral.memory.stats",
                "payload_json": serde_json::json!({"tenant_id": "t", "user_id": "u"}).to_string()
            })
            .to_string(),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(v.get("active_count").is_some());
        assert!(v.get("entity_count").is_some());
        assert!(v.get("tag_count").is_some());
    }


    // === FULL END-TO-END MEMORY LIFECYCLE TEST ===

    #[test]
    fn full_memory_lifecycle_e2e() {
        let mut store = TestMemoryStore::new();

        // === PHASE 1: INGEST ===
        let ingest_response = ingest_memory(
            &mut store,
            IngestMemoryRequest {
                embedding: None,
                id: Some("e2e_mem_1".to_string()),
                tenant_id: "e2e_tenant".to_string(),
                user_id: "e2e_user".to_string(),
                session_id: Some("e2e_session".to_string()),
                content: "Atlas uses PostgreSQL for durable memory storage".to_string(),
                content_type: ContentType::Fact,
                memory_type: MemoryType::Semantic,
                source_type: SourceType::Manual,
                source_message_ids: vec![],
                importance_score: 0.9,
                confidence_score: Some(0.95),
                entities: vec!["Atlas".to_string(), "PostgreSQL".to_string()],
                tags: vec!["backend".to_string(), "database".to_string()],
                privacy_level: PrivacyLevel::Private,
                graph_hints: vec![],
                policy: IngestionPolicy {
                    min_importance_score: 0.0,
                    min_confidence_score: 0.0,
                },
                trace_id: None,
            },
        )
        .unwrap();
        assert_eq!(ingest_response.status, IngestStatus::Accepted);
        let memory_id = ingest_response.record_id.unwrap();
        assert_eq!(memory_id, "e2e_mem_1");

        // === PHASE 2: VERIFY INGESTED RECORD ===
        let mut record = store
            .get_memory("e2e_tenant", "e2e_user", &memory_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            record.content,
            "Atlas uses PostgreSQL for durable memory storage"
        );
        assert_eq!(record.importance_score, 0.9);
        assert_eq!(record.entities, vec!["Atlas", "PostgreSQL"]);
        assert_eq!(record.tags, vec!["backend", "database"]);
        assert_eq!(record.status, MemoryStatus::Active);

        // === PHASE 3: RETRIEVE BY QUERY ===
        let retrieval = retrieve(
            &mut store,
            RetrievalRequest::test("e2e_tenant", "e2e_user", "PostgreSQL"),
        )
        .unwrap();
        assert_eq!(retrieval.items.len(), 1);
        assert_eq!(retrieval.items[0].memory_id, "e2e_mem_1");
        assert!(retrieval.items[0].retrieval_score > 0.0);

        // === PHASE 4: RETRIEVE WITH ENTITY BOOST ===
        let retrieval = retrieve(
            &mut store,
            RetrievalRequest {
                query_vector: None,
                tenant_id: "e2e_tenant".to_string(),
                user_id: "e2e_user".to_string(),
                session_id: None,
                query_text: "backend".to_string(),
                entities: vec!["Atlas".to_string()],
                intent_topic: None,
                token_budget: 1800,
                privacy_scope: vec![PrivacyLevel::Private],
                top_k_vector: 12,
                max_graph_hops: 2,
                lane: None,
                policy_version: None,
                trace_id: None,
                scoring_weights: Some(ScoringWeights {
                    semantic_similarity: 0.5,
                    recency: 0.2,
                    importance: 0.2,
                    access: 0.1,
                }),
                filters: None,
            },
        )
        .unwrap();
        assert!(!retrieval.items.is_empty());

        // === PHASE 5: UPDATE MEMORY ===
        record.add_tag("production");
        record.add_entity("Redis");
        record
            .patch(
                Some("Atlas uses PostgreSQL and Redis".to_string()),
                None,
                Some(0.95),
                None,
                None,
            )
            .unwrap();
        store.upsert_memory(record.clone()).unwrap();

        let updated = store
            .get_memory("e2e_tenant", "e2e_user", &memory_id)
            .unwrap()
            .unwrap();
        assert_eq!(updated.content, "Atlas uses PostgreSQL and Redis");
        assert!(updated.has_tag("production"));
        assert!(updated.has_entity("Redis"));
        assert_eq!(updated.importance_score, 0.95);

        // === PHASE 6: LIST MEMORIES ===
        let all_memories = store
            .list_memories("e2e_tenant", "e2e_user", &[PrivacyLevel::Private], false)
            .unwrap();
        assert_eq!(all_memories.len(), 1);

        // === PHASE 7: COUNT MEMORIES ===
        let count = store
            .count_memories("e2e_tenant", "e2e_user", false)
            .unwrap();
        assert_eq!(count, 1);

        // === PHASE 8: LIST ENTITIES AND TAGS ===
        let entities = store.list_entities("e2e_tenant", "e2e_user").unwrap();
        assert!(entities.contains(&"Atlas".to_string()));
        assert!(entities.contains(&"PostgreSQL".to_string()));
        assert!(entities.contains(&"Redis".to_string()));
        let tags = store.list_tags("e2e_tenant", "e2e_user").unwrap();
        assert!(tags.contains(&"backend".to_string()));
        assert!(tags.contains(&"production".to_string()));

        // === PHASE 9: SEARCH BY TAG ===
        let tagged = store
            .list_memories("e2e_tenant", "e2e_user", &[PrivacyLevel::Private], false)
            .unwrap()
            .into_iter()
            .filter(|r| r.has_tag("backend"))
            .collect::<Vec<_>>();
        assert_eq!(tagged.len(), 1);

        // === PHASE 10: SEARCH BY ENTITY ===
        let by_entity = store
            .list_memories("e2e_tenant", "e2e_user", &[PrivacyLevel::Private], false)
            .unwrap()
            .into_iter()
            .filter(|r| r.has_entity("PostgreSQL"))
            .collect::<Vec<_>>();
        assert_eq!(by_entity.len(), 1);

        // === PHASE 11: GRAPH GRAPHIFICATION ===
        let graph_output = crate::graph::graphify_record(&record, &[]).unwrap();
        assert!(!graph_output.nodes.is_empty());
        assert!(!graph_output.relationships.is_empty());
        crate::graph::merge_graph(&mut store, graph_output).unwrap();

        // === PHASE 12: GRAPH TRAVERSAL ===
        let graph_ids = store
            .graph_memory_ids(
                "e2e_tenant",
                "e2e_user",
                "Atlas",
                1,
                &[PrivacyLevel::Private],
            )
            .unwrap();
        assert!(graph_ids.contains(&memory_id));

        // === PHASE 13: SESSION MANAGEMENT ===
        let session_response = crate::runtime::session::append_session_message(
            &mut store,
            crate::runtime::session::AppendSessionMessageRequest {
                privacy_level: PrivacyLevel::Private,
                tenant_id: "e2e_tenant".to_string(),
                user_id: "e2e_user".to_string(),
                session_id: "e2e_session".to_string(),
                role: "user".to_string(),
                content: "Tell me about Atlas".to_string(),
                idempotency_key: None,
            },
            20,
        )
        .unwrap();
        assert_eq!(session_response.hot_tail_count, 1);

        // === PHASE 14: WORKING CONTEXT ASSEMBLY ===
        let mut ctx_request = RetrievalRequest::test("e2e_tenant", "e2e_user", "Atlas");
        ctx_request.session_id = Some("e2e_session".to_string());
        let context =
            crate::runtime::session::assemble_working_context(&mut store, ctx_request, 20).unwrap();
        assert!(!context.session_tail.is_empty());
        assert!(!context.retrieved_memory_ids.is_empty());

        // === PHASE 15: REMINDER SCHEDULING ===
        let scheduled = crate::runtime::reminders::schedule_reminder(
            &mut store,
            crate::runtime::reminders::ScheduleReminderRequest {
                channel: "webhook".into(),
                allowed_channels: vec!["webhook".into()],
                quiet_hours: None,
                expires_at: None,
                priority: None,
                tenant_id: "e2e_tenant".to_string(),
                user_id: "e2e_user".to_string(),
                source_memory_id: memory_id.clone(),
                kind: crate::prospective::ReminderKind::FollowUp,
                title: "Check Atlas migration".to_string(),
                due_at: "9999999999".to_string(),
                timezone: "UTC".to_string(),
                trace_id: None,
            },
        )
        .unwrap();
        assert_eq!(scheduled.reminder.status, ReminderStatus::Scheduled);

        // === PHASE 16: REMINDER EXECUTION ===
        let due_result = crate::runtime::reminders::execute_due_reminders(
            &mut store,
            crate::runtime::reminders::ExecuteDueRemindersRequest {
                privacy_scope: vec![PrivacyLevel::Private],
                lease_seconds: 300,
                tenant_id: "e2e_tenant".to_string(),
                user_id: "e2e_user".to_string(),
                due_at_or_before: "9999999999".to_string(),
                actor: "system".to_string(),
                retry_delay_seconds: 60,
                max_retries: Some(3),
                dispatch_policy_version: None,
                retry_strategy_id: None,
                trace_id: None,
            },
        )
        .unwrap();
        assert_eq!(due_result.results.len(), 1);
        assert_eq!(due_result.results[0].status, ReminderStatus::Completed);

        // === PHASE 17: CONSOLIDATION ===
        let mut candidate = IngestMemoryRequest::new(
            "e2e_tenant",
            "e2e_user",
            "Atlas uses PostgreSQL",
            ContentType::Fact,
            MemoryType::Semantic,
            SourceType::Manual,
            IngestionPolicy {
                min_importance_score: 0.0,
                min_confidence_score: 0.0,
            },
        );
        candidate.importance_score = 0.9;
        candidate.confidence_score = Some(0.9);
        candidate.source_message_ids = vec![session_response.message_id.clone()];
        let consolidated = crate::runtime::consolidation::consolidate_session(
            &mut store,
            crate::runtime::consolidation::ConsolidationRequest {
                version: "v1".into(),
                summary: Some("Atlas uses PostgreSQL".into()),
                candidates: vec![candidate],
                chunk_messages: 100,
                tenant_id: "e2e_tenant".to_string(),
                user_id: "e2e_user".to_string(),
                session_id: "e2e_session".to_string(),
                lane: ConsolidationLane::Fast,
                policy: IngestionPolicy {
                    min_importance_score: 0.0,
                    min_confidence_score: 0.0,
                },
            },
        )
        .unwrap();
        assert_eq!(consolidated.job.status, "completed");
        assert!(!consolidated.accepted.is_empty());

        // === PHASE 18: FORGET WITH SOFT DELETE ===
        let soft_deleted = forget_memory(
            &mut store,
            ForgetMemoryRequest {
                tenant_id: "e2e_tenant".to_string(),
                user_id: "e2e_user".to_string(),
                memory_id: memory_id.clone(),
                actor: "e2e_user".to_string(),
                reason: "testing forget".to_string(),
                redact: false,
            },
        )
        .unwrap();
        assert_eq!(soft_deleted.transition.from, MemoryStatus::Active);
        assert_eq!(soft_deleted.transition.to, MemoryStatus::SoftDeleted);

        let forgotten = store
            .get_memory("e2e_tenant", "e2e_user", &memory_id)
            .unwrap()
            .unwrap();
        assert_eq!(forgotten.status, MemoryStatus::SoftDeleted);
        assert_eq!(forgotten.content, "Atlas uses PostgreSQL and Redis");

        // === PHASE 19: LIST INCLUDING INACTIVE ===
        let all = store
            .list_memories("e2e_tenant", "e2e_user", &[PrivacyLevel::Private], true)
            .unwrap();
        assert!(!all.is_empty());
        let _active_only = store
            .list_memories("e2e_tenant", "e2e_user", &[PrivacyLevel::Private], false)
            .unwrap();

        // === PHASE 20: RE-INGEST AND REDACT ===
        let re_response = ingest_memory(
            &mut store,
            IngestMemoryRequest {
                embedding: None,
                id: Some("e2e_mem_2".to_string()),
                tenant_id: "e2e_tenant".to_string(),
                user_id: "e2e_user".to_string(),
                session_id: None,
                content: "Redis provides hot caching".to_string(),
                content_type: ContentType::Fact,
                memory_type: MemoryType::Semantic,
                source_type: SourceType::Manual,
                source_message_ids: vec![],
                importance_score: 0.7,
                confidence_score: Some(0.8),
                entities: vec!["Redis".to_string()],
                tags: vec!["cache".to_string()],
                privacy_level: PrivacyLevel::Private,
                graph_hints: vec![],
                policy: IngestionPolicy {
                    min_importance_score: 0.0,
                    min_confidence_score: 0.0,
                },
                trace_id: None,
            },
        )
        .unwrap();
        let mem2_id = re_response.record_id.unwrap();

        let redacted = forget_memory(
            &mut store,
            ForgetMemoryRequest {
                tenant_id: "e2e_tenant".to_string(),
                user_id: "e2e_user".to_string(),
                memory_id: mem2_id.clone(),
                actor: "e2e_user".to_string(),
                reason: "GDPR right to erasure".to_string(),
                redact: true,
            },
        )
        .unwrap();
        assert!(redacted.redaction_transition.is_some());
        let redacted_record = store
            .get_memory("e2e_tenant", "e2e_user", &mem2_id)
            .unwrap()
            .unwrap();
        assert_eq!(redacted_record.content, "[redacted]");
        assert!(redacted_record.entities.is_empty());
        assert!(redacted_record.tags.is_empty());

        // === PHASE 21: STATISTICS ===
        let _active_count = store
            .count_memories("e2e_tenant", "e2e_user", false)
            .unwrap();
        let total_count = store
            .count_memories("e2e_tenant", "e2e_user", true)
            .unwrap();
        assert!(total_count >= 2);

        // === PHASE 22: MEMORY TYPE VALIDATION ===
        let working = MemoryRecord::new(
            "wk1",
            "e2e_tenant",
            "e2e_user",
            "ephemeral note",
            ContentType::Note,
            MemoryType::Working,
            SourceType::Realtime,
        );
        assert!(working.validate().is_ok());

        let invalid_working = MemoryRecord::new(
            "wk2",
            "e2e_tenant",
            "e2e_user",
            "bad working memory",
            ContentType::Note,
            MemoryType::Working,
            SourceType::Manual,
        );
        assert!(invalid_working.validate().is_err());

        // === PHASE 23: REMINDER LIFECYCLE ===
        let mut reminder = crate::prospective::ReminderRecord::new(
            "e2e_tenant",
            "e2e_user",
            "mem_1",
            crate::prospective::ReminderKind::Commitment,
            "Deploy migration",
            "9999999999",
            "UTC",
        )
        .unwrap();
        assert_eq!(reminder.status, ReminderStatus::Scheduled);
        reminder
            .transition(ReminderStatus::Due, "system", "window reached")
            .unwrap();
        assert_eq!(reminder.status, ReminderStatus::Due);
        reminder
            .transition(ReminderStatus::Dispatched, "system", "start")
            .unwrap();
        assert_eq!(reminder.status, ReminderStatus::Dispatched);
        reminder
            .transition(ReminderStatus::Completed, "system", "done")
            .unwrap();
        assert_eq!(reminder.status, ReminderStatus::Completed);
        assert!(!reminder.is_due_visible());

        // === PHASE 24: GRAPH OPERATIONS ===
        let n1 =
            crate::graph::GraphNode::new("e2e_tenant", "e2e_user", "Technology", "PostgreSQL", 0.9)
                .unwrap();
        let n2 = crate::graph::GraphNode::new("e2e_tenant", "e2e_user", "Technology", "Redis", 0.8)
            .unwrap();
        store.merge_node(n1).unwrap();
        store.merge_node(n2).unwrap();
        let edge = crate::graph::GraphEdge::new(
            "e2e_tenant",
            "e2e_user",
            "Technology:postgresql",
            "DEPENDS_ON",
            "Technology:redis",
            0.7,
            "mem_1",
        )
        .unwrap();
        store.merge_edge(edge).unwrap();
        assert!(store.graph_nodes.len() >= 2);
        assert!(!store.graph_edges.is_empty());

        // === PHASE 25: CONFIG VALIDATION ===
        let config = valid_test_config();
        assert!(config.validate().is_ok());

        let mut bad_config = valid_test_config();
        bad_config.retrieval_policy.token_budget = 0;
        assert!(bad_config.validate().is_err());

        // === FINAL: VERIFY STORE INTEGRITY ===
        let final_count = store
            .count_memories("e2e_tenant", "e2e_user", true)
            .unwrap();
        assert!(final_count >= 2);

        println!("E2E lifecycle test passed: 25 phases, all operations verified");
    }
}
