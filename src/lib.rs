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
        retrieval::{retrieve, RetrievalRequest, SourcePath},
        runtime::{
            self,
            consolidation::{consolidate_session, ConsolidationLane, ConsolidationRequest},
            governance::{forget_memory, ForgetMemoryRequest},
            reembed::plan_reembed,
            reminders::{schedule_reminder, ScheduleReminderRequest},
            session::{
                append_session_message, assemble_working_context, AppendSessionMessageRequest,
            },
        },
        store::{MemoryIndexStore, ReminderStore, TestMemoryStore},
        topology::{all_profiles, requires_store, StoreRole},
    };

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
                .list_due_reminders("usr_1", "9999999999")
                .unwrap()
                .len(),
            1
        );
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
                postgres_url: "postgres://nextral".to_string(),
                redis_url: "redis://nextral".to_string(),
                qdrant_url: "http://qdrant:6334".to_string(),
                neo4j_url: "neo4j://neo4j:7687".to_string(),
                s3_endpoint: "http://minio:9000".to_string(),
                s3_bucket: "nextral".to_string(),
                s3_region: "us-east-1".to_string(),
                s3_access_key_env: "NEXTRAL_S3_ACCESS_KEY".to_string(),
                s3_secret_key_env: "NEXTRAL_S3_SECRET_KEY".to_string(),
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
                service_token_env: Some("NEXTRAL_SERVICE_TOKEN".to_string()),
            },
            observability: ObservabilityConfig {
                enabled: false,
                otlp_endpoint: None,
                prometheus_bind: None,
            },
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn session_consolidation_reminder_forget_and_reembed_paths_work() {
        let mut store = TestMemoryStore::new();
        let appended = append_session_message(
            &mut store,
            AppendSessionMessageRequest {
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

        let consolidated = consolidate_session(
            &mut store,
            ConsolidationRequest {
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
                user_id: "usr_1".to_string(),
                source_memory_id: memory_id.clone(),
                kind: crate::prospective::ReminderKind::FollowUp,
                title: "Check Atlas migration".to_string(),
                due_at: "9999999999".to_string(),
                timezone: "configured-by-user".to_string(),
            },
        )
        .unwrap();
        assert_eq!(scheduled.receipts[0].operation, "upsert_reminder");

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
        assert_eq!(forgotten.receipts.len(), 5);

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
}
