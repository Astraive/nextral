use crate::{
    adapters::{
        neo4j::Neo4jAdapter,
        postgres::{audit_write, PostgresAdapter},
        qdrant::QdrantAdapter,
        redis::RedisAdapter,
        s3::S3Adapter,
    },
    config::{IngestionPolicy, NextralConfig, RuntimeBackend},
    contracts::{CoreError, CoreResult},
    domain::{AuditEvent, GraphEdge, GraphNode, ProceduralPolicy, SessionMessage, StoreReceipt},
    memory::{MemoryRecord, MemoryStatus, PrivacyLevel},
    ports::{
        ArchiveObject, ArchiveReceipt, Neo4jPort, ObjectArchivePort, PostgresPort, QdrantPort,
        TenantUserScope, VectorPoint, VectorSearchRequest,
    },
    prospective::ReminderRecord,
    runtime::lifecycle::{timestamp_epoch, worker_owner, LifecycleStore},
    store::{AuditSink, GraphStore, MemoryIndexStore, ReminderStore, SessionStore},
    testkit::TestMemoryStore,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub struct ProductionStore {
    pub config: NextralConfig,
    pub postgres: PostgresAdapter,
    pub redis: RedisAdapter,
    pub qdrant: QdrantAdapter,
    pub neo4j: Neo4jAdapter,
    pub archive: S3Adapter,
    pub collection: String,
}
pub enum RuntimeStore {
    Test(Box<TestMemoryStore>),
    Production(Box<ProductionStore>),
}
#[derive(serde::Serialize, serde::Deserialize)]
struct EmbeddingRoute {
    collection: String,
    dimension: u32,
    provider: String,
    model: String,
}
fn scope(tenant: &str, user: &str) -> TenantUserScope {
    TenantUserScope {
        tenant_id: tenant.into(),
        user_id: user.into(),
    }
}
fn receipt(backend: &str, operation: &str, id: &str, result: CoreResult<()>) -> StoreReceipt {
    StoreReceipt {
        backend: backend.into(),
        operation: operation.into(),
        target_id: Some(id.into()),
        status: match result {
            Ok(()) => "ok".into(),
            Err(e) => format!("failed:{e}"),
        },
    }
}
fn vector(record: &MemoryRecord, values: &[f32]) -> CoreResult<VectorPoint> {
    Ok(VectorPoint {
        memory_id: record.id.clone(),
        tenant_id: record.tenant_id.clone(),
        user_id: record.user_id.clone(),
        vector: values.to_vec(),
        privacy_level: record.privacy_level.clone(),
        status: record.status.clone(),
        content_type: serde_json::to_value(&record.content_type)?
            .as_str()
            .unwrap_or("")
            .into(),
        memory_type: serde_json::to_value(&record.memory_type)?
            .as_str()
            .unwrap_or("")
            .into(),
        schema_version: record.schema_version.clone(),
    })
}
fn validate_vector(values: &[f32], dimension: usize) -> CoreResult<()> {
    if dimension == 0 || values.len() != dimension || values.iter().any(|v| !v.is_finite()) {
        return Err(CoreError::InvalidInput(format!(
            "host vector must contain exactly {dimension} finite values"
        )));
    }
    Ok(())
}
fn references_memory(value: &Value, memory: &MemoryRecord) -> bool {
    match value {
        Value::Object(fields) => fields.iter().any(|(name, value)| match name.as_str() {
            "memory_id" | "record_id" | "source_memory_id" => value.as_str() == Some(&memory.id),
            "memory_ids" | "source_ids" | "checkpoint_memory_ids" => value
                .as_array()
                .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(&memory.id))),
            "source_message_ids" => value.as_array().is_some_and(|ids| {
                ids.iter().any(|id| {
                    id.as_str().is_some_and(|id| {
                        memory.source_message_ids.iter().any(|source| source == id)
                    })
                })
            }),
            _ => references_memory(value, memory),
        }),
        Value::Array(values) => values.iter().any(|value| references_memory(value, memory)),
        _ => false,
    }
}

pub(crate) fn lifecycle_state_references(key: &str, value: &Value, memory: &MemoryRecord) -> bool {
    let owner = [
        value,
        &value["request"],
        &value["summary"],
        &value["reminder"],
    ]
    .into_iter()
    .find(|scope| scope["user_id"].is_string());
    if owner.is_some_and(|scope| {
        scope["user_id"].as_str() != Some(&memory.user_id)
            || scope["tenant_id"]
                .as_str()
                .is_some_and(|tenant| tenant != memory.tenant_id)
    }) {
        return false;
    }
    let same_session = !memory.source_message_ids.is_empty()
        && memory.session_id.as_deref().is_some_and(|session| {
            owner.is_some_and(|scope| scope["session_id"].as_str() == Some(session))
        });
    match key.split('/').next() {
        Some("graph") => {
            value["record"]["tenant_id"].as_str() == Some(&memory.tenant_id)
                && value["record"]["user_id"].as_str() == Some(&memory.user_id)
                && value["record"]["id"].as_str() == Some(&memory.id)
        }
        Some("summary" | "scheduled-job") => {
            owner.is_some() && (same_session || references_memory(value, memory))
        }
        Some("consolidation") => same_session || references_memory(value, memory),
        Some("reembed" | "reminder" | "dispatch") => {
            owner.is_some() && references_memory(value, memory)
        }
        Some("collection-record") => {
            owner.is_some()
                && (value["id"].as_str() == Some(&memory.id) || references_memory(value, memory))
        }
        Some("idempotency") => references_memory(value, memory),
        Some("archive") => {
            owner.is_some()
                && value.get("messages").is_some()
                && (same_session || references_memory(value, memory))
        }
        _ => false,
    }
}

pub(crate) fn scrub_lifecycle_state(
    key: &str,
    mut value: Value,
    memory: &MemoryRecord,
) -> Option<Value> {
    if key.starts_with("forgotten-source/") || key.starts_with("forget/") {
        return Some(value);
    }
    if key.starts_with("scheduled-job/") {
        if value["request"]["trigger"] == "retention" && value["request"]["consolidation"].is_null()
        {
            return Some(value);
        }
        if !matches!(
            value["status"].as_str(),
            Some("completed" | "terminal_failed" | "cancelled")
        ) {
            value["status"] = json!("cancelled");
        }
        value["last_error"] = json!("source forgotten");
        value["request"]["consolidation"] = Value::Null;
        return Some(value);
    }
    if key.starts_with("reembed/") {
        value["report"]["status"] = json!("failed");
        value["report"]["last_error"] = json!("source forgotten");
        for field in ["input_hashes", "source_fingerprints"] {
            if let Some(values) = value[field].as_object_mut() {
                values.remove(&memory.id);
            }
        }
        if let Some(vectors) = value["expected_directions"].as_object_mut() {
            vectors.remove(&memory.id);
        }
        return Some(value);
    }
    if key.starts_with("consolidation/") {
        value["response"]["job"]["status"] = json!("failed");
        value["response"]["job"]["last_error"] = json!("source forgotten");
        return Some(value);
    }
    if key.starts_with("reminder/") || key.starts_with("dispatch/") {
        let reminder = if key.starts_with("reminder/") {
            &mut value["reminder"]
        } else {
            &mut value
        };
        reminder["title"] = json!("[redacted]");
        reminder["details"] = json!("");
        if !matches!(
            reminder["status"].as_str(),
            Some("completed" | "cancelled" | "expired")
        ) {
            reminder["status"] = json!("cancelled");
            reminder["terminal_reason"] = json!("source_unavailable");
        }
        return Some(value);
    }
    if key.starts_with("idempotency/") {
        value["response"] = Value::Null;
        value["invalidated_by_forget"] = json!(true);
        return Some(value);
    }
    None
}

fn archive_source_ids(body: &Value) -> CoreResult<Vec<String>> {
    let mut ids = Vec::new();
    for field in ["source_message_ids", "messages"] {
        let Some(values) = body.get(field) else {
            continue;
        };
        let values = values
            .as_array()
            .ok_or_else(|| CoreError::InvalidInput("archive sources must be arrays".into()))?;
        for value in values {
            let source = if field == "messages" {
                value.get("id")
            } else {
                Some(value)
            };
            let id = source
                .and_then(Value::as_str)
                .filter(|id| !id.trim().is_empty())
                .ok_or_else(|| {
                    CoreError::InvalidInput("archive source message ID required".into())
                })?;
            ids.push(id.to_owned());
        }
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}
fn archived_transcript_sources(body: &Value, tenant: &str, user: &str) -> CoreResult<Vec<String>> {
    let messages = body["messages"]
        .as_array()
        .ok_or_else(|| CoreError::InvalidInput("archive transcript messages required".into()))?;
    if body["tenant_id"].as_str() != Some(tenant)
        || body["user_id"].as_str() != Some(user)
        || body["session_id"].as_str().is_none_or(str::is_empty)
        || messages.is_empty()
        || messages.len() > 100_000
    {
        return Err(CoreError::Conflict(
            "archive transcript scope mismatch".into(),
        ));
    }
    let mut ids = Vec::with_capacity(messages.len());
    for message in messages {
        if message["tenant_id"].as_str() != Some(tenant)
            || message["user_id"].as_str() != Some(user)
            || message["session_id"] != body["session_id"]
            || message["role"]
                .as_str()
                .is_none_or(|role| role.trim().is_empty())
            || message["content"]
                .as_str()
                .is_none_or(|content| content.trim().is_empty())
        {
            return Err(CoreError::Conflict(
                "archive transcript payload or scope mismatch".into(),
            ));
        }
        let id = message["id"]
            .as_str()
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| CoreError::InvalidInput("archive message ID required".into()))?;
        ids.push(id.to_owned());
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

fn required_archive_sources<T>(store: &T, memory: &MemoryRecord) -> CoreResult<Vec<String>>
where
    T: MemoryIndexStore + SessionStore,
{
    if memory.session_id.is_none() && memory.source_message_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut sources = memory.source_message_ids.clone();
    if let Some(session) = &memory.session_id {
        let messages = store.session_tail(&memory.tenant_id, &memory.user_id, session, 100_001)?;
        if messages.len() > 100_000 {
            return Err(CoreError::InvalidInput(
                "retention session exceeds archive bound".into(),
            ));
        }
        sources.extend(messages.into_iter().map(|message| message.id));
    }
    let privacy = [
        PrivacyLevel::Shared,
        PrivacyLevel::Private,
        PrivacyLevel::Sensitive,
        PrivacyLevel::Restricted,
    ];
    if !memory.source_message_ids.is_empty() {
        for derived in store.list_memories(&memory.tenant_id, &memory.user_id, &privacy, true)? {
            if derived
                .source_message_ids
                .iter()
                .any(|id| memory.source_message_ids.contains(id))
            {
                sources.extend(derived.source_message_ids);
            }
        }
    }
    sources.sort_unstable();
    sources.dedup();
    Ok(sources)
}

fn require_source_coverage(
    required: &[String],
    covered: &std::collections::HashSet<String>,
) -> CoreResult<()> {
    if required.iter().any(|source| !covered.contains(source)) {
        return Err(CoreError::InvalidInput(
            "retention requires verified archive source coverage".into(),
        ));
    }
    Ok(())
}

impl RuntimeStore {
    pub fn configured(config: NextralConfig) -> CoreResult<Self> {
        config.validate()?;
        match config.backend {
            RuntimeBackend::TestMemory => Ok(Self::Test(Box::default())),
            RuntimeBackend::ProductionStores => {
                Ok(Self::Production(Box::new(ProductionStore::new(config)?)))
            }
        }
    }
    pub fn readiness(&self) -> Value {
        match self {
            Self::Test(_) => {
                json!({"fail_fast":false,"backends":[{"backend":"test_memory","status":"ready","degraded_signature":null}]})
            }
            Self::Production(s) => {
                let probes = [
                    ("postgres", s.postgres.readiness().map(|_| ())),
                    ("redis", s.redis.readiness().map(|_| ())),
                    ("qdrant", s.qdrant.readiness().map(|_| ())),
                    ("neo4j", s.neo4j.readiness().map(|_| ())),
                    ("object_store", s.archive.readiness().map(|_| ())),
                ];
                let failed = probes.iter().any(|(_, r)| r.is_err());
                json!({"fail_fast":failed,"backends":probes.into_iter().map(|(backend,result)|match result{Ok(())=>json!({"backend":backend,"status":"ready","degraded_signature":null}),Err(e)=>json!({"backend":backend,"status":"unavailable","degraded_signature":e.to_string()})}).collect::<Vec<_>>()})
            }
        }
    }
    pub fn migrate(&self) -> CoreResult<Value> {
        match self {
            Self::Test(_) => Ok(json!({"status":"not_applicable","backend":"test_memory"})),
            Self::Production(s) => {
                s.postgres.migrate()?;
                s.qdrant.ensure_collection(
                    &s.collection,
                    s.config.embedding.dimension,
                    "Cosine",
                )?;
                Ok(json!({"status":"completed","backend":"production_stores"}))
            }
        }
    }
    pub fn reminders(&self, tenant: &str, user: &str) -> CoreResult<Vec<ReminderRecord>> {
        match self {
            Self::Test(s) => Ok(s
                .reminders
                .iter()
                .filter(|r| r.tenant_id == tenant && r.user_id == user)
                .cloned()
                .collect()),
            Self::Production(s) => s.postgres.reminders(&scope(tenant, user)),
        }
    }
    pub fn state_list(&self, tenant: &str, prefix: &str) -> CoreResult<Vec<(String, Value)>> {
        match self {
            Self::Test(s) => Ok(s
                .lifecycle_state
                .iter()
                .filter(|((t, k), _)| t == tenant && k.starts_with(prefix))
                .map(|((_, k), v)| (k.clone(), v.clone()))
                .collect()),
            Self::Production(s) => s.postgres.state_list(tenant, prefix),
        }
    }
    pub fn drain_outbox(&mut self, tenant: &str) -> CoreResult<Value> {
        match self {
            Self::Test(_) => Ok(json!({"completed":0,"failed":0})),
            Self::Production(s) => s.drain_outbox(tenant),
        }
    }
    pub(crate) fn policy_privacy_scope(
        &self,
        requested: Option<Vec<PrivacyLevel>>,
    ) -> Vec<PrivacyLevel> {
        let allowed = match self {
            Self::Test(_) => vec![
                PrivacyLevel::Private,
                PrivacyLevel::Shared,
                PrivacyLevel::Sensitive,
            ],
            Self::Production(s) => s.config.retrieval_policy.privacy_scope.clone(),
        };
        match requested {
            Some(mut scope) => {
                scope.retain(|p| allowed.contains(p));
                scope
            }
            None => allowed,
        }
    }
    pub(crate) fn policy_upsert(
        &mut self,
        mut policy: ProceduralPolicy,
    ) -> CoreResult<(ProceduralPolicy, Vec<StoreReceipt>)> {
        let key = format!("policy/{}/{}", policy.user_id, policy.id);
        let mut audit = AuditEvent::new(
            worker_owner(),
            &policy.tenant_id,
            &policy.user_id,
            crate::domain::AuditAction::WriteAccepted,
            "procedural_policy",
            Some(policy.id.clone()),
            "procedural policy upserted",
        );
        audit.user_id = Some(policy.user_id.clone());
        let backend = match self {
            Self::Test(s) => {
                if let Some(previous) = s
                    .lifecycle_state
                    .get(&(policy.tenant_id.clone(), key.clone()))
                {
                    policy.created_at =
                        serde_json::from_value::<ProceduralPolicy>(previous.clone())?.created_at;
                }
                s.lifecycle_state.insert(
                    (policy.tenant_id.clone(), key),
                    serde_json::to_value(&policy)?,
                );
                s.emit_audit(audit)?;
                "test_memory"
            }
            Self::Production(s) => {
                let canonical = s
                    .postgres
                    .policy_mutate(
                        &scope(&policy.tenant_id, &policy.user_id),
                        &key,
                        Some(&serde_json::to_value(&policy)?),
                        &audit_write(&audit)?,
                    )?
                    .ok_or_else(|| CoreError::Io("policy write was not acknowledged".into()))?;
                policy = serde_json::from_value(canonical)?;
                s.drain_outbox(&policy.tenant_id)?;
                "postgres"
            }
        };
        let receipts = vec![receipt(backend, "policy_upsert", &policy.id, Ok(()))];
        Ok((policy, receipts))
    }
    pub(crate) fn policy_remove(
        &mut self,
        t: &str,
        u: &str,
        id: &str,
    ) -> CoreResult<Vec<StoreReceipt>> {
        let key = format!("policy/{u}/{id}");
        let mut audit = AuditEvent::new(
            worker_owner(),
            t,
            u,
            crate::domain::AuditAction::Transition,
            "procedural_policy",
            Some(id.into()),
            "procedural policy removed",
        );
        audit.user_id = Some(u.into());
        let backend = match self {
            Self::Test(s) => {
                s.lifecycle_state
                    .remove(&(t.into(), key))
                    .ok_or_else(|| CoreError::NotFound("procedural policy not found".into()))?;
                s.emit_audit(audit)?;
                "test_memory"
            }
            Self::Production(s) => {
                s.postgres
                    .policy_mutate(&scope(t, u), &key, None, &audit_write(&audit)?)?;
                s.drain_outbox(t)?;
                "postgres"
            }
        };
        Ok(vec![receipt(backend, "policy_remove", id, Ok(()))])
    }
}
impl ProductionStore {
    pub fn new(config: NextralConfig) -> CoreResult<Self> {
        let stores = config
            .stores
            .as_ref()
            .ok_or_else(|| CoreError::InvalidInput("production stores required".into()))?;
        let collection = stores.qdrant_collection.clone().ok_or_else(|| {
            CoreError::InvalidInput("stores.qdrant_collection is required".into())
        })?;
        let mut redis = RedisAdapter::new(&stores.redis_url, &config.cache.key_prefix)?;
        let mut qdrant = QdrantAdapter::new(&stores.qdrant_url, &collection)?;
        let mut neo4j = Neo4jAdapter::new(&stores.neo4j_url)?;
        let mut archive = S3Adapter::new(
            &stores.s3_endpoint,
            &stores.s3_bucket,
            &stores.s3_region,
            &stores.s3_access_key_env,
            &stores.s3_secret_key_env,
        )?;
        if stores.enforce_tls == Some(true) || stores.transport_profile.as_deref() == Some("strict")
        {
            redis.hardening.require_tls = true;
            qdrant.hardening.require_tls = true;
            neo4j.hardening.require_tls = true;
            archive.hardening.require_tls = true;
        }
        Ok(Self {
            postgres: PostgresAdapter::new(&stores.postgres_url)?,
            redis,
            qdrant,
            neo4j,
            archive,
            config,
            collection,
        })
    }
    fn route(&self, t: &str, u: &str) -> CoreResult<EmbeddingRoute> {
        if let Some(value) = self
            .postgres
            .state_load(t, &format!("alias/{}/{u}", self.collection))?
        {
            return serde_json::from_value(value).map_err(CoreError::from);
        }
        Ok(EmbeddingRoute {
            collection: self.collection.clone(),
            dimension: self.config.embedding.dimension,
            provider: serde_json::to_value(&self.config.embedding.kind)?
                .as_str()
                .unwrap_or("")
                .into(),
            model: self.config.embedding.model.clone(),
        })
    }
    fn protect_record(&self, mut r: MemoryRecord) -> CoreResult<MemoryRecord> {
        for source in &r.source_message_ids {
            if let Some(tombstone) = self.postgres.state_load(
                &r.tenant_id,
                &format!("forgotten-source/{}/{}", r.user_id, source),
            )? {
                if r.status == MemoryStatus::Active {
                    r.status = MemoryStatus::SoftDeleted;
                }
                if tombstone["redact"].as_bool() == Some(true) {
                    r.content = "[redacted]".into();
                    r.entities.clear();
                    r.tags.clear();
                    r.embedding = None;
                }
            }
        }
        Ok(r)
    }
    fn sync_payload(
        &self,
        payload: &Value,
        event_id: &str,
        event_owner: &str,
        graph_only: bool,
    ) -> CoreResult<()> {
        let snapshot: MemoryRecord = serde_json::from_value(payload["record"].clone())?;
        let owner = worker_owner();
        let key = format!("privacy/{}", snapshot.user_id);
        if !self.postgres.claim(
            &snapshot.tenant_id,
            &key,
            &owner,
            timestamp_epoch(&crate::memory::now_timestamp())?,
            300,
        )? {
            return Err(CoreError::Conflict(
                "privacy propagation is leased by another worker".into(),
            ));
        }
        let renew = || -> CoreResult<()> {
            if !self
                .postgres
                .renew_outbox(&snapshot.tenant_id, event_id, event_owner)?
                || !self.postgres.claim(
                    &snapshot.tenant_id,
                    &key,
                    &owner,
                    timestamp_epoch(&crate::memory::now_timestamp())?,
                    300,
                )?
            {
                return Err(CoreError::Conflict("outbox/privacy lease was lost".into()));
            }
            Ok(())
        };
        let result = (|| {
            let r = self
                .get_memory(&snapshot.tenant_id, &snapshot.user_id, &snapshot.id)?
                .ok_or_else(|| CoreError::NotFound("outbox canonical memory missing".into()))?;
            let s = scope(&r.tenant_id, &r.user_id);
            let route = self.route(&r.tenant_id, &r.user_id)?;
            if r.status != MemoryStatus::Active {
                renew()?;
                for collection in self.qdrant.collections()? {
                    renew()?;
                    self.qdrant.delete_scoped(&collection, &s, &r.id)?;
                }
                renew()?;
                self.neo4j.redact_memory_edges(&s, &r.id)?;
            } else {
                if !graph_only {
                    let v = r.embedding.as_deref().ok_or_else(|| {
                        CoreError::InvalidInput("persisted embedding missing".into())
                    })?;
                    validate_vector(v, route.dimension as usize)?;
                    renew()?;
                    self.qdrant
                        .upsert_record(&route.collection, &vector(&r, v)?, &r)?;
                }
                let key = crate::runtime::graphify::graph_state_key(&r.user_id, &r.id);
                if let Some(plan) = self.postgres.state_load(&r.tenant_id, &key)? {
                    if plan["source_fingerprint"].as_str()
                        == Some(crate::runtime::graphify::source_fingerprint(&r)?.as_str())
                    {
                        let nodes: Vec<GraphNode> = serde_json::from_value(plan["nodes"].clone())?;
                        let edges: Vec<GraphEdge> = serde_json::from_value(plan["edges"].clone())?;
                        renew()?;
                        self.neo4j.replace_memory_graph(&s, &r.id, &nodes, &edges)?;
                        renew()?;
                        self.postgres.acknowledge_graph(&r, &plan["revision"])?;
                    }
                }
            }
            renew()?;
            self.redis.invalidate_scope(&s).inspect_err(|_| {
                crate::runtime::observability::record_store_error(
                    crate::runtime::observability::StoreBackend::Redis,
                );
            })?;
            Ok(())
        })();
        let release = self.postgres.release(&snapshot.tenant_id, &key, &owner);
        match result {
            Ok(()) => release,
            Err(error) => {
                let _ = release;
                Err(error)
            }
        }
    }
    fn policy_records(&self, t: &str, u: &str) -> CoreResult<Vec<ProceduralPolicy>> {
        let s = scope(t, u);
        let version = self
            .postgres
            .state_load(t, &format!("policy-version/{u}"))?
            .unwrap_or(Value::Null);
        match self.redis.get_scoped_cache(&s, "policies") {
            Ok(Some(raw)) => {
                if let Ok(cached) = serde_json::from_str::<Value>(&raw) {
                    if cached["version"] == version {
                        if let Ok(records) = serde_json::from_value::<Vec<ProceduralPolicy>>(
                            cached["policies"].clone(),
                        ) {
                            if records.iter().all(|p| p.tenant_id == t && p.user_id == u) {
                                return Ok(records);
                            }
                        }
                    }
                }
            }
            Ok(None) => {}
            Err(_) => crate::runtime::observability::record_store_error(
                crate::runtime::observability::StoreBackend::Redis,
            ),
        }
        let records = self
            .postgres
            .state_list(t, &format!("policy/{u}/"))?
            .into_iter()
            .map(|(_, v)| serde_json::from_value::<ProceduralPolicy>(v).map_err(CoreError::from))
            .collect::<CoreResult<Vec<_>>>()?;
        if records.iter().any(|p| p.tenant_id != t || p.user_id != u) {
            return Err(CoreError::Conflict(
                "persisted policy scope mismatch".into(),
            ));
        }
        let cached = json!({"version":version,"policies":records});
        if self
            .redis
            .put_scoped_cache(
                &s,
                "policies",
                &cached.to_string(),
                self.config.cache.policy_ttl_seconds,
            )
            .is_err()
        {
            crate::runtime::observability::record_store_error(
                crate::runtime::observability::StoreBackend::Redis,
            );
        }
        Ok(records)
    }
    fn drain_outbox(&self, tenant: &str) -> CoreResult<Value> {
        let owner = worker_owner();
        let mut completed = 0;
        let mut failed = 0;
        for _ in 0..100 {
            let Some((id, kind, payload)) = self
                .postgres
                .claim_outbox(tenant, &owner)?
                .into_iter()
                .next()
            else {
                break;
            };
            let result = match kind.as_str() {
                "memory.sync" => self.sync_payload(&payload, &id, &owner, false),
                "graph.replay" => self.sync_payload(&payload, &id, &owner, true),
                "policy.invalidate" => self
                    .redis
                    .invalidate_scope(&scope(
                        payload["tenant_id"].as_str().ok_or_else(|| {
                            CoreError::Serialization("policy invalidation tenant missing".into())
                        })?,
                        payload["user_id"].as_str().ok_or_else(|| {
                            CoreError::Serialization("policy invalidation user missing".into())
                        })?,
                    ))
                    .inspect_err(|_| {
                        crate::runtime::observability::record_store_error(
                            crate::runtime::observability::StoreBackend::Redis,
                        );
                    }),
                _ => Err(CoreError::InvalidInput(format!(
                    "unknown outbox event {kind}"
                ))),
            };
            let error = result.err().map(|e| e.to_string());
            self.postgres
                .finish_outbox(tenant, &id, &owner, error.as_deref())?;
            if error.is_some() {
                failed += 1
            } else {
                completed += 1
            }
        }
        Ok(json!({"completed":completed,"failed":failed}))
    }
    fn mutate(
        &self,
        r: &MemoryRecord,
        nodes: &[GraphNode],
        edges: &[GraphEdge],
        audit: &AuditEvent,
    ) -> CoreResult<Vec<String>> {
        let payload = json!({"record":r,"nodes":nodes,"edges":edges});
        let id =
            self.postgres
                .commit_memory(r, &audit_write(audit)?, &payload, &self.collection)?;
        let _ = self.drain_outbox(&r.tenant_id)?;
        Ok(vec![
            "postgres:memory+audit+outbox:committed".into(),
            format!("outbox:{id}:durable"),
        ])
    }
}
impl MemoryIndexStore for ProductionStore {
    fn prepare_retrieval(&self, r: &mut crate::retrieval::RetrievalRequest) -> CoreResult<()> {
        let route = self.route(&r.tenant_id, &r.user_id)?;
        validate_vector(
            r.query_vector.as_deref().ok_or_else(|| {
                CoreError::InvalidInput("query_vector is required by production runtime".into())
            })?,
            route.dimension as usize,
        )?;
        let p = &self.config.retrieval_policy;
        r.token_budget = r.token_budget.min(p.token_budget);
        r.top_k_vector = r.top_k_vector.min(p.top_k_vector);
        r.max_graph_hops = r.max_graph_hops.min(p.max_graph_hops);
        r.privacy_scope.retain(|v| p.privacy_scope.contains(v));
        r.scoring_weights = Some(p.scoring_weights.clone());
        Ok(())
    }
    fn prepare_embedding(&self, r: &mut MemoryRecord, embedding: Option<&[f32]>) -> CoreResult<()> {
        let route = self.route(&r.tenant_id, &r.user_id)?;
        let values = embedding.ok_or_else(|| {
            CoreError::InvalidInput("embedding is required by production runtime".into())
        })?;
        validate_vector(values, route.dimension as usize)?;
        r.embedding = Some(values.to_vec());
        r.embedding_dim = Some(route.dimension);
        r.embedding_model = Some(route.model);
        r.embedding_provider = Some(route.provider);
        r.extraction_model = Some(self.config.extraction.model.clone());
        r.extraction_provider = Some(
            serde_json::to_value(&self.config.extraction.kind)?
                .as_str()
                .unwrap_or("")
                .into(),
        );
        Ok(())
    }
    fn ingestion_policy(&self, p: &IngestionPolicy) -> IngestionPolicy {
        IngestionPolicy {
            min_importance_score: p
                .min_importance_score
                .max(self.config.ingestion_policy.min_importance_score),
            min_confidence_score: p
                .min_confidence_score
                .max(self.config.ingestion_policy.min_confidence_score),
        }
    }
    fn vector_candidates(
        &self,
        t: &str,
        u: &str,
        _query: &str,
        q: Option<&[f32]>,
        k: usize,
        p: &[PrivacyLevel],
    ) -> CoreResult<Vec<(String, f32)>> {
        let route = self.route(t, u)?;
        let values = q.ok_or_else(|| CoreError::InvalidInput("query_vector required".into()))?;
        validate_vector(values, route.dimension as usize)?;
        self.qdrant
            .search(
                &route.collection,
                &VectorSearchRequest {
                    scope: scope(t, u),
                    query_vector: values.to_vec(),
                    privacy_scope: p.to_vec(),
                    top_k: k.min(1000),
                },
            )
            .map(|hits| hits.into_iter().map(|h| (h.memory_id, h.score)).collect())
    }
    fn commit_ingestion(
        &mut self,
        r: MemoryRecord,
        n: Vec<GraphNode>,
        e: Vec<GraphEdge>,
        a: AuditEvent,
    ) -> CoreResult<Vec<String>> {
        self.mutate(&r, &n, &e, &a)
    }
    fn commit_graph_replay(
        &mut self,
        r: &MemoryRecord,
        nodes: &[GraphNode],
        edges: &[GraphEdge],
        audit: &AuditEvent,
    ) -> CoreResult<Vec<String>> {
        crate::graph::validate_source_graph(&r.tenant_id, &r.user_id, &r.id, nodes, edges)?;
        let payload = json!({"record":r,"nodes":nodes,"edges":edges});
        let id = self
            .postgres
            .commit_graph_replay(r, &audit_write(audit)?, &payload)?;
        let _ = self.drain_outbox(&r.tenant_id)?;
        let mut receipts = vec![
            "postgres:graph+audit+outbox:committed".into(),
            format!("outbox:{id}:durable"),
        ];
        let key = crate::runtime::graphify::graph_state_key(&r.user_id, &r.id);
        if self
            .postgres
            .state_load(&r.tenant_id, &key)?
            .is_some_and(|plan| {
                plan["revision"].is_string() && plan["revision"] == plan["applied_revision"]
            })
        {
            receipts.push("neo4j:source:replaced".into());
        }
        Ok(receipts)
    }
    fn upsert_memory(&mut self, r: MemoryRecord) -> CoreResult<()> {
        let a = AuditEvent {
            id: worker_owner(),
            tenant_id: r.tenant_id.clone(),
            user_id: Some(r.user_id.clone()),
            actor: r.user_id.clone(),
            action: crate::domain::AuditAction::WriteAccepted,
            target_type: "memory".into(),
            target_id: Some(r.id.clone()),
            reason: "memory updated".into(),
            request_id: None,
            trace_id: None,
            metadata_json: "{}".into(),
            created_at: crate::memory::now_timestamp(),
        };
        let output = crate::graph::graphify_record(&r, &[])?;
        self.mutate(&r, &output.nodes, &output.relationships, &a)
            .map(|_| ())
    }
    fn get_memory(&self, t: &str, u: &str, id: &str) -> CoreResult<Option<MemoryRecord>> {
        self.postgres
            .get_memory(&scope(t, u), id)?
            .map(|r| self.protect_record(r))
            .transpose()
    }
    fn get_active_memories(&self, t: &str, u: &str, ids: &[&str]) -> CoreResult<Vec<MemoryRecord>> {
        self.postgres.get_active_memories(&scope(t, u), ids)
    }
    fn list_memories(
        &self,
        t: &str,
        u: &str,
        p: &[PrivacyLevel],
        inactive: bool,
    ) -> CoreResult<Vec<MemoryRecord>> {
        Ok(self
            .postgres
            .list_memories(&scope(t, u))?
            .into_iter()
            .map(|r| self.protect_record(r))
            .collect::<CoreResult<Vec<_>>>()?
            .into_iter()
            .filter(|r| {
                p.contains(&r.privacy_level) && (inactive || r.status == MemoryStatus::Active)
            })
            .collect())
    }
    fn update_memory(&mut self, r: MemoryRecord) -> CoreResult<()> {
        if self.get_memory(&r.tenant_id, &r.user_id, &r.id)?.is_none() {
            return Err(CoreError::NotFound("memory not found".into()));
        }
        self.upsert_memory(r)
    }
    fn record_accesses(&mut self, t: &str, u: &str, ids: &[&str]) -> CoreResult<()> {
        self.postgres.record_accesses(&scope(t, u), ids)
    }
    fn count_memories(&self, t: &str, u: &str, inactive: bool) -> CoreResult<usize> {
        Ok(self
            .postgres
            .list_memories(&scope(t, u))?
            .iter()
            .filter(|r| inactive || r.status == MemoryStatus::Active)
            .count())
    }
    fn list_entities(&self, t: &str, u: &str) -> CoreResult<Vec<String>> {
        let mut v: Vec<_> = self
            .postgres
            .list_memories(&scope(t, u))?
            .into_iter()
            .filter(|r| r.status == MemoryStatus::Active)
            .flat_map(|r| r.entities)
            .collect();
        v.sort();
        v.dedup();
        Ok(v)
    }
    fn list_tags(&self, t: &str, u: &str) -> CoreResult<Vec<String>> {
        let mut v: Vec<_> = self
            .postgres
            .list_memories(&scope(t, u))?
            .into_iter()
            .filter(|r| r.status == MemoryStatus::Active)
            .flat_map(|r| r.tags)
            .collect();
        v.sort();
        v.dedup();
        Ok(v)
    }
}
impl GraphStore for ProductionStore {
    fn merge_node(&mut self, n: GraphNode) -> CoreResult<()> {
        self.neo4j.merge_node(&n)
    }
    fn merge_edge(&mut self, e: GraphEdge) -> CoreResult<()> {
        self.neo4j.merge_edge(&e)
    }
    fn graph_memory_ids(
        &self,
        t: &str,
        u: &str,
        q: &str,
        h: u8,
        p: &[PrivacyLevel],
    ) -> CoreResult<Vec<String>> {
        let entities = q.split_whitespace().map(str::to_owned).collect::<Vec<_>>();
        let scope = scope(t, u);
        let ids = self.neo4j.related_memory_ids(&scope, &entities, h)?;
        self.postgres.current_graph_memory_ids(&scope, &ids, p)
    }
}
impl AuditSink for ProductionStore {
    fn emit_audit(&mut self, e: AuditEvent) -> CoreResult<()> {
        self.postgres.write_audit(&audit_write(&e)?)
    }
}
impl SessionStore for ProductionStore {
    fn eligible_sessions(
        &self,
        t: &str,
        u: &str,
        cutoff: &str,
        limit: usize,
    ) -> CoreResult<Vec<crate::domain::EligibleSession>> {
        self.postgres.eligible_sessions(&scope(t, u), cutoff, limit)
    }
    fn append_session_message(&mut self, m: SessionMessage) -> CoreResult<()> {
        self.postgres.append_message(&m)?;
        let s = scope(&m.tenant_id, &m.user_id);
        let tail = self.postgres.session_tail(&s, &m.session_id, 20)?;
        let values = tail
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()?;
        if self
            .redis
            .hot_tail_replace(
                &s,
                &m.session_id,
                &values,
                self.config.cache.session_ttl_seconds,
            )
            .is_err()
        {
            crate::runtime::observability::record_store_error(
                crate::runtime::observability::StoreBackend::Redis,
            );
        }
        Ok(())
    }
    fn hot_tail_status(&self, t: &str, u: &str, id: &str) -> CoreResult<(usize, Vec<String>)> {
        let s = scope(t, u);
        let (head, count) = self.postgres.session_head(&s, id)?;
        match self.redis.hot_tail_read(&s, id, 20) {
            Ok(values) => {
                let tail = values
                    .into_iter()
                    .map(|v| serde_json::from_str::<SessionMessage>(&v))
                    .collect::<Result<Vec<_>, _>>()?;
                if tail.len() == count && tail.last().map(|m| &m.id) == head.as_ref() {
                    Ok((count, Vec::new()))
                } else {
                    Ok((
                        0,
                        vec!["redis_hot_tail_missing_or_stale:warm_fallback".into()],
                    ))
                }
            }
            Err(_) => {
                crate::runtime::observability::record_store_error(
                    crate::runtime::observability::StoreBackend::Redis,
                );
                Ok((0, vec!["redis_unavailable:warm_fallback".into()]))
            }
        }
    }
    fn session_tail(
        &self,
        t: &str,
        u: &str,
        id: &str,
        limit: usize,
    ) -> CoreResult<Vec<SessionMessage>> {
        Ok(self.session_tail_with_status(t, u, id, limit)?.0)
    }
    fn session_tail_with_status(
        &self,
        t: &str,
        u: &str,
        id: &str,
        limit: usize,
    ) -> CoreResult<(Vec<SessionMessage>, Vec<String>)> {
        if limit == 0 {
            return Ok((Vec::new(), Vec::new()));
        }
        let s = scope(t, u);
        let mut degraded_reasons = Vec::new();
        if limit <= 20 {
            if let Ok(values) = self.redis.hot_tail_read(&s, id, 20) {
                if let Ok(mut tail) = values
                    .into_iter()
                    .map(|v| serde_json::from_str::<SessionMessage>(&v))
                    .collect::<Result<Vec<_>, _>>()
                {
                    let (head, count) = self.postgres.session_head(&s, id)?;
                    if tail.len() == count
                        && tail.last().map(|m| &m.id) == head.as_ref()
                        && tail
                            .iter()
                            .all(|m| m.tenant_id == t && m.user_id == u && m.session_id == id)
                    {
                        if tail.len() > limit {
                            tail.drain(..tail.len() - limit);
                        }
                        return Ok((tail, degraded_reasons));
                    }
                }
                degraded_reasons.push("redis_hot_tail_missing_or_stale:warm_fallback".into());
            } else {
                crate::runtime::observability::record_store_error(
                    crate::runtime::observability::StoreBackend::Redis,
                );
                degraded_reasons.push("redis_unavailable:warm_fallback".into());
            }
        }
        Ok((self.postgres.session_tail(&s, id, limit)?, degraded_reasons))
    }
}
impl ReminderStore for ProductionStore {
    fn upsert_reminder(&mut self, r: ReminderRecord) -> CoreResult<()> {
        self.postgres.upsert_reminder(&r)
    }
    fn list_due_reminders(
        &self,
        t: &str,
        u: &str,
        cutoff: &str,
    ) -> CoreResult<Vec<ReminderRecord>> {
        let now = crate::runtime::lifecycle::timestamp_nanos(cutoff)?;
        self.postgres
            .reminders(&scope(t, u))?
            .into_iter()
            .filter(|r| r.is_due_visible())
            .filter_map(|r| {
                match crate::runtime::lifecycle::timestamp_nanos(
                    r.next_attempt_at.as_deref().unwrap_or(&r.due_at),
                ) {
                    Ok(v) if v <= now => Some(Ok(r)),
                    Ok(_) => None,
                    Err(e) => Some(Err(e)),
                }
            })
            .collect()
    }
}

macro_rules! dispatch {($trait:ident {$($name:ident($receiver:ty $(,$arg:ident:$ty:ty)*)->$ret:ty;)*})=>{impl $trait for RuntimeStore{$(fn $name(self:$receiver $(,$arg:$ty)*)->$ret{match self{RuntimeStore::Test(s)=>s.$name($($arg),*),RuntimeStore::Production(s)=>s.$name($($arg),*)}})*}}}
dispatch!(MemoryIndexStore {
 prepare_retrieval(&Self,r:&mut crate::retrieval::RetrievalRequest)->CoreResult<()>;
 prepare_embedding(&Self,r:&mut MemoryRecord,v:Option<&[f32]>)->CoreResult<()>;
 ingestion_policy(&Self,p:&IngestionPolicy)->IngestionPolicy;
 vector_candidates(&Self,t:&str,u:&str,q:&str,v:Option<&[f32]>,k:usize,p:&[PrivacyLevel])->CoreResult<Vec<(String,f32)>>;
 commit_ingestion(&mut Self,r:MemoryRecord,n:Vec<GraphNode>,e:Vec<GraphEdge>,a:AuditEvent)->CoreResult<Vec<String>>;
 commit_graph_replay(&mut Self,r:&MemoryRecord,n:&[GraphNode],e:&[GraphEdge],a:&AuditEvent)->CoreResult<Vec<String>>;
 upsert_memory(&mut Self,r:MemoryRecord)->CoreResult<()>;
 get_memory(&Self,t:&str,u:&str,id:&str)->CoreResult<Option<MemoryRecord>>;
 get_active_memories(&Self,t:&str,u:&str,ids:&[&str])->CoreResult<Vec<MemoryRecord>>;
 list_memories(&Self,t:&str,u:&str,p:&[PrivacyLevel],inactive:bool)->CoreResult<Vec<MemoryRecord>>;
 update_memory(&mut Self,r:MemoryRecord)->CoreResult<()>;
 record_accesses(&mut Self,t:&str,u:&str,ids:&[&str])->CoreResult<()>;
 count_memories(&Self,t:&str,u:&str,inactive:bool)->CoreResult<usize>;
 list_entities(&Self,t:&str,u:&str)->CoreResult<Vec<String>>;
 list_tags(&Self,t:&str,u:&str)->CoreResult<Vec<String>>;
});
dispatch!(GraphStore {merge_node(&mut Self,n:GraphNode)->CoreResult<()>;merge_edge(&mut Self,e:GraphEdge)->CoreResult<()>;graph_memory_ids(&Self,t:&str,u:&str,q:&str,h:u8,p:&[PrivacyLevel])->CoreResult<Vec<String>>;});
dispatch!(AuditSink {emit_audit(&mut Self,e:AuditEvent)->CoreResult<()>;});
dispatch!(SessionStore {eligible_sessions(&Self,t:&str,u:&str,c:&str,l:usize)->CoreResult<Vec<crate::domain::EligibleSession>>;hot_tail_status(&Self,t:&str,u:&str,s:&str)->CoreResult<(usize,Vec<String>)>;append_session_message(&mut Self,m:SessionMessage)->CoreResult<()>;session_tail(&Self,t:&str,u:&str,s:&str,l:usize)->CoreResult<Vec<SessionMessage>>;session_tail_with_status(&Self,t:&str,u:&str,s:&str,l:usize)->CoreResult<(Vec<SessionMessage>,Vec<String>)>;});
dispatch!(ReminderStore {upsert_reminder(&mut Self,r:ReminderRecord)->CoreResult<()>;list_due_reminders(&Self,t:&str,u:&str,c:&str)->CoreResult<Vec<ReminderRecord>>;});

impl LifecycleStore for TestMemoryStore {
    fn lifecycle_load(&self, t: &str, k: &str) -> CoreResult<Option<Value>> {
        Ok(self.lifecycle_state.get(&(t.into(), k.into())).cloned())
    }
    fn lifecycle_list(&self, t: &str, p: &str) -> CoreResult<Vec<Value>> {
        Ok(self
            .lifecycle_state
            .iter()
            .filter(|((tenant, k), _)| tenant == t && k.starts_with(p))
            .map(|(_, v)| v.clone())
            .collect())
    }
    fn lifecycle_save(&mut self, t: &str, k: &str, v: &Value) -> CoreResult<StoreReceipt> {
        self.lifecycle_state.insert((t.into(), k.into()), v.clone());
        Ok(receipt("test_memory", "save", k, Ok(())))
    }
    fn lifecycle_save_claimed(
        &mut self,
        t: &str,
        k: &str,
        o: &str,
        v: &Value,
    ) -> CoreResult<Option<Value>> {
        let key = (t.into(), k.into());
        let now = timestamp_epoch(&crate::memory::now_timestamp())?;
        if !self
            .lifecycle_leases
            .get(&key)
            .is_some_and(|(owner, until)| owner == o && *until > now)
        {
            return Err(CoreError::Conflict("scheduled job claim was lost".into()));
        }
        let current = self
            .lifecycle_state
            .get(&key)
            .ok_or_else(|| CoreError::NotFound("scheduled job state missing".into()))?;
        if matches!(
            current["status"].as_str(),
            Some("completed" | "terminal_failed" | "cancelled")
        ) || current.get("request") != v.get("request")
        {
            return Ok(Some(current.clone()));
        }
        self.lifecycle_state.insert(key, v.clone());
        Ok(None)
    }
    fn lifecycle_claim(
        &mut self,
        t: &str,
        k: &str,
        o: &str,
        now: u64,
        lease: u64,
    ) -> CoreResult<bool> {
        let key = (t.into(), k.into());
        if let Some((owner, until)) = self.lifecycle_leases.get(&key) {
            if owner != o && *until > now {
                return Ok(false);
            }
        }
        self.lifecycle_leases
            .insert(key, (o.into(), now.saturating_add(lease)));
        Ok(true)
    }
    fn lifecycle_release(&mut self, t: &str, k: &str, o: &str) -> CoreResult<()> {
        let key = (t.into(), k.into());
        if self
            .lifecycle_leases
            .get(&key)
            .is_some_and(|(owner, _)| owner == o)
        {
            self.lifecycle_leases.remove(&key);
        }
        Ok(())
    }
    fn lifecycle_archive(&mut self, t: &str, k: &str, v: &Value) -> CoreResult<StoreReceipt> {
        self.lifecycle_save(t, &format!("archive/{k}"), v)
    }
    fn lifecycle_verify_archive_sources(&self, memory: &MemoryRecord) -> CoreResult<()> {
        let required = required_archive_sources(self, memory)?;
        if required.is_empty() {
            return Ok(());
        }
        let mut covered = std::collections::HashSet::new();
        for ((tenant, key), body) in &self.lifecycle_state {
            if tenant == &memory.tenant_id
                && key.starts_with("archive/")
                && body["tenant_id"].as_str() == Some(&memory.tenant_id)
                && body["user_id"].as_str() == Some(&memory.user_id)
                && body["messages"].is_array()
            {
                covered.extend(archived_transcript_sources(
                    body,
                    &memory.tenant_id,
                    &memory.user_id,
                )?);
            }
        }
        require_source_coverage(&required, &covered)
    }
    fn lifecycle_forget(
        &mut self,
        t: &str,
        u: &str,
        id: &str,
        redact: bool,
    ) -> CoreResult<Vec<StoreReceipt>> {
        let record = self
            .memories
            .iter()
            .find(|r| r.tenant_id == t && r.user_id == u && r.id == id)
            .cloned()
            .ok_or_else(|| CoreError::NotFound("memory not found".into()))?;
        let mut affected = vec![record.clone()];
        for memory in &mut self.memories {
            if memory.tenant_id == t
                && memory.user_id == u
                && memory.id != id
                && memory
                    .source_message_ids
                    .iter()
                    .any(|source| record.source_message_ids.contains(source))
            {
                if matches!(memory.status, MemoryStatus::Active | MemoryStatus::Archived) {
                    memory.transition(MemoryStatus::SoftDeleted, u, "source forgotten")?;
                }
                if redact && memory.status == MemoryStatus::SoftDeleted {
                    memory.transition(MemoryStatus::Redacted, u, "source redacted")?;
                }
                affected.push(memory.clone());
            }
        }
        self.graph_edges.retain(|e| {
            !(e.tenant_id == t
                && e.user_id == u
                && e.source_memory_ids
                    .iter()
                    .any(|id| affected.iter().any(|r| &r.id == id)))
        });
        self.session_messages.retain(|m| {
            !(m.tenant_id == t && m.user_id == u && record.source_message_ids.contains(&m.id))
        });
        self.lifecycle_vectors
            .retain(|(tenant, user, _, memory), _| {
                tenant != t || user != u || !affected.iter().any(|r| &r.id == memory)
            });
        for memory in &affected {
            self.lifecycle_state.retain(|(tenant, k), v| {
                if tenant != t {
                    return true;
                }
                if !lifecycle_state_references(k, v, memory) {
                    return true;
                }
                match scrub_lifecycle_state(k, std::mem::take(v), memory) {
                    Some(value) => {
                        *v = value;
                        true
                    }
                    None => false,
                }
            });
        }
        for reminder in self.reminders.iter_mut().filter(|r| {
            r.tenant_id == t
                && r.user_id == u
                && affected.iter().any(|m| m.id == r.source_memory_id)
        }) {
            reminder.title = "[redacted]".into();
            reminder.details.clear();
            if !matches!(
                reminder.status,
                crate::prospective::ReminderStatus::Completed
                    | crate::prospective::ReminderStatus::Cancelled
                    | crate::prospective::ReminderStatus::Expired
            ) {
                reminder.status = crate::prospective::ReminderStatus::Cancelled;
                reminder.terminal_reason =
                    Some(crate::domain::reminder::ReminderTerminalReason::SourceUnavailable);
            }
        }
        Ok(vec![
            receipt("test_memory", "forget", id, Ok(())),
            receipt("test_graph", "forget", id, Ok(())),
        ])
    }
    fn lifecycle_policies(&self, t: &str, u: &str) -> CoreResult<Vec<ProceduralPolicy>> {
        self.lifecycle_state
            .iter()
            .filter(|((tenant, k), _)| tenant == t && k.starts_with(&format!("policy/{u}/")))
            .map(|(_, v)| serde_json::from_value(v.clone()).map_err(CoreError::from))
            .collect()
    }
    fn lifecycle_dispatch(&mut self, r: &ReminderRecord) -> CoreResult<StoreReceipt> {
        self.lifecycle_save(
            &r.tenant_id,
            &format!("dispatch/{}", r.dedupe_key),
            &serde_json::to_value(r)?,
        )?;
        Ok(receipt("test_host", "dispatch", &r.id, Ok(())))
    }
    fn lifecycle_reembed_write(
        &mut self,
        t: &str,
        c: &str,
        m: &MemoryRecord,
        v: &[f32],
    ) -> CoreResult<StoreReceipt> {
        if m.tenant_id != t {
            return Err(CoreError::InvalidInput("reembed tenant mismatch".into()));
        }
        validate_vector(v, v.len())?;
        self.lifecycle_vectors.insert(
            (t.into(), m.user_id.clone(), c.into(), m.id.clone()),
            v.to_vec(),
        );
        Ok(receipt("test_vector", "upsert", &m.id, Ok(())))
    }
    fn lifecycle_reembed_count(&self, t: &str, u: &str, c: &str) -> CoreResult<usize> {
        Ok(self
            .lifecycle_vectors
            .keys()
            .filter(|(tenant, user, collection, _)| tenant == t && user == u && collection == c)
            .count())
    }
    fn lifecycle_reembed_read(
        &self,
        t: &str,
        u: &str,
        c: &str,
        id: &str,
    ) -> CoreResult<Option<Vec<f32>>> {
        Ok(self
            .lifecycle_vectors
            .get(&(t.into(), u.into(), c.into(), id.into()))
            .cloned())
    }
    fn lifecycle_reembed_cutover(
        &mut self,
        t: &str,
        u: &str,
        s: &str,
        shadow: &str,
    ) -> CoreResult<StoreReceipt> {
        self.lifecycle_save(t, &format!("alias/{s}/{u}"), &json!(shadow))
    }
}
impl LifecycleStore for ProductionStore {
    fn lifecycle_list(&self, t: &str, p: &str) -> CoreResult<Vec<Value>> {
        Ok(self
            .postgres
            .state_list(t, p)?
            .into_iter()
            .map(|(_, v)| v)
            .collect())
    }
    fn lifecycle_load(&self, t: &str, k: &str) -> CoreResult<Option<Value>> {
        self.postgres.state_load(t, k)
    }
    fn lifecycle_save(&mut self, t: &str, k: &str, v: &Value) -> CoreResult<StoreReceipt> {
        self.postgres.state_save(t, k, v)?;
        Ok(receipt("postgres", "save", k, Ok(())))
    }
    fn lifecycle_save_claimed(
        &mut self,
        t: &str,
        k: &str,
        o: &str,
        v: &Value,
    ) -> CoreResult<Option<Value>> {
        self.postgres.state_save_claimed(t, k, o, v)
    }
    fn lifecycle_claim(&mut self, t: &str, k: &str, o: &str, n: u64, l: u64) -> CoreResult<bool> {
        self.postgres.claim(t, k, o, n, l)
    }
    fn lifecycle_release(&mut self, t: &str, k: &str, o: &str) -> CoreResult<()> {
        self.postgres.release(t, k, o)
    }
    fn lifecycle_archive(&mut self, t: &str, k: &str, v: &Value) -> CoreResult<StoreReceipt> {
        let user = v
            .get("user_id")
            .and_then(Value::as_str)
            .ok_or_else(|| CoreError::InvalidInput("archive body user_id required".into()))?;
        crate::store::validate_scope_component(user, "user_id")?;
        let session = v
            .get("session_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if v["tenant_id"].as_str() != Some(t) {
            return Err(CoreError::InvalidInput(
                "archive body tenant mismatch".into(),
            ));
        }
        let sources = archive_source_ids(v)?;
        let bytes = serde_json::to_vec(v)?;
        let hash = format!("{:x}", Sha256::digest(&bytes));
        let object = ArchiveObject {
            tenant_id: t.into(),
            user_id: user.into(),
            session_id: session.clone(),
            memory_id: v["memory_id"].as_str().map(str::to_owned),
            object_kind: format!("lifecycle-{}", crate::memory::deterministic_id(&[k])),
            content_sha256: hash,
            bytes,
        };
        let archive = self.archive.receipt_for_object(&object)?;
        let manifest_key = format!("archive/{k}/{}", archive.content_sha256);
        let mut manifest = json!({"receipt":archive,"user_id":user,"session_id":session,
            "memory_id":object.memory_id,"source_message_ids":sources,"upload_status":"pending"});
        self.postgres.state_save(t, &manifest_key, &manifest)?;
        self.archive.put_object(&object)?;
        manifest["upload_status"] = json!("stored");
        self.postgres.state_save(t, &manifest_key, &manifest)?;
        Ok(receipt(
            "object_store",
            "archive",
            &archive.object_key,
            Ok(()),
        ))
    }
    fn lifecycle_verify_archive_sources(&self, memory: &MemoryRecord) -> CoreResult<()> {
        let required = required_archive_sources(self, memory)?;
        if required.is_empty() {
            return Ok(());
        }
        let mut covered = std::collections::HashSet::new();
        for (_, manifest) in self.postgres.state_list(&memory.tenant_id, "archive/")? {
            if manifest["user_id"].as_str() != Some(&memory.user_id)
                || manifest["tombstoned"] == true
                || manifest["upload_status"] != "stored"
            {
                continue;
            }
            let listed = archive_source_ids(&manifest)?;
            if !listed
                .iter()
                .any(|source| required.binary_search(source).is_ok())
            {
                continue;
            }
            let archive: ArchiveReceipt = serde_json::from_value(manifest["receipt"].clone())?;
            let bytes = self
                .archive
                .get_verified_object(&memory.tenant_id, &archive)?;
            let body: Value = serde_json::from_slice(&bytes)?;
            if body["session_id"] != manifest["session_id"] {
                return Err(CoreError::Conflict("archive source scope mismatch".into()));
            }
            let actual = archived_transcript_sources(&body, &memory.tenant_id, &memory.user_id)?;
            if actual != listed {
                return Err(CoreError::Conflict(
                    "archive source manifest mismatch".into(),
                ));
            }
            covered.extend(actual);
        }
        require_source_coverage(&required, &covered)
    }
    fn lifecycle_forget(
        &mut self,
        t: &str,
        u: &str,
        id: &str,
        redact: bool,
    ) -> CoreResult<Vec<StoreReceipt>> {
        let s = scope(t, u);
        let record = self
            .postgres
            .get_memory(&s, id)?
            .ok_or_else(|| CoreError::NotFound("memory not found".into()))?;
        let mut receipts = Vec::new();
        let mut affected = vec![record.clone()];
        let mut cascade_error = None;
        for mut derived in self.postgres.list_memories(&s)?.into_iter().filter(|r| {
            r.id != id
                && r.source_message_ids
                    .iter()
                    .any(|source| record.source_message_ids.contains(source))
        }) {
            if matches!(
                derived.status,
                MemoryStatus::Active | MemoryStatus::Archived
            ) {
                derived.transition(MemoryStatus::SoftDeleted, u, "source forgotten")?;
            }
            if redact && derived.status == MemoryStatus::SoftDeleted {
                derived.transition(MemoryStatus::Redacted, u, "source redacted")?;
            }
            if let Err(error) = self.upsert_memory(derived.clone()) {
                cascade_error = Some(error);
            }
            affected.push(derived);
        }
        let vectors = (|| {
            for collection in self.qdrant.collections()? {
                for memory in &affected {
                    self.qdrant.delete_scoped(&collection, &s, &memory.id)?;
                }
            }
            Ok(())
        })();
        receipts.push(receipt("qdrant", "forget", id, vectors));
        let graph = (|| {
            for memory in &affected {
                self.neo4j.redact_memory_edges(&s, &memory.id)?;
            }
            Ok(())
        })();
        receipts.push(receipt("neo4j", "forget", id, graph));
        receipts.push(receipt(
            "redis",
            "forget",
            id,
            self.redis.invalidate_scope(&s),
        ));
        let archives = (|| {
            for (k, mut v) in self.postgres.state_list(t, "archive/")? {
                if v["user_id"].as_str() != Some(u) || v["tombstoned"] == true {
                    continue;
                }
                let archive: ArchiveReceipt = serde_json::from_value(v["receipt"].clone())?;
                if v.get("source_message_ids").is_none() {
                    let bytes = match self.archive.get_verified_object(t, &archive) {
                        Ok(bytes) => bytes,
                        Err(CoreError::NotFound(_)) => {
                            self.postgres
                                .state_save(t, &k, &json!({"tombstoned":true}))?;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    let body: Value = serde_json::from_slice(&bytes)?;
                    if body["tenant_id"].as_str() != Some(t) || body["user_id"].as_str() != Some(u)
                    {
                        return Err(CoreError::Conflict("archive body scope mismatch".into()));
                    }
                    v["source_message_ids"] = json!(archive_source_ids(&body)?);
                    self.postgres.state_save(t, &k, &v)?;
                }
                if !v["source_message_ids"].is_array() {
                    return Err(CoreError::InvalidInput(
                        "archive source manifest is invalid".into(),
                    ));
                }
                if affected.iter().any(|memory| {
                    memory
                        .session_id
                        .as_deref()
                        .is_some_and(|session| v["session_id"].as_str() == Some(session))
                        || references_memory(&v, memory)
                }) {
                    self.archive.tombstone_object(
                        t,
                        &archive.object_key,
                        "forget source propagation",
                    )?;
                    self.postgres
                        .state_save(t, &k, &json!({"tombstoned":true}))?;
                }
            }
            Ok(())
        })();
        receipts.push(receipt("object_store", "forget", id, archives));
        let canonical = (|| {
            for memory in &affected {
                self.postgres.scrub_sources(&s, memory)?;
            }
            if let Some(error) = cascade_error {
                return Err(error);
            }
            Ok(())
        })();
        receipts.push(receipt("postgres", "forget_sources", id, canonical));
        Ok(receipts)
    }
    fn lifecycle_policies(&self, t: &str, u: &str) -> CoreResult<Vec<ProceduralPolicy>> {
        self.policy_records(t, u)
    }
    fn lifecycle_dispatch(&mut self, r: &ReminderRecord) -> CoreResult<StoreReceipt> {
        if r.channel != "webhook" {
            return Err(CoreError::InvalidInput(
                "only the configured webhook channel can dispatch".into(),
            ));
        }
        let endpoint = std::env::var("NEXTRAL_REMINDER_WEBHOOK_URL").map_err(|_| {
            CoreError::InvalidInput("NEXTRAL_REMINDER_WEBHOOK_URL required for dispatch".into())
        })?;
        crate::adapters::transport::validate_transport_url(
            &endpoint,
            self.config.stores.as_ref().is_some_and(|s| {
                s.enforce_tls == Some(true) || s.transport_profile.as_deref() == Some("strict")
            }),
        )
        .map_err(CoreError::InvalidInput)?;
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| CoreError::Io(e.to_string()))?;
        let mut request = client
            .post(endpoint)
            .header("Idempotency-Key", &r.dedupe_key)
            .json(r);
        if let Ok(token) = std::env::var("NEXTRAL_REMINDER_WEBHOOK_TOKEN") {
            request = request.bearer_auth(token);
        }
        let response = request.send().map_err(|e| CoreError::Io(e.to_string()))?;
        if !response.status().is_success() {
            return Err(CoreError::Io(format!(
                "host webhook returned {}",
                response.status()
            )));
        }
        Ok(receipt("host_webhook", "dispatch", &r.id, Ok(())))
    }
    fn lifecycle_reembed_write(
        &mut self,
        t: &str,
        c: &str,
        m: &MemoryRecord,
        v: &[f32],
    ) -> CoreResult<StoreReceipt> {
        if m.tenant_id != t {
            return Err(CoreError::InvalidInput("reembed tenant mismatch".into()));
        }
        validate_vector(v, v.len())?;
        let original = self
            .get_memory(t, &m.user_id, &m.id)?
            .ok_or_else(|| CoreError::NotFound("source memory missing".into()))?;
        self.postgres.state_save(
            t,
            &format!(
                "collection-record/{}/{}/{}",
                self.collection, m.user_id, m.id
            ),
            &serde_json::to_value(&original)?,
        )?;
        let mut staged = m.clone();
        staged.embedding = Some(v.to_vec());
        staged.embedding_dim = Some(v.len() as u32);
        self.qdrant.ensure_collection(c, v.len() as u32, "Cosine")?;
        self.qdrant
            .upsert_record(c, &vector(&staged, v)?, &staged)?;
        self.postgres.state_save(
            t,
            &format!("collection-record/{c}/{}/{}", m.user_id, m.id),
            &serde_json::to_value(&staged)?,
        )?;
        Ok(receipt("qdrant", "reembed", &m.id, Ok(())))
    }
    fn lifecycle_reembed_count(&self, t: &str, u: &str, c: &str) -> CoreResult<usize> {
        if !self.qdrant.collections()?.iter().any(|name| name == c) {
            return Ok(0);
        }
        Ok(self.qdrant.count_scoped(c, &scope(t, u))? as usize)
    }
    fn lifecycle_reembed_read(
        &self,
        t: &str,
        u: &str,
        c: &str,
        id: &str,
    ) -> CoreResult<Option<Vec<f32>>> {
        self.qdrant.read_scoped_vector(c, &scope(t, u), id)
    }
    fn lifecycle_reembed_cutover(
        &mut self,
        t: &str,
        u: &str,
        s: &str,
        shadow: &str,
    ) -> CoreResult<StoreReceipt> {
        if s != self.collection {
            return Err(CoreError::InvalidInput(
                "source_collection must equal configured logical collection".into(),
            ));
        }
        self.postgres.cutover(t, u, s, shadow)?;
        Ok(receipt("postgres", "scoped_read_cutover", s, Ok(())))
    }
}
dispatch!(LifecycleStore {
 lifecycle_load(&Self,t:&str,k:&str)->CoreResult<Option<Value>>;
 lifecycle_list(&Self,t:&str,p:&str)->CoreResult<Vec<Value>>;
 lifecycle_save(&mut Self,t:&str,k:&str,v:&Value)->CoreResult<StoreReceipt>;
 lifecycle_save_claimed(&mut Self,t:&str,k:&str,o:&str,v:&Value)->CoreResult<Option<Value>>;
 lifecycle_claim(&mut Self,t:&str,k:&str,o:&str,n:u64,l:u64)->CoreResult<bool>;
 lifecycle_release(&mut Self,t:&str,k:&str,o:&str)->CoreResult<()>;
 lifecycle_archive(&mut Self,t:&str,k:&str,v:&Value)->CoreResult<StoreReceipt>;
 lifecycle_verify_archive_sources(&Self,m:&MemoryRecord)->CoreResult<()>;
 lifecycle_forget(&mut Self,t:&str,u:&str,id:&str,r:bool)->CoreResult<Vec<StoreReceipt>>;
 lifecycle_policies(&Self,t:&str,u:&str)->CoreResult<Vec<ProceduralPolicy>>;
 lifecycle_dispatch(&mut Self,r:&ReminderRecord)->CoreResult<StoreReceipt>;
 lifecycle_reembed_write(&mut Self,t:&str,c:&str,m:&MemoryRecord,v:&[f32])->CoreResult<StoreReceipt>;
 lifecycle_reembed_count(&Self,t:&str,u:&str,c:&str)->CoreResult<usize>;
 lifecycle_reembed_read(&Self,t:&str,u:&str,c:&str,id:&str)->CoreResult<Option<Vec<f32>>>;
 lifecycle_reembed_cutover(&mut Self,t:&str,u:&str,s:&str,c:&str)->CoreResult<StoreReceipt>;
});

#[cfg(test)]
mod privacy_regressions {
    use super::*;
    use crate::memory::{ContentType, MemoryType, SourceType};

    fn source() -> MemoryRecord {
        let mut record = MemoryRecord::new(
            "memory",
            "tenant",
            "owner",
            "Private source",
            ContentType::Fact,
            MemoryType::Semantic,
            SourceType::Manual,
        );
        record.source_message_ids = vec!["message".into()];
        record
    }

    #[test]
    fn stale_completion_cannot_restore_a_forgotten_job_request() {
        let mut store = TestMemoryStore::new();
        store.upsert_memory(source()).unwrap();
        let key = "scheduled-job/owner/job";
        let mut stale = json!({"status":"running","request":{"tenant_id":"tenant","user_id":"owner",
            "trigger":"close","consolidation":{"candidates":[{"source_message_ids":["message"],"content":"Private source"}]}}});
        store.lifecycle_save("tenant", key, &stale).unwrap();
        let now = timestamp_epoch(&crate::memory::now_timestamp()).unwrap();
        store
            .lifecycle_claim("tenant", key, "worker", now, 300)
            .unwrap();
        crate::runtime::governance::forget_memory(
            &mut store,
            crate::runtime::governance::ForgetMemoryRequest {
                tenant_id: "tenant".into(),
                user_id: "owner".into(),
                memory_id: "memory".into(),
                actor: "owner".into(),
                reason: "privacy regression".into(),
                redact: true,
            },
        )
        .unwrap();
        stale["status"] = json!("completed");
        let result = store.lifecycle_save_claimed("tenant", key, "worker", &stale);
        assert!(!matches!(result, Ok(None)));
        let persisted = store.lifecycle_load("tenant", key).unwrap().unwrap();
        assert_eq!(persisted["status"], "cancelled");
        assert!(persisted["request"]["consolidation"].is_null());
    }

    #[test]
    fn retention_progress_survives_its_own_source_deletion() {
        let mut store = TestMemoryStore::new();
        store.upsert_memory(source()).unwrap();
        let key = "scheduled-job/owner/retention";
        let mut job = json!({"status":"running","request":{"tenant_id":"tenant","user_id":"owner",
            "trigger":"retention","memory_ids":["memory"],"consolidation":null}});
        store.lifecycle_save("tenant", key, &job).unwrap();
        let now = timestamp_epoch(&crate::memory::now_timestamp()).unwrap();
        store
            .lifecycle_claim("tenant", key, "worker", now, 300)
            .unwrap();
        crate::runtime::governance::forget_memory(
            &mut store,
            crate::runtime::governance::ForgetMemoryRequest {
                tenant_id: "tenant".into(),
                user_id: "owner".into(),
                memory_id: "memory".into(),
                actor: "owner".into(),
                reason: "retention regression".into(),
                redact: true,
            },
        )
        .unwrap();
        job["status"] = json!("completed");
        assert!(store
            .lifecycle_save_claimed("tenant", key, "worker", &job)
            .unwrap()
            .is_none());
        assert_eq!(
            store.lifecycle_load("tenant", key).unwrap().unwrap()["status"],
            "completed"
        );
    }

    #[test]
    fn archive_provenance_matches_sources_without_a_canonical_session() {
        let body = json!({"messages":[{"id":"message","content":"Private source"},{"id":"peer","content":"Other source"}]});
        let manifest =
            json!({"user_id":"owner","source_message_ids":archive_source_ids(&body).unwrap()});
        let record = source();
        assert!(record.session_id.is_none());
        assert!(references_memory(&manifest, &record));
        let mut unrelated = record;
        unrelated.source_message_ids = vec!["unrelated".into()];
        assert!(!references_memory(&manifest, &unrelated));
    }
}
