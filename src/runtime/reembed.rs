use crate::{
    contracts::{CoreError, CoreResult},
    domain::{AuditAction, AuditEvent, StoreReceipt},
    memory::{deterministic_id, now_timestamp, MemoryStatus, PrivacyLevel},
    runtime::lifecycle::{
        acknowledged, durable_save, timestamp_epoch, worker_owner, LifecycleStore,
    },
    store::{AuditSink, MemoryIndexStore},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, time::Instant};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReembedPlan {
    pub id: String,
    pub tenant_id: String,
    pub source_collection: String,
    pub shadow_collection: String,
    pub target_embedding_provider: String,
    pub target_embedding_model: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReembedStatus {
    Planned,
    Running,
    Completed,
    Failed,
}
impl std::fmt::Display for ReembedStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Planned => "planned",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        })
    }
}

pub fn plan_reembed(
    tenant_id: &str,
    source_collection: &str,
    shadow_collection: &str,
    target_embedding_provider: &str,
    target_embedding_model: &str,
) -> CoreResult<ReembedPlan> {
    crate::store::validate_scope_component(tenant_id, "tenant_id")?;
    crate::store::validate_scope_component(source_collection, "source_collection")?;
    crate::store::validate_scope_component(shadow_collection, "shadow_collection")?;
    for (name, value) in [
        ("tenant_id", tenant_id),
        ("source_collection", source_collection),
        ("shadow_collection", shadow_collection),
        ("target_embedding_provider", target_embedding_provider),
        ("target_embedding_model", target_embedding_model),
    ] {
        if value.trim().is_empty() {
            return Err(CoreError::InvalidInput(format!("{name} is required")));
        }
    }
    if source_collection == shadow_collection {
        return Err(CoreError::InvalidInput(
            "shadow collection must differ from source collection".into(),
        ));
    }
    Ok(ReembedPlan {
        id: deterministic_id(&[
            tenant_id,
            source_collection,
            shadow_collection,
            target_embedding_provider,
            target_embedding_model,
        ]),
        tenant_id: tenant_id.into(),
        source_collection: source_collection.into(),
        shadow_collection: shadow_collection.into(),
        target_embedding_provider: target_embedding_provider.into(),
        target_embedding_model: target_embedding_model.into(),
        status: "planned".into(),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HostEmbedding {
    pub memory_id: String,
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReembedExecutionRequest {
    pub plan: ReembedPlan,
    pub user_id: String,
    #[serde(default)]
    pub vectors: Vec<HostEmbedding>,
    pub target_dimension: usize,
    #[serde(default)]
    pub cutover: bool,
    #[serde(default)]
    pub rollback: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReembedExecutionReport {
    pub plan_id: String,
    pub status: String,
    pub source_count: usize,
    pub embedded_count: usize,
    pub failed_count: usize,
    pub duration_ms: u64,
    pub checkpoint_memory_ids: Vec<String>,
    pub validated_count: usize,
    #[serde(default)]
    pub cutover_applied: bool,
    pub receipts: Vec<StoreReceipt>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Checkpoint {
    user_id: String,
    created_at: String,
    dimension: usize,
    source_ids: Vec<String>,
    source_fingerprints: BTreeMap<String, String>,
    input_hashes: BTreeMap<String, String>,
    expected_directions: BTreeMap<String, Vec<f32>>,
    report: ReembedExecutionReport,
}

pub fn execute_reembed<T>(
    store: &mut T,
    request: ReembedExecutionRequest,
) -> CoreResult<ReembedExecutionReport>
where
    T: LifecycleStore + MemoryIndexStore + AuditSink,
{
    crate::runtime::lifecycle::validate_identity(&request.plan.tenant_id, &request.user_id, None)?;
    let canonical = plan_reembed(
        &request.plan.tenant_id,
        &request.plan.source_collection,
        &request.plan.shadow_collection,
        &request.plan.target_embedding_provider,
        &request.plan.target_embedding_model,
    )?;
    if canonical.id != request.plan.id
        || request.plan.status != canonical.status
        || request.target_dimension == 0
        || u32::try_from(request.target_dimension).is_err()
    {
        return Err(CoreError::InvalidInput(
            "invalid plan identity, user, or target dimension".into(),
        ));
    }
    let owner = worker_owner();
    let lock = format!("privacy/{}", request.user_id);
    if !store.lifecycle_claim(
        &request.plan.tenant_id,
        &lock,
        &owner,
        timestamp_epoch(&now_timestamp())?,
        300,
    )? {
        return Err(CoreError::InvalidInput(
            "user lifecycle is leased by another worker".into(),
        ));
    }
    let result = execute_claimed(store, &request, &owner, &lock);
    let release = store.lifecycle_release(&request.plan.tenant_id, &lock, &owner);
    match result {
        Ok(report) => {
            release?;
            Ok(report)
        }
        Err(error) => {
            let _ = release;
            Err(error)
        }
    }
}

fn execute_claimed<T>(
    store: &mut T,
    request: &ReembedExecutionRequest,
    owner: &str,
    lock: &str,
) -> CoreResult<ReembedExecutionReport>
where
    T: LifecycleStore + MemoryIndexStore + AuditSink,
{
    let plan = &request.plan;
    let key = format!("reembed/{}/{}", request.user_id, plan.id);
    let start = Instant::now();
    let mut checkpoint: Checkpoint =
        if let Some(value) = store.lifecycle_load(&plan.tenant_id, &key)? {
            serde_json::from_value(value)?
        } else {
            let mut records = store.list_memories(
                &plan.tenant_id,
                &request.user_id,
                &[
                    PrivacyLevel::Private,
                    PrivacyLevel::Shared,
                    PrivacyLevel::Sensitive,
                    PrivacyLevel::Restricted,
                ],
                false,
            )?;
            records.retain(|record| {
                record.status == MemoryStatus::Active
                    && record.tenant_id == plan.tenant_id
                    && record.user_id == request.user_id
            });
            records.sort_by(|a, b| a.id.cmp(&b.id));
            if records.is_empty() {
                return Err(CoreError::InvalidInput(
                    "cannot complete reembedding with zero source memories".into(),
                ));
            }
            if store.lifecycle_reembed_count(
                &plan.tenant_id,
                &request.user_id,
                &plan.shadow_collection,
            )? != 0
            {
                return Err(CoreError::InvalidInput(
                    "new plan requires an empty user-scoped shadow collection".into(),
                ));
            }
            let source_fingerprints = records
                .iter()
                .map(|record| Ok((record.id.clone(), source_fingerprint(record)?)))
                .collect::<CoreResult<BTreeMap<_, _>>>()?;
            let source_ids = records
                .into_iter()
                .map(|record| record.id)
                .collect::<Vec<_>>();
            Checkpoint {
                user_id: request.user_id.clone(),
                created_at: now_timestamp(),
                dimension: request.target_dimension,
                report: ReembedExecutionReport {
                    plan_id: plan.id.clone(),
                    status: "running".into(),
                    source_count: source_ids.len(),
                    embedded_count: 0,
                    failed_count: 0,
                    duration_ms: 0,
                    checkpoint_memory_ids: Vec::new(),
                    validated_count: 0,
                    cutover_applied: false,
                    receipts: Vec::new(),
                    last_error: None,
                },
                source_ids,
                source_fingerprints,
                input_hashes: BTreeMap::new(),
                expected_directions: BTreeMap::new(),
            }
        };
    if checkpoint.user_id != request.user_id || checkpoint.dimension != request.target_dimension {
        return Err(CoreError::InvalidInput(
            "checkpoint scope or dimension mismatch".into(),
        ));
    }
    if request.rollback {
        if !checkpoint.report.cutover_applied {
            return Err(CoreError::InvalidInput(
                "only an acknowledged cut-over plan can be rolled back".into(),
            ));
        }
        let rollback = (|| {
            let receipt = store.lifecycle_reembed_cutover(
                &plan.tenant_id,
                &request.user_id,
                &plan.source_collection,
                &plan.source_collection,
            )?;
            if !acknowledged(&receipt) {
                return Err(CoreError::InvalidInput("rollback not acknowledged".into()));
            }
            Ok(receipt)
        })();
        match rollback {
            Ok(receipt) => {
                checkpoint.report.receipts.push(receipt);
                checkpoint.report.cutover_applied = false;
                checkpoint.report.status = "rolled_back".into();
                checkpoint.report.last_error = None;
            }
            Err(error) => {
                checkpoint.report.last_error = Some(error.to_string());
                save_checkpoint(store, plan, &key, &checkpoint)?;
                return Err(error);
            }
        }
        checkpoint.report.duration_ms = checkpoint
            .report
            .duration_ms
            .saturating_add(start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64);
        save_checkpoint(store, plan, &key, &checkpoint)?;
        return Ok(checkpoint.report);
    }
    if checkpoint.report.status == "rolled_back" {
        return Err(CoreError::InvalidInput(
            "rolled-back plan cannot be replayed; create a new shadow collection".into(),
        ));
    }
    let already_completed = checkpoint.report.status == "completed";
    let mut vectors = BTreeMap::new();
    for embedding in &request.vectors {
        if !checkpoint.source_ids.contains(&embedding.memory_id)
            || !valid_vector(&embedding.vector, request.target_dimension)
            || vectors
                .insert(embedding.memory_id.clone(), embedding.vector.as_slice())
                .is_some()
        {
            return Err(CoreError::InvalidInput("vectors must be unique, finite, nonzero, dimension-correct and belong to the source snapshot".into()));
        }
    }
    save_checkpoint(store, plan, &key, &checkpoint)?;
    let mut item_in_progress = false;
    let work: CoreResult<()> = (|| {
        let mut audit = AuditEvent::new(
            deterministic_id(&[&plan.id, &request.user_id, "start"]),
            &plan.tenant_id,
            "system",
            AuditAction::ReembedStarted,
            "reembed",
            Some(plan.id.clone()),
            "host-vector backfill started",
        );
        audit.user_id = Some(request.user_id.clone());
        store.emit_audit(audit)?;
        for id in checkpoint.source_ids.clone() {
            crate::runtime::lifecycle::renew_lease(store, &plan.tenant_id, lock, owner, 300)?;
            item_in_progress = true;
            let mut record = store
                .get_memory(&plan.tenant_id, &request.user_id, &id)?
                .ok_or_else(|| {
                    CoreError::NotFound("source memory disappeared during backfill".into())
                })?;
            if record.status != MemoryStatus::Active {
                return Err(CoreError::InvalidInput(
                    "source was forgotten; shadow must not be cut over".into(),
                ));
            }
            if checkpoint.source_fingerprints.get(&id) != Some(&source_fingerprint(&record)?) {
                return Err(CoreError::InvalidInput(
                    "source changed after the backfill snapshot".into(),
                ));
            }
            for source in &record.source_message_ids {
                if store
                    .lifecycle_load(
                        &plan.tenant_id,
                        &format!("forgotten-source/{}/{}", request.user_id, source),
                    )?
                    .is_some()
                {
                    return Err(CoreError::InvalidInput(
                        "forgotten source cannot be reembedded".into(),
                    ));
                }
            }
            if let Some(vector) = vectors.get(&id) {
                let hash = deterministic_id(&[&serde_json::to_string(vector)?]);
                if let Some(previous) = checkpoint.input_hashes.get(&id) {
                    if previous != &hash {
                        return Err(CoreError::InvalidInput(
                            "checkpoint vector inputs cannot change on replay".into(),
                        ));
                    }
                }
                if !checkpoint.report.checkpoint_memory_ids.contains(&id) {
                    record.embedding_provider = Some(plan.target_embedding_provider.clone());
                    record.embedding_model = Some(plan.target_embedding_model.clone());
                    record.embedding_dim =
                        Some(u32::try_from(request.target_dimension).map_err(|_| {
                            CoreError::InvalidInput("dimension exceeds u32".into())
                        })?);
                    record.embedding = None;
                    let receipt = store.lifecycle_reembed_write(
                        &plan.tenant_id,
                        &plan.shadow_collection,
                        &record,
                        vector,
                    )?;
                    if !acknowledged(&receipt) {
                        return Err(CoreError::InvalidInput(
                            "shadow vector write was not acknowledged".into(),
                        ));
                    }
                    checkpoint.input_hashes.insert(id.clone(), hash);
                    checkpoint
                        .expected_directions
                        .insert(id.clone(), normalized_direction(vector));
                    checkpoint.report.checkpoint_memory_ids.push(id.clone());
                    checkpoint.report.embedded_count =
                        checkpoint.report.checkpoint_memory_ids.len();
                    checkpoint.report.receipts.push(receipt);
                    save_checkpoint(store, plan, &key, &checkpoint)?;
                }
            }
            item_in_progress = false;
        }
        if checkpoint.report.embedded_count < checkpoint.report.source_count {
            checkpoint.report.status = "awaiting_vectors".into();
            return Ok(());
        }
        let persisted_count = store.lifecycle_reembed_count(
            &plan.tenant_id,
            &request.user_id,
            &plan.shadow_collection,
        )?;
        if persisted_count != checkpoint.report.source_count {
            return Err(CoreError::InvalidInput(
                "measured shadow count differs from source snapshot".into(),
            ));
        }
        let current = store.list_memories(
            &plan.tenant_id,
            &request.user_id,
            &[
                PrivacyLevel::Private,
                PrivacyLevel::Shared,
                PrivacyLevel::Sensitive,
                PrivacyLevel::Restricted,
            ],
            false,
        )?;
        let current = current
            .iter()
            .filter(|record| {
                record.status == MemoryStatus::Active
                    && record.tenant_id == plan.tenant_id
                    && record.user_id == request.user_id
            })
            .map(|record| Ok((record.id.clone(), source_fingerprint(record)?)))
            .collect::<CoreResult<BTreeMap<_, _>>>()?;
        if current != checkpoint.source_fingerprints {
            return Err(CoreError::InvalidInput(
                "active source snapshot changed before read cutover".into(),
            ));
        }
        checkpoint.report.validated_count = 0;
        for id in &checkpoint.source_ids {
            crate::runtime::lifecycle::renew_lease(store, &plan.tenant_id, lock, owner, 300)?;
            item_in_progress = true;
            let vector = store
                .lifecycle_reembed_read(
                    &plan.tenant_id,
                    &request.user_id,
                    &plan.shadow_collection,
                    id,
                )?
                .ok_or_else(|| CoreError::NotFound("shadow vector is unreadable".into()))?;
            let expected = checkpoint.expected_directions.get(id).ok_or_else(|| {
                CoreError::InvalidInput("checkpoint lacks host-vector validation input".into())
            })?;
            let actual = normalized_direction(&vector);
            if !valid_vector(&vector, request.target_dimension)
                || actual
                    .iter()
                    .zip(expected)
                    .any(|(actual, expected)| (actual - expected).abs() > 1e-5)
            {
                return Err(CoreError::InvalidInput(
                    "measured shadow vector differs from acknowledged host input".into(),
                ));
            }
            checkpoint.report.validated_count += 1;
            item_in_progress = false;
        }
        if request.cutover && !checkpoint.report.cutover_applied {
            crate::runtime::lifecycle::renew_lease(store, &plan.tenant_id, lock, owner, 300)?;
            let receipt = store.lifecycle_reembed_cutover(
                &plan.tenant_id,
                &request.user_id,
                &plan.source_collection,
                &plan.shadow_collection,
            )?;
            if !acknowledged(&receipt) {
                return Err(CoreError::InvalidInput(
                    "read cutover was not acknowledged".into(),
                ));
            }
            checkpoint.report.receipts.push(receipt);
            checkpoint.report.cutover_applied = true;
            save_checkpoint(store, plan, &key, &checkpoint)?;
        }
        if checkpoint.report.cutover_applied {
            checkpoint.report.status = "completed".into();
            let mut audit = AuditEvent::new(
                deterministic_id(&[&plan.id, &request.user_id, "complete"]),
                &plan.tenant_id,
                "system",
                AuditAction::ReembedCompleted,
                "reembed",
                Some(plan.id.clone()),
                "measured shadow validation and read cutover completed",
            );
            audit.user_id = Some(request.user_id.clone());
            store.emit_audit(audit)?;
        } else {
            checkpoint.report.status = "validated".into();
        }
        Ok(())
    })();
    checkpoint.report.duration_ms = checkpoint
        .report
        .duration_ms
        .saturating_add(start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64);
    match work {
        Ok(()) => {
            checkpoint.report.last_error = None;
            checkpoint.report.failed_count = 0;
        }
        Err(error) => {
            checkpoint.report.status = "failed".into();
            checkpoint.report.failed_count = usize::from(item_in_progress);
            checkpoint.report.last_error = Some(error.to_string());
            save_checkpoint(store, plan, &key, &checkpoint)?;
            return Err(error);
        }
    }
    crate::runtime::lifecycle::renew_lease(store, &plan.tenant_id, lock, owner, 300)?;
    save_checkpoint(store, plan, &key, &checkpoint)?;
    if !already_completed && checkpoint.report.status == "completed" {
        crate::runtime::observability::record_job(
            crate::runtime::observability::JobOutcome::Completed,
            timestamp_epoch(&now_timestamp())?
                .saturating_sub(timestamp_epoch(&checkpoint.created_at)?),
        );
    }
    Ok(checkpoint.report)
}

fn save_checkpoint(
    store: &mut impl LifecycleStore,
    plan: &ReembedPlan,
    key: &str,
    checkpoint: &Checkpoint,
) -> CoreResult<()> {
    durable_save(
        store,
        &plan.tenant_id,
        key,
        &serde_json::to_value(checkpoint)?,
    )?;
    Ok(())
}
fn valid_vector(vector: &[f32], dimension: usize) -> bool {
    vector.len() == dimension
        && vector.iter().all(|value| value.is_finite())
        && vector.iter().any(|value| *value != 0.0)
}
fn normalized_direction(vector: &[f32]) -> Vec<f32> {
    let norm = vector
        .iter()
        .map(|value| f64::from(*value).powi(2))
        .sum::<f64>()
        .sqrt();
    vector
        .iter()
        .map(|value| (f64::from(*value) / norm) as f32)
        .collect()
}

fn source_fingerprint(record: &crate::memory::MemoryRecord) -> CoreResult<String> {
    Ok(deterministic_id(&[&serde_json::to_string(
        &serde_json::json!({"content":record.content,"privacy_level":record.privacy_level,"source_message_ids":record.source_message_ids}),
    )?]))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_and_shadow_must_be_distinct() {
        assert!(plan_reembed("t", "same", "same", "host", "model").is_err());
    }
    #[test]
    fn nonfinite_host_vectors_are_rejected() {
        assert!(!valid_vector(&[f32::NAN, 1.0], 2));
    }
    #[test]
    fn zero_vectors_are_not_valid_backfill() {
        assert!(!valid_vector(&[0.0, 0.0], 2));
    }

    fn source_fixture() -> (crate::store::TestMemoryStore, ReembedExecutionRequest) {
        let mut store = crate::store::TestMemoryStore::new();
        let record = crate::memory::MemoryRecord::new(
            "m",
            "t",
            "u",
            "A real source record",
            crate::memory::ContentType::Fact,
            crate::memory::MemoryType::Semantic,
            crate::memory::SourceType::Manual,
        );
        store.upsert_memory(record).unwrap();
        let request = ReembedExecutionRequest {
            plan: plan_reembed("t", "source", "shadow", "host", "new-model").unwrap(),
            user_id: "u".into(),
            vectors: Vec::new(),
            target_dimension: 2,
            cutover: false,
            rollback: false,
        };
        (store, request)
    }

    #[test]
    fn missing_host_vectors_leave_durable_backfill_waiting() {
        let (mut store, request) = source_fixture();
        let report = execute_reembed(&mut store, request).unwrap();
        assert_eq!(
            (
                report.status.as_str(),
                report.source_count,
                report.embedded_count
            ),
            ("awaiting_vectors", 1, 0)
        );
    }

    #[test]
    fn measured_shadow_cutover_and_rollback_change_the_same_read_route() {
        let (mut store, mut request) = source_fixture();
        request.vectors = vec![HostEmbedding {
            memory_id: "m".into(),
            vector: vec![2.0, 0.0],
        }];
        request.cutover = true;
        let report = execute_reembed(&mut store, request.clone()).unwrap();
        assert_eq!(
            (report.status.as_str(), report.validated_count),
            ("completed", 1)
        );
        assert_eq!(
            store.lifecycle_load("t", "alias/source/u").unwrap(),
            Some(serde_json::json!("shadow"))
        );
        request.rollback = true;
        request.vectors.clear();
        assert_eq!(
            execute_reembed(&mut store, request).unwrap().status,
            "rolled_back"
        );
        assert_eq!(
            store.lifecycle_load("t", "alias/source/u").unwrap(),
            Some(serde_json::json!("source"))
        );
    }

    #[test]
    fn corrupt_shadow_readback_persists_failed_validation() {
        let (mut store, mut request) = source_fixture();
        request.vectors = vec![HostEmbedding {
            memory_id: "m".into(),
            vector: vec![1.0, 0.0],
        }];
        execute_reembed(&mut store, request.clone()).unwrap();
        let record = store.get_memory("t", "u", "m").unwrap().unwrap();
        store
            .lifecycle_reembed_write("t", "shadow", &record, &[0.0, 1.0])
            .unwrap();
        request.vectors.clear();
        request.cutover = true;
        assert!(execute_reembed(&mut store, request.clone()).is_err());
        let state = store
            .lifecycle_load("t", &format!("reembed/u/{}", request.plan.id))
            .unwrap()
            .unwrap();
        assert_eq!(state["report"]["status"], "failed");
    }

    #[test]
    fn shared_shadow_count_read_and_cutover_never_use_peer_user_scope() {
        let (mut store, mut request) = source_fixture();
        let peer = crate::memory::MemoryRecord::new(
            "peer-m",
            "t",
            "peer",
            "Peer source record",
            crate::memory::ContentType::Fact,
            crate::memory::MemoryType::Semantic,
            crate::memory::SourceType::Manual,
        );
        store.upsert_memory(peer.clone()).unwrap();
        store
            .lifecycle_reembed_write("t", "shadow", &peer, &[0.0, 1.0])
            .unwrap();
        request.vectors = vec![HostEmbedding {
            memory_id: "m".into(),
            vector: vec![1.0, 0.0],
        }];
        request.cutover = true;
        let report = execute_reembed(&mut store, request).unwrap();
        assert_eq!(
            (
                report.source_count,
                report.embedded_count,
                report.validated_count
            ),
            (1, 1, 1)
        );
        assert_eq!(
            store
                .lifecycle_reembed_read("t", "u", "shadow", "m")
                .unwrap(),
            Some(vec![1.0, 0.0])
        );
        assert_eq!(
            store
                .lifecycle_reembed_read("t", "peer", "shadow", "peer-m")
                .unwrap(),
            Some(vec![0.0, 1.0])
        );
        assert_eq!(
            store
                .lifecycle_reembed_read("t", "peer", "shadow", "m")
                .unwrap(),
            None
        );
        assert!(store
            .lifecycle_load("t", "alias/source/peer")
            .unwrap()
            .is_none());
        assert_eq!(
            store.get_memory("t", "peer", "peer-m").unwrap().unwrap(),
            peer
        );
    }
}
