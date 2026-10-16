use crate::{
    config::IngestionPolicy,
    contracts::{CoreError, CoreResult},
    domain::{AuditAction, AuditEvent, RuntimeJob, SessionMessage, SessionSummary, StoreReceipt},
    ingestion::{ingest_memory, IngestMemoryRequest, IngestMemoryResponse, IngestStatus},
    memory::{deterministic_id, now_timestamp, MemoryStatus, PrivacyLevel, SourceType},
    runtime::lifecycle::{
        acknowledged, durable_save, timestamp_epoch, worker_owner, LifecycleStore,
    },
    store::{AuditSink, GraphStore, MemoryIndexStore, SessionStore},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsolidationLane {
    Fast,
    Deep,
}

fn default_version() -> String {
    "v1".into()
}
fn default_chunk_messages() -> usize {
    100
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConsolidationRequest {
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: String,
    pub lane: ConsolidationLane,
    pub policy: IngestionPolicy,
    #[serde(default = "default_version")]
    pub version: String,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub candidates: Vec<IngestMemoryRequest>,
    #[serde(default = "default_chunk_messages")]
    pub chunk_messages: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConsolidationResponse {
    pub job: RuntimeJob,
    pub accepted: Vec<IngestMemoryResponse>,
    pub receipts: Vec<StoreReceipt>,
}

/// Returns deterministic source chunks; embeddings and scores must be supplied by the host.
pub fn transcript_chunks(
    messages: &[SessionMessage],
    chunk_messages: usize,
) -> CoreResult<Vec<(String, Vec<String>)>> {
    if chunk_messages == 0 {
        return Err(CoreError::InvalidInput(
            "chunk_messages must be positive".into(),
        ));
    }
    Ok(messages
        .chunks(chunk_messages)
        .map(|chunk| {
            let capacity = chunk
                .iter()
                .map(|message| message.role.len() + message.content.len() + 3)
                .sum::<usize>();
            let mut content = String::with_capacity(capacity);
            for (index, message) in chunk.iter().enumerate() {
                if index > 0 {
                    content.push('\n');
                }
                content.push_str(&message.role);
                content.push_str(": ");
                content.push_str(&message.content);
            }
            (
                content,
                chunk.iter().map(|message| message.id.clone()).collect(),
            )
        })
        .collect())
}

pub(crate) fn source_privacy(message: &SessionMessage) -> CoreResult<PrivacyLevel> {
    match message.source_metadata.get("privacy_level") {
        Some(value) => Ok(serde_json::from_value(value.clone())?),
        None => Ok(PrivacyLevel::Private),
    }
}

pub(crate) fn privacy_rank(level: &PrivacyLevel) -> u8 {
    match level {
        PrivacyLevel::Shared => 0,
        PrivacyLevel::Private => 1,
        PrivacyLevel::Sensitive => 2,
        PrivacyLevel::Restricted => 3,
    }
}

pub fn consolidate_session<T>(
    store: &mut T,
    request: ConsolidationRequest,
) -> CoreResult<ConsolidationResponse>
where
    T: SessionStore + MemoryIndexStore + AuditSink + GraphStore + LifecycleStore,
{
    crate::runtime::lifecycle::validate_identity(
        &request.tenant_id,
        &request.user_id,
        Some(&request.session_id),
    )?;
    if request.version.trim().is_empty() || request.candidates.is_empty() {
        return Err(CoreError::InvalidInput(
            "version and explicit host candidates are required".into(),
        ));
    }
    if request.lane == ConsolidationLane::Fast
        && request.summary.as_ref().is_none_or(|s| s.trim().is_empty())
    {
        return Err(CoreError::InvalidInput(
            "fast consolidation requires an explicit host summary".into(),
        ));
    }
    let tenant = request.tenant_id.clone();
    let lock = format!("privacy/{}", request.user_id);
    let owner = worker_owner();
    if !store.lifecycle_claim(
        &tenant,
        &lock,
        &owner,
        timestamp_epoch(&now_timestamp())?,
        300,
    )? {
        return Err(CoreError::InvalidInput(
            "user lifecycle is leased by another worker".into(),
        ));
    }
    let result = consolidate_claimed(store, request, &owner, &lock);
    let release = store.lifecycle_release(&tenant, &lock, &owner);
    match result {
        Ok(response) => {
            release?;
            Ok(response)
        }
        Err(error) => {
            let _ = release;
            Err(error)
        }
    }
}

fn consolidate_claimed<T>(
    store: &mut T,
    request: ConsolidationRequest,
    owner: &str,
    lock: &str,
) -> CoreResult<ConsolidationResponse>
where
    T: SessionStore + MemoryIndexStore + AuditSink + GraphStore + LifecycleStore,
{
    // Load one beyond the bound to reject truncation instead of silently archiving a partial transcript.
    let messages = store.session_tail(
        &request.tenant_id,
        &request.user_id,
        &request.session_id,
        100_001,
    )?;
    if messages.is_empty() || messages.len() > 100_000 {
        return Err(CoreError::InvalidInput(
            "session must contain 1..=100000 messages".into(),
        ));
    }
    let mut source_privacy_by_id = std::collections::HashMap::with_capacity(messages.len());
    for message in &messages {
        if message.tenant_id != request.tenant_id
            || message.user_id != request.user_id
            || message.session_id != request.session_id
        {
            return Err(CoreError::InvalidInput(
                "session store returned an out-of-scope message".into(),
            ));
        }
        crate::store::validate_scope_component(&message.id, "source_message_id")?;
        if source_privacy_by_id
            .insert(message.id.as_str(), source_privacy(message)?)
            .is_some()
        {
            return Err(CoreError::InvalidInput(
                "session contains duplicate source message IDs".into(),
            ));
        }
        let key = format!("forgotten-source/{}/{}", request.user_id, message.id);
        if store.lifecycle_load(&request.tenant_id, &key)?.is_some() {
            return Err(CoreError::InvalidInput(
                "session contains forgotten source material".into(),
            ));
        }
    }
    let chunks = if request.lane == ConsolidationLane::Deep {
        transcript_chunks(&messages, request.chunk_messages)?
    } else {
        Vec::new()
    };
    if request.lane == ConsolidationLane::Deep && request.candidates.len() != chunks.len() {
        return Err(CoreError::InvalidInput(
            "deep consolidation requires one host candidate per deterministic chunk".into(),
        ));
    }
    for (index, candidate) in request.candidates.iter().enumerate() {
        if candidate.tenant_id != request.tenant_id
            || candidate.user_id != request.user_id
            || candidate.source_message_ids.is_empty()
        {
            return Err(CoreError::InvalidInput(
                "candidate identity and source IDs must match the session".into(),
            ));
        }
        for id in &candidate.source_message_ids {
            let source = source_privacy_by_id.get(id.as_str()).ok_or_else(|| {
                CoreError::InvalidInput("candidate references an unknown source message".into())
            })?;
            if privacy_rank(&candidate.privacy_level) < privacy_rank(source) {
                return Err(CoreError::InvalidInput(
                    "candidate cannot weaken source privacy".into(),
                ));
            }
        }
        if request.lane == ConsolidationLane::Deep
            && (candidate.content != chunks[index].0
                || candidate.source_message_ids != chunks[index].1)
        {
            return Err(CoreError::InvalidInput(
                "deep candidate must match deterministic transcript chunk".into(),
            ));
        }
    }
    let lane = match request.lane {
        ConsolidationLane::Fast => "fast",
        ConsolidationLane::Deep => "deep",
    };
    let job_id = deterministic_id(&[
        &request.tenant_id,
        &request.user_id,
        &request.session_id,
        lane,
        &request.version,
    ]);
    let key = format!("consolidation/{job_id}");
    let fingerprint = deterministic_id(&[
        &serde_json::to_string(&request)?,
        &serde_json::to_string(&messages)?,
    ]);
    let previous = store.lifecycle_load(&request.tenant_id, &key)?;
    if let Some(previous) = &previous {
        if previous["fingerprint"].as_str() != Some(fingerprint.as_str()) {
            return Err(CoreError::InvalidInput(
                "consolidation version already identifies different inputs".into(),
            ));
        }
        if previous["response"]["job"]["status"] == "completed" {
            return Ok(serde_json::from_value(previous["response"].clone())?);
        }
    }
    let now = now_timestamp();
    let mut response = match previous {
        Some(value) => serde_json::from_value::<ConsolidationResponse>(value["response"].clone())?,
        None => ConsolidationResponse {
            job: RuntimeJob {
                id: job_id.clone(),
                tenant_id: request.tenant_id.clone(),
                job_type: format!("consolidation_{lane}"),
                target_id: request.session_id.clone(),
                status: "running".into(),
                attempts: 0,
                last_error: None,
                created_at: now.clone(),
                updated_at: now,
            },
            accepted: Vec::new(),
            receipts: Vec::new(),
        },
    };
    response.job.attempts = response.job.attempts.saturating_add(1);
    response.job.status = "running".into();
    save_progress(store, &request, &key, &fingerprint, &response)?;
    let work: CoreResult<()> = (|| {
        store.emit_audit(AuditEvent::new(
            deterministic_id(&[&job_id, "queued"]),
            &request.tenant_id,
            "system",
            AuditAction::ConsolidationQueued,
            "session",
            Some(request.session_id.clone()),
            "host consolidation inputs accepted",
        ))?;
        if request.lane == ConsolidationLane::Deep
            && !response
                .receipts
                .iter()
                .any(|r| r.operation == "archive_transcript" && acknowledged(r))
        {
            crate::runtime::lifecycle::renew_lease(store, &request.tenant_id, lock, owner, 300)?;
            let mut receipt = store.lifecycle_archive(&request.tenant_id, &format!("sessions/{}/{}/{}.json", request.user_id, request.session_id, job_id), &serde_json::json!({"tenant_id":request.tenant_id,"user_id":request.user_id,"session_id":request.session_id,"messages": messages, "version": request.version}))?;
            if !acknowledged(&receipt) {
                return Err(CoreError::InvalidInput(
                    "archive was not acknowledged".into(),
                ));
            }
            receipt.operation = "archive_transcript".into();
            response.receipts.push(receipt);
            save_progress(store, &request, &key, &fingerprint, &response)?;
        }
        for (index, candidate) in request
            .candidates
            .iter()
            .enumerate()
            .skip(response.accepted.len())
        {
            crate::runtime::lifecycle::renew_lease(store, &request.tenant_id, lock, owner, 300)?;
            let mut candidate = candidate.clone();
            let id = deterministic_id(&[&job_id, &index.to_string(), &candidate.content]);
            if let Some(record) = store.get_memory(&request.tenant_id, &request.user_id, &id)? {
                if record.status != MemoryStatus::Active {
                    return Err(CoreError::InvalidInput(
                        "forgotten consolidation output cannot be replayed".into(),
                    ));
                }
            }
            candidate.id = Some(id);
            candidate.session_id = Some(request.session_id.clone());
            candidate.source_type = if request.lane == ConsolidationLane::Fast {
                SourceType::FastLane
            } else {
                SourceType::DeepLane
            };
            if request.lane == ConsolidationLane::Deep {
                candidate.memory_type = crate::memory::MemoryType::Episodic;
            }
            candidate.policy = request.policy.clone();
            let accepted = ingest_memory(store, candidate)?;
            if accepted.status != IngestStatus::Accepted {
                return Err(CoreError::InvalidInput(format!(
                    "host candidate rejected: {}",
                    accepted.validation_errors.join("; ")
                )));
            }
            response.accepted.push(accepted);
            save_progress(store, &request, &key, &fingerprint, &response)?;
        }
        if let Some(summary) = &request.summary {
            crate::runtime::lifecycle::renew_lease(store, &request.tenant_id, lock, owner, 300)?;
            let summary = SessionSummary {
                id: deterministic_id(&[&job_id, "summary"]),
                tenant_id: request.tenant_id.clone(),
                user_id: request.user_id.clone(),
                session_id: request.session_id.clone(),
                summary: summary.clone(),
                source_message_ids: messages.iter().map(|m| m.id.clone()).collect(),
                created_at: now_timestamp(),
            };
            let privacy = source_privacy_by_id
                .values()
                .max_by_key(|level| privacy_rank(level))
                .cloned()
                .unwrap_or(PrivacyLevel::Private);
            response.receipts.push(durable_save(store, &request.tenant_id, &format!("summary/{}/{}", request.user_id, request.session_id), &serde_json::json!({"summary": summary, "privacy_level": privacy, "version": request.version}))?);
        }
        store.emit_audit(AuditEvent::new(
            deterministic_id(&[&job_id, "completed"]),
            &request.tenant_id,
            "system",
            AuditAction::ConsolidationCompleted,
            "session",
            Some(request.session_id.clone()),
            "consolidation completed",
        ))?;
        Ok(())
    })();
    crate::runtime::lifecycle::renew_lease(store, &request.tenant_id, lock, owner, 300)?;
    response.job.updated_at = now_timestamp();
    match work {
        Ok(()) => {
            response.job.status = "completed".into();
            response.job.last_error = None;
        }
        Err(error) => {
            response.job.status = "retry_pending".into();
            response.job.last_error = Some(error.to_string());
            save_progress(store, &request, &key, &fingerprint, &response)?;
            crate::runtime::observability::record_job(
                crate::runtime::observability::JobOutcome::Retried,
                timestamp_epoch(&now_timestamp())?
                    .saturating_sub(timestamp_epoch(&response.job.created_at)?),
            );
            return Err(error);
        }
    }
    save_progress(store, &request, &key, &fingerprint, &response)?;
    crate::runtime::observability::record_job(
        crate::runtime::observability::JobOutcome::Completed,
        timestamp_epoch(&now_timestamp())?
            .saturating_sub(timestamp_epoch(&response.job.created_at)?),
    );
    Ok(response)
}

fn save_progress(
    store: &mut impl LifecycleStore,
    request: &ConsolidationRequest,
    key: &str,
    fingerprint: &str,
    response: &ConsolidationResponse,
) -> CoreResult<()> {
    durable_save(
        store,
        &request.tenant_id,
        key,
        &serde_json::json!({"tenant_id":request.tenant_id,"user_id":request.user_id,"session_id":request.session_id,"fingerprint":fingerprint,"response":response}),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deep_chunks_preserve_order_and_source_ids() {
        let a = SessionMessage::new("t", "u", "s", "user", "one", "1");
        let b = SessionMessage::new("t", "u", "s", "assistant", "two", "2");
        let chunks = transcript_chunks(&[a.clone(), b.clone()], 1).unwrap();
        assert_eq!(
            chunks,
            vec![
                ("user: one".into(), vec![a.id]),
                ("assistant: two".into(), vec![b.id])
            ]
        );
    }
    #[test]
    fn zero_chunk_size_is_rejected_without_panicking() {
        assert!(transcript_chunks(&[], 0).is_err());
    }

    fn host_fixture() -> (crate::store::TestMemoryStore, ConsolidationRequest) {
        let mut store = crate::store::TestMemoryStore::new();
        let message = SessionMessage::new("t", "u", "s", "user", "Keep PostgreSQL", "message-1");
        store.append_session_message(message.clone()).unwrap();
        let policy = IngestionPolicy {
            min_importance_score: 0.2,
            min_confidence_score: 0.2,
        };
        let mut candidate = IngestMemoryRequest::new(
            "t",
            "u",
            "Uses PostgreSQL",
            crate::memory::ContentType::Fact,
            crate::memory::MemoryType::Semantic,
            SourceType::Manual,
            policy.clone(),
        );
        candidate.source_message_ids = vec![message.id];
        candidate.importance_score = 0.63;
        candidate.confidence_score = Some(0.74);
        let request = ConsolidationRequest {
            tenant_id: "t".into(),
            user_id: "u".into(),
            session_id: "s".into(),
            lane: ConsolidationLane::Fast,
            policy,
            version: "host-v1".into(),
            summary: Some("PostgreSQL was selected".into()),
            candidates: vec![candidate],
            chunk_messages: 100,
        };
        (store, request)
    }

    #[test]
    fn fast_consolidation_preserves_host_scores_and_replays_without_duplicates() {
        let (mut store, request) = host_fixture();
        let first = consolidate_session(&mut store, request.clone()).unwrap();
        let replay = consolidate_session(&mut store, request).unwrap();
        assert_eq!(first, replay);
        assert_eq!(store.memories.len(), 1);
        assert_eq!(
            (
                store.memories[0].importance_score,
                store.memories[0].confidence_score
            ),
            (0.63, Some(0.74))
        );
    }

    #[test]
    fn fast_consolidation_rejects_weaker_source_privacy() {
        let (mut store, mut request) = host_fixture();
        request.candidates[0].privacy_level = PrivacyLevel::Shared;
        assert!(consolidate_session(&mut store, request).is_err());
        assert!(store.memories.is_empty());
    }

    #[test]
    fn consolidation_version_cannot_be_reused_for_changed_host_output() {
        let (mut store, mut request) = host_fixture();
        consolidate_session(&mut store, request.clone()).unwrap();
        request.summary = Some("Different host output".into());
        assert!(consolidate_session(&mut store, request).is_err());
    }

    #[test]
    fn deep_consolidation_archives_and_embeds_explicit_deterministic_chunks() {
        let (mut store, mut request) = host_fixture();
        request.lane = ConsolidationLane::Deep;
        request.summary = None;
        request.chunk_messages = 1;
        request.candidates[0].content = "user: Keep PostgreSQL".into();
        let response = consolidate_session(&mut store, request.clone()).unwrap();
        assert_eq!(
            store.memories[0].memory_type,
            crate::memory::MemoryType::Episodic
        );
        assert!(response
            .receipts
            .iter()
            .any(|receipt| receipt.operation == "archive_transcript" && acknowledged(receipt)));
        let archive = store
            .lifecycle_load(
                "t",
                &format!("archive/sessions/u/s/{}.json", response.job.id),
            )
            .unwrap()
            .unwrap();
        assert_eq!(archive["user_id"], "u");
        assert_eq!(archive["messages"][0]["content"], "Keep PostgreSQL");
        assert_eq!(consolidate_session(&mut store, request).unwrap(), response);
    }
}
