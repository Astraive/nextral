use crate::{
    contracts::{CoreError, CoreResult},
    domain::{AuditAction, AuditEvent, StoreReceipt},
    memory::{deterministic_id, now_timestamp, MemoryStatus, TransitionMetadata},
    runtime::lifecycle::{
        acknowledged, durable_save, timestamp_epoch, worker_owner, LifecycleStore,
    },
    store::{AuditSink, MemoryIndexStore},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ForgetMemoryRequest {
    pub tenant_id: String,
    pub user_id: String,
    pub memory_id: String,
    pub actor: String,
    pub reason: String,
    pub redact: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ForgetMemoryResponse {
    pub transition: TransitionMetadata,
    pub redaction_transition: Option<TransitionMetadata>,
    pub receipts: Vec<StoreReceipt>,
    pub status: String,
    pub attempts: u32,
    pub last_error: Option<String>,
}

pub fn forget_memory<T>(
    store: &mut T,
    request: ForgetMemoryRequest,
) -> CoreResult<ForgetMemoryResponse>
where
    T: MemoryIndexStore + LifecycleStore + AuditSink,
{
    crate::runtime::lifecycle::validate_identity(&request.tenant_id, &request.user_id, None)?;
    crate::store::validate_scope_component(&request.memory_id, "memory_id")?;
    if request.actor.trim().is_empty() || request.reason.trim().is_empty() {
        return Err(CoreError::InvalidInput(
            "forget actor and reason are required".into(),
        ));
    }
    let lock = format!("privacy/{}", request.user_id);
    let owner = worker_owner();
    if !store.lifecycle_claim(
        &request.tenant_id,
        &lock,
        &owner,
        timestamp_epoch(&now_timestamp())?,
        300,
    )? {
        return Err(CoreError::InvalidInput(
            "user lifecycle is leased by another worker".into(),
        ));
    }
    let result = forget_claimed(store, &request);
    let release = store.lifecycle_release(&request.tenant_id, &lock, &owner);
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

fn forget_claimed<T>(
    store: &mut T,
    request: &ForgetMemoryRequest,
) -> CoreResult<ForgetMemoryResponse>
where
    T: MemoryIndexStore + LifecycleStore + AuditSink,
{
    let key = format!("forget/{}/{}", request.user_id, request.memory_id);
    let existing = store.lifecycle_load(&request.tenant_id, &key)?;
    let mut record = store
        .get_memory(&request.tenant_id, &request.user_id, &request.memory_id)?
        .ok_or_else(|| CoreError::NotFound("memory not found".into()))?;
    let mut response: ForgetMemoryResponse = if let Some(value) = existing {
        serde_json::from_value(value)?
    } else {
        let transition = match record.status {
            MemoryStatus::Active => {
                record.transition(MemoryStatus::SoftDeleted, &request.actor, &request.reason)?
            }
            MemoryStatus::SoftDeleted | MemoryStatus::Redacted => TransitionMetadata {
                memory_id: record.id.clone(),
                from: record.status.clone(),
                to: record.status.clone(),
                actor: request.actor.clone(),
                reason: "existing canonical tombstone acknowledged".into(),
                changed_at: record.updated_at.clone(),
            },
            MemoryStatus::Archived => {
                record.transition(MemoryStatus::SoftDeleted, &request.actor, &request.reason)?
            }
        };
        ForgetMemoryResponse {
            transition,
            redaction_transition: None,
            receipts: Vec::new(),
            status: "incomplete".into(),
            attempts: 0,
            last_error: None,
        }
    };
    if response.status == "completed"
        && (!request.redact
            || response.redaction_transition.is_some()
            || record.status == MemoryStatus::Redacted)
    {
        return Ok(response);
    }
    response.attempts = response.attempts.saturating_add(1);
    response.status = "incomplete".into();
    // The durable intent and source tombstones precede every destructive effect.
    durable_save(
        store,
        &request.tenant_id,
        &key,
        &serde_json::to_value(&response)?,
    )?;
    for source in &record.source_message_ids {
        durable_save(
            store,
            &request.tenant_id,
            &format!("forgotten-source/{}/{}", request.user_id, source),
            &serde_json::json!({"memory_id":request.memory_id,"redact":request.redact}),
        )?;
    }
    if matches!(record.status, MemoryStatus::Active | MemoryStatus::Archived) {
        response.transition =
            record.transition(MemoryStatus::SoftDeleted, &request.actor, &request.reason)?;
    }
    if request.redact && record.status == MemoryStatus::SoftDeleted {
        response.redaction_transition =
            Some(record.transition(MemoryStatus::Redacted, &request.actor, &request.reason)?);
    }
    durable_save(
        store,
        &request.tenant_id,
        &key,
        &serde_json::to_value(&response)?,
    )?;
    store.update_memory(record)?;
    store.emit_audit(AuditEvent::new(
        deterministic_id(&[&key, "requested"]),
        &request.tenant_id,
        &request.actor,
        AuditAction::ForgetRequested,
        "memory",
        Some(request.memory_id.clone()),
        &request.reason,
    ))?;
    match store.lifecycle_forget(
        &request.tenant_id,
        &request.user_id,
        &request.memory_id,
        request.redact,
    ) {
        Ok(receipts) => {
            // Retain backend-specific receipts so operators can identify incomplete propagation.
            for receipt in receipts {
                if let Some(previous) = response
                    .receipts
                    .iter_mut()
                    .find(|r| r.backend == receipt.backend && r.operation == receipt.operation)
                {
                    *previous = receipt;
                } else {
                    response.receipts.push(receipt);
                }
            }
            if !response.receipts.is_empty() && response.receipts.iter().all(acknowledged) {
                response.status = "completed".into();
                response.last_error = None;
                store.emit_audit(AuditEvent::new(
                    deterministic_id(&[&key, "completed"]),
                    &request.tenant_id,
                    &request.actor,
                    AuditAction::ForgetCompleted,
                    "memory",
                    Some(request.memory_id.clone()),
                    &request.reason,
                ))?;
            } else {
                response.last_error =
                    Some("one or more stores have not acknowledged forget propagation".into());
            }
        }
        Err(error) => {
            response.last_error = Some(error.to_string());
        }
    }
    durable_save(
        store,
        &request.tenant_id,
        &key,
        &serde_json::to_value(&response)?,
    )?;
    crate::runtime::observability::record_forget(response.status == "completed");
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::session::{append_session_message, AppendSessionMessageRequest};

    fn fixture() -> (
        crate::store::TestMemoryStore,
        ForgetMemoryRequest,
        AppendSessionMessageRequest,
    ) {
        let mut store = crate::store::TestMemoryStore::new();
        let append:AppendSessionMessageRequest=serde_json::from_value(serde_json::json!({"tenant_id":"t","user_id":"u","session_id":"s","role":"user","content":"private fact","idempotency_key":"once"})).unwrap();
        let message = append_session_message(&mut store, append.clone(), 10).unwrap();
        let mut record = crate::memory::MemoryRecord::new(
            "m",
            "t",
            "u",
            "private fact",
            crate::memory::ContentType::Fact,
            crate::memory::MemoryType::Semantic,
            crate::memory::SourceType::Manual,
        );
        record.session_id = Some("s".into());
        record.source_message_ids = vec![message.message_id];
        store.upsert_memory(record).unwrap();
        let forget = ForgetMemoryRequest {
            tenant_id: "t".into(),
            user_id: "u".into(),
            memory_id: "m".into(),
            actor: "u".into(),
            reason: "explicit request".into(),
            redact: true,
        };
        (store, forget, append)
    }

    #[test]
    fn forgotten_source_message_cannot_be_replayed_into_session() {
        let (mut store, request, append) = fixture();
        let response = forget_memory(&mut store, request.clone()).unwrap();
        assert_eq!(response.status, "completed");
        assert!(append_session_message(&mut store, append, 10).is_err());
        assert_eq!(
            store.get_memory("t", "u", "m").unwrap().unwrap().status,
            MemoryStatus::Redacted
        );
        assert_eq!(forget_memory(&mut store, request).unwrap(), response);
    }

    #[test]
    fn incomplete_forget_resumes_from_durable_per_store_receipts() {
        let (mut store, request, _) = fixture();
        let mut record = store.get_memory("t", "u", "m").unwrap().unwrap();
        let transition = record
            .transition(MemoryStatus::SoftDeleted, "u", "explicit request")
            .unwrap();
        store.update_memory(record).unwrap();
        let checkpoint = ForgetMemoryResponse {
            transition,
            redaction_transition: None,
            receipts: vec![StoreReceipt {
                backend: "test_graph".into(),
                operation: "forget".into(),
                target_id: Some("m".into()),
                status: "failed".into(),
            }],
            status: "incomplete".into(),
            attempts: 1,
            last_error: Some("interrupted propagation".into()),
        };
        store
            .lifecycle_save(
                "t",
                "forget/u/m",
                &serde_json::to_value(checkpoint).unwrap(),
            )
            .unwrap();
        let response = forget_memory(&mut store, request).unwrap();
        assert_eq!(
            (response.status.as_str(), response.attempts),
            ("completed", 2)
        );
        assert!(response.receipts.iter().all(acknowledged));
    }
}
