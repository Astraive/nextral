use crate::{
    contracts::{CoreError, CoreResult},
    domain::{SessionMessage, SessionSummary, WorkingContext},
    memory::{deterministic_id, estimate_tokens, PrivacyLevel},
    retrieval::{retrieve, RetrievalRequest},
    runtime::{
        consolidation::source_privacy,
        lifecycle::{durable_save, LifecycleStore},
    },
    store::{GraphStore, MemoryIndexStore, SessionStore},
};
use serde::{Deserialize, Serialize};

fn default_privacy() -> PrivacyLevel {
    PrivacyLevel::Private
}

fn default_eligible_limit() -> usize {
    100
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EligibleSessionsRequest {
    pub tenant_id: String,
    pub user_id: String,
    pub inactive_at_or_before: String,
    #[serde(default = "default_eligible_limit")]
    pub limit: usize,
}

pub fn eligible_sessions(
    store: &impl SessionStore,
    request: EligibleSessionsRequest,
) -> CoreResult<Vec<crate::domain::EligibleSession>> {
    crate::runtime::lifecycle::validate_identity(&request.tenant_id, &request.user_id, None)?;
    if !(1..=1000).contains(&request.limit) {
        return Err(CoreError::InvalidInput(
            "eligible session limit must be 1..=1000".into(),
        ));
    }
    crate::runtime::lifecycle::timestamp_nanos(&request.inactive_at_or_before)?;
    store.eligible_sessions(
        &request.tenant_id,
        &request.user_id,
        &request.inactive_at_or_before,
        request.limit,
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendSessionMessageRequest {
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: String,
    pub role: String,
    pub content: String,
    pub idempotency_key: Option<String>,
    #[serde(default = "default_privacy")]
    pub privacy_level: PrivacyLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendSessionMessageResponse {
    pub message_id: String,
    pub hot_tail_count: usize,
    #[serde(default)]
    pub degraded_reasons: Vec<String>,
}

pub fn append_session_message<T>(
    store: &mut T,
    request: AppendSessionMessageRequest,
    hot_tail_limit: usize,
) -> CoreResult<AppendSessionMessageResponse>
where
    T: SessionStore + LifecycleStore,
{
    let tenant = request.tenant_id.clone();
    let user = request.user_id.clone();
    crate::runtime::lifecycle::with_user_lease(store, &tenant, &user, |store, _claim| {
        append_claimed(store, request, hot_tail_limit)
    })
}

fn append_claimed<T>(
    store: &mut T,
    request: AppendSessionMessageRequest,
    hot_tail_limit: usize,
) -> CoreResult<AppendSessionMessageResponse>
where
    T: SessionStore + LifecycleStore,
{
    crate::store::validate_scope_component(&request.session_id, "session_id")?;
    if request.role.trim().is_empty() || request.content.trim().is_empty() {
        return Err(CoreError::InvalidInput(
            "role and content are required".into(),
        ));
    }
    if hot_tail_limit == 0 {
        return Err(CoreError::InvalidInput(
            "hot_tail_limit must be non-zero".into(),
        ));
    }
    let idempotency_key = request.idempotency_key.unwrap_or_else(|| {
        deterministic_id(&[
            &request.tenant_id,
            &request.user_id,
            &request.session_id,
            &request.role,
            &request.content,
        ])
    });
    let mut message = SessionMessage::new(
        request.tenant_id.clone(),
        request.user_id.clone(),
        request.session_id.clone(),
        request.role,
        request.content,
        idempotency_key,
    );
    message.source_metadata = serde_json::json!({"privacy_level":request.privacy_level});
    if store
        .lifecycle_load(
            &request.tenant_id,
            &format!("forgotten-source/{}/{}", request.user_id, message.id),
        )?
        .is_some()
    {
        return Err(CoreError::InvalidInput(
            "forgotten session message cannot be replayed".into(),
        ));
    }
    let message_id = message.id.clone();
    store.append_session_message(message)?;
    // Durable activity is the scheduling seam for host-supplied consolidation work.
    durable_save(
        store,
        &request.tenant_id,
        &format!(
            "session-activity/{}/{}",
            request.user_id, request.session_id
        ),
        &serde_json::json!({"last_activity_at":crate::memory::now_timestamp(),"state":"open"}),
    )?;
    let (hot_tail_count, degraded_reasons) =
        store.hot_tail_status(&request.tenant_id, &request.user_id, &request.session_id)?;
    Ok(AppendSessionMessageResponse {
        message_id,
        hot_tail_count: hot_tail_count.min(hot_tail_limit),
        degraded_reasons,
    })
}

pub fn assemble_working_context<T>(
    store: &mut T,
    request: RetrievalRequest,
    hot_tail_limit: usize,
) -> CoreResult<WorkingContext>
where
    T: SessionStore + MemoryIndexStore + GraphStore + LifecycleStore,
{
    assemble_claimed(store, request, hot_tail_limit)
}

fn assemble_claimed<T>(
    store: &mut T,
    mut request: RetrievalRequest,
    hot_tail_limit: usize,
) -> CoreResult<WorkingContext>
where
    T: SessionStore + MemoryIndexStore + GraphStore + LifecycleStore,
{
    if let Some(session) = request.session_id.as_deref() {
        crate::store::validate_scope_component(session, "session_id")?;
    }
    if request.privacy_scope.is_empty() {
        request.privacy_scope.push(PrivacyLevel::Private);
    }
    store.prepare_retrieval(&mut request)?;
    if request.token_budget == 0 {
        return Err(CoreError::InvalidInput(
            "working context token_budget must be positive".into(),
        ));
    }
    let initial_budget = request.token_budget;
    let mut remaining = initial_budget;
    let mut policies = store.lifecycle_policies(&request.tenant_id, &request.user_id)?;
    policies.retain(|policy| {
        policy.is_visible_to(&request.tenant_id, &request.user_id, &request.privacy_scope)
    });
    policies.sort_by(|a, b| a.id.cmp(&b.id));
    policies.retain(|policy| {
        consume_budget(
            &mut remaining,
            estimate_tokens(&policy.name).saturating_add(estimate_tokens(&policy.body)),
        )
    });
    let mut session_summary = None;
    let mut session_tail = Vec::new();
    let mut degraded_reasons = Vec::new();
    if let Some(session_id) = &request.session_id {
        if let Some(value) = store.lifecycle_load(
            &request.tenant_id,
            &format!("summary/{}/{}", request.user_id, session_id),
        )? {
            let privacy: PrivacyLevel = serde_json::from_value(value["privacy_level"].clone())?;
            let summary: SessionSummary = serde_json::from_value(value["summary"].clone())?;
            let mut forgotten = false;
            for source in &summary.source_message_ids {
                forgotten |= store
                    .lifecycle_load(
                        &request.tenant_id,
                        &format!("forgotten-source/{}/{}", request.user_id, source),
                    )?
                    .is_some();
            }
            if !forgotten
                && summary.tenant_id == request.tenant_id
                && summary.user_id == request.user_id
                && summary.session_id == *session_id
                && request.privacy_scope.contains(&privacy)
                && consume_budget(&mut remaining, estimate_tokens(&summary.summary))
            {
                session_summary = Some(summary);
            }
        }
        let (tail, tail_degradation) = store.session_tail_with_status(
            &request.tenant_id,
            &request.user_id,
            session_id,
            hot_tail_limit,
        )?;
        degraded_reasons = tail_degradation;
        for message in tail.into_iter().rev() {
            if message.tenant_id != request.tenant_id
                || message.user_id != request.user_id
                || message.session_id != *session_id
                || !request.privacy_scope.contains(&source_privacy(&message)?)
            {
                continue;
            }
            if store
                .lifecycle_load(
                    &request.tenant_id,
                    &format!("forgotten-source/{}/{}", request.user_id, message.id),
                )?
                .is_some()
            {
                continue;
            }
            if consume_budget(
                &mut remaining,
                estimate_tokens(&message.role).saturating_add(estimate_tokens(&message.content)),
            ) {
                session_tail.push(message);
            }
        }
        session_tail.reverse();
    }
    let tenant_id = request.tenant_id.clone();
    let user_id = request.user_id.clone();
    let session_id = request.session_id.clone();
    let retrieved_memories = if remaining > 0 && !request.privacy_scope.is_empty() {
        request.token_budget = remaining;
        let retrieval = retrieve(store, request)?;
        retrieval
            .items
            .into_iter()
            .filter(|item| consume_budget(&mut remaining, item.token_estimate))
            .collect()
    } else {
        Vec::new()
    };
    Ok(WorkingContext {
        tenant_id,
        user_id,
        session_id,
        session_tail,
        retrieved_memory_ids: retrieved_memories
            .iter()
            .map(|item| item.memory_id.clone())
            .collect(),
        procedural_policy_ids: policies.iter().map(|policy| policy.id.clone()).collect(),
        procedural_policies: policies,
        session_summary,
        retrieved_memories,
        token_estimate: initial_budget - remaining,
        degraded_reasons,
    })
}

fn consume_budget(remaining: &mut u32, cost: u32) -> bool {
    if cost > *remaining {
        false
    } else {
        *remaining -= cost;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn eligibility_uses_scoped_latest_instants_and_deterministic_metadata() {
        let mut store = crate::store::TestMemoryStore::new();
        for (tenant, user, session, time, key) in [
            ("t", "u", "old", "1", "1"),
            ("t", "u", "a", "1970-01-01T01:00:02+01:00", "2"),
            ("t", "u", "a", "2", "3"),
            ("t", "u", "b", "1970-01-01T00:00:02Z", "4"),
            ("t", "u", "B", "2", "10"),
            ("t", "u", "active", "1", "5"),
            ("t", "u", "active", "3", "6"),
            ("t", "u", "future", "9999999999", "7"),
            ("other", "u", "foreign", "0", "8"),
            ("t", "other", "foreign", "0", "9"),
        ] {
            let mut message = SessionMessage::new(tenant, user, session, "user", "private", key);
            message.created_at = time.into();
            store.append_session_message(message).unwrap();
        }
        let request = EligibleSessionsRequest {
            tenant_id: "t".into(),
            user_id: "u".into(),
            inactive_at_or_before: "1970-01-01T00:00:02Z".into(),
            limit: 4,
        };
        let selected = eligible_sessions(&store, request.clone()).unwrap();
        let metadata = selected.iter().map(|item| {
            (
                item.session_id.as_str(),
                crate::runtime::lifecycle::timestamp_nanos(&item.last_message_at).unwrap(),
                item.message_count,
            )
        });
        assert!(
            metadata.eq([
                ("old", 1_000_000_000, 1),
                ("B", 2_000_000_000, 1),
                ("a", 2_000_000_000, 2),
                ("b", 2_000_000_000, 1),
            ]),
            "{selected:?}"
        );
        assert_eq!(
            eligible_sessions(
                &store,
                EligibleSessionsRequest {
                    limit: 1,
                    ..request
                }
            )
            .unwrap(),
            selected[..1]
        );
    }

    #[test]
    fn eligibility_validates_boundaries_and_timestamp_inputs() {
        let store = crate::store::TestMemoryStore::new();
        let request: EligibleSessionsRequest = serde_json::from_value(serde_json::json!({
            "tenant_id":"t", "user_id":"u", "inactive_at_or_before":"0",
        }))
        .unwrap();
        for limit in [0, 1001] {
            assert!(eligible_sessions(
                &store,
                EligibleSessionsRequest {
                    limit,
                    ..request.clone()
                }
            )
            .is_err());
        }
        assert!(eligible_sessions(
            &store,
            EligibleSessionsRequest {
                inactive_at_or_before: "not-a-time".into(),
                ..request.clone()
            }
        )
        .is_err());
        assert!(eligible_sessions(
            &store,
            EligibleSessionsRequest {
                tenant_id: "../t".into(),
                ..request.clone()
            }
        )
        .is_err());
        assert!(eligible_sessions(
            &store,
            EligibleSessionsRequest {
                limit: 1000,
                ..request
            }
        )
        .unwrap()
        .is_empty());
    }

    #[test]
    fn message_truncation_preserves_utf8_boundary() {
        let message = SessionMessage::new("t", "u", "s", "user", "界".repeat(40_000), "key");
        assert_eq!(message.content.len(), 99_999);
    }
    #[test]
    fn whole_context_budget_never_underflows() {
        let mut remaining = 3;
        assert!(!consume_budget(&mut remaining, 4));
        assert_eq!(remaining, 3);
    }

    #[test]
    fn working_context_budget_includes_scoped_policy_and_session_tail() {
        let mut store = crate::store::TestMemoryStore::new();
        let own =
            crate::domain::ProceduralPolicy::new("t", "u", "p", "one two", PrivacyLevel::Private)
                .unwrap();
        let other = crate::domain::ProceduralPolicy::new(
            "t",
            "someone-else",
            "hidden",
            "secret",
            PrivacyLevel::Private,
        )
        .unwrap();
        store
            .lifecycle_save(
                "t",
                &format!("policy/u/{}", own.id),
                &serde_json::to_value(&own).unwrap(),
            )
            .unwrap();
        store
            .lifecycle_save(
                "t",
                &format!("policy/u/{}", other.id),
                &serde_json::to_value(other).unwrap(),
            )
            .unwrap();
        store
            .append_session_message(SessionMessage::new(
                "t",
                "u",
                "s",
                "user",
                "tail must fit too",
                "one",
            ))
            .unwrap();
        let mut request = RetrievalRequest::test("t", "u", "query");
        request.session_id = Some("s".into());
        request.token_budget = 3;
        let context = assemble_working_context(&mut store, request, 10).unwrap();
        assert_eq!(context.procedural_policy_ids, vec![own.id]);
        assert!(context.session_tail.is_empty());
        assert_eq!(context.token_estimate, 3);
    }
}
