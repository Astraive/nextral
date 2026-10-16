use crate::{
    contracts::{CoreError, CoreResult},
    domain::{MemoryRecord, ProceduralPolicy, ReminderRecord, StoreReceipt},
};
use serde_json::Value;

/// Durable state and externally acknowledged effects used by lifecycle workers.
/// Claims must be atomic, expire at `now_epoch + lease_seconds`, and fence other owners.
pub trait LifecycleStore {
    fn lifecycle_load(&self, tenant: &str, key: &str) -> CoreResult<Option<Value>>;
    fn lifecycle_list(&self, tenant: &str, prefix: &str) -> CoreResult<Vec<Value>>;
    fn lifecycle_save(
        &mut self,
        tenant: &str,
        key: &str,
        value: &Value,
    ) -> CoreResult<StoreReceipt>;
    /// Atomically fence the claim; return scrubbed or terminal state instead of replacing it.
    /// None means the proposed progress was saved.
    fn lifecycle_save_claimed(
        &mut self,
        tenant: &str,
        key: &str,
        owner: &str,
        value: &Value,
    ) -> CoreResult<Option<Value>>;
    fn lifecycle_claim(
        &mut self,
        tenant: &str,
        key: &str,
        owner: &str,
        now_epoch: u64,
        lease_seconds: u64,
    ) -> CoreResult<bool>;
    fn lifecycle_release(&mut self, tenant: &str, key: &str, owner: &str) -> CoreResult<()>;
    fn lifecycle_archive(
        &mut self,
        tenant: &str,
        key: &str,
        body: &Value,
    ) -> CoreResult<StoreReceipt>;
    /// Verify actual archived source coverage before scheduled source erasure.
    fn lifecycle_verify_archive_sources(&self, memory: &MemoryRecord) -> CoreResult<()>;
    /// Return one real acknowledgement or failure receipt for every configured backend.
    /// Remove derived summaries, source transcripts, archives and shadow vectors as well.
    fn lifecycle_forget(
        &mut self,
        tenant: &str,
        user: &str,
        memory: &str,
        redact: bool,
    ) -> CoreResult<Vec<StoreReceipt>>;
    fn lifecycle_policies(&self, tenant: &str, user: &str) -> CoreResult<Vec<ProceduralPolicy>>;
    /// Dispatch the stored reminder to a configured host callback/webhook using its dedupe key.
    fn lifecycle_dispatch(&mut self, reminder: &ReminderRecord) -> CoreResult<StoreReceipt>;
    fn lifecycle_reembed_write(
        &mut self,
        tenant: &str,
        collection: &str,
        memory: &MemoryRecord,
        vector: &[f32],
    ) -> CoreResult<StoreReceipt>;
    fn lifecycle_reembed_count(
        &self,
        tenant: &str,
        user: &str,
        collection: &str,
    ) -> CoreResult<usize>;
    fn lifecycle_reembed_read(
        &self,
        tenant: &str,
        user: &str,
        collection: &str,
        memory_id: &str,
    ) -> CoreResult<Option<Vec<f32>>>;
    fn lifecycle_reembed_cutover(
        &mut self,
        tenant: &str,
        user: &str,
        source: &str,
        shadow: &str,
    ) -> CoreResult<StoreReceipt>;
}

pub fn renew_lease(
    store: &mut impl LifecycleStore,
    tenant: &str,
    key: &str,
    owner: &str,
    lease_seconds: u64,
) -> CoreResult<()> {
    if !store.lifecycle_claim(
        tenant,
        key,
        owner,
        timestamp_epoch(&crate::memory::now_timestamp())?,
        lease_seconds,
    )? {
        return Err(CoreError::InvalidInput(
            "worker lost its durable lifecycle lease".into(),
        ));
    }
    Ok(())
}

pub fn with_user_lease<T, R>(
    store: &mut T,
    tenant: &str,
    user: &str,
    work: impl FnOnce(&mut T) -> CoreResult<R>,
) -> CoreResult<R>
where
    T: LifecycleStore,
{
    validate_identity(tenant, user, None)?;
    let owner = worker_owner();
    let key = format!("privacy/{user}");
    renew_lease(store, tenant, &key, &owner, 300)?;
    let result = work(store);
    let release = store.lifecycle_release(tenant, &key, &owner);
    match result {
        Ok(value) => {
            release?;
            Ok(value)
        }
        Err(error) => {
            let _ = release;
            Err(error)
        }
    }
}

pub fn validate_identity(tenant: &str, user: &str, session: Option<&str>) -> CoreResult<()> {
    crate::store::validate_scope_component(tenant, "tenant_id")?;
    crate::store::validate_scope_component(user, "user_id")?;
    if let Some(session) = session {
        crate::store::validate_scope_component(session, "session_id")?;
    }
    Ok(())
}

pub fn timestamp_nanos(value: &str) -> CoreResult<u128> {
    if let Ok(epoch) = value.parse::<u64>() {
        return Ok(u128::from(epoch) * 1_000_000_000);
    }
    let parsed = chrono::DateTime::parse_from_rfc3339(value).map_err(|_| {
        CoreError::InvalidInput("timestamp must be epoch seconds or RFC3339".into())
    })?;
    let seconds = u64::try_from(parsed.timestamp())
        .map_err(|_| CoreError::InvalidInput("timestamp must not precede the Unix epoch".into()))?;
    Ok(u128::from(seconds) * 1_000_000_000 + u128::from(parsed.timestamp_subsec_nanos()))
}

/// Whole seconds for leases and measured ages; use nanoseconds for due comparisons.
pub fn timestamp_epoch(value: &str) -> CoreResult<u64> {
    u64::try_from(timestamp_nanos(value)? / 1_000_000_000)
        .map_err(|_| CoreError::InvalidInput("timestamp exceeds epoch range".into()))
}

pub fn canonical_timestamp(value: &str) -> CoreResult<String> {
    format_timestamp_nanos(timestamp_nanos(value)?)
}

pub fn timestamp_add_seconds(value: &str, seconds: u64) -> CoreResult<String> {
    let nanos = timestamp_nanos(value)?
        .checked_add(u128::from(seconds) * 1_000_000_000)
        .ok_or_else(|| CoreError::InvalidInput("timestamp arithmetic overflow".into()))?;
    format_timestamp_nanos(nanos)
}

fn format_timestamp_nanos(nanos: u128) -> CoreResult<String> {
    let seconds = u64::try_from(nanos / 1_000_000_000)
        .map_err(|_| CoreError::InvalidInput("timestamp exceeds epoch range".into()))?;
    let fractional = (nanos % 1_000_000_000) as u32;
    if fractional == 0 {
        return Ok(seconds.to_string());
    }
    let seconds = i64::try_from(seconds)
        .map_err(|_| CoreError::InvalidInput("timestamp exceeds RFC3339 range".into()))?;
    let timestamp = chrono::DateTime::from_timestamp(seconds, fractional)
        .ok_or_else(|| CoreError::InvalidInput("timestamp exceeds RFC3339 range".into()))?;
    Ok(timestamp.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
}

pub fn acknowledged(receipt: &StoreReceipt) -> bool {
    matches!(receipt.status.as_str(), "ok" | "acknowledged")
}

pub fn durable_save(
    store: &mut impl LifecycleStore,
    tenant: &str,
    key: &str,
    value: &Value,
) -> CoreResult<StoreReceipt> {
    let receipt = store.lifecycle_save(tenant, key, value)?;
    if !acknowledged(&receipt) {
        return Err(CoreError::InvalidInput(
            "durable state write was not acknowledged".into(),
        ));
    }
    Ok(receipt)
}

pub fn worker_owner() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    format!(
        "{}-{}-{}",
        std::process::id(),
        crate::memory::now_timestamp(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equivalent_timestamp_interfaces_have_identical_epoch() {
        assert_eq!(
            timestamp_epoch("2026-01-01T01:00:00+01:00").unwrap(),
            timestamp_epoch("1767225600").unwrap()
        );
    }

    #[test]
    fn invalid_timestamp_does_not_silently_become_epoch_zero() {
        assert!(timestamp_epoch("later").is_err());
    }

    #[test]
    fn fractional_rfc3339_due_times_preserve_order_and_retry_precision() {
        let earlier = timestamp_nanos("2026-01-01T00:00:00.100Z").unwrap();
        let later = timestamp_nanos("2026-01-01T00:00:00.900Z").unwrap();
        assert!(earlier < later);
        assert_eq!(
            timestamp_add_seconds("2026-01-01T01:00:00.900+01:00", 60).unwrap(),
            "2026-01-01T00:01:00.900Z"
        );
    }

    #[test]
    fn ambiguous_identity_key_components_are_rejected() {
        assert!(validate_identity("t", "a/b", Some("c")).is_err());
    }
}
