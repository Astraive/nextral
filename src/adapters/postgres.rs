use crate::{
    adapters::AdapterHealth,
    contracts::{CoreError, CoreResult},
    memory::MemoryRecord,
    ports::{AuditWrite, PostgresPort, TenantUserScope},
    prospective::ReminderRecord,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresAdapter {
    pub url: String,
}

impl PostgresAdapter {
    pub fn new(url: impl Into<String>) -> CoreResult<Self> {
        let url = url.into();
        if url.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "postgres_url is required".to_string(),
            ));
        }
        Ok(Self { url })
    }

    pub fn migration_sql(&self) -> &'static str {
        include_str!("../../migrations/postgres/0001_core_schema.sql")
    }

    pub fn health(&self) -> AdapterHealth {
        AdapterHealth::configured("postgres")
    }
}

impl PostgresPort for PostgresAdapter {
    fn upsert_memory(&self, _record: &MemoryRecord) -> CoreResult<()> {
        Err(CoreError::InvalidInput(
            "postgres driver execution is not enabled in this build".to_string(),
        ))
    }

    fn get_memory(
        &self,
        _scope: &TenantUserScope,
        _memory_id: &str,
    ) -> CoreResult<Option<MemoryRecord>> {
        Err(CoreError::InvalidInput(
            "postgres driver execution is not enabled in this build".to_string(),
        ))
    }

    fn append_session_message(
        &self,
        _scope: &TenantUserScope,
        _session_id: &str,
        _role: &str,
        _content: &str,
    ) -> CoreResult<String> {
        Err(CoreError::InvalidInput(
            "postgres driver execution is not enabled in this build".to_string(),
        ))
    }

    fn upsert_reminder(&self, _reminder: &ReminderRecord) -> CoreResult<()> {
        Err(CoreError::InvalidInput(
            "postgres driver execution is not enabled in this build".to_string(),
        ))
    }

    fn write_audit(&self, _event: &AuditWrite) -> CoreResult<()> {
        Err(CoreError::InvalidInput(
            "postgres driver execution is not enabled in this build".to_string(),
        ))
    }

    fn enqueue_outbox(
        &self,
        _tenant_id: &str,
        _event_type: &str,
        _aggregate_id: &str,
        _payload_json: &str,
    ) -> CoreResult<String> {
        Err(CoreError::InvalidInput(
            "postgres driver execution is not enabled in this build".to_string(),
        ))
    }
}
