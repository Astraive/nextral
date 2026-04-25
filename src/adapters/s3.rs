use crate::{
    adapters::AdapterHealth,
    contracts::{CoreError, CoreResult},
    ports::{ArchiveObject, ArchiveReceipt, ObjectArchivePort},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Adapter {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key_env: String,
    pub secret_key_env: String,
}

impl S3Adapter {
    pub fn new(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        region: impl Into<String>,
        access_key_env: impl Into<String>,
        secret_key_env: impl Into<String>,
    ) -> CoreResult<Self> {
        let adapter = Self {
            endpoint: endpoint.into(),
            bucket: bucket.into(),
            region: region.into(),
            access_key_env: access_key_env.into(),
            secret_key_env: secret_key_env.into(),
        };
        for (name, value) in [
            ("s3_endpoint", &adapter.endpoint),
            ("s3_bucket", &adapter.bucket),
            ("s3_region", &adapter.region),
            ("s3_access_key_env", &adapter.access_key_env),
            ("s3_secret_key_env", &adapter.secret_key_env),
        ] {
            if value.trim().is_empty() {
                return Err(CoreError::InvalidInput(format!("{name} is required")));
            }
        }
        Ok(adapter)
    }

    pub fn archive_policy_json(&self) -> &'static str {
        include_str!("../../migrations/s3/archive_policy.json")
    }

    pub fn health(&self) -> AdapterHealth {
        AdapterHealth::configured("s3")
    }
}

impl ObjectArchivePort for S3Adapter {
    fn put_object(&self, _object: &ArchiveObject) -> CoreResult<ArchiveReceipt> {
        Err(CoreError::InvalidInput(
            "s3 driver execution is not enabled in this build".to_string(),
        ))
    }

    fn tombstone_object(
        &self,
        _tenant_id: &str,
        _object_key: &str,
        _reason: &str,
    ) -> CoreResult<()> {
        Err(CoreError::InvalidInput(
            "s3 driver execution is not enabled in this build".to_string(),
        ))
    }
}
