use crate::{
    adapters::{
        transport::{maybe_add_bearer_auth, validate_transport_url, TransportHardeningProfile},
        AdapterHealth,
    },
    contracts::{CoreError, CoreResult},
    ports::{ArchiveObject, ArchiveReceipt, ObjectArchivePort},
};
use reqwest::blocking::Client;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Adapter {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key_env: String,
    pub secret_key_env: String,
    pub hardening: TransportHardeningProfile,
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
            hardening: TransportHardeningProfile::baseline(Some("NEXTRAL_S3_API_KEY")),
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
        validate_transport_url(&adapter.endpoint, adapter.hardening.require_tls)
            .map_err(CoreError::InvalidInput)?;
        Ok(adapter)
    }

    pub fn archive_policy_json(&self) -> &'static str {
        include_str!("../../migrations/s3/archive_policy.json")
    }

    pub fn health(&self) -> AdapterHealth {
        AdapterHealth::configured("s3")
    }

    pub fn readiness(&self) -> CoreResult<Value> {
        let response = maybe_add_bearer_auth(
            Client::new().get(format!(
                "{}/{}",
                self.endpoint.trim_end_matches('/'),
                self.bucket
            )),
            self.hardening.token_env.as_deref(),
        )
            .send()
            .map_err(|error| CoreError::Io(error.to_string()))?;
        Ok(serde_json::json!({
            "status": response.status().as_u16(),
            "bucket": self.bucket,
            "endpoint": self.endpoint,
        }))
    }
}

impl ObjectArchivePort for S3Adapter {
    fn put_object(&self, object: &ArchiveObject) -> CoreResult<ArchiveReceipt> {
        let object_key = format!(
            "tenants/{}/users/{}/sessions/{}/memories/{}/{}.bin",
            object.tenant_id,
            object.user_id,
            object.session_id.as_deref().unwrap_or("unknown_session"),
            object.memory_id.as_deref().unwrap_or("unknown_memory"),
            object.object_kind
        );
        let response = maybe_add_bearer_auth(
            Client::new().put(format!(
                "{}/{}/{}",
                self.endpoint.trim_end_matches('/'),
                self.bucket,
                object_key
            )),
            self.hardening.token_env.as_deref(),
        )
        .body(object.bytes.clone())
            .header("x-amz-meta-tenant-id", object.tenant_id.clone())
            .header("x-amz-meta-user-id", object.user_id.clone())
            .header(
                "x-amz-meta-memory-id",
                object.memory_id.clone().unwrap_or_default(),
            )
            .header("x-amz-meta-content-sha256", object.content_sha256.clone())
            .send()
            .map_err(|error| CoreError::Io(error.to_string()))?;
        if !response.status().is_success() {
            return Err(CoreError::Io(format!(
                "s3 put_object failed: {}",
                response.status()
            )));
        }
        Ok(ArchiveReceipt {
            bucket: self.bucket.clone(),
            object_key,
            content_sha256: object.content_sha256.clone(),
        })
    }

    fn tombstone_object(
        &self,
        _tenant_id: &str,
        object_key: &str,
        _reason: &str,
    ) -> CoreResult<()> {
        let response = maybe_add_bearer_auth(
            Client::new().delete(format!(
                "{}/{}/{}",
                self.endpoint.trim_end_matches('/'),
                self.bucket,
                object_key
            )),
            self.hardening.token_env.as_deref(),
        )
            .send()
            .map_err(|error| CoreError::Io(error.to_string()))?;
        if !response.status().is_success() {
            return Err(CoreError::Io(format!(
                "s3 tombstone_object failed: {}",
                response.status()
            )));
        }
        Ok(())
    }
}
