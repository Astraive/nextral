use crate::{
    adapters::{
        transport::{
            env_secret, hardened_client, key_component, store_failure, validate_transport_url,
            TransportHardeningProfile,
        },
        AdapterHealth,
    },
    contracts::{CoreError, CoreResult},
    ports::{ArchiveObject, ArchiveReceipt, ObjectArchivePort},
    runtime::observability::StoreBackend,
};
use hmac::{Hmac, Mac};
use reqwest::{blocking::Response, Method, Url};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

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
            hardening: TransportHardeningProfile::baseline(None),
        };
        for (name, value) in [
            ("s3_bucket", &adapter.bucket),
            ("s3_region", &adapter.region),
            ("s3_access_key_env", &adapter.access_key_env),
            ("s3_secret_key_env", &adapter.secret_key_env),
        ] {
            if value.trim().is_empty() {
                return Err(CoreError::InvalidInput(format!("{name} is required")));
            }
        }
        if !adapter
            .bucket
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
        {
            return Err(CoreError::InvalidInput("invalid S3 bucket name".into()));
        }
        validate_transport_url(&adapter.endpoint, false).map_err(CoreError::InvalidInput)?;
        Ok(adapter)
    }

    pub fn archive_policy_json(&self) -> &'static str {
        include_str!("../../migrations/s3/archive_policy.json")
    }
    pub fn health(&self) -> AdapterHealth {
        AdapterHealth::configured("s3")
    }

    fn object_url(&self, key: Option<&str>) -> CoreResult<Url> {
        validate_transport_url(&self.endpoint, self.hardening.require_tls)
            .map_err(CoreError::InvalidInput)?;
        let mut url =
            Url::parse(&self.endpoint).map_err(|e| CoreError::InvalidInput(e.to_string()))?;
        let mut path = url
            .path_segments_mut()
            .map_err(|_| CoreError::InvalidInput("invalid S3 base URL".into()))?;
        path.pop_if_empty().push(&self.bucket);
        if let Some(key) = key {
            for part in key.split('/') {
                path.push(part);
            }
        }
        drop(path);
        Ok(url)
    }

    fn request(
        &self,
        method: Method,
        key: Option<&str>,
        bytes: Vec<u8>,
        mut headers: BTreeMap<String, String>,
    ) -> CoreResult<Response> {
        let url = self.object_url(key)?;
        let access = env_secret(&self.access_key_env)?;
        let secret = env_secret(&self.secret_key_env)?;
        let now = chrono::Utc::now();
        let timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = now.format("%Y%m%d").to_string();
        let mut host = url
            .host()
            .ok_or_else(|| CoreError::InvalidInput("S3 host is missing".into()))?
            .to_string();
        if let Some(port) = url.port() {
            host.push_str(&format!(":{port}"));
        }
        headers.insert("host".into(), host);
        headers.insert("x-amz-date".into(), timestamp.clone());
        let digest = compute_sha256(&bytes);
        headers.insert("x-amz-content-sha256".into(), digest.clone());
        if let Ok(token) = std::env::var("AWS_SESSION_TOKEN") {
            if !token.is_empty() {
                headers.insert("x-amz-security-token".into(), token);
            }
        }
        let authorization = authorization(
            &SigningIdentity {
                access: &access,
                secret: &secret,
                region: &self.region,
            },
            &date,
            &timestamp,
            method.as_str(),
            url.path(),
            &headers,
            &digest,
        )?;
        let mut request = hardened_client(&self.hardening)?
            .request(method, url)
            .body(bytes);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        request
            .header("authorization", authorization)
            .send()
            .map_err(|e| {
                store_failure(
                    StoreBackend::S3,
                    CoreError::Io(format!("S3 request failed: {e}")),
                )
            })
    }

    pub fn readiness(&self) -> CoreResult<Value> {
        let response = self.request(Method::HEAD, None, Vec::new(), BTreeMap::new())?;
        require_success(response, "bucket readiness")?;
        Ok(json!({"status":"ready", "bucket":self.bucket}))
    }

    fn validate_key(&self, tenant_id: &str, key: &str) -> CoreResult<()> {
        if tenant_id.is_empty()
            || !key.starts_with(&format!("tenants/{}/", key_component(tenant_id)))
            || key
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
            || key.contains(['\\', '?', '#', '%'])
        {
            return Err(CoreError::InvalidInput(
                "archive object key does not belong to tenant or is unsafe".into(),
            ));
        }
        Ok(())
    }

    /// Plan an immutable key; upload verifies the declared checksum.
    pub fn receipt_for_object(&self, object: &ArchiveObject) -> CoreResult<ArchiveReceipt> {
        if object.tenant_id.is_empty() || object.user_id.is_empty() || object.object_kind.is_empty()
        {
            return Err(CoreError::InvalidInput(
                "archive tenant, user and kind are required".into(),
            ));
        }
        if object.bytes.len() > 100 * 1024 * 1024 {
            return Err(CoreError::InvalidInput(
                "archive object exceeds 100 MiB".into(),
            ));
        }
        if object.content_sha256.len() != 64
            || !object
                .content_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(CoreError::InvalidInput(
                "archive content_sha256 is invalid".into(),
            ));
        }
        Ok(ArchiveReceipt {
            bucket: self.bucket.clone(),
            object_key: format!(
                "tenants/{}/users/{}/sessions/{}/memories/{}/{}/{}.bin",
                key_component(&object.tenant_id),
                key_component(&object.user_id),
                optional_component(object.session_id.as_deref()),
                optional_component(object.memory_id.as_deref()),
                key_component(&object.object_kind),
                object.content_sha256
            ),
            content_sha256: object.content_sha256.clone(),
        })
    }

    pub fn get_verified_object(
        &self,
        tenant_id: &str,
        receipt: &ArchiveReceipt,
    ) -> CoreResult<Vec<u8>> {
        if receipt.bucket != self.bucket {
            return Err(CoreError::InvalidInput(
                "archive receipt bucket mismatch".into(),
            ));
        }
        self.validate_key(tenant_id, &receipt.object_key)?;
        let response = self.request(
            Method::GET,
            Some(&receipt.object_key),
            Vec::new(),
            BTreeMap::new(),
        )?;
        if response.status().as_u16() == 404 {
            return Err(CoreError::NotFound("archive object not found".into()));
        }
        let response = require_success(response, "get object")?;
        if response
            .headers()
            .get("x-amz-meta-nextral-tombstone")
            .and_then(|value| value.to_str().ok())
            == Some("true")
        {
            return Err(CoreError::NotFound("archive object is tombstoned".into()));
        }
        let bytes = response
            .bytes()
            .map_err(|e| store_failure(StoreBackend::S3, CoreError::Io(e.to_string())))?
            .to_vec();
        if compute_sha256(&bytes) != receipt.content_sha256 {
            return Err(store_failure(
                StoreBackend::S3,
                CoreError::Io("archive integrity check failed".into()),
            ));
        }
        Ok(bytes)
    }
}

impl ObjectArchivePort for S3Adapter {
    fn put_object(&self, object: &ArchiveObject) -> CoreResult<ArchiveReceipt> {
        let receipt = self.receipt_for_object(object)?;
        if compute_sha256(&object.bytes) != object.content_sha256 {
            return Err(CoreError::InvalidInput(
                "archive content_sha256 mismatch".into(),
            ));
        }
        let mut headers = BTreeMap::new();
        headers.insert(
            "x-amz-meta-content-sha256".into(),
            receipt.content_sha256.clone(),
        );
        headers.insert(
            "x-amz-meta-tenant-id".into(),
            key_component(&object.tenant_id),
        );
        headers.insert("x-amz-meta-user-id".into(), key_component(&object.user_id));
        headers.insert("if-none-match".into(), "*".into());
        let response = self.request(
            Method::PUT,
            Some(&receipt.object_key),
            object.bytes.clone(),
            headers,
        )?;
        if response.status().as_u16() == 412 {
            let existing = require_success(
                self.request(
                    Method::HEAD,
                    Some(&receipt.object_key),
                    Vec::new(),
                    BTreeMap::new(),
                )?,
                "inspect immutable object",
            )?;
            if existing
                .headers()
                .get("x-amz-meta-content-sha256")
                .and_then(|value| value.to_str().ok())
                != Some(receipt.content_sha256.as_str())
            {
                return Err(CoreError::Conflict(
                    "archive object is tombstoned or differs from its receipt".into(),
                ));
            }
        } else {
            require_success(response, "put object")?;
        }
        Ok(receipt)
    }

    fn tombstone_object(&self, tenant_id: &str, object_key: &str, reason: &str) -> CoreResult<()> {
        self.validate_key(tenant_id, object_key)?;
        let bytes = serde_json::to_vec(
            &json!({"object_key":object_key,"reason":reason,"at":chrono::Utc::now().to_rfc3339()}),
        )?;
        require_success(
            self.request(
                Method::PUT,
                Some(object_key),
                bytes,
                BTreeMap::from([("x-amz-meta-nextral-tombstone".into(), "true".into())]),
            )?,
            "write tombstone",
        )?;
        Ok(())
    }
}

fn optional_component(value: Option<&str>) -> String {
    value
        .map(|s| format!("some-{}", key_component(s)))
        .unwrap_or_else(|| "none".into())
}
fn compute_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn hmac(key: &[u8], value: &str) -> CoreResult<Vec<u8>> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(key).map_err(|e| CoreError::InvalidInput(e.to_string()))?;
    mac.update(value.as_bytes());
    Ok(mac.finalize().into_bytes().to_vec())
}
struct SigningIdentity<'a> {
    access: &'a str,
    secret: &'a str,
    region: &'a str,
}

fn authorization(
    identity: &SigningIdentity<'_>,
    date: &str,
    timestamp: &str,
    method: &str,
    path: &str,
    headers: &BTreeMap<String, String>,
    digest: &str,
) -> CoreResult<String> {
    let SigningIdentity {
        access,
        secret,
        region,
    } = *identity;
    let signed = headers
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers: String = headers
        .iter()
        .map(|(key, value)| {
            format!(
                "{key}:{}\n",
                value.split_whitespace().collect::<Vec<_>>().join(" ")
            )
        })
        .collect();
    let canonical = format!("{method}\n{path}\n\n{canonical_headers}\n{signed}\n{digest}");
    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        compute_sha256(canonical.as_bytes())
    );
    let key = hmac(format!("AWS4{secret}").as_bytes(), date)?;
    let key = hmac(&key, region)?;
    let key = hmac(&key, "s3")?;
    let key = hmac(&key, "aws4_request")?;
    let signature = hmac(&key, &string_to_sign)?
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    Ok(format!("AWS4-HMAC-SHA256 Credential={access}/{scope}, SignedHeaders={signed}, Signature={signature}"))
}
fn require_success(response: Response, operation: &str) -> CoreResult<Response> {
    if !response.status().is_success() {
        return Err(store_failure(
            StoreBackend::S3,
            CoreError::Io(format!("S3 {operation} failed: {}", response.status())),
        ));
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tenant_keys_reject_collision_and_traversal() {
        let adapter =
            S3Adapter::new("http://localhost:9000", "bucket", "us-east-1", "A", "B").unwrap();
        assert!(adapter
            .validate_key("a/b", "tenants/6162/users/75/file")
            .is_err());
        assert!(adapter.validate_key("ab", "tenants/6162/../other").is_err());
        assert!(adapter
            .validate_key("ab", "tenants/6162/users/75/file")
            .is_ok());
        assert_ne!(optional_component(None), optional_component(Some("none")));
    }
    #[test]
    fn invalid_checksum_fails_before_credentials_or_network() {
        let adapter = S3Adapter::new(
            "http://localhost:1",
            "bucket",
            "us-east-1",
            "UNSET_ACCESS",
            "UNSET_SECRET",
        )
        .unwrap();
        let object = ArchiveObject {
            tenant_id: "t".into(),
            user_id: "u".into(),
            session_id: None,
            memory_id: None,
            object_kind: "record".into(),
            content_sha256: "bad".into(),
            bytes: b"abc".to_vec(),
        };
        assert!(adapter
            .put_object(&object)
            .unwrap_err()
            .to_string()
            .contains("sha256"));
    }
    #[test]
    fn readiness_signs_credentials_and_rejects_access_denied() {
        use std::{
            io::{Read, Write},
            net::TcpListener,
            time::Duration,
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = socket.read(&mut buffer).unwrap();
                assert!(count > 0);
                request.extend_from_slice(&buffer[..count]);
            }
            socket
                .write_all(
                    b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            String::from_utf8(request).unwrap()
        });
        std::env::set_var("NEXTRAL_S3_TEST_ACCESS_SIGNING", "example-access");
        std::env::set_var("NEXTRAL_S3_TEST_SECRET_SIGNING", "example-secret");
        let adapter = S3Adapter::new(
            format!("http://{address}"),
            "bucket",
            "us-east-1",
            "NEXTRAL_S3_TEST_ACCESS_SIGNING",
            "NEXTRAL_S3_TEST_SECRET_SIGNING",
        )
        .unwrap();
        let result = adapter.readiness();
        std::env::remove_var("NEXTRAL_S3_TEST_ACCESS_SIGNING");
        std::env::remove_var("NEXTRAL_S3_TEST_SECRET_SIGNING");
        let request = server.join().unwrap();
        assert!(result.unwrap_err().to_string().contains("403"));
        assert!(request.starts_with("HEAD /bucket HTTP/1.1"));
        let request = request.to_ascii_lowercase();
        assert!(request.contains("authorization: aws4-hmac-sha256 credential=example-access/"));
        assert!(request.contains("/us-east-1/s3/aws4_request"));
        assert!(!request.contains("bearer"));
        assert!(!request.contains("example-secret"));
    }
    #[test]
    #[ignore = "requires private MinIO bucket and NEXTRAL_S3_ENDPOINT/BUCKET/REGION/ACCESS_KEY/SECRET_KEY"]
    fn real_private_bucket_round_trips_integrity_and_deletes_tombstoned_bytes() {
        let adapter = S3Adapter::new(
            std::env::var("NEXTRAL_S3_ENDPOINT").unwrap(),
            std::env::var("NEXTRAL_S3_BUCKET").unwrap(),
            std::env::var("NEXTRAL_S3_REGION").unwrap(),
            "NEXTRAL_S3_ACCESS_KEY",
            "NEXTRAL_S3_SECRET_KEY",
        )
        .unwrap();
        adapter.readiness().unwrap();
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let bytes = b"credentialed immutable archive".to_vec();
        let object = ArchiveObject {
            tenant_id: format!("tenant/../{unique}"),
            user_id: "user:?".into(),
            session_id: None,
            memory_id: Some("mem/non-uuid".into()),
            object_kind: "record".into(),
            content_sha256: compute_sha256(&bytes),
            bytes,
        };
        let receipt = adapter.put_object(&object).unwrap();
        assert_eq!(
            adapter
                .get_verified_object(&object.tenant_id, &receipt)
                .unwrap(),
            object.bytes
        );
        assert!(adapter
            .get_verified_object("other-tenant", &receipt)
            .is_err());
        let mut corrupt = receipt.clone();
        corrupt.content_sha256 = "wrong".into();
        assert!(adapter
            .get_verified_object(&object.tenant_id, &corrupt)
            .is_err());
        adapter
            .tombstone_object(&object.tenant_id, &receipt.object_key, "regression cleanup")
            .unwrap();
        assert!(adapter
            .get_verified_object(&object.tenant_id, &receipt)
            .is_err());
        assert!(matches!(
            adapter.put_object(&object),
            Err(CoreError::Conflict(_))
        ));
    }
}
