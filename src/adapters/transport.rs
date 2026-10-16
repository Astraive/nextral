#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportHardeningProfile {
    pub require_tls: bool,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub token_env: Option<String>,
}

impl TransportHardeningProfile {
    pub fn strict(token_env: Option<&str>) -> Self {
        Self::strict_with_timeouts(token_env, 2000, 4000)
    }

    pub fn strict_with_timeouts(
        token_env: Option<&str>,
        connect_timeout_ms: u64,
        request_timeout_ms: u64,
    ) -> Self {
        Self {
            require_tls: true,
            connect_timeout_ms,
            request_timeout_ms,
            token_env: token_env.map(|value| value.to_string()),
        }
    }

    pub fn baseline(token_env: Option<&str>) -> Self {
        Self::baseline_with_timeouts(token_env, 2000, 4000)
    }

    pub fn baseline_with_timeouts(
        token_env: Option<&str>,
        connect_timeout_ms: u64,
        request_timeout_ms: u64,
    ) -> Self {
        Self {
            require_tls: false,
            connect_timeout_ms,
            request_timeout_ms,
            token_env: token_env.map(|value| value.to_string()),
        }
    }
}

pub fn validate_transport_url(url: &str, require_tls: bool) -> Result<(), String> {
    let url = reqwest::Url::parse(url).map_err(|_| "invalid HTTP endpoint".to_string())?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err("endpoint must use http:// or https://".to_string());
    }
    if require_tls && url.scheme() != "https" {
        return Err("strict transport profile requires https:// endpoint".to_string());
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("HTTP endpoint must not contain credentials, query or fragment".to_string());
    }
    Ok(())
}

pub fn hardened_client(
    profile: &TransportHardeningProfile,
) -> crate::contracts::CoreResult<reqwest::blocking::Client> {
    use crate::contracts::CoreError;
    use std::time::Duration;
    if profile.connect_timeout_ms == 0 || profile.request_timeout_ms == 0 {
        return Err(CoreError::InvalidInput(
            "transport timeouts must be non-zero".into(),
        ));
    }
    reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_millis(profile.connect_timeout_ms))
        .timeout(Duration::from_millis(profile.request_timeout_ms))
        .build()
        .map_err(|error| CoreError::Io(error.to_string()))
}

pub fn env_secret(name: &str) -> crate::contracts::CoreResult<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            crate::contracts::CoreError::InvalidInput(format!(
                "credential environment variable {name} is missing or empty"
            ))
        })
}

pub(crate) fn store_failure(
    backend: crate::runtime::observability::StoreBackend,
    error: crate::contracts::CoreError,
) -> crate::contracts::CoreError {
    crate::runtime::observability::record_store_error(backend);
    error
}

/// Hex encoding is reversible and never drops or truncates identity bytes.
pub fn key_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value.bytes() {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 15) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn keys_preserve_distinct_identity_boundaries() {
        assert_ne!(key_component("a/b"), key_component("ab"));
        assert_ne!(key_component(""), key_component("."));
        assert_ne!(key_component("é"), key_component("e"));
    }
    #[test]
    fn strict_transport_rejects_plaintext_and_url_credentials() {
        assert!(validate_transport_url("http://localhost", true).is_err());
        assert!(validate_transport_url("https://user:secret@example.com", false).is_err());
        assert!(validate_transport_url("https://example.com", true).is_ok());
    }
}
