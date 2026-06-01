use reqwest::blocking::RequestBuilder;
use std::env;

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

    pub fn strict_with_timeouts(token_env: Option<&str>, connect_timeout_ms: u64, request_timeout_ms: u64) -> Self {
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

    pub fn baseline_with_timeouts(token_env: Option<&str>, connect_timeout_ms: u64, request_timeout_ms: u64) -> Self {
        Self {
            require_tls: false,
            connect_timeout_ms,
            request_timeout_ms,
            token_env: token_env.map(|value| value.to_string()),
        }
    }
}

pub fn maybe_add_bearer_auth(
    request: RequestBuilder,
    token_env: Option<&str>,
) -> RequestBuilder {
    if let Some(env_key) = token_env {
        if let Ok(value) = env::var(env_key) {
            if !value.trim().is_empty() {
                return request.bearer_auth(value);
            }
        }
    }
    request
}

pub fn validate_transport_url(url: &str, require_tls: bool) -> Result<(), String> {
    if require_tls && !url.starts_with("https://") {
        return Err("strict transport profile requires https:// endpoint".to_string());
    }
    Ok(())
}
