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
        Self {
            require_tls: true,
            connect_timeout_ms: 2000,
            request_timeout_ms: 4000,
            token_env: token_env.map(|value| value.to_string()),
        }
    }

    pub fn baseline(token_env: Option<&str>) -> Self {
        Self {
            require_tls: false,
            connect_timeout_ms: 2000,
            request_timeout_ms: 4000,
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
