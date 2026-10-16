use crate::{
    config::{NextralConfig, RuntimeBackend},
    contracts::{CoreError, CoreResult},
};
use jsonwebtoken::{decode, decode_header, jwk::JwkSet, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use subtle::ConstantTimeEq;

#[derive(Clone)]
pub(crate) struct Authenticator {
    service_token: Option<Arc<str>>,
    jwt: Option<Arc<JwtConfig>>,
    anonymous_test: bool,
}

struct JwtConfig {
    issuer: String,
    audience: String,
    keys: JwkSet,
}

#[derive(Clone)]
pub(crate) enum Principal {
    Service,
    User { tenant_id: String, user_id: String },
}

#[derive(Clone, Deserialize)]
struct Claims {
    tenant_id: String,
    sub: String,
}

impl Authenticator {
    pub(crate) fn new(config: &NextralConfig) -> CoreResult<Self> {
        let service_token = config
            .auth
            .service_token_env
            .as_ref()
            .map(|name| {
                std::env::var(name)
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .map(Arc::<str>::from)
                    .ok_or_else(|| {
                        CoreError::InvalidInput(format!(
                            "service token environment variable {name} is missing or empty"
                        ))
                    })
            })
            .transpose()?;
        let jwt = match (
            &config.auth.issuer,
            &config.auth.audience,
            &config.auth.jwks_url,
        ) {
            (None, None, None) => None,
            (Some(issuer), Some(audience), Some(url)) => {
                if !url.starts_with("https://") {
                    return Err(CoreError::InvalidInput(
                        "auth.jwks_url must use HTTPS".into(),
                    ));
                }
                let mut client = reqwest::blocking::Client::builder()
                    .timeout(std::time::Duration::from_secs(10))
                    .redirect(reqwest::redirect::Policy::none());
                if let Some(name) = &config.auth.jwks_ca_pem_env {
                    let pem = std::env::var(name)
                        .ok()
                        .filter(|value| !value.trim().is_empty())
                        .ok_or_else(|| {
                            CoreError::InvalidInput(
                                "JWKS CA environment variable is missing or empty".into(),
                            )
                        })?;
                    let certificate =
                        reqwest::Certificate::from_pem(pem.as_bytes()).map_err(|_| {
                            CoreError::InvalidInput(
                                "JWKS CA environment variable must contain a PEM certificate"
                                    .into(),
                            )
                        })?;
                    client = client.add_root_certificate(certificate);
                }
                let client = client.build().map_err(|e| CoreError::Io(e.to_string()))?;
                let response = client
                    .get(url)
                    .send()
                    .and_then(|r| r.error_for_status())
                    .map_err(|e| CoreError::Io(format!("JWKS fetch failed: {e}")))?;
                let bytes = std::io::Read::take(response, 1024 * 1024 + 1);
                let keys: JwkSet = serde_json::from_reader(bytes)?;
                if keys.keys.is_empty() {
                    return Err(CoreError::InvalidInput("JWKS contains no keys".into()));
                }
                Some(Arc::new(JwtConfig {
                    issuer: issuer.clone(),
                    audience: audience.clone(),
                    keys,
                }))
            }
            _ => {
                return Err(CoreError::InvalidInput(
                    "issuer, audience and jwks_url must be configured together".into(),
                ))
            }
        };
        let anonymous_test = config.backend == RuntimeBackend::TestMemory
            && service_token.is_none()
            && jwt.is_none();
        if service_token.is_none() && jwt.is_none() && !anonymous_test {
            return Err(CoreError::InvalidInput(
                "network services require a service token or JWT authentication".into(),
            ));
        }
        Ok(Self {
            service_token,
            jwt,
            anonymous_test,
        })
    }

    pub(crate) fn authenticate(
        &self,
        authorization: Option<&str>,
    ) -> Result<Principal, super::ApiError> {
        let token = match authorization {
            Some(value) => value
                .strip_prefix("Bearer ")
                .filter(|v| !v.is_empty())
                .ok_or_else(|| super::ApiError::unauthorized("Bearer token required"))?,
            None if self.anonymous_test => return Ok(Principal::Service),
            None => return Err(super::ApiError::unauthorized("Bearer token required")),
        };
        if let Some(expected) = &self.service_token {
            if bool::from(expected.as_bytes().ct_eq(token.as_bytes())) {
                return Ok(Principal::Service);
            }
        }
        let jwt = self
            .jwt
            .as_ref()
            .ok_or_else(|| super::ApiError::unauthorized("invalid bearer token"))?;
        let header = decode_header(token)
            .map_err(|_| super::ApiError::unauthorized("invalid JWT header"))?;
        if !matches!(header.alg, Algorithm::RS256 | Algorithm::ES256) {
            return Err(super::ApiError::unauthorized(
                "JWT algorithm is not permitted",
            ));
        }
        let key = header
            .kid
            .as_deref()
            .and_then(|kid| jwt.keys.find(kid))
            .ok_or_else(|| super::ApiError::unauthorized("unknown JWT signing key"))?;
        let key = DecodingKey::from_jwk(key)
            .map_err(|_| super::ApiError::unauthorized("invalid JWT signing key"))?;
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[&jwt.issuer]);
        validation.set_audience(&[&jwt.audience]);
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        let claims = decode::<Claims>(token, &key, &validation)
            .map_err(|_| super::ApiError::unauthorized("JWT validation failed"))?
            .claims;
        if claims.tenant_id.trim().is_empty() || claims.sub.trim().is_empty() {
            return Err(super::ApiError::unauthorized(
                "JWT tenant_id and sub are required",
            ));
        }
        Ok(Principal::User {
            tenant_id: claims.tenant_id,
            user_id: claims.sub,
        })
    }
}

impl Principal {
    pub(crate) fn authorize(&self, payload: &Value) -> Result<(), super::ApiError> {
        let tenant = payload
            .get("tenant_id")
            .and_then(Value::as_str)
            .filter(|v| !v.trim().is_empty());
        let user = payload
            .get("user_id")
            .and_then(Value::as_str)
            .filter(|v| !v.trim().is_empty());
        let (Some(tenant), Some(user)) = (tenant, user) else {
            return Err(super::ApiError::invalid(
                "tenant_id and user_id are required",
            ));
        };
        if tenant.len() > 128 || user.len() > 128 {
            return Err(super::ApiError::invalid(
                "tenant_id and user_id must not exceed 128 bytes",
            ));
        }
        if let Self::User { tenant_id, user_id } = self {
            if tenant != tenant_id || user != user_id {
                return Err(super::ApiError::forbidden(
                    "request scope does not match authenticated identity",
                ));
            }
            for field in ["actor_id", "actor"] {
                if payload
                    .get(field)
                    .and_then(Value::as_str)
                    .is_some_and(|actor| actor != user_id)
                {
                    return Err(super::ApiError::forbidden(
                        "actor does not match authenticated identity",
                    ));
                }
            }
        }
        Ok(())
    }
}
