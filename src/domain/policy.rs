use crate::memory::{deterministic_id, now_timestamp, PrivacyLevel};
use serde::{Deserialize, Serialize};

/// A named procedural policy encoding behavioral preferences for a user.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProceduralPolicy {
    pub id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub name: String,
    pub body: String,
    pub privacy_level: PrivacyLevel,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

impl ProceduralPolicy {
    pub fn new(
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
        name: impl Into<String>,
        body: impl Into<String>,
        privacy_level: PrivacyLevel,
    ) -> crate::contracts::CoreResult<Self> {
        let tenant_id = tenant_id.into();
        let user_id = user_id.into();
        let name = name.into();
        let body = body.into();
        if tenant_id.trim().is_empty() {
            return Err(crate::contracts::CoreError::InvalidInput(
                "tenant_id is required".to_string(),
            ));
        }
        if user_id.trim().is_empty() {
            return Err(crate::contracts::CoreError::InvalidInput(
                "user_id is required".to_string(),
            ));
        }
        if name.trim().is_empty() {
            return Err(crate::contracts::CoreError::InvalidInput(
                "name is required".to_string(),
            ));
        }
        if body.trim().is_empty() {
            return Err(crate::contracts::CoreError::InvalidInput(
                "body cannot be empty".to_string(),
            ));
        }
        let now = now_timestamp();
        Ok(Self {
            id: deterministic_id(&[&tenant_id, &user_id, &name, &body]),
            tenant_id,
            user_id,
            name,
            body,
            privacy_level,
            enabled: true,
            created_at: now.clone(),
            updated_at: now,
        })
    }
}
