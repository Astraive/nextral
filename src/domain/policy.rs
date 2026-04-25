use crate::memory::{deterministic_id, now_timestamp, PrivacyLevel};
use serde::{Deserialize, Serialize};

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
    ) -> Self {
        let tenant_id = tenant_id.into();
        let user_id = user_id.into();
        let name = name.into();
        let body = body.into();
        let now = now_timestamp();
        Self {
            id: deterministic_id(&[&tenant_id, &user_id, &name, &body]),
            tenant_id,
            user_id,
            name,
            body,
            privacy_level,
            enabled: true,
            created_at: now.clone(),
            updated_at: now,
        }
    }
}
