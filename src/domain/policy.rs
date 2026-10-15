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
        crate::store::validate_scope_component(&tenant_id, "tenant_id")?;
        crate::store::validate_scope_component(&user_id, "user_id")?;
        let name = name.into();
        let body = body.into();
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

    pub fn is_visible_to(
        &self,
        tenant_id: &str,
        user_id: &str,
        privacy_scope: &[PrivacyLevel],
    ) -> bool {
        self.enabled
            && self.tenant_id == tenant_id
            && self.user_id == user_id
            && privacy_scope.contains(&self.privacy_level)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn enabled_policy_never_crosses_user_scope() {
        let policy = ProceduralPolicy::new(
            "tenant",
            "alice",
            "format",
            "Use bullet points",
            PrivacyLevel::Private,
        )
        .unwrap();
        assert!(!policy.is_visible_to("tenant", "bob", &[PrivacyLevel::Private]));
    }
}
