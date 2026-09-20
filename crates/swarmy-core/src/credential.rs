//! Provider credentials. Secret fields intentionally do not implement `Debug`.
use std::collections::BTreeMap;

use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::AgentId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CredentialScope {
    Cluster,
    Agent(AgentId),
}

impl std::fmt::Display for CredentialScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cluster => f.write_str("cluster"),
            Self::Agent(id) => write!(f, "agent:{id}"),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialRecord {
    pub kind: CredentialKind,
    pub updated_at: Timestamp,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CredentialKind {
    ApiKey {
        key: String,
        extra: BTreeMap<String, String>,
    },
    OAuth {
        access: String,
        refresh: String,
        expires_at: Timestamp,
        extra: BTreeMap<String, String>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialStatus {
    Ready,
    Expired,
    NeedsLogin,
}

impl CredentialRecord {
    #[must_use]
    pub fn status(&self, now: Timestamp) -> CredentialStatus {
        match &self.kind {
            CredentialKind::ApiKey { key, extra }
                if key.is_empty() || extra.get("needs_login").is_some_and(|v| v == "true") =>
            {
                CredentialStatus::NeedsLogin
            }
            CredentialKind::OAuth {
                access,
                refresh,
                extra,
                ..
            } if access.is_empty()
                || (refresh.is_empty()
                    && extra
                        .get("token_source")
                        .is_none_or(|source| source != "azure_cli"))
                || extra.get("needs_login").is_some_and(|v| v == "true") =>
            {
                CredentialStatus::NeedsLogin
            }
            CredentialKind::OAuth { expires_at, .. } if *expires_at <= now => {
                CredentialStatus::Expired
            }
            _ => CredentialStatus::Ready,
        }
    }

    /// Refresh within five minutes, but report actual expiration in status output.
    #[must_use]
    pub fn needs_refresh(&self, now: Timestamp) -> bool {
        matches!(&self.kind, CredentialKind::OAuth { expires_at, .. }
            if expires_at.as_second() < now.as_second().saturating_add(300))
    }

    #[must_use]
    pub const fn kind_name(&self) -> &'static str {
        match self.kind {
            CredentialKind::ApiKey { .. } => "api_key",
            CredentialKind::OAuth { .. } => "oauth",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_and_refresh_margin() {
        let now = Timestamp::from_second(1000).unwrap();
        for (expiry, status, refresh) in [
            (999, CredentialStatus::Expired, true),
            (1000, CredentialStatus::Expired, true),
            (1299, CredentialStatus::Ready, true),
            (1300, CredentialStatus::Ready, false),
        ] {
            let record = CredentialRecord {
                kind: CredentialKind::OAuth {
                    access: "access".into(),
                    refresh: "refresh".into(),
                    expires_at: Timestamp::from_second(expiry).unwrap(),
                    extra: BTreeMap::new(),
                },
                updated_at: now,
            };
            assert_eq!(record.status(now), status);
            assert_eq!(record.needs_refresh(now), refresh);
            assert!(
                crate::decode::<CredentialRecord>(&crate::encode(&record).unwrap()).unwrap()
                    == record
            );
        }
        let record = CredentialRecord {
            kind: CredentialKind::ApiKey {
                key: String::new(),
                extra: BTreeMap::new(),
            },
            updated_at: now,
        };
        assert_eq!(record.status(now), CredentialStatus::NeedsLogin);
    }
}
