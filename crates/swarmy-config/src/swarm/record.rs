//! Durable state from swarm creation and attachment.
use super::spec::validate_swarm_name;
use crate::Error;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt, path::PathBuf};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmRecord {
    pub name: String,
    pub created_at: jiff::Timestamp,
    #[serde(default)]
    pub api: SwarmApi,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket_keys: Option<StaticKeys>,
    /// Machines per group, as created or adopted.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub hosts: BTreeMap<String, Vec<RecordedHost>>,
    /// Directory holding this swarm's `OpenTofu` working directories and state
    /// (normally `<record dir>/tofu`); unset until a module group is applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tofu_state: Option<PathBuf>,
    /// Bucket created by the stack group; credentials remain in the cloud store.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tofu_bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tofu_bucket_endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tofu_bucket_region: Option<String>,
}
/// Static S3 credentials kept in the private swarm record, never in the spec.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticKeys {
    pub access_key: String,
    pub secret_key: String,
}
impl fmt::Debug for StaticKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StaticKeys")
            .field("access_key", &"..redacted..")
            .field("secret_key", &"..redacted..")
            .finish()
    }
}
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmApi {
    /// Unset until `up` generates it (local) or reads it from the node (remote).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// Set for local and attached APIs; absent when an SSH tunnel decides the address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}
impl fmt::Debug for SwarmApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SwarmApi")
            .field("token", &self.token.as_ref().map(|_| "..redacted.."))
            .field("url", &self.url)
            .finish()
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedHost {
    pub name: String,
    pub public_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_address: Option<String>,
    pub ssh_user: String,
    pub ssh_key: PathBuf,
    /// Set when provisioning this host completes.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub provisioned: bool,
}
impl SwarmRecord {
    /// # Errors
    /// Returns `Decode` or `Registry` for invalid names.
    pub fn parse(text: &str) -> Result<Self, Error> {
        let record: Self = toml::from_str(text)?;
        record.validate()?;
        Ok(record)
    }
    /// # Errors
    /// Returns `Registry` or `Encode`.
    pub fn to_toml(&self) -> Result<String, Error> {
        self.validate()?;
        Ok(toml::to_string_pretty(self)?)
    }
    pub(super) fn validate(&self) -> Result<(), Error> {
        validate_swarm_name(&self.name)
            .map_err(|_| Error::Registry(format!("invalid swarm name \"{}\"", self.name)))
    }
}
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn record_round_trip_and_redaction() {
        let host = RecordedHost {
            name: "test".into(),
            public_address: "203.0.113.10".into(),
            private_address: None,
            ssh_user: "root".into(),
            ssh_key: "/test".into(),
            provisioned: false,
        };
        let text = toml::to_string(&host).unwrap();
        assert!(!text.contains("provisioned"));
        assert!(!toml::from_str::<RecordedHost>(&text).unwrap().provisioned);
        let record = SwarmRecord {
            bucket_keys: None,
            name: "demo".into(),
            created_at: "2026-10-26T00:00:00Z".parse().unwrap(),
            api: SwarmApi {
                token: Some("very-secret-token".into()),
                url: Some("http://localhost".into()),
            },
            hosts: BTreeMap::from([(
                "workers".into(),
                vec![RecordedHost {
                    provisioned: false,
                    name: "box".into(),
                    public_address: "203.0.113.10".into(),
                    private_address: None,
                    ssh_user: "root".into(),
                    ssh_key: "/keys/box".into(),
                }],
            )]),
            tofu_state: Some("/state/tofu".into()),
            tofu_bucket: Some("swarmy-demo-0123456789ab".into()),
            tofu_bucket_endpoint: None,
            tofu_bucket_region: None,
        };
        assert_eq!(
            SwarmRecord::parse(&record.to_toml().unwrap()).unwrap(),
            record
        );
        assert!(!format!("{record:?}").contains("very-secret-token"));
    }
}

#[cfg(test)]
mod bucket_tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;

    #[test]
    fn static_keys_round_trip_without_debug_leak() {
        let keys = StaticKeys {
            access_key: "access-private".into(),
            secret_key: "secret-private".into(),
        };
        let debug = format!("{keys:?}");
        assert!(!debug.contains("access-private"));
        assert!(!debug.contains("secret-private"));
        let text = toml::to_string(&keys).unwrap();
        assert_eq!(toml::from_str::<StaticKeys>(&text).unwrap(), keys);
    }
}
