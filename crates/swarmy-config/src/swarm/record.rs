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
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub hosts: BTreeMap<String, Vec<RecordedHost>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tofu_state: Option<PathBuf>,
}
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmApi {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
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
