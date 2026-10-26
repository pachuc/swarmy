//! Placement always names groups; local services require a declared local group.
use crate::{Error, remote::is_valid_name};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmSpec {
    pub name: String,
    pub lifecycle: Lifecycle,
    pub groups: BTreeMap<String, GroupSpec>,
    pub placement: Placement,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Lifecycle {
    Ephemeral,
    Persistent,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupSpec {
    pub module: Module,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hosts: Vec<HostSpec>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostSpec {
    pub address: String,
    pub ssh_user: String,
    pub ssh_key: PathBuf,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum Module {
    HetznerCloud,
    Aws,
    Existing,
    Local,
    Path(PathBuf),
}
impl TryFrom<String> for Module {
    type Error = String;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "hetzner-cloud" => Ok(Self::HetznerCloud),
            "aws" => Ok(Self::Aws),
            "existing" => Ok(Self::Existing),
            "local" => Ok(Self::Local),
            _ if value.contains('/') => Ok(Self::Path(PathBuf::from(value))),
            _ => Err(format!(
                "module \"{value}\" is not one of hetzner-cloud, aws, existing, local; a path to your own module must contain a slash"
            )),
        }
    }
}
impl From<Module> for String {
    fn from(m: Module) -> Self {
        match m {
            Module::HetznerCloud => "hetzner-cloud".into(),
            Module::Aws => "aws".into(),
            Module::Existing => "existing".into(),
            Module::Local => "local".into(),
            Module::Path(p) => p.to_string_lossy().into_owned(),
        }
    }
}
impl Module {
    fn label(&self) -> String {
        self.clone().into()
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Placement {
    pub stack: String,
    pub services: String,
    pub nodes: Vec<String>,
}
/// # Errors
/// Returns `Spec` for invalid names.
pub fn validate_swarm_name(name: &str) -> Result<(), Error> {
    if is_valid_name(name) {
        Ok(())
    } else {
        Err(Error::Spec(format!(
            "swarm name \"{name}\" must be 1-64 letters, digits, hyphens, or underscores"
        )))
    }
}
impl GroupSpec {
    #[must_use]
    pub fn machines(&self) -> u32 {
        match self.module {
            Module::Existing => u32::try_from(self.hosts.len()).unwrap_or(u32::MAX),
            Module::Local => 1,
            _ => self.count.unwrap_or(0),
        }
    }
}
impl SwarmSpec {
    /// # Errors
    /// Returns `Decode` or `Spec`.
    pub fn parse(text: &str) -> Result<Self, Error> {
        let spec: Self = toml::from_str(text)?;
        spec.validate()?;
        Ok(spec)
    }
    /// # Errors
    /// Returns `Spec` or `Encode`.
    pub fn to_toml(&self) -> Result<String, Error> {
        self.validate()?;
        Ok(toml::to_string_pretty(self)?)
    }
    /// # Errors
    /// Returns the first violated rule as `Spec`.
    pub fn validate(&self) -> Result<(), Error> {
        validate_swarm_name(&self.name)?;
        if self.groups.is_empty() {
            return Err(Error::Spec(format!(
                "swarm \"{}\" has no groups",
                self.name
            )));
        }
        for (g, group) in &self.groups {
            if !is_valid_name(g) {
                return Err(Error::Spec(format!(
                    "group name \"{g}\" must be 1-64 letters, digits, hyphens, or underscores"
                )));
            }
            let failure = match &group.module {
                Module::HetznerCloud | Module::Aws | Module::Path(_) => {
                    let m = group.module.label();
                    if group.count.is_none() {
                        Some(format!("group \"{g}\": module {m} needs count"))
                    } else if group.count == Some(0) {
                        Some(format!("group \"{g}\": count must be at least 1"))
                    } else if !group.hosts.is_empty() {
                        Some(format!(
                            "group \"{g}\": only module = \"existing\" groups list hosts"
                        ))
                    } else if !matches!(group.module, Module::Path(_)) && group.size.is_none() {
                        Some(format!("group \"{g}\": module {m} needs size"))
                    } else if !matches!(group.module, Module::Path(_)) && group.location.is_none() {
                        Some(format!("group \"{g}\": module {m} needs location"))
                    } else {
                        None
                    }
                }
                Module::Existing => {
                    if group.hosts.is_empty() {
                        Some(format!(
                            "group \"{g}\": module = \"existing\" needs at least one entry in hosts"
                        ))
                    } else if group.count.is_some() {
                        Some(format!(
                            "group \"{g}\": an existing group's count is its number of hosts; remove count"
                        ))
                    } else if group.size.is_some() || group.location.is_some() {
                        Some(format!(
                            "group \"{g}\": size and location apply only to groups that create machines"
                        ))
                    } else if group.hosts.iter().any(|h| h.address.is_empty()) {
                        Some(format!("group \"{g}\": host address must not be empty"))
                    } else {
                        None
                    }
                }
                Module::Local => (group.count.is_some()
                    || group.size.is_some()
                    || group.location.is_some()
                    || !group.hosts.is_empty())
                .then(|| {
                    format!("group \"{g}\": a local group takes no count, size, location, or hosts")
                }),
            };
            if let Some(message) = failure {
                return Err(Error::Spec(message));
            }
        }
        for (field, g) in [
            ("stack", &self.placement.stack),
            ("services", &self.placement.services),
        ] {
            if !self.groups.contains_key(g) {
                return Err(Error::Spec(format!(
                    "placement.{field}: no group named \"{g}\""
                )));
            }
        }
        if self.placement.nodes.is_empty() {
            return Err(Error::Spec(
                "placement.nodes must name at least one group".into(),
            ));
        }
        let mut seen = BTreeSet::new();
        for g in &self.placement.nodes {
            let Some(group) = self.groups.get(g) else {
                return Err(Error::Spec(format!(
                    "placement.nodes: no group named \"{g}\""
                )));
            };
            if !seen.insert(g) {
                return Err(Error::Spec(format!(
                    "placement.nodes lists group \"{g}\" twice"
                )));
            }
            if group.module == Module::Local {
                return Err(Error::Spec(format!(
                    "placement.nodes: group \"{g}\" is local; this machine can run the stack or the services, not nodes"
                )));
            }
        }
        for g in self.groups.keys() {
            if g != &self.placement.stack && g != &self.placement.services && !seen.contains(g) {
                return Err(Error::Spec(format!(
                    "group \"{g}\" is not used by placement"
                )));
            }
        }
        Ok(())
    }
}
