//! Placement always names groups. To run services on this machine, declare
//! `[groups.local] module = "local"` and set `services = "local"`. The value
//! is a group name, not a keyword; `swarm create --local` will write that group.
use crate::{Error, is_opentofu_name, names::is_valid_name};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};
use swarmy_core::{HourlyPrice, MAX_ELASTIC_MACHINES};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmSpec {
    pub name: String,
    pub lifecycle: Lifecycle,
    /// Login that owns the node directory and units on every host (default `swarmy`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_user: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<SwarmBucket>,
    pub groups: BTreeMap<String, GroupSpec>,
    pub placement: Placement,
}
/// Public bucket coordinates; credentials are held only in the private record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmBucket {
    pub name: String,
    pub endpoint: String,
    pub region: String,
    #[serde(default)]
    pub prefix: crate::ObjectPrefix,
    #[serde(default = "yes")]
    pub conditional_create: bool,
}
const fn yes() -> bool {
    true
}
impl SwarmBucket {
    /// Validate the bucket coordinates before provisioning changes any state.
    /// # Errors
    /// Returns a spec error for invalid bucket coordinates.
    pub fn validate(&self) -> Result<(), Error> {
        crate::BucketSpec {
            bucket: self.name.clone(),
            ..Default::default()
        }
        .validate_name()
        .map_err(|error| Error::Spec(format!("bucket: {error}")))?;
        let url = url::Url::parse(&self.endpoint).map_err(|_| {
            Error::Spec("bucket: endpoint must be an http or https URL with a host".into())
        })?;
        if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
            return Err(Error::Spec(
                "bucket: endpoint must be an http or https URL with a host".into(),
            ));
        }
        if self.region.is_empty() {
            return Err(Error::Spec("bucket: region must not be empty".into()));
        }
        Ok(())
    }
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
    /// Local groups only: an absolute directory for the backing stack.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stack_dir: Option<PathBuf>,
    /// Sandboxes per host for groups in placement.nodes (default 64).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandboxes: Option<u32>,
    /// Sandbox scratch: a block device or `dir:/path`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_storage: Option<String>,
    /// The swarm's provisioner owns this group's machines.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub elastic: bool,
    /// Maximum elastic machine count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<u32>,
    /// Exact hourly price as a decimal string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_per_hour: Option<String>,
    /// ISO 4217 currency code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostSpec {
    pub address: String,
    pub ssh_user: String,
    pub ssh_key: PathBuf,
    /// Address other hosts use to reach this one (default: address).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_address: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum Module {
    HetznerCloud,
    Aws,
    Scaleway,
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
            "scaleway" => Ok(Self::Scaleway),
            "existing" => Ok(Self::Existing),
            "local" => Ok(Self::Local),
            _ if value.contains('/') => Ok(Self::Path(PathBuf::from(value))),
            _ => Err(format!(
                "module \"{value}\" is not one of hetzner-cloud, aws, scaleway, existing, local; a path to your own module must contain a slash"
            )),
        }
    }
}
impl From<Module> for String {
    fn from(m: Module) -> Self {
        match m {
            Module::HetznerCloud => "hetzner-cloud".into(),
            Module::Aws => "aws".into(),
            Module::Scaleway => "scaleway".into(),
            Module::Existing => "existing".into(),
            Module::Local => "local".into(),
            Module::Path(p) => p.to_string_lossy().into_owned(),
        }
    }
}
impl Module {
    /// The stored cloud provider whose keys create this module's machines.
    /// None for hosts you own, this machine, and your own module paths.
    #[must_use]
    pub fn machine_provider(&self) -> Option<crate::cloud::CloudProvider> {
        use crate::cloud::CloudProvider;
        match self {
            Self::HetznerCloud => Some(CloudProvider::HetznerCloud),
            Self::Aws => Some(CloudProvider::Aws),
            Self::Scaleway => Some(CloudProvider::Scaleway),
            Self::Existing | Self::Local | Self::Path(_) => None,
        }
    }
    fn label(&self) -> String {
        self.clone().into()
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Placement {
    pub stack: String,
    pub services: String,
    #[serde(default)]
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
    pub fn hourly_price(&self) -> Option<HourlyPrice> {
        if !self.elastic {
            return None;
        }
        HourlyPrice::parse(self.price_per_hour.as_deref()?, self.currency.as_deref()?).ok()
    }
    #[must_use]
    pub fn machines(&self) -> u32 {
        if self.elastic {
            return 0;
        }
        match self.module {
            Module::Existing => u32::try_from(self.hosts.len()).unwrap_or(u32::MAX),
            Module::Local => 1,
            _ => self.count.unwrap_or(0),
        }
    }
}
fn elastic_failure(swarm: &str, g: &str, group: &GroupSpec) -> Option<String> {
    if !group.elastic {
        return (group.max.is_some() || group.price_per_hour.is_some() || group.currency.is_some())
            .then(|| {
                format!(
                    "group \"{g}\": max, price_per_hour and currency apply only to elastic groups"
                )
            });
    }
    if matches!(
        group.module,
        Module::Existing | Module::Local | Module::Path(_)
    ) {
        return Some(format!(
            "group \"{g}\": elastic groups need a built-in module that creates machines, not {}",
            group.module.label()
        ));
    }
    if group.count.is_some() {
        return Some(format!(
            "group \"{g}\": an elastic group has no count; the provisioner sets its machine count between 0 and max"
        ));
    }
    if !matches!(group.max, Some(1..=MAX_ELASTIC_MACHINES)) {
        return Some(format!(
            "group \"{g}\": an elastic group needs max between 1 and 50"
        ));
    }
    let (Some(amount), Some(currency)) = (&group.price_per_hour, &group.currency) else {
        return Some(format!(
            "group \"{g}\": an elastic group needs price_per_hour and currency, for example price_per_hour = \"0.75\" and currency = \"EUR\""
        ));
    };
    if let Err(error) = HourlyPrice::parse(amount, currency) {
        return Some(format!("group \"{g}\": {error}"));
    }
    if !is_opentofu_name(swarm) || !is_opentofu_name(g) {
        return Some(format!(
            "group \"{g}\": elastic groups need swarm and group names of 1-20 lowercase letters, digits and hyphens that start with a letter and do not end with a hyphen"
        ));
    }
    None
}
fn group_failure(swarm: &str, g: &str, group: &GroupSpec, node_only: bool) -> Option<String> {
    if let Some(message) = elastic_failure(swarm, g, group) {
        return Some(message);
    }
    if group.stack_dir.is_some() && group.module != Module::Local {
        return Some(format!(
            "group \"{g}\": stack_dir applies only to local groups"
        ));
    }
    match &group.module {
        Module::HetznerCloud | Module::Aws | Module::Scaleway | Module::Path(_) => {
            let m = group.module.label();
            if group.count.is_none() && !group.elastic {
                Some(format!("group \"{g}\": module {m} needs count"))
            } else if group.count == Some(0) && !node_only && !group.elastic {
                Some(format!("group \"{g}\": count must be at least 1; only a group that runs nodes and neither the stack nor the services can have count 0"))
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
            } else if group.hosts.iter().any(|h| h.private_address.as_deref() == Some("")) {
                Some(format!("group \"{g}\": host private_address must not be empty"))
            } else if group.hosts.iter().any(|h| h.address.is_empty()) {
                Some(format!("group \"{g}\": host address must not be empty"))
            } else {
                None
            }
        }
        Module::Local => (group.count.is_some()
            || group.size.is_some()
            || group.location.is_some()
            || !group.hosts.is_empty()
            || group.sandboxes.is_some()
            || group.local_storage.is_some())
        .then(|| format!("group \"{g}\": a local group takes no count, size, location, hosts, sandboxes, or local_storage"))
        .or_else(|| {
            group
                .stack_dir
                .as_ref()
                .filter(|p| !p.is_absolute())
                .map(|_| format!("group \"{g}\": stack_dir must be an absolute path"))
        }),
    }
}
impl SwarmSpec {
    /// The local group when stack and services run here without sandbox nodes.
    #[must_use]
    pub fn local_group(&self) -> Option<&GroupSpec> {
        (self.placement.stack == self.placement.services && self.placement.nodes.is_empty())
            .then(|| self.groups.get(&self.placement.stack))
            .flatten()
            .filter(|g| g.module == Module::Local)
    }
    /// Build a local swarm spec.
    /// # Errors
    /// Returns `Spec` for an invalid name or relative stack directory.
    pub fn local(
        name: &str,
        lifecycle: Lifecycle,
        stack_dir: Option<PathBuf>,
    ) -> Result<Self, Error> {
        let spec = Self {
            name: name.into(),
            lifecycle,
            service_user: None,
            bucket: None,
            groups: [(
                "local".into(),
                GroupSpec {
                    module: Module::Local,
                    size: None,
                    location: None,
                    count: None,
                    hosts: vec![],
                    stack_dir,
                    sandboxes: None,
                    local_storage: None,

                    elastic: false,
                    max: None,
                    price_per_hour: None,
                    currency: None,
                },
            )]
            .into(),
            placement: Placement {
                stack: "local".into(),
                services: "local".into(),
                nodes: vec![],
            },
        };
        spec.validate()?;
        Ok(spec)
    }
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
        if let Some(bucket) = &self.bucket {
            bucket.validate()?;
        }
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
            let node_only = self.placement.nodes.contains(g)
                && *g != self.placement.stack
                && *g != self.placement.services;
            if let Some(message) = group_failure(&self.name, g, group, node_only) {
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
        if self.placement.nodes.is_empty() && self.local_group().is_none() {
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
        for (g, group) in &self.groups {
            if group.elastic
                && (!self.placement.nodes.contains(g)
                    || self.placement.stack == *g
                    || self.placement.services == *g)
            {
                return Err(Error::Spec(format!(
                    "group \"{g}\" is elastic, so it must be listed in placement.nodes and run neither the stack nor the services"
                )));
            }
        }
        for (g, group) in &self.groups {
            if group.sandboxes.is_some_and(|n| n > 0) && !seen.contains(g) {
                return Err(Error::Spec(format!(
                    "group \"{g}\" does not run nodes; remove sandboxes"
                )));
            }
        }
        if let Some(user) = &self.service_user {
            crate::validate_service_user(user)
                .map_err(|e| Error::Spec(format!("service_user: {e}")))?;
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
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    const CLOUD: &str = r#"
name = "fleet"
lifecycle = "persistent" # or "ephemeral"
[groups.control]
module = "hetzner-cloud" # "hetzner-cloud", "aws", "scaleway", "existing", "local", or a path to your own module
size = "cpx41"
location = "fsn1"
count = 1
[groups.workers]
module = "hetzner-cloud"
size = "ccx33"
location = "fsn1"
count = 2
[placement]
stack = "control"
services = "control" # or "local" (the laptop runs the services)
nodes = ["workers"] # or ["control"] for one box like hz
"#;
    const EXISTING: &str = r#"
name = "box"
lifecycle = "ephemeral"
[groups.box]
module = "existing"
hosts = [{ address = "203.0.113.10", ssh_user = "root", ssh_key = "/keys/box" }]
[placement]
stack = "box"
services = "box"
nodes = ["box"]
"#;
    const LOCAL: &str = r#"
name = "local-services"
lifecycle = "persistent"
[groups.box]
module = "existing"
hosts = [{ address = "203.0.113.10", ssh_user = "root", ssh_key = "/keys/box" }]
[groups.local]
module = "local"
[placement]
stack = "box"
services = "local"
nodes = ["box"]
"#;
    #[test]
    fn elastic_group_round_trip_and_validation() {
        type ElasticCase = (fn(&mut SwarmSpec), &'static str);
        let mut spec = SwarmSpec::parse(CLOUD).unwrap();
        let group = spec.groups.get_mut("workers").unwrap();
        group.elastic = true;
        group.count = None;
        group.max = Some(2);
        group.price_per_hour = Some("0.75".into());
        group.currency = Some("EUR".into());
        assert_eq!(group.machines(), 0);
        assert_eq!(group.hourly_price().unwrap().amount(), "0.75");
        assert_eq!(SwarmSpec::parse(&spec.to_toml().unwrap()).unwrap(), spec);
        let base = spec.clone();
        let cases: Vec<ElasticCase> = vec![
            (
                (|s| s.groups.get_mut("workers").unwrap().elastic = false),
                "max, price_per_hour and currency apply only to elastic groups",
            ),
            (
                (|s| s.groups.get_mut("workers").unwrap().module = Module::Local),
                "elastic groups need a built-in module that creates machines, not local",
            ),
            (
                (|s| s.groups.get_mut("workers").unwrap().count = Some(1)),
                "an elastic group has no count",
            ),
            (
                (|s| s.groups.get_mut("workers").unwrap().max = Some(0)),
                "an elastic group needs max between 1 and 50",
            ),
            (
                (|s| s.groups.get_mut("workers").unwrap().price_per_hour = None),
                "an elastic group needs price_per_hour and currency",
            ),
            (
                |s| {
                    s.groups.get_mut("workers").unwrap().price_per_hour = Some("1e3".into());
                },
                "price_per_hour must be a decimal",
            ),
            (
                (|s| s.name = "Not-OpenTofu".into()),
                "elastic groups need swarm and group names of 1-20 lowercase letters",
            ),
            (
                (|s| s.placement.nodes = vec!["control".into()]),
                "is elastic, so it must be listed in placement.nodes",
            ),
        ];
        for (mutate, expected) in cases {
            let mut changed = base.clone();
            mutate(&mut changed);
            assert!(
                changed
                    .validate()
                    .unwrap_err()
                    .to_string()
                    .contains(expected),
                "{expected}"
            );
        }
    }
    #[test]
    fn count_zero_is_only_for_module_node_groups() {
        let mut spec = SwarmSpec::parse(CLOUD).unwrap();
        spec.groups.get_mut("workers").unwrap().count = Some(0);
        for module in [
            Module::HetznerCloud,
            Module::Scaleway,
            Module::Path("/m".into()),
        ] {
            spec.groups.get_mut("workers").unwrap().module = module;
            spec.validate().unwrap();
            let parsed = SwarmSpec::parse(&spec.to_toml().unwrap()).unwrap();
            assert_eq!(parsed.groups["workers"].count, Some(0));
            assert_eq!(parsed.groups["workers"].machines(), 0);
        }
        spec.groups.get_mut("control").unwrap().count = Some(0);
        let message = "count must be at least 1; only a group that runs nodes and neither the stack nor the services can have count 0";
        assert!(spec.validate().unwrap_err().to_string().contains(message));
        spec.placement.nodes.push("control".into());
        assert!(spec.validate().unwrap_err().to_string().contains(message));
        let scaleway = CLOUD.replace(
            "module = \"hetzner-cloud\"\nsize = \"ccx33\"",
            "module = \"scaleway\"\nsize = \"L4-1-24G\"",
        );
        assert_eq!(
            SwarmSpec::parse(&scaleway).unwrap().groups["workers"].module,
            Module::Scaleway
        );
    }
    #[test]
    fn new_fields_round_trip_and_defaults() {
        let mut spec = SwarmSpec::parse(EXISTING).unwrap();
        assert_eq!(spec.service_user, None);
        assert_eq!(spec.groups["box"].sandboxes, None);
        assert_eq!(spec.groups["box"].hosts[0].private_address, None);
        spec.service_user = Some("swarmy-user".into());
        let group = spec.groups.get_mut("box").unwrap();
        group.sandboxes = Some(2);
        group.local_storage = Some("dir:/scratch".into());
        group.hosts[0].private_address = Some("203.0.113.11".into());
        assert_eq!(SwarmSpec::parse(&spec.to_toml().unwrap()).unwrap(), spec);
    }
    #[test]
    fn new_field_validation() {
        let mut spec = SwarmSpec::local("dev", Lifecycle::Ephemeral, None).unwrap();
        spec.groups.get_mut("local").unwrap().sandboxes = Some(2);
        assert!(
            spec.validate()
                .unwrap_err()
                .to_string()
                .contains("local group takes no count")
        );
        let mut spec = SwarmSpec::parse(CLOUD).unwrap();
        spec.groups.get_mut("control").unwrap().sandboxes = Some(2);
        assert!(
            spec.validate()
                .unwrap_err()
                .to_string()
                .contains("does not run nodes; remove sandboxes")
        );
        spec.groups.get_mut("control").unwrap().sandboxes = None;
        spec.service_user = Some("a b".into());
        assert!(
            spec.validate()
                .unwrap_err()
                .to_string()
                .contains("service_user:")
        );
    }
    #[test]
    fn local_spec_round_trips() {
        let spec = SwarmSpec::local("dev", Lifecycle::Ephemeral, None).unwrap();
        let text = spec.to_toml().unwrap();
        assert!(text.contains("module = \"local\""));
        assert!(text.contains("nodes = []"));
        assert_eq!(SwarmSpec::parse(&text).unwrap(), spec);
        assert!(spec.local_group().is_some());
    }
    #[test]
    fn local_spec_without_nodes_key_parses() {
        let text = SwarmSpec::local("dev", Lifecycle::Ephemeral, None)
            .unwrap()
            .to_toml()
            .unwrap();
        assert!(
            SwarmSpec::parse(&text.replace("nodes = []\n", ""))
                .unwrap()
                .local_group()
                .is_some()
        );
    }
    #[test]
    fn stack_dir_rules() {
        let mut cloud = SwarmSpec::parse(CLOUD).unwrap();
        cloud.groups.get_mut("control").unwrap().stack_dir = Some("/tmp/stack".into());
        assert_eq!(
            cloud.validate().unwrap_err().to_string(),
            "invalid swarm spec: group \"control\": stack_dir applies only to local groups"
        );
        assert_eq!(
            SwarmSpec::local("dev", Lifecycle::Ephemeral, Some("relative".into()))
                .unwrap_err()
                .to_string(),
            "invalid swarm spec: group \"local\": stack_dir must be an absolute path"
        );
        assert!(SwarmSpec::local("dev", Lifecycle::Ephemeral, Some("/tmp/stack".into())).is_ok());
    }
    #[test]
    fn local_group_is_none_for_split_and_cloud() {
        assert!(SwarmSpec::parse(LOCAL).unwrap().local_group().is_none());
        assert!(SwarmSpec::parse(CLOUD).unwrap().local_group().is_none());
    }
    #[test]
    fn examples_round_trip() {
        for text in [CLOUD, EXISTING, LOCAL] {
            let spec = SwarmSpec::parse(text).unwrap();
            assert_eq!(SwarmSpec::parse(&spec.to_toml().unwrap()).unwrap(), spec);
        }
        let cloud = SwarmSpec::parse(CLOUD).unwrap();
        assert_eq!(cloud.groups.len(), 2);
        assert_eq!(cloud.groups["control"].machines(), 1);
        assert_eq!(cloud.groups["workers"].machines(), 2);
        assert_eq!(cloud.lifecycle, Lifecycle::Persistent);
        assert_eq!(cloud.placement.nodes, ["workers"]);
        let path = CLOUD.replace("hetzner-cloud", "./modules/mine");
        assert!(matches!(
            SwarmSpec::parse(&path).unwrap().groups["control"].module,
            Module::Path(_)
        ));
        let invalid = CLOUD.replace("hetzner-cloud", "hetzner");
        assert!(
            SwarmSpec::parse(&invalid)
                .unwrap_err()
                .to_string()
                .contains("module \"hetzner\" is not one of hetzner-cloud, aws, scaleway, existing, local; a path to your own module must contain a slash")
        );
    }
    #[test]
    fn rejects_unknown_fields() {
        assert!(SwarmSpec::parse(&CLOUD.replace("size =", "sizes =")).is_err());
        assert!(SwarmSpec::parse(&format!("nodes = 3\n{EXISTING}")).is_err());
        assert!(
            SwarmSpec::parse(&EXISTING.replace("ssh_user =", "port = 22, ssh_user =")).is_err()
        );
    }
    #[test]
    fn validation_order_and_messages() {
        type Case = (Box<dyn Fn(&mut SwarmSpec)>, &'static str);
        let base = SwarmSpec::parse(CLOUD).unwrap();
        let cases: Vec<Case> = vec![
            (
                Box::new(|s| s.name = "bad name".into()),
                "swarm name \"bad name\" must be 1-64 letters, digits, hyphens, or underscores",
            ),
            (
                Box::new(|s| s.groups.clear()),
                "swarm \"fleet\" has no groups",
            ),
            (
                Box::new(|s| {
                    let g = s.groups.remove("control").unwrap();
                    s.groups.insert("bad name".into(), g);
                }),
                "group name \"bad name\" must be 1-64 letters, digits, hyphens, or underscores",
            ),
            (
                Box::new(|s| s.groups.get_mut("control").unwrap().count = None),
                "group \"control\": module hetzner-cloud needs count",
            ),
            (
                Box::new(|s| s.groups.get_mut("control").unwrap().count = Some(0)),
                "group \"control\": count must be at least 1; only a group that runs nodes and neither the stack nor the services can have count 0",
            ),
            (
                Box::new(|s| {
                    s.groups.get_mut("control").unwrap().hosts.push(HostSpec {
                        private_address: None,
                        address: "x".into(),
                        ssh_user: "root".into(),
                        ssh_key: "x".into(),
                    });
                }),
                "group \"control\": only module = \"existing\" groups list hosts",
            ),
            (
                Box::new(|s| s.groups.get_mut("control").unwrap().size = None),
                "group \"control\": module hetzner-cloud needs size",
            ),
            (
                Box::new(|s| s.groups.get_mut("control").unwrap().location = None),
                "group \"control\": module hetzner-cloud needs location",
            ),
            (
                Box::new(|s| s.placement.stack = "missing".into()),
                "placement.stack: no group named \"missing\"",
            ),
            (
                Box::new(|s| s.placement.nodes.clear()),
                "placement.nodes must name at least one group",
            ),
            (
                Box::new(|s| s.placement.nodes = vec!["gpu".into()]),
                "placement.nodes: no group named \"gpu\"",
            ),
            (
                Box::new(|s| s.placement.nodes = vec!["workers".into(), "workers".into()]),
                "placement.nodes lists group \"workers\" twice",
            ),
            (
                Box::new(|s| s.placement.nodes = vec!["control".into()]),
                "group \"workers\" is not used by placement",
            ),
        ];
        for (change, expected) in cases {
            let mut spec = base.clone();
            change(&mut spec);
            assert_eq!(
                spec.validate().unwrap_err().to_string(),
                format!("invalid swarm spec: {expected}")
            );
        }
    }
    #[test]
    fn existing_and_local_validation() {
        for (text, change, expected) in [
            (
                EXISTING,
                "hosts",
                "group \"box\": module = \"existing\" needs at least one entry in hosts",
            ),
            (
                EXISTING,
                "count",
                "group \"box\": an existing group's count is its number of hosts; remove count",
            ),
            (
                EXISTING,
                "size",
                "group \"box\": size and location apply only to groups that create machines",
            ),
            (
                EXISTING,
                "address",
                "group \"box\": host address must not be empty",
            ),
            (
                LOCAL,
                "local fields",
                "group \"local\": a local group takes no count, size, location, hosts, sandboxes, or local_storage",
            ),
            (
                LOCAL,
                "local nodes",
                "placement.nodes: group \"local\" is local; this machine can run the stack or the services, not nodes",
            ),
        ] {
            let mut spec = SwarmSpec::parse(text).unwrap();
            match change {
                "hosts" => spec.groups.get_mut("box").unwrap().hosts.clear(),
                "count" => spec.groups.get_mut("box").unwrap().count = Some(1),
                "size" => spec.groups.get_mut("box").unwrap().size = Some("x".into()),
                "address" => spec.groups.get_mut("box").unwrap().hosts[0].address.clear(),
                "local fields" => spec.groups.get_mut("local").unwrap().count = Some(1),
                "local nodes" => spec.placement.nodes = vec!["local".into()],
                _ => unreachable!(),
            }
            assert_eq!(
                spec.validate().unwrap_err().to_string(),
                format!("invalid swarm spec: {expected}")
            );
        }
    }
}

#[cfg(test)]
mod bucket_tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;

    #[test]
    fn bucket_coordinates_validate() {
        let mut bucket = SwarmBucket {
            name: "example-bucket".into(),
            endpoint: "https://objects.example.invalid".into(),
            region: "eu-west-1".into(),
            prefix: crate::ObjectPrefix::default(),
            conditional_create: true,
        };
        bucket.validate().unwrap();
        bucket.name = "bad.name".into();
        assert!(
            bucket
                .validate()
                .unwrap_err()
                .to_string()
                .contains("bucket:")
        );
        bucket.name = "example-bucket".into();
        bucket.endpoint = "ftp://objects.example.invalid".into();
        assert!(
            bucket
                .validate()
                .unwrap_err()
                .to_string()
                .contains("endpoint")
        );
        bucket.endpoint = "https://objects.example.invalid".into();
        bucket.region.clear();
        assert!(
            bucket
                .validate()
                .unwrap_err()
                .to_string()
                .contains("region")
        );
    }
}
