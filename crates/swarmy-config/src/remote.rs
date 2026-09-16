use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// EC2 placement and resource ownership for remote development nodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RemoteSettings {
    pub region: String,
    pub subnet: String,
    pub security_group: String,
    pub instance_type: String,
    pub disk_gb: i32,
    pub image: Option<String>,
    pub managed_by_tag: String,
}

impl Default for RemoteSettings {
    fn default() -> Self {
        Self {
            region: "us-east-1".into(),
            subnet: String::new(),
            security_group: String::new(),
            instance_type: "m6id.xlarge".into(),
            disk_gb: 100,
            image: None,
            managed_by_tag: "swarmy".into(),
        }
    }
}

/// Local connection and cleanup record, stored as `remote/<name>.json`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteNode {
    pub name: String,
    pub region: String,
    pub instance_id: String,
    pub public_ip: String,
    pub private_ip: String,
    pub key_path: PathBuf,
    pub ssh_user: String,
    pub ports: RemotePorts,
    #[serde(default)]
    pub nodes: Vec<RemoteNode>,
    /// UTC timestamp in RFC 3339 format.
    pub created_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct RemotePorts {
    pub fdb: u16,
    pub nats: u16,
    pub s3: u16,
}

impl Default for RemotePorts {
    fn default() -> Self {
        Self {
            fdb: 4500,
            nats: 4222,
            s3: 8333,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Settings;

    #[test]
    fn defaults_and_overrides() {
        let settings: Settings = toml::from_str("[remote]\nsubnet = 'subnet-test'\nsecurity_group = 'sg-test'\nmanaged_by_tag = 'codex-launcher'").unwrap();
        assert_eq!(settings.remote.region, "us-east-1");
        assert_eq!(settings.remote.instance_type, "m6id.xlarge");
        assert_eq!(settings.remote.disk_gb, 100);
        assert_eq!(settings.remote.managed_by_tag, "codex-launcher");
        assert!(settings.remote.image.is_none());
        let settings = Settings {
            remote: super::RemoteSettings {
                image: Some("ami-test".into()),
                ..settings.remote
            },
            ..settings
        };
        let decoded: Settings = toml::from_str(&settings.to_toml().unwrap()).unwrap();
        assert_eq!(decoded.remote.image.as_deref(), Some("ami-test"));
    }
}
