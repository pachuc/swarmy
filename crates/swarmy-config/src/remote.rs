//! Files shared by remote provisioning and local tunnel commands.
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{Error, Settings};

/// Location of the scheduler, worker, and inference gateway.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RemoteServices {
    #[default]
    Laptop,
    Node,
}

impl std::str::FromStr for RemoteServices {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "laptop" => Ok(Self::Laptop),
            "node" => Ok(Self::Node),
            _ => Err("services must be laptop or node"),
        }
    }
}

/// EC2 placement, resource ownership, and the selected tunnel profile.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RemoteSettings {
    pub services: RemoteServices,
    pub region: String,
    pub bucket: Option<String>,
    pub subnet: Option<String>,
    pub security_group: Option<String>,
    pub instance_type: String,
    pub disk_gb: u32,
    pub image: Option<String>,
    pub profile: Option<String>,
    pub managed_by_tag: String,
}

impl Default for RemoteSettings {
    fn default() -> Self {
        Self {
            services: RemoteServices::Laptop,
            region: "us-east-1".into(),
            bucket: None,
            subnet: None,
            security_group: None,
            instance_type: "m6id.xlarge".into(),
            disk_gb: 100,
            image: None,
            profile: None,
            managed_by_tag: "swarmy".into(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

/// Provisioned instance state, stored at `<state_dir>/remote/<name>.json`.
/// Additional instances use the same contract in `nodes`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteNode {
    pub name: String,
    pub region: String,
    pub instance_id: String,
    pub public_ip: String,
    pub private_ip: String,
    pub key_path: PathBuf,
    #[serde(default = "ssh_user")]
    pub ssh_user: String,
    #[serde(default)]
    pub ports: RemotePorts,
    #[serde(default)]
    pub nodes: Vec<RemoteNode>,
    #[serde(default = "default_sandboxes")]
    pub sandboxes: u32,
    /// Registered image built for this stack, set only after a successful build.
    #[serde(default)]
    pub default_image: Option<String>,
    /// Resolved launch configuration, retained so joins do not depend on later edits.
    #[serde(default)]
    pub launch_settings: Option<RemoteSettings>,
    /// UTC timestamp in RFC 3339 format.
    pub created_at: String,
}

impl RemoteNode {
    #[must_use]
    pub fn bucket(&self) -> Option<&str> {
        self.launch_settings.as_ref()?.bucket.as_deref()
    }
}

#[must_use]
pub const fn default_sandboxes() -> u32 {
    64
}

fn ssh_user() -> String {
    "ubuntu".into()
}

/// Local endpoints and ownership information for one SSH control master.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteProfile {
    pub name: String,
    pub socket_path: PathBuf,
    pub pid: u32,
    pub ports: RemotePorts,
    #[serde(default)]
    pub remote_ports: RemotePorts,
    pub fdb_cluster_file: PathBuf,
    pub nats_url: String,
    pub s3_endpoint: String,
    #[serde(default)]
    pub s3_bucket: Option<String>,
    #[serde(default)]
    pub s3_region: Option<String>,
    #[serde(default)]
    pub default_image: Option<String>,
}

impl RemoteProfile {
    /// `FoundationDB` checks that the connected port matches the advertised port.
    /// # Errors
    /// Rejects remapped coordinator ports before starting the native client.
    pub fn validate_fdb_port(&self) -> Result<(), Error> {
        if self.ports.fdb != self.remote_ports.fdb {
            return Err(Error::Remote(
                "FoundationDB cannot use a remapped port; disconnect, free the advertised FoundationDB port, and reconnect (or use a separate network namespace)",
            ));
        }
        Ok(())
    }

    /// Apply stack endpoints and its default image, preserving credentials and namespaces.
    pub fn apply(&self, settings: &mut Settings) {
        settings.fdb_cluster_file = self.fdb_cluster_file.to_string_lossy().into_owned();
        settings.nats_url.clone_from(&self.nats_url);
        settings.s3_endpoint.clone_from(&self.s3_endpoint);
        if let (Some(bucket), Some(region)) = (&self.s3_bucket, &self.s3_region) {
            settings.s3_bucket.clone_from(bucket);
            settings.s3_region.clone_from(region);
            settings.s3_access_key.clear();
            settings.s3_secret_key.clear();
        }
        if self.default_image.is_some() {
            settings.default_image.clone_from(&self.default_image);
        }
    }

    /// # Errors
    /// Returns errors for missing or invalid profiles.
    pub fn read(state: &Path, name: &str) -> Result<Self, Error> {
        let profile: Self =
            serde_json::from_slice(&std::fs::read(remote_path(state, name, "profile.json")?)?)?;
        if profile.name != name {
            return Err(Error::Remote("profile name does not match filename"));
        }
        Ok(profile)
    }
}

/// Validate names before using them as filenames or shell output.
/// # Errors
/// Rejects empty names, path traversal, and shell metacharacters.
pub fn validate_remote_name(name: &str) -> Result<(), Error> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(Error::Remote(
            "names must contain 1-64 letters, digits, hyphens, or underscores",
        ));
    }
    Ok(())
}

/// # Errors
/// Rejects invalid remote names.
pub fn remote_path(state: &Path, name: &str, extension: &str) -> Result<PathBuf, Error> {
    validate_remote_name(name)?;
    Ok(state.join("remote").join(format!("{name}.{extension}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn defaults_and_overrides() {
        let settings: Settings = toml::from_str("[remote]\nsubnet = 'subnet-test'\nsecurity_group = 'sg-test'\nmanaged_by_tag = 'codex-launcher'").unwrap();
        assert_eq!(settings.remote.region, "us-east-1");
        assert_eq!(settings.remote.instance_type, "m6id.xlarge");
        assert_eq!(settings.remote.disk_gb, 100);
        assert_eq!(settings.remote.subnet.as_deref(), Some("subnet-test"));
        assert_eq!(settings.remote.security_group.as_deref(), Some("sg-test"));
        assert_eq!(settings.remote.managed_by_tag, "codex-launcher");
        assert!(settings.remote.image.is_none());
        assert!(settings.remote.profile.is_none());
        let settings = Settings {
            remote: RemoteSettings {
                image: Some("ami-test".into()),
                ..settings.remote
            },
            ..settings
        };
        let decoded: Settings = toml::from_str(&settings.to_toml().unwrap()).unwrap();
        assert_eq!(decoded.remote.image.as_deref(), Some("ami-test"));
        assert_eq!(decoded.remote.managed_by_tag, "codex-launcher");
    }

    #[test]
    fn shared_state_defaults_and_round_trip() {
        let node: RemoteNode = serde_json::from_str(r#"{"name":"local","region":"local","instance_id":"i-local","public_ip":"127.0.0.1","private_ip":"127.0.0.1","key_path":"/tmp/key","created_at":"2026-09-16T00:00:00Z"}"#).unwrap();
        assert_eq!(node.ssh_user, "ubuntu");
        assert_eq!(node.ports, RemotePorts::default());
        assert!(node.nodes.is_empty());
        let encoded = serde_json::to_vec(&node).unwrap();
        assert_eq!(
            serde_json::from_slice::<RemoteNode>(&encoded)
                .unwrap()
                .instance_id,
            "i-local"
        );
        let settings = Settings::default();
        assert_eq!(settings.remote.instance_type, "m6id.xlarge");
        assert_eq!(settings.remote.disk_gb, 100);
        assert_eq!(settings.remote.managed_by_tag, "swarmy");
        assert!(settings.remote.subnet.is_none());
        assert!(settings.remote.security_group.is_none());
    }

    #[test]
    fn older_profiles_preserve_the_configured_default() {
        let profile: RemoteProfile = serde_json::from_str(r#"{"name":"old","socket_path":"socket","pid":1,"ports":{},"fdb_cluster_file":"cluster","nats_url":"nats://localhost:4222","s3_endpoint":"http://localhost:8333"}"#).unwrap();
        assert!(profile.default_image.is_none());
        let mut settings = Settings {
            default_image: Some("configured:tag".into()),
            ..Settings::default()
        };
        profile.apply(&mut settings);
        assert_eq!(settings.default_image.as_deref(), Some("configured:tag"));
    }

    #[test]
    fn bucket_profile_uses_laptop_credentials_and_region() {
        let mut profile: RemoteProfile = serde_json::from_str(r#"{"name":"remote","socket_path":"socket","pid":1,"ports":{},"fdb_cluster_file":"cluster","nats_url":"nats://localhost:4222","s3_endpoint":"","s3_bucket":"bucket","s3_region":"eu-west-1"}"#).unwrap();
        let mut settings = Settings::default();
        profile.apply(&mut settings);
        assert_eq!(settings.s3_bucket, "bucket");
        assert_eq!(settings.s3_region, "eu-west-1");
        assert!(settings.s3_endpoint.is_empty());
        assert!(settings.s3_access_key.is_empty());
        assert!(settings.s3_secret_key.is_empty());
        profile.s3_bucket = None;
        profile.apply(&mut settings);
        assert_eq!(settings.s3_bucket, "bucket");
    }

    #[test]
    fn selected_profile_overrides_discovered_config_and_endpoint_environment() {
        let root = tempfile::tempdir().unwrap();
        let state = root.path().join(".swarmy");
        std::fs::create_dir_all(state.join("remote")).unwrap();
        std::fs::create_dir_all(root.path().join("nested")).unwrap();
        std::fs::write(
            state.join("config.toml"),
            "provider = 'fake'\n[remote]\nprofile = 'test'\n",
        )
        .unwrap();
        let profile = RemoteProfile {
            name: "test".into(),
            socket_path: state.join("socket"),
            pid: 123,
            remote_ports: RemotePorts::default(),
            ports: RemotePorts {
                fdb: 4500,
                nats: 14222,
                s3: 18333,
            },
            fdb_cluster_file: state.join("test.cluster"),
            nats_url: "nats://127.0.0.1:14222".into(),
            s3_endpoint: "http://127.0.0.1:18333".into(),
            s3_bucket: None,
            s3_region: None,
            default_image: Some("base-ubuntu:test".into()),
        };
        std::fs::write(
            remote_path(&state, "test", "profile.json").unwrap(),
            serde_json::to_vec(&profile).unwrap(),
        )
        .unwrap();
        let env = BTreeMap::from([
            ("SWARMY_DEFAULT_IMAGE".into(), "local:old".into()),
            ("SWARMY_NATS_URL".into(), "nats://wrong:4222".into()),
            ("SWARMY_S3_BUCKET".into(), "custom".into()),
        ]);
        let loaded = Settings::load_from(&root.path().join("nested"), &env).unwrap();
        assert_eq!(
            loaded.settings.default_image.as_deref(),
            Some("base-ubuntu:test")
        );
        assert_eq!(
            loaded.settings.session_image(None).unwrap(),
            "base-ubuntu:test"
        );
        assert_eq!(
            loaded
                .settings
                .session_image(Some("custom:override"))
                .unwrap(),
            "custom:override"
        );
        assert_eq!(loaded.settings.nats_url, profile.nats_url);
        assert_eq!(loaded.settings.s3_endpoint, profile.s3_endpoint);
        assert_eq!(
            Path::new(&loaded.settings.fdb_cluster_file),
            profile.fdb_cluster_file
        );
        assert_eq!(loaded.settings.s3_bucket, "custom");
        assert_eq!(loaded.settings.environment()["SWARMY_REMOTE"], "test");
        let mut invalid = profile;
        invalid.ports.fdb = 14500;
        assert!(invalid.validate_fdb_port().is_err());
        std::fs::write(
            remote_path(&state, "test", "profile.json").unwrap(),
            serde_json::to_vec(&invalid).unwrap(),
        )
        .unwrap();
        assert!(Settings::load_from(root.path(), &env).is_err());

        let mut env = env;
        env.insert("SWARMY_REMOTE".into(), "missing".into());
        assert!(Settings::load_from(root.path(), &env).is_err());
        env.insert("SWARMY_REMOTE".into(), "../test".into());
        assert!(Settings::load_from(root.path(), &env).is_err());
    }
}
