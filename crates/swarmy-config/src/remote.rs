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

/// EC2-only settings.
#[derive(Clone, Debug, Serialize)]
pub struct AwsSettings {
    pub subnet: Option<String>,
    pub security_group: Option<String>,
    pub instance_type: String,
    pub image: Option<String>,
    /// Instance profile override. Unset means `swarmy-{remote}` when the
    /// remote uses an object bucket.
    pub iam_role: Option<String>,
}

impl Default for AwsSettings {
    fn default() -> Self {
        Self {
            subnet: None,
            security_group: None,
            instance_type: "m6id.xlarge".into(),
            image: None,
            iam_role: None,
        }
    }
}

/// Placement, resource ownership, and the selected tunnel profile.
#[derive(Clone, Debug, Serialize)]
pub struct RemoteSettings {
    /// Cloud provider; only `aws` exists today.
    pub provider: String,
    pub services: RemoteServices,
    pub region: String,
    pub bucket: Option<String>,
    pub disk_gb: u32,
    pub managed_by_tag: String,
    pub profile: Option<String>,
    pub aws: AwsSettings,
}

impl Default for RemoteSettings {
    fn default() -> Self {
        Self {
            provider: "aws".into(),
            services: RemoteServices::Laptop,
            region: "us-east-1".into(),
            bucket: None,
            disk_gb: 100,
            managed_by_tag: "swarmy".into(),
            profile: None,
            aws: AwsSettings::default(),
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RemoteSettingsHelper {
    provider: String,
    services: RemoteServices,
    region: String,
    bucket: Option<String>,
    disk_gb: u32,
    managed_by_tag: String,
    profile: Option<String>,
    aws: AwsHelper,
}

impl Default for RemoteSettingsHelper {
    fn default() -> Self {
        Self {
            provider: "aws".into(),
            services: RemoteServices::Laptop,
            region: "us-east-1".into(),
            bucket: None,
            disk_gb: 100,
            managed_by_tag: "swarmy".into(),
            profile: None,
            aws: AwsHelper::default(),
        }
    }
}

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct AwsHelper {
    subnet: Option<String>,
    security_group: Option<String>,
    instance_type: Option<String>,
    image: Option<String>,
    iam_role: Option<String>,
}

impl<'de> Deserialize<'de> for RemoteSettings {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let helper = RemoteSettingsHelper::deserialize(deserializer)?;
        Ok(Self {
            provider: helper.provider,
            services: helper.services,
            region: helper.region,
            bucket: helper.bucket,
            disk_gb: helper.disk_gb,
            managed_by_tag: helper.managed_by_tag,
            profile: helper.profile,
            aws: AwsSettings {
                subnet: helper.aws.subnet,
                security_group: helper.aws.security_group,
                instance_type: helper
                    .aws
                    .instance_type
                    .unwrap_or_else(|| "m6id.xlarge".into()),
                image: helper.aws.image,
                iam_role: helper.aws.iam_role,
            },
        })
    }
}

impl RemoteSettings {
    /// Node credentials for a remote: the IAM role override, or
    /// `swarmy-{remote}` when the remote uses an object bucket.
    #[must_use]
    pub fn instance_profile(&self, remote: &str) -> Option<String> {
        self.bucket.as_ref().map(|_| {
            self.aws
                .iam_role
                .clone()
                .unwrap_or_else(|| format!("swarmy-{remote}"))
        })
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
    /// Whether EC2 launch could have happened before its id was saved.
    pub launch_attempted: bool,
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

    /// Settings selecting the cloud provider for this remote. Records saved
    /// before launch settings existed fall back to defaults in the node's
    /// region, so teardown never depends on a later configuration edit.
    #[must_use]
    pub fn cloud_settings(&self) -> RemoteSettings {
        match &self.launch_settings {
            Some(settings) => settings.clone(),
            None => RemoteSettings {
                region: self.region.clone(),
                ..Default::default()
            },
        }
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
    pub api_url: Option<String>,
    #[serde(default)]
    pub api_token: Option<String>,
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
        settings.fdb_cluster_file = self.fdb_cluster_file.clone();
        settings.bus.nats_url.clone_from(&self.nats_url);
        settings.s3.endpoint.clone_from(&self.s3_endpoint);
        settings.api.url.clone_from(&self.api_url);
        if let Some(token) = &self.api_token {
            settings.api.token.clone_from(token);
        }
        if let (Some(bucket), Some(region)) = (&self.s3_bucket, &self.s3_region) {
            settings.s3.bucket.clone_from(bucket);
            settings.s3.region.clone_from(region);
            settings.s3.access_key.clear();
            settings.s3.secret_key.clear();
        }
        if self.default_image.is_some() {
            settings
                .selection
                .default_image
                .clone_from(&self.default_image);
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

    use crate::tests::load_with_remote;

    #[test]
    fn defaults_and_overrides() {
        let settings: Settings = toml::from_str("[remote]\nmanaged_by_tag = 'codex-launcher'\n[remote.aws]\nsubnet = 'subnet-test'\nsecurity_group = 'sg-test'").unwrap();
        assert_eq!(settings.remote.aws.subnet.as_deref(), Some("subnet-test"));
        assert_eq!(
            settings.remote.aws.security_group.as_deref(),
            Some("sg-test")
        );
        assert_eq!(settings.remote.managed_by_tag, "codex-launcher");
        let settings = Settings {
            remote: RemoteSettings {
                aws: AwsSettings {
                    image: Some("ami-test".into()),
                    ..settings.remote.aws.clone()
                },
                ..settings.remote
            },
            ..settings
        };
        let decoded: Settings = toml::from_str(&settings.to_toml().unwrap()).unwrap();
        assert_eq!(decoded.remote.aws.image.as_deref(), Some("ami-test"));
        assert_eq!(decoded.remote.managed_by_tag, "codex-launcher");
    }

    #[test]
    fn aws_sub_table_rejects_flat_keys() {
        assert!(toml::from_str::<Settings>("[remote]\nsubnet = 'old'").is_err());
        let nested: Settings = toml::from_str(
            "[remote.aws]\nsubnet = 'subnet-only'\nsecurity_group = 'sg-only'\nimage = 'ami-nested'\niam_role = 'custom-role'\n",
        )
        .unwrap();
        assert_eq!(nested.remote.provider, "aws");
        assert_eq!(nested.remote.aws.instance_type, "m6id.xlarge");
        assert_eq!(nested.remote.aws.subnet.as_deref(), Some("subnet-only"));
        assert_eq!(nested.remote.aws.security_group.as_deref(), Some("sg-only"));
        assert_eq!(nested.remote.aws.image.as_deref(), Some("ami-nested"));
        assert_eq!(nested.remote.aws.iam_role.as_deref(), Some("custom-role"));

        // Unknown keys are rejected in either table.
        assert!(toml::from_str::<Settings>("[remote]\nsubnet_typo = 'x'").is_err());
        assert!(toml::from_str::<Settings>("[remote.aws]\nsubnet_typo = 'x'").is_err());
    }

    #[test]
    fn saved_launch_settings_read_nested_json() {
        let node: RemoteNode = serde_json::from_str(
            r#"{"name":"old","region":"us-east-1","instance_id":"i-old","public_ip":"127.0.0.1","private_ip":"127.0.0.1","key_path":"/tmp/key","launch_settings":{"aws":{"instance_type":"m6i.large"},"disk_gb":40},"launch_attempted":true,"created_at":"2026-09-16T00:00:00Z"}"#,
        )
        .unwrap();
        let saved = node.launch_settings.clone().unwrap();
        assert_eq!(saved.aws.instance_type, "m6i.large");
        assert_eq!(saved.disk_gb, 40);
        assert_eq!(node.cloud_settings().region, "us-east-1");

        let bare: RemoteNode = serde_json::from_str(r#"{"name":"bare","region":"eu-west-1","instance_id":"","public_ip":"","private_ip":"","key_path":"/tmp/key","launch_attempted":true,"created_at":"2026-09-16T00:00:00Z"}"#).unwrap();
        let fallback = bare.cloud_settings();
        assert_eq!(fallback.provider, "aws");
        assert_eq!(fallback.region, "eu-west-1");
        assert_eq!(fallback.aws.instance_type, "m6id.xlarge");

        let settings = RemoteSettings {
            bucket: Some("test-bucket".into()),
            ..RemoteSettings::default()
        };
        assert_eq!(
            settings.instance_profile("demo").as_deref(),
            Some("swarmy-demo")
        );
        let overridden = RemoteSettings {
            aws: AwsSettings {
                iam_role: Some("custom-role".into()),
                ..AwsSettings::default()
            },
            ..settings
        };
        assert_eq!(
            overridden.instance_profile("demo").as_deref(),
            Some("custom-role")
        );
        assert!(RemoteSettings::default().instance_profile("demo").is_none());
    }

    #[test]
    fn shared_state_defaults_and_round_trip() {
        let node: RemoteNode = serde_json::from_str(r#"{"name":"local","region":"local","instance_id":"i-local","public_ip":"127.0.0.1","private_ip":"127.0.0.1","key_path":"/tmp/key","launch_attempted":true,"created_at":"2026-09-16T00:00:00Z"}"#).unwrap();
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
        assert_eq!(settings.remote.managed_by_tag, "swarmy");
        assert!(settings.remote.aws.subnet.is_none());
        assert!(settings.remote.aws.security_group.is_none());
    }

    #[test]
    fn older_profiles_preserve_the_configured_default() {
        let profile: RemoteProfile = serde_json::from_str(r#"{"name":"old","socket_path":"socket","pid":1,"ports":{},"fdb_cluster_file":"cluster","nats_url":"nats://localhost:4222","s3_endpoint":"http://localhost:8333"}"#).unwrap();
        assert!(profile.default_image.is_none());
        let mut settings = Settings {
            selection: crate::SelectionSettings {
                default_image: Some("configured:tag".into()),
                ..crate::SelectionSettings::default()
            },
            ..Settings::default()
        };
        profile.apply(&mut settings);
        assert_eq!(
            settings.selection.default_image.as_deref(),
            Some("configured:tag")
        );
    }

    #[test]
    fn selected_profile_overrides_discovered_config_and_endpoint_environment() {
        let root = tempfile::tempdir().unwrap();
        let state = root.path().join(".swarmy");
        std::fs::create_dir_all(state.join("remote")).unwrap();
        std::fs::create_dir_all(root.path().join("nested")).unwrap();
        std::fs::write(
            state.join("config.toml"),
            "[selection]\nprovider = 'fake'\n[remote]\nprofile = 'test'\n",
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
            api_url: Some("http://127.0.0.1:18742".into()),
            api_token: Some("fixture-token".into()),
            s3_bucket: None,
            s3_region: None,
            default_image: Some("base-ubuntu:test".into()),
        };
        // A profile with its own bucket and region takes the laptop's regional
        // credentials into the service settings without an endpoint override.
        let mut regional_profile = profile.clone();
        regional_profile.s3_bucket = Some("bucket".into());
        regional_profile.s3_region = Some("eu-west-1".into());
        regional_profile.s3_endpoint.clear();
        let mut regional_settings = Settings::default();
        regional_profile.apply(&mut regional_settings);
        assert_eq!(regional_settings.s3.bucket, "bucket");
        assert_eq!(regional_settings.s3.region, "eu-west-1");
        assert!(regional_settings.s3.endpoint.is_empty());
        assert!(regional_settings.s3.access_key.is_empty());
        assert!(regional_settings.s3.secret_key.is_empty());
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
        let loaded = load_with_remote(&root.path().join("nested"), &env).unwrap();
        assert_eq!(
            loaded.settings.selection.default_image.as_deref(),
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
        assert_eq!(loaded.settings.bus.nats_url, profile.nats_url);
        assert_eq!(loaded.settings.s3.endpoint, profile.s3_endpoint);
        assert_eq!(loaded.settings.api.url, profile.api_url);
        assert_eq!(loaded.settings.api.token, "fixture-token");
        assert_eq!(
            loaded.settings.fdb_cluster_file,
            profile.fdb_cluster_file
        );
        assert_eq!(loaded.settings.s3.bucket, "custom");
        assert_eq!(loaded.settings.environment()["SWARMY_REMOTE"], "test");
        let mut invalid = profile;
        invalid.ports.fdb = 14500;
        assert!(invalid.validate_fdb_port().is_err());
        std::fs::write(
            remote_path(&state, "test", "profile.json").unwrap(),
            serde_json::to_vec(&invalid).unwrap(),
        )
        .unwrap();
        assert!(load_with_remote(root.path(), &env).is_err());

        let mut env = env;
        env.insert("SWARMY_REMOTE".into(), "missing".into());
        assert!(load_with_remote(root.path(), &env).is_err());
        env.insert("SWARMY_REMOTE".into(), "../test".into());
        assert!(load_with_remote(root.path(), &env).is_err());
    }
}
