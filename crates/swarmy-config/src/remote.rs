//! Files shared by remote provisioning and local tunnel commands.
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{Error, ObjectPrefix, Settings};

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

/// Credential source for a remote object bucket. The secret value only ever
/// lives in 0600 state files and 0600 node environment files; [`std::fmt::Debug`]
/// redacts both keys so logs and status output can print the description safely.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum BucketCredentials {
    /// AWS today: nodes reach the bucket through the instance role.
    #[default]
    InstanceRole,
    /// Any S3-compatible bucket reached by endpoint URL with static keys.
    StaticKeys {
        access_key: String,
        secret_key: String,
    },
}

impl std::fmt::Debug for BucketCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InstanceRole => formatter.write_str("InstanceRole"),
            Self::StaticKeys { .. } => formatter.write_str("StaticKeys(..redacted..)"),
        }
    }
}

/// Provider-neutral description of the object bucket backing a remote.
///
/// One type covers both AWS S3 through an IAM instance role (empty endpoint,
/// [`BucketCredentials::InstanceRole`]) and any S3-compatible bucket reached
/// by endpoint URL with static keys. `up`, `add-node`, `connect`, `upgrade`,
/// and the node environment all use this; there are no parallel AWS and
/// non-AWS copies.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(default)]
pub struct BucketSpec {
    /// Storage endpoint override for S3-compatible providers. Empty selects
    /// the AWS regional endpoints.
    pub endpoint: String,
    /// Bucket region. Empty falls back to the remote's configured region.
    pub region: String,
    /// Bucket name.
    pub bucket: String,
    /// Object namespace inside the bucket shared by every service.
    pub prefix: ObjectPrefix,
    /// How nodes authenticate to the bucket.
    pub credentials: BucketCredentials,
    /// Write chunks and manifests with a create-only PUT
    /// (`If-None-Match: *`). Providers that reject the header need `false`,
    /// which makes the call a plain PUT. The objects are content-addressed,
    /// so overwriting identical bytes is safe. Carried into the node
    /// environment and the connect profile, which is what the nodes read.
    pub conditional_create: bool,
}

fn default_conditional_create() -> bool {
    true
}

impl Default for BucketSpec {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            region: String::new(),
            bucket: String::new(),
            prefix: ObjectPrefix::default(),
            credentials: BucketCredentials::default(),
            conditional_create: true,
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum BucketSpecHelper {
    /// Shorthand for an AWS instance-role bucket: `bucket = "name"`.
    Name(String),
    Table(BucketSpecTable),
}

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct BucketSpecTable {
    endpoint: String,
    region: String,
    bucket: String,
    prefix: ObjectPrefix,
    credentials: BucketCredentials,
    #[serde(default = "default_conditional_create")]
    conditional_create: bool,
}

impl<'de> Deserialize<'de> for BucketSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match BucketSpecHelper::deserialize(deserializer)? {
            BucketSpecHelper::Name(bucket) => Ok(Self {
                bucket,
                ..Self::default()
            }),
            BucketSpecHelper::Table(table) => Ok(Self {
                endpoint: table.endpoint,
                region: table.region,
                bucket: table.bucket,
                prefix: table.prefix,
                credentials: table.credentials,
                conditional_create: table.conditional_create,
            }),
        }
    }
}

impl BucketSpec {
    /// An AWS instance-role bucket named `bucket`.
    #[must_use]
    pub fn aws(bucket: &str) -> Self {
        Self {
            bucket: bucket.into(),
            ..Self::default()
        }
    }

    /// Whether this bucket uses the AWS regional endpoints with an instance
    /// role. Anything else is an S3-compatible bucket with static keys.
    #[must_use]
    pub fn is_aws(&self) -> bool {
        self.endpoint.is_empty() && self.credentials == BucketCredentials::InstanceRole
    }

    /// Whether nodes need static keys uploaded to reach this bucket.
    #[must_use]
    pub fn needs_static_keys(&self) -> bool {
        matches!(self.credentials, BucketCredentials::StaticKeys { .. })
    }

    /// Fill an unset region from the remote's configured region so saved
    /// records always carry a concrete region.
    pub fn resolve_region(&mut self, fallback: &str) {
        if self.region.is_empty() {
            fallback.clone_into(&mut self.region);
        }
    }

    /// Short description without secrets, for logs and status output. The
    /// region is filled from the remote's configured region when the remote
    /// is saved, so it is always concrete here.
    #[must_use]
    pub fn describe(&self) -> String {
        if self.endpoint.is_empty() {
            format!("{} ({})", self.bucket, self.region)
        } else {
            format!("{} at {} ({})", self.bucket, self.endpoint, self.region)
        }
    }

    /// Copy the bucket coordinates into service settings. Static keys become
    /// the S3 credentials; the instance-role source clears them so nodes fall
    /// back to the instance-metadata provider. The create-only switch travels
    /// with the description, which is what the nodes read.
    pub fn apply_to_settings(&self, settings: &mut Settings) {
        settings.s3.endpoint.clone_from(&self.endpoint);
        if !self.region.is_empty() {
            settings.s3.region.clone_from(&self.region);
        }
        settings.s3.bucket.clone_from(&self.bucket);
        settings.s3.prefix.clone_from(&self.prefix);
        settings.s3.conditional_create = self.conditional_create;
        match &self.credentials {
            BucketCredentials::InstanceRole => {
                settings.s3.access_key.clear();
                settings.s3.secret_key.clear();
            }
            BucketCredentials::StaticKeys {
                access_key,
                secret_key,
            } => {
                settings.s3.access_key.clone_from(access_key);
                settings.s3.secret_key.clone_from(secret_key);
            }
        }
    }

    /// Check the bucket name before creating cloud resources. HTTPS
    /// virtual-hosted requests fail for other names, so reject them early.
    /// # Errors
    /// Rejects names that are not 3-63 character lowercase DNS labels.
    pub fn validate_name(&self) -> Result<(), Error> {
        if !valid_bucket_name(&self.bucket) {
            return Err(Error::Remote(
                "bucket must be a 3-63 character lowercase DNS name without dots (HTTPS virtual-hosted S3 requires this)",
            ));
        }
        Ok(())
    }
}

/// Marker object recording swarm ownership under a prefix, for providers
/// without bucket-tag support. One implementation serves the bucket
/// description and the provisioning client.
#[must_use]
pub fn ownership_marker_key(prefix: &str) -> String {
    if prefix.is_empty() {
        ".swarmy-owner".into()
    } else {
        format!("{prefix}/.swarmy-owner")
    }
}

/// S3 bucket names are lowercase DNS labels; HTTPS virtual-hosted requests
/// fail otherwise, so reject them before creating cloud resources.
fn valid_bucket_name(bucket: &str) -> bool {
    fn dns(byte: u8) -> bool {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
    }
    (3..=63).contains(&bucket.len())
        && bucket.bytes().all(dns)
        && !bucket.starts_with('-')
        && !bucket.ends_with('-')
}
/// Placement, resource ownership, and the selected tunnel profile.
#[derive(Clone, Debug, Serialize)]
pub struct RemoteSettings {
    /// Cloud provider; only `aws` exists today.
    pub provider: String,
    pub services: RemoteServices,
    pub region: String,
    /// Object bucket backing the remote, if any. One description covers both
    /// AWS instance-role buckets (empty endpoint) and S3-compatible buckets
    /// reached by endpoint URL with static keys.
    pub bucket: Option<BucketSpec>,
    pub disk_gb: u32,
    pub managed_by_tag: String,
    pub profile: Option<String>,
    /// Login that owns the checkout and runs the node units. Plain servers
    /// use `swarmy`; EC2 launches write `ubuntu` explicitly.
    pub service_user: String,
    /// Local disk for sandbox data: a block device to format and mount at
    /// `/mnt/swarmy-local` (for example `/dev/nvme1n1`) or `dir:/path` for an
    /// existing directory to use directly. Sandbox nodes require it;
    /// control-only nodes leave it empty for the root disk.
    pub local_storage: String,
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
            service_user: default_service_user(),
            local_storage: String::new(),
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
    bucket: Option<BucketSpec>,
    disk_gb: u32,
    managed_by_tag: String,
    profile: Option<String>,
    service_user: String,
    local_storage: String,
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
            // Empty means the record predates the setting; `RemoteNode::service_user`
            // falls back to the SSH login it was provisioned with. Configuration
            // files fill the new default on load (`Settings::read`).
            service_user: String::new(),
            local_storage: String::new(),
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
        // Keep an absent service user empty so saved node records fall back to
        // the SSH login they were provisioned with. Configuration files fill
        // the new default on load; see `Settings::read`.
        Ok(Self {
            provider: helper.provider,
            services: helper.services,
            region: helper.region,
            bucket: helper.bucket,
            disk_gb: helper.disk_gb,
            managed_by_tag: helper.managed_by_tag,
            profile: helper.profile,
            service_user: helper.service_user,
            local_storage: helper.local_storage,
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
    /// `swarmy-{remote}` when the remote uses an AWS instance-role bucket.
    /// Static-key buckets need no IAM role, so they report no profile.
    #[must_use]
    pub fn instance_profile(&self, remote: &str) -> Option<String> {
        self.bucket.as_ref().and_then(|spec| {
            if spec.is_aws() {
                Some(
                    self.aws
                        .iam_role
                        .clone()
                        .unwrap_or_else(|| format!("swarmy-{remote}")),
                )
            } else {
                None
            }
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
    /// Bucket name backing this remote, if any.
    #[must_use]
    pub fn bucket(&self) -> Option<&str> {
        self.launch_settings
            .as_ref()?
            .bucket
            .as_ref()
            .map(|spec| spec.bucket.as_str())
    }

    /// Full bucket description backing this remote, with the region filled
    /// from the node's region when the saved record leaves it unset.
    #[must_use]
    pub fn bucket_spec(&self) -> Option<BucketSpec> {
        let mut spec = self.launch_settings.as_ref()?.bucket.clone()?;
        spec.resolve_region(&self.region);
        Some(spec)
    }

    /// Login that owns the checkout and runs the node units. Saved launch
    /// settings win; records written before the setting existed fall back
    /// to the SSH login, which matched the service user on those nodes.
    #[must_use]
    pub fn service_user(&self) -> &str {
        match &self.launch_settings {
            Some(settings) if !settings.service_user.is_empty() => &settings.service_user,
            _ => &self.ssh_user,
        }
    }

    /// Local disk setting for sandbox data; see `RemoteSettings::local_storage`.
    #[must_use]
    pub fn local_storage(&self) -> &str {
        match &self.launch_settings {
            Some(settings) => settings.local_storage.as_str(),
            None => "",
        }
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

fn default_service_user() -> String {
    "swarmy".into()
}

impl RemoteSettings {
    /// Fill the new default for configuration files. Saved node records keep
    /// an empty service user so `RemoteNode::service_user` falls back to the
    /// SSH login they were provisioned with.
    pub fn normalize_service_user(&mut self) {
        if self.service_user.is_empty() {
            self.service_user = default_service_user();
        }
    }
}

/// Validate service logins before using them in paths or shell output.
///
/// # Errors
///
/// Rejects empty names and shell metacharacters.
pub fn validate_service_user(user: &str) -> Result<(), Error> {
    if !user.is_empty()
        && user.len() <= 64
        && user
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        Ok(())
    } else {
        Err(Error::Remote(
            "service user must contain 1-64 letters, digits, hyphens, or underscores",
        ))
    }
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
    /// Local tunnel endpoint for the `SeaweedFS` object store. Empty when the
    /// remote uses an object bucket (see `bucket`); bucket coordinates,
    /// including a static-key endpoint, live in the bucket description.
    pub s3_endpoint: String,
    #[serde(default)]
    pub api_url: Option<String>,
    #[serde(default)]
    pub api_token: Option<String>,
    /// Object bucket backing the remote. The credential source is either the
    /// instance role or static keys; the secret itself is redacted in logs.
    #[serde(default)]
    pub bucket: Option<BucketSpec>,
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

    /// Apply stack endpoints and its default image, preserving namespaces.
    /// A bucket description carries the endpoint, region, bucket, prefix,
    /// and credential source; static keys become the S3 credentials while
    /// the instance-role source clears them.
    pub fn apply(&self, settings: &mut Settings) {
        settings
            .store
            .cluster_file
            .clone_from(&self.fdb_cluster_file);
        settings.bus.nats_url.clone_from(&self.nats_url);
        settings.s3.endpoint.clone_from(&self.s3_endpoint);
        settings.api.url.clone_from(&self.api_url);
        if let Some(token) = &self.api_token {
            settings.api.token.clone_from(token);
        }
        if let Some(spec) = &self.bucket {
            spec.apply_to_settings(settings);
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
            bucket: Some(BucketSpec::aws("test-bucket")),
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
        // Static-key buckets need no IAM role or instance profile.
        let settings = RemoteSettings {
            bucket: Some(BucketSpec {
                endpoint: "https://objects.example.invalid".into(),
                region: "eu-west-1".into(),
                bucket: "test-bucket".into(),
                credentials: BucketCredentials::StaticKeys {
                    access_key: "test-access".into(),
                    secret_key: "test-secret".into(),
                },
                ..BucketSpec::default()
            }),
            ..RemoteSettings::default()
        };
        assert!(settings.instance_profile("demo").is_none());
        let debug = format!("{:?}", settings.bucket.as_ref().unwrap());
        assert!(!debug.contains("test-access"));
        assert!(!debug.contains("test-secret"));
    }

    #[test]
    fn bucket_specs_parse_shorthand_table_and_validate() {
        let shorthand: Settings = toml::from_str("[remote]\nbucket = 'test-bucket'").unwrap();
        let spec = shorthand.remote.bucket.unwrap();
        assert_eq!(spec.bucket, "test-bucket");
        assert!(spec.is_aws());
        assert!(!spec.needs_static_keys());
        assert!(spec.conditional_create);
        assert!(spec.describe().starts_with("test-bucket ("));
        // The table form carries endpoint, region, prefix, and static keys.
        let table: Settings = toml::from_str(
            "[remote.bucket]\nendpoint = 'https://objects.example.invalid'\nregion = 'eu-west-1'\nbucket = 'test-bucket'\nprefix = 'runs/team'\nconditional_create = false\n[remote.bucket.credentials]\nsource = 'static_keys'\naccess_key = 'test-access'\nsecret_key = 'test-secret'\n",
        )
        .unwrap();
        let spec = table.remote.bucket.clone().unwrap();
        assert!(!spec.is_aws());
        assert!(spec.needs_static_keys());
        assert!(!spec.conditional_create);
        assert_eq!(spec.prefix.as_str(), "runs/team");
        assert_eq!(
            spec.describe(),
            "test-bucket at https://objects.example.invalid (eu-west-1)"
        );
        spec.validate_name().unwrap();
        // Unknown keys are rejected in the bucket table.
        assert!(toml::from_str::<Settings>("[remote.bucket]\nbucket_typo = 'x'").is_err());
        for bad in ["bad/bucket", "bad.bucket", "ab", "-lead", "trail-"] {
            let spec = BucketSpec::aws(bad);
            assert!(spec.validate_name().is_err(), "{bad}");
        }
        // Static keys reach service settings; the instance role clears them.
        let mut settings = Settings::default();
        spec.apply_to_settings(&mut settings);
        assert_eq!(settings.s3.access_key, "test-access");
        assert_eq!(settings.s3.secret_key, "test-secret");
        assert_eq!(settings.s3.prefix.as_str(), "runs/team");
        assert!(!settings.s3.conditional_create);
        BucketSpec::aws("test-bucket").apply_to_settings(&mut settings);
        assert!(settings.s3.access_key.is_empty());
        assert!(settings.s3.secret_key.is_empty());
        // A table round trip keeps every field, including the secret.
        let decoded: Settings = toml::from_str(&table.to_toml().unwrap()).unwrap();
        assert_eq!(decoded.remote.bucket, table.remote.bucket);
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
    fn service_user_and_local_storage_defaults() {
        let settings = Settings::default();
        assert_eq!(settings.remote.service_user, "swarmy");
        assert!(settings.remote.local_storage.is_empty());
        let parsed: Settings =
            toml::from_str("[remote]\nservice_user = 'ubuntu'\nlocal_storage = 'dir:/srv/local'")
                .unwrap();
        assert_eq!(parsed.remote.service_user, "ubuntu");
        assert_eq!(parsed.remote.local_storage, "dir:/srv/local");
        let round_trip: Settings = toml::from_str(&parsed.to_toml().unwrap()).unwrap();
        assert_eq!(round_trip.remote.service_user, "ubuntu");
        assert_eq!(round_trip.remote.local_storage, "dir:/srv/local");

        // Records saved before the setting existed keep their SSH login.
        // Paths are resolved on the host from that login (`~user`), never
        // from a laptop-side `/home/<user>` guess.
        let legacy: RemoteNode = serde_json::from_str(r#"{"name":"old","region":"us-east-1","instance_id":"i-old","public_ip":"127.0.0.1","private_ip":"127.0.0.1","key_path":"/tmp/key","launch_attempted":true,"created_at":"2026-09-16T00:00:00Z"}"#).unwrap();
        assert_eq!(legacy.service_user(), "ubuntu");
        assert_eq!(legacy.local_storage(), "");

        // Saved launch settings without the new field keep the login they were
        // provisioned with instead of taking the new default.
        let existing: RemoteNode = serde_json::from_str(
            r#"{"name":"existing","region":"us-east-1","instance_id":"i-old","public_ip":"127.0.0.1","private_ip":"127.0.0.1","key_path":"/tmp/key","launch_settings":{"aws":{"instance_type":"m6id.xlarge"},"disk_gb":100},"launch_attempted":true,"created_at":"2026-09-16T00:00:00Z"}"#,
        )
        .unwrap();
        assert_eq!(existing.service_user(), "ubuntu");

        // Configuration files without the field take the new default on load.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "[remote]\nregion = 'us-east-1'\n").unwrap();
        let loaded = Settings::read(&path).unwrap();
        assert_eq!(loaded.remote.service_user, "swarmy");

        // New records resolve through their saved launch settings.
        let node: RemoteNode = serde_json::from_str(
            r#"{"name":"new","region":"us-east-1","instance_id":"i-new","public_ip":"127.0.0.1","private_ip":"127.0.0.1","key_path":"/tmp/key","ssh_user":"root","launch_settings":{"service_user":"swarmy","local_storage":"/dev/md0"},"launch_attempted":true,"created_at":"2026-09-16T00:00:00Z"}"#,
        )
        .unwrap();
        assert_eq!(node.service_user(), "swarmy");
        assert_eq!(node.local_storage(), "/dev/md0");

        assert!(validate_service_user("swarmy").is_ok());
        assert!(validate_service_user("deploy-1").is_ok());
        for invalid in ["", "has space", "semi;colon", "$(injected)", "dq\"quote"] {
            assert!(validate_service_user(invalid).is_err());
        }
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
            bucket: None,
            default_image: Some("base-ubuntu:test".into()),
        };
        // A profile with its own bucket and region takes the laptop's regional
        // credentials into the service settings without an endpoint override.
        let mut regional_profile = profile.clone();
        regional_profile.bucket = Some(BucketSpec {
            region: "eu-west-1".into(),
            bucket: "bucket".into(),
            ..BucketSpec::default()
        });
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
        assert_eq!(loaded.settings.store.cluster_file, profile.fdb_cluster_file);
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

    #[test]
    fn static_key_profile_carries_endpoint_and_keys() {
        let profile = RemoteProfile {
            name: "test".into(),
            socket_path: "socket".into(),
            pid: 1,
            ports: RemotePorts::default(),
            remote_ports: RemotePorts::default(),
            fdb_cluster_file: "cluster".into(),
            nats_url: "nats://127.0.0.1:14222".into(),
            s3_endpoint: String::new(),
            api_url: None,
            api_token: None,
            bucket: Some(BucketSpec {
                endpoint: "https://objects.example.invalid".into(),
                region: "eu-west-1".into(),
                bucket: "bucket".into(),
                prefix: "runs/team".parse().unwrap(),
                credentials: BucketCredentials::StaticKeys {
                    access_key: "test-access".into(),
                    secret_key: "test-secret".into(),
                },
            }),
            default_image: None,
        };
        let mut settings = Settings::default();
        profile.apply(&mut settings);
        assert_eq!(settings.s3.endpoint, "https://objects.example.invalid");
        assert_eq!(settings.s3.bucket, "bucket");
        assert_eq!(settings.s3.region, "eu-west-1");
        assert_eq!(settings.s3.access_key, "test-access");
        assert_eq!(settings.s3.secret_key, "test-secret");
        assert_eq!(settings.s3.prefix.as_str(), "runs/team");
        // The serialized profile keeps the keys; its debug form does not.
        let encoded = serde_json::to_vec(&profile).unwrap();
        let decoded: RemoteProfile = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded.bucket, profile.bucket);
        let debug = format!("{profile:?}");
        assert!(!debug.contains("test-access"), "{debug}");
        assert!(!debug.contains("test-secret"), "{debug}");
    }
}
