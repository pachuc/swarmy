//! Cloud substrate for remote development stacks.
//!
//! Everything that knows about a cloud provider lives here behind one public
//! interface, so a second provider is an implementation, not a rewrite. The
//! [`Cloud`] trait speaks in provider-neutral types ([`MachineSpec`],
//! [`Machine`], [`ObjectBucket`]); `Aws` implements it with EC2, SSM, S3,
//! and IAM. [`Host`] covers the SSH half of provisioning and stays
//! provider-independent. See `docs/cloud-substrate.md` for the contract a
//! second provider must implement.
// The CLI owns presentation. Provisioning reports lines through this process-level
// sink because work may continue on different Tokio threads during an upgrade.
static OUTPUT: std::sync::OnceLock<fn(&str, bool)> = std::sync::OnceLock::new();

/// Install the CLI's output handler before starting provisioning.
pub fn set_output_sink(sink: fn(&str, bool)) {
    let _ = OUTPUT.set(sink);
}

#[cfg(feature = "remote")]
static PROMPT: std::sync::OnceLock<fn(&str) -> std::io::Result<String>> =
    std::sync::OnceLock::new();

#[cfg(feature = "remote")]
/// Install the CLI's interactive prompt handler.
pub fn set_prompt_sink(sink: fn(&str) -> std::io::Result<String>) {
    let _ = PROMPT.set(sink);
}

#[cfg(feature = "remote")]
fn prompt(message: &str) -> std::io::Result<String> {
    PROMPT
        .get()
        .ok_or_else(|| std::io::Error::other("interactive prompt handler not installed"))?(
        message
    )
}

fn emit(message: &str, stderr: bool) {
    if let Some(sink) = OUTPUT.get() {
        sink(message, stderr);
    } else if stderr {
        tracing::warn!("{message}");
    } else {
        tracing::info!("{message}");
    }
}

macro_rules! cloud_out {
    ($($arg:tt)*) => { $crate::emit(&format!($($arg)*), false) };
}
macro_rules! cloud_err {
    ($($arg:tt)*) => { $crate::emit(&format!($($arg)*), true) };
}

mod command;
pub mod ssh;
pub use command::{Command, select};
#[cfg(feature = "remote")]
mod remote;
mod services;
#[cfg(feature = "remote")]
pub use remote::{Aws, DeletionPlan, RunOutcome, ServiceOptions, for_settings, run};

/// Failures returned to clients of the remote provisioning entry point.
///
/// Only failures callers act on have their own variant: missing permissions
/// (retried or reported with the operation name), AWS failures (reported with
/// the operation, code, and message from the SDK metadata), missing or
/// duplicate state, SSH failures (reported with the attempted command), and
/// control-plane client failures. Everything else is an arbitrary cause kept
/// as its source for the binary to render with `{:#}`.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unknown cloud provider '{0}': only 'aws' is supported")]
    UnsupportedProvider(String),
    #[error("no remote node named {0}; run swarmy remote up {0}")]
    NotFound(String),
    #[error("remote node {0} already exists; run swarmy remote down {0} first")]
    AlreadyExists(String),
    #[error("missing permission for {operation}")]
    MissingPermission {
        operation: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("{operation}: {code:?} {message:?}")]
    Aws {
        operation: String,
        code: Option<String>,
        message: Option<String>,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("ssh {command} failed")]
    Ssh {
        command: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("ssh {command} failed with {status}")]
    SshStatus {
        command: String,
        status: std::process::ExitStatus,
    },
    #[error(transparent)]
    Client(#[from] swarmy_client::Error),
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl Error {
    /// An arbitrary failure with no structured cause to preserve.
    pub(crate) fn other(message: impl Into<String>) -> Self {
        Self::Other(Box::new(std::io::Error::other(message.into())))
    }

    #[cfg(feature = "remote")]
    fn permission(&self) -> Option<&str> {
        match self {
            Self::MissingPermission { operation, .. } => Some(operation),
            _ => None,
        }
    }
}

macro_rules! other_from {
    ($($t:ty),* $(,)?) => {
        $(impl From<$t> for Error {
            fn from(error: $t) -> Self {
                Self::Other(Box::new(error))
            }
        })*
    };
}

other_from!(
    std::io::Error,
    serde_json::Error,
    toml::de::Error,
    std::string::FromUtf8Error,
    std::num::TryFromIntError,
    swarmy_config::Error,
    ulid::DecodeError,
    jiff::Error,
    tempfile::PersistError,
    std::path::StripPrefixError,
    std::net::AddrParseError,
    tokio::time::error::Elapsed,
);

#[cfg(feature = "remote")]
other_from!(aws_sdk_s3::error::BuildError,);

pub type Result<T> = std::result::Result<T, Error>;

use std::future::Future;

use swarmy_config::{RemoteNode, RemoteSettings};

/// Provider-neutral description of one machine to create.
///
/// Generic shape (`cpus`, `memory_mib`, `disk_gb`) is authoritative for
/// providers that size machines directly. AWS sizes from `instance_type`
/// instead and ignores `cpus` and `memory_mib`; the lookup in
/// [`MachineSpec::from_settings`] fills them when the type is known and
/// leaves them zero otherwise.
#[derive(Clone, Debug)]
pub struct MachineSpec {
    /// Human name; AWS also uses it for the `Name` tag and the client token.
    pub name: String,
    /// Image reference; an AMI id on AWS.
    pub image: String,
    /// Key pair name; AWS imports it with [`Cloud::import_ssh_key`] before
    /// launch and passes the name to `RunInstances`.
    pub key_name: String,
    /// Raw public key bytes for providers that install keys inline at
    /// creation. AWS ignores this field because keys are imported separately.
    pub ssh_public_key: Vec<u8>,
    /// vCPUs for providers that size machines directly.
    pub cpus: u32,
    /// Memory in MiB for providers that size machines directly.
    pub memory_mib: u32,
    /// Root disk size in GiB.
    pub disk_gb: u32,
    /// Provider-specific shape; the EC2 instance type on AWS.
    pub instance_type: String,
    /// Placement or network selection; the subnet id on AWS.
    pub subnet: Option<String>,
    /// Network membership; the security group id on AWS.
    pub security_group: Option<String>,
    /// Value of the `managed-by` tag.
    pub managed_by: String,
    /// Node credentials to attach; the IAM instance profile on AWS.
    pub profile: Option<String>,
    /// First-boot script. AWS leaves this unset because provisioning runs
    /// over SSH after launch instead.
    pub bootstrap: Option<Vec<u8>>,
}

impl MachineSpec {
    /// Build a launch request from saved remote settings.
    ///
    /// `profile` carries the node credentials (the `swarmy-{remote}` instance
    /// profile when the remote uses an object bucket). `ssh_public_key` is
    /// the key about to be imported with [`Cloud::import_ssh_key`].
    #[must_use]
    pub fn from_settings(
        name: &str,
        image: &str,
        key_name: &str,
        ssh_public_key: Vec<u8>,
        settings: &RemoteSettings,
        profile: Option<String>,
    ) -> Self {
        let (cpus, memory_mib) = instance_shape(&settings.aws.instance_type);
        Self {
            name: name.into(),
            image: image.into(),
            key_name: key_name.into(),
            ssh_public_key,
            cpus,
            memory_mib,
            disk_gb: settings.disk_gb,
            instance_type: settings.aws.instance_type.clone(),
            subnet: settings.aws.subnet.clone(),
            security_group: settings.aws.security_group.clone(),
            managed_by: settings.managed_by_tag.clone(),
            profile,
            bootstrap: None,
        }
    }
}

/// Provider-neutral view of one machine. Missing machines are `None`.
#[derive(Clone, Debug)]
pub struct Machine {
    /// Provider's machine id (the EC2 instance id on AWS).
    pub id: String,
    pub public_ip: String,
    pub private_ip: String,
    /// Lifecycle state as reported by the provider (`pending`, `running`,
    /// `terminated`, and similar).
    pub state: String,
}

/// Provider-neutral description of the object bucket backing a remote.
///
/// The node credentials guard this bucket until teardown.
#[derive(Clone, Debug)]
pub struct ObjectBucket {
    /// Bucket name.
    pub name: String,
    /// Bucket region.
    pub region: String,
    /// Owning remote; AWS derives the default IAM role from this.
    pub owner: String,
    /// Storage endpoint override for S3-compatible providers. AWS leaves
    /// this unset and uses its regional endpoints.
    pub endpoint: Option<String>,
    /// Credentials attached to nodes; the IAM instance profile on AWS.
    pub node_credentials: Option<String>,
}

/// Whether a cloud resource belongs to this remote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ownership {
    Owned,
    Absent,
    Unmanaged,
}

/// The cloud boundary. Only the provider implementation knows about provider
/// APIs; callers use machines, keys, images, and buckets. Missing resources
/// are represented by `None`.
pub trait Cloud {
    /// Create the bucket and the node credentials guarding it, idempotently.
    fn ensure_bucket(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<()>>;
    /// Resolve the stock machine image for the configured region.
    fn base_image(&self) -> impl Future<Output = Result<String>>;
    /// Import an SSH public key under `name` and tag it with `owner`.
    fn import_ssh_key(
        &self,
        name: &str,
        public_key: Vec<u8>,
        owner: &str,
    ) -> impl Future<Output = Result<()>>;
    /// Create a machine and return its id. The launch token is the key name,
    /// so a lost response is recoverable with [`Cloud::find_by_tag`].
    fn create(&self, spec: &MachineSpec) -> impl Future<Output = Result<String>>;
    /// Describe a machine by id, or `None` when it does not exist.
    fn get(&self, id: &str) -> impl Future<Output = Result<Option<Machine>>>;
    /// Find a machine created under a launch token (the key name).
    fn find_by_tag(&self, token: &str) -> impl Future<Output = Result<Option<String>>>;
    /// Terminate a machine; already-terminated or missing machines are
    /// success.
    fn destroy(&self, id: &str) -> impl Future<Output = Result<()>>;
    /// Check both ownership tags before destructive operations.
    fn bucket_ownership(&self, name: &str, owner: &str) -> impl Future<Output = Result<Ownership>>;
    /// Check profile and role separately; an unowned profile must never be altered.
    fn role_ownership(
        &self,
        name: &str,
        owner: &str,
    ) -> impl Future<Output = Result<(Ownership, Ownership)>>;
    /// Explicitly adopt resources after the operator confirms their names.
    fn tag_bucket(&self, name: &str, owner: &str) -> impl Future<Output = Result<()>>;
    fn tag_node_role(&self, name: &str, owner: &str) -> impl Future<Output = Result<()>>;
    /// Empty and delete an owned bucket; return false if it was already absent.
    fn delete_bucket(&self, name: &str, owner: &str) -> impl Future<Output = Result<bool>>;
    /// Delete the instance profile and its role; return whether the profile and role were present.
    fn delete_node_role(
        &self,
        name: &str,
        owner: &str,
    ) -> impl Future<Output = Result<(bool, bool)>>;
    /// Delete an SSH key; missing keys are success.
    fn delete_ssh_key(&self, name: &str) -> impl Future<Output = Result<()>>;
}

/// Key generation and provisioning over SSH, replaceable by a fake in tests.
pub trait Host {
    fn services(
        &self,
        node: &RemoteNode,
        address: &str,
        options: &services::Options<'_>,
    ) -> impl Future<Output = Result<()>>;
    fn generate_key(&self, node: &RemoteNode) -> impl Future<Output = Result<Vec<u8>>>;
    fn build_image(
        &self,
        node: &RemoteNode,
        address: &str,
        recipe: &std::path::Path,
    ) -> impl Future<Output = Result<()>>;
    fn provision(
        &self,
        node: &RemoteNode,
        primary: Option<&RemoteNode>,
    ) -> impl Future<Output = Result<String>>;
}

impl Host for ssh::Ssh {
    async fn services(
        &self,
        node: &RemoteNode,
        address: &str,
        options: &services::Options<'_>,
    ) -> Result<()> {
        services::install(node, address, options).await
    }

    async fn build_image(
        &self,
        node: &RemoteNode,
        address: &str,
        recipe: &std::path::Path,
    ) -> Result<()> {
        ssh::build_image(node, address, recipe).await
    }

    async fn generate_key(&self, node: &RemoteNode) -> Result<Vec<u8>> {
        ssh::generate_key(node).await
    }

    async fn provision(&self, node: &RemoteNode, primary: Option<&RemoteNode>) -> Result<String> {
        ssh::Ssh::provision(self, node, primary).await
    }
}

/// Generic shape for known EC2 instance types. Unknown types leave the
/// generic shape unset; the provider-specific `instance_type` stays
/// authoritative on AWS.
fn instance_shape(instance_type: &str) -> (u32, u32) {
    match instance_type {
        "m6i.large" => (2, 8 * 1024),
        "m6id.xlarge" => (4, 16 * 1024),
        "m6id.4xlarge" => (16, 64 * 1024),
        _ => (0, 0),
    }
}
