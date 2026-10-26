//! Provision existing remote hosts over SSH and manage their static-key
//! object buckets through [`Cloud`]. Cloud swarm machines come from the
//! `OpenTofu` modules in `infra/modules`, run by `swarmy-infra`.
//! See `docs/cloud-substrate.md`.
// The CLI owns presentation. Provisioning reports lines through this process-level
// sink because work may continue on different Tokio threads during an upgrade.
static OUTPUT: std::sync::OnceLock<fn(&str, bool)> = std::sync::OnceLock::new();

/// Install the CLI's output handler before starting provisioning.
pub fn set_output_sink(sink: fn(&str, bool)) {
    ignore_best_effort(OUTPUT.set(sink), "install log sink");
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
pub mod node_files;
pub mod release;
pub mod ssh;
pub use command::{BucketArgs, Command, select};
#[cfg(feature = "remote")]
mod remote;
mod services;
#[cfg(feature = "remote")]
pub use remote::{DeletionPlan, RunOutcome, ServiceOptions, run};

/// Failures returned to clients of the remote provisioning entry point.
///
/// Only failures callers act on have their own variant: missing permissions
/// (retried or reported with the operation name), AWS failures (reported
/// with the operation, code, and message from the SDK metadata), missing or
/// duplicate state, SSH failures (reported with the attempted command), and
/// control-plane client failures. Everything else is an arbitrary cause kept
/// as its source for the binary to render with its source chain.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("no remote node named {0}; create it with swarmy remote adopt")]
    NotFound(String),
    #[error("remote node {0} already exists; run swarmy remote down {0} first")]
    AlreadyExists(String),
    #[error("missing permission for {operation}")]
    MissingPermission {
        operation: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("{operation}: {code}: {message}")]
    Aws {
        operation: String,
        code: String,
        message: String,
    },
    #[error("ssh {command} failed")]
    Ssh {
        /// Names the attempted operation, such as "provision remote node".
        command: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("ssh {command} failed with {status}")]
    SshStatus {
        /// Names the attempted operation, such as "provision remote node".
        command: String,
        status: std::process::ExitStatus,
    },
    #[error("SSH is unreachable at both instance addresses")]
    SshUnavailable,
    #[error("API at {endpoint}")]
    Client {
        endpoint: String,
        #[source]
        source: swarmy_client::Error,
    },
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

/// A site message paired with its cause. `Display` shows the message so
/// logs read the same with or without the chain; [`swarmy_core::error_chain`]
/// appends the cause.
#[derive(Debug)]
struct WithCause {
    message: String,
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl std::fmt::Display for WithCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for WithCause {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

impl Error {
    /// An arbitrary failure with no structured cause to preserve.
    pub(crate) fn other(message: impl Into<String>) -> Self {
        Self::Other(Box::new(std::io::Error::other(message.into())))
    }

    /// A site message keeping its cause, as anyhow's `.context()` did.
    pub(crate) fn context(
        source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
        message: impl Into<String>,
    ) -> Self {
        Self::Other(Box::new(WithCause {
            message: message.into(),
            source: source.into(),
        }))
    }

    /// Fail with `message` unless `condition` holds.
    pub(crate) fn ensure(condition: bool, message: impl Into<String>) -> Result<()> {
        if condition {
            Ok(())
        } else {
            Err(Self::other(message))
        }
    }

    /// A `map_err` closure tagging a spawn or I/O failure with the attempted operation.
    pub(crate) fn ssh<E>(command: &str) -> impl FnOnce(E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let command = command.to_owned();
        move |source| Self::Ssh {
            command,
            source: source.into(),
        }
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
    reqwest::Error,
    swarmy_infra::download::Error,
    url::ParseError,
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
use swarmy_core::ignore_best_effort;

use swarmy_config::RemoteNode;

/// Provider-neutral description of the object bucket backing a remote.
///
/// The bucket description is [`swarmy_config::BucketSpec`]: endpoint, region,
/// bucket name, prefix, and static keys. There is one description everywhere. [`std::fmt::Debug`] redacts static keys through the
/// description.
#[derive(Clone, Debug)]
pub struct ObjectBucket {
    /// The one bucket description, with the region filled from the remote's
    /// configured region.
    pub spec: swarmy_config::BucketSpec,
    /// Owning remote.
    pub owner: String,
}

impl ObjectBucket {
    /// Build the provider-neutral bucket from one bucket description.
    #[must_use]
    pub fn from_spec(owner: &str, spec: &swarmy_config::BucketSpec, fallback_region: &str) -> Self {
        let mut spec = spec.clone();
        spec.resolve_region(fallback_region);
        Self {
            spec,
            owner: owner.into(),
        }
    }
}

/// Whether `remote down` removed the bucket itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BucketRemoval {
    /// The bucket itself was deleted.
    Removed,
    /// The bucket was already absent.
    Absent,
    /// The swarm's prefix scope was deleted but the bucket remains because
    /// it retains content outside that scope.
    Retained,
}

/// Whether a cloud resource belongs to this remote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ownership {
    Owned,
    Absent,
    Unmanaged,
}

/// The object-storage boundary for remote buckets.
pub trait Cloud {
    /// Create or adopt a bucket and record its ownership.
    fn ensure_bucket(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<()>>;
    /// Verify static-key access before recording state.
    fn verify_bucket_access(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<()>>;
    /// Check ownership before a destructive operation.
    fn bucket_ownership(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<Ownership>>;
    /// Delete the owned prefix scope and bucket when empty.
    fn delete_bucket(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<BucketRemoval>>;
}

/// Key generation and provisioning over SSH, replaceable by a fake in tests.
pub trait Host {
    fn services(
        &self,
        node: &RemoteNode,
        address: &str,
        options: &services::Options<'_>,
    ) -> impl Future<Output = Result<()>>;
    fn build_image(
        &self,
        node: &RemoteNode,
        address: &str,
        recipe: &std::path::Path,
    ) -> impl Future<Output = Result<()>>;
    /// Copy an operator-owned bootstrap key into remote state for adoption.
    fn adopt_key(
        &self,
        node: &RemoteNode,
        source: &std::path::Path,
    ) -> impl Future<Output = Result<()>>;
    /// Stop services and remove swarmy files on an adopted host.
    fn decommission(&self, node: &RemoteNode) -> impl Future<Output = Result<()>>;
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

    async fn adopt_key(&self, node: &RemoteNode, source: &std::path::Path) -> Result<()> {
        ssh::adopt_key(node, source).await
    }

    async fn decommission(&self, node: &RemoteNode) -> Result<()> {
        ssh::Ssh::decommission(self, node).await
    }

    async fn provision(&self, node: &RemoteNode, primary: Option<&RemoteNode>) -> Result<String> {
        ssh::Ssh::provision(self, node, primary).await
    }
}
