//! Local node control protocol. The Unix socket is accessible only to its owner.
use serde::{Deserialize, Serialize};
use swarmy_core::{
    BlockDevice, ExecOutput, ExecRequest, ExecResult, PauseHandle, RuntimeCaps, Sandbox,
    SandboxSpec,
};

/// One request per connection; exec emits output frames followed by one result.
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Create {
        spec: SandboxSpec,
        disk: BlockDevice,
    },
    Exec {
        sandbox: Sandbox,
        request: ExecRequest,
    },
    Pause(Sandbox),
    Resume(PauseHandle),
    Destroy(Sandbox),
    Checkpoint(Sandbox),
    Capabilities,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Sandbox(Sandbox),
    Paused(PauseHandle),
    Output(ExecOutput),
    Exited(ExecResult),
    Capabilities(RuntimeCaps),
    Destroyed,
    Checkpointed(swarmy_core::ManifestId),
    Error(String),
}

/// Failures in node hosting and volume operations.
///
/// Only failures callers branch on have their own variant; everything else
/// is an arbitrary cause kept as its source in `Other` for the daemon entry
/// point to render with its source chain.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl Error {
    /// An arbitrary failure with no structured cause to preserve.
    pub fn other(message: impl Into<String>) -> Self {
        Self::Other(Box::new(std::io::Error::other(message.into())))
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
    base64::DecodeError,
    tokio::time::error::Elapsed,
    tokio::task::JoinError,
    std::fmt::Error,
    swarmy_store::StoreError,
    swarmy_bus::Error,
    swarmy_sandbox::Error,
    swarmy_volume::VolumeError,
    swarmy_config::Error,
    swarmy_store::blob::BlobError,
    swarmy_volume::server::Error,
    swarmy_image::ImageError,
    jiff::Error,
    std::io::Error,
    serde_json::Error,
);

pub type Result<T> = std::result::Result<T, Error>;
