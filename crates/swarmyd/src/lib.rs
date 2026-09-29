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
/// The transparent variants are the error sources the node deals with;
/// everything else is an arbitrary cause kept as its source in `Other` for
/// the daemon entry point to render with `{:#}`.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] swarmy_store::StoreError),
    #[error(transparent)]
    Bus(#[from] swarmy_bus::Error),
    #[error(transparent)]
    Sandbox(#[from] swarmy_sandbox::Error),
    #[error(transparent)]
    Volume(#[from] swarmy_volume::VolumeError),
    #[error(transparent)]
    Config(#[from] swarmy_config::Error),
    #[error(transparent)]
    Blob(#[from] swarmy_store::blob::BlobError),
    #[error(transparent)]
    VolumeServer(#[from] swarmy_volume::server::Error),
    #[error(transparent)]
    Timestamp(#[from] jiff::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
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
);

pub type Result<T> = std::result::Result<T, Error>;
