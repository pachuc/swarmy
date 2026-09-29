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
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Message(String),
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
    Image(#[from] swarmy_image::ImageError),
    #[error(transparent)]
    VolumeServer(#[from] swarmy_volume::server::Error),
    #[error(transparent)]
    Timestamp(#[from] jiff::Error),
    #[error(transparent)]
    Base64(#[from] base64::DecodeError),
    #[error(transparent)]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Format(#[from] std::fmt::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("{message}")]
    Context {
        message: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait ErrorContext<T> {
    fn context(self, message: &'static str) -> Result<T>;
}

impl<T, E> ErrorContext<T> for std::result::Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn context(self, message: &'static str) -> Result<T> {
        self.map_err(|source| Error::Context {
            message: message.to_owned(),
            source: Box::new(source),
        })
    }
}

impl<T> ErrorContext<T> for Option<T> {
    fn context(self, message: &'static str) -> Result<T> {
        self.ok_or_else(|| Error::Message(message.to_owned()))
    }
}

#[macro_export]
macro_rules! node_bail {
    ($($arg:tt)*) => { return Err($crate::Error::Message(format!($($arg)*))) };
}

#[macro_export]
macro_rules! node_ensure {
    ($condition:expr, $($arg:tt)*) => {
        if !$condition {
            $crate::node_bail!($($arg)*);
        }
    };
}
