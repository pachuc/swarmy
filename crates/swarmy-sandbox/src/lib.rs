//! Sandbox execution backed by durable virtual disks.
mod cdi;
mod credentials;
mod metadata;
mod runc;
pub use cdi::{GpuEdits, NodeGpu};
pub use metadata::SandboxMetadata;
pub use runc::{RuncRuntime, RuntimeOptions, ScratchPolicy, pasta_pid_file};
pub use swarmy_core::{
    BlockDevice, ExecOutput, ExecRequest, ExecResult, PauseHandle, RuntimeCaps, Sandbox,
    SandboxSpec,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Metadata(#[from] swarmy_volume::MetadataError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Volume(#[from] swarmy_volume::server::Error),
    #[error("sandbox operation failed: {0}")]
    Operation(String),
    #[error("sandbox is missing or already exists")]
    State,
}
pub type Result<T> = std::result::Result<T, Error>;
