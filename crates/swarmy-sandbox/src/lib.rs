//! Sandbox execution backed by durable virtual disks.
mod credentials;
mod runc;
use async_trait::async_trait;
pub use runc::{RuncRuntime, ScratchPolicy};
pub use swarmy_core::{
    BlockDevice, ExecOutput, ExecRequest, ExecResult, PauseHandle, RuntimeCaps, Sandbox,
    SandboxSpec,
};
use tokio::sync::mpsc;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] swarmy_store::StoreError),
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

#[async_trait]
pub trait SandboxRuntime: Send + Sync {
    /// # Errors
    /// Returns attachment, mount, or container startup errors.
    async fn create(&self, spec: SandboxSpec, disk: BlockDevice) -> Result<Sandbox>;
    /// Stream stdout and stderr through a bounded channel. Timeout includes
    /// output backpressure and kills all processes in this sandbox.
    /// # Errors
    /// Returns invalid-command, process, or output transport errors.
    async fn exec(
        &self,
        sb: &Sandbox,
        request: ExecRequest,
        output: mpsc::Sender<ExecOutput>,
    ) -> Result<ExecResult>;
    /// # Errors
    /// Returns stop, unmount, or final flush errors.
    async fn pause(&self, sb: &Sandbox) -> Result<PauseHandle>;
    /// # Errors
    /// Returns the same errors as a cold create.
    async fn resume(&self, handle: PauseHandle) -> Result<Sandbox>;
    /// # Errors
    /// Returns stop, unmount, detach, or final flush errors.
    async fn destroy(&self, sb: Sandbox) -> Result<()>;
    fn capabilities(&self) -> RuntimeCaps;
}
