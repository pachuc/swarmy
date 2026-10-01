//! The inference gateway engine: claims inference work from the bus,
//! resolves provider credentials, streams provider responses, and commits
//! terminal events to the store. `main.rs` only assembles and serves it.
mod attempt;
mod commit;
pub mod config;
pub mod credentials;
pub mod dispatch;
pub mod providers;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Settings(#[from] swarmy_config::Error),
    #[error(transparent)]
    Store(#[from] swarmy_store::StoreError),
    #[error(transparent)]
    Provider(#[from] swarmy_llm::Error),
    #[error(transparent)]
    Bus(#[from] swarmy_bus::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Encode(#[from] swarmy_core::EncodingError),
    #[error(transparent)]
    Blob(#[from] swarmy_store::blob::BlobError),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Semaphore(#[from] tokio::sync::AcquireError),
    #[error(transparent)]
    Jiff(#[from] jiff::Error),
    #[error("{0}")]
    Configuration(&'static str),
    /// An internal invariant broke: the work stream ended, a stored
    /// selection diverged from its delivery, or a claim was replaced.
    /// These carry static messages because they name gateway states,
    /// not failures a caller can match on.
    #[error("{0}")]
    Internal(&'static str),
}

pub type Result<T> = std::result::Result<T, Error>;
