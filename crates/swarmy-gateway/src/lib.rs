//! Provider discovery shared by the gateway and local diagnostics.
pub mod config;
pub mod cost;
pub mod credentials;
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
    #[error("{0}")]
    Configuration(&'static str),
}

pub type Result<T> = std::result::Result<T, Error>;
