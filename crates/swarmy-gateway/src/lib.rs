//! Provider discovery shared by the gateway and local diagnostics.
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
    #[error("{0}")]
    Configuration(&'static str),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Boot the `FoundationDB` network once per test process. The engine and
/// credential test modules share this instead of keeping separate guards:
/// the client library panics if the API version is selected twice.
#[cfg(test)]
pub(crate) fn test_network() {
    static NETWORK: std::sync::OnceLock<foundationdb::api::NetworkAutoStop> =
        std::sync::OnceLock::new();
    NETWORK.get_or_init(swarmy_store::boot);
}
