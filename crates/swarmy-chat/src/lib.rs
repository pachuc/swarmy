#[cfg(feature = "terminal")]
pub mod client_chat;
pub mod client_conversation;
#[cfg(feature = "terminal")]
mod input;
use swarmy_client::api_client;
use thiserror::Error;

/// Chat failures: API and transport errors stay typed so the terminal UI
/// can requeue transient append races; usage mistakes and turn outcomes
/// are plain messages for the operator.
#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Client(#[from] swarmy_client::Error),
    #[error("session is not idle")]
    NotIdle,
    #[error("terminal input closed")]
    InputClosed,
    #[error("worker did not pick up session within 30 seconds")]
    PickupTimeout(#[from] tokio::time::error::Elapsed),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Message(String),
}

pub type Result<T> = std::result::Result<T, Error>;
