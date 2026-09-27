#[cfg(feature = "terminal")]
pub mod client_chat;
pub mod client_conversation;
#[cfg(feature = "terminal")]
mod input;
use swarmy_client::api_client;
