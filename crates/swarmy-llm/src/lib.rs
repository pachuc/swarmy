//! Shared inference contracts and subscription-backed `ChatGPT` inference.

pub mod auth;
pub mod chatgpt;
pub mod fake;
pub mod responses;

use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use swarmy_core::{Message, Part};

/// Provider-neutral input built from durable core messages.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Request {
    pub system_prompt: String,
    pub messages: Vec<Message>,
    pub tools: Vec<ToolDefinition>,
    pub settings: GenerationSettings,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GenerationSettings {
    pub model: String,
    pub max_output_tokens: Option<u64>,
    pub temperature: Option<f64>,
    pub reasoning_effort: Option<ReasoningEffort>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    None,
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_output_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    EndTurn,
    ToolCalls,
    MaxOutputTokens,
    ContentFilter,
    Incomplete(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response {
    pub parts: Vec<Part>,
    pub stop_reason: StopReason,
    pub usage: TokenUsage,
}

/// Output indices correlate concurrent text, reasoning, and function arguments.
/// `PartDone` replaces partial content at that index. `Completed` is authoritative.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Delta {
    Text {
        output_index: usize,
        text: String,
    },
    Reasoning {
        output_index: usize,
        text: String,
    },
    ToolArguments {
        output_index: usize,
        arguments: String,
    },
    PartDone {
        output_index: usize,
        part: Part,
    },
    Completed(Response),
}

/// A successful stream ends with exactly one `Completed`; a failed stream ends
/// with an error. Dropping the stream cancels the request.
pub type ProviderStream = BoxStream<'static, Result<Delta, Error>>;

/// Object-safe interface so the gateway can select a provider at runtime.
pub trait Provider: Send + Sync {
    fn request(&self, request: Request) -> ProviderStream;
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("credential I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("HTTP transport failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("HTTP request failed with status {0}")]
    Status(reqwest::StatusCode),
    #[error("invalid ChatGPT credentials: {0}")]
    Credentials(&'static str),
    #[error("credential account cannot change")]
    AccountChanged,
    #[error("invalid Responses stream: {0}")]
    Protocol(String),
    #[error("device login timed out after 15 minutes")]
    LoginTimeout,
    #[error("blocking credential operation failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("fake provider has no response for turn {0}")]
    UnscriptedTurn(usize),
}
