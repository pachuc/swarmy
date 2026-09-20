//! Shared inference contracts and subscription-backed `ChatGPT` inference.

pub mod auth;
pub mod catalog;
pub mod chatgpt;
pub mod fake;
pub mod responses;

use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
use swarmy_core::{Message, Part, RequestId, SessionId};

use auth::CredentialStore;
use catalog::{Api, ModelInfo, ProviderInfo};

/// Resolved credentials for a protocol client. Cloud credentials can add variants.
#[derive(Clone)]
#[non_exhaustive]
pub enum ClientAuth {
    None,
    ApiKey(String),
    Bearer(String),
    ChatGpt(Arc<dyn CredentialStore>),
    Headers(BTreeMap<String, String>),
}

/// Construct a client for the catalog's selected wire protocol.
///
/// # Errors
/// Returns `Unsupported` for protocols awaiting implementation, or a credential
/// or HTTP configuration error when constructing the `ChatGPT` client.
pub fn client_for(
    provider: &ProviderInfo,
    model: &ModelInfo,
    auth: ClientAuth,
) -> Result<Arc<dyn Provider>, Error> {
    let api = model.api.unwrap_or(provider.api);
    // Keep separate arms so protocol implementations can land independently.
    match api {
        Api::AnthropicMessages => Err(Error::Unsupported(Api::AnthropicMessages)),
        Api::OpenAiResponses => Err(Error::Unsupported(Api::OpenAiResponses)),
        Api::OpenAiCodexResponses => match auth {
            ClientAuth::ChatGpt(store) => Ok(Arc::new(chatgpt::ChatGptProvider::new(store)?)),
            _ => Err(Error::Credentials("ChatGPT requires a credential store")),
        },
        Api::OpenAiCompletions => Err(Error::Unsupported(Api::OpenAiCompletions)),
        Api::GoogleGenerativeAi => Err(Error::Unsupported(Api::GoogleGenerativeAi)),
        Api::GoogleVertex => Err(Error::Unsupported(Api::GoogleVertex)),
        Api::BedrockConverse => Err(Error::Unsupported(Api::BedrockConverse)),
        Api::Fake => Err(Error::Unsupported(Api::Fake)),
    }
}

/// Durable inference work, shared by step workers and gateways.
/// `request_id` must equal `RequestId::for_step(session_id, step)`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InferenceJob {
    pub session_id: SessionId,
    pub step: u64,
    pub request_id: RequestId,
    pub request: Request,
    #[serde(default)]
    pub provider: String,
}

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
    #[serde(with = "swarmy_core::json")]
    pub parameters: Value,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GenerationSettings {
    pub model: String,
    pub max_output_tokens: Option<u64>,
    pub temperature: Option<f64>,
    pub reasoning_effort: Option<ReasoningEffort>,
}

pub use swarmy_core::ReasoningEffort;

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
    #[error("unsupported provider API: {0:?}")]
    Unsupported(Api),
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

#[cfg(test)]
mod job_tests {
    use super::*;

    #[tokio::test]
    async fn catalog_dispatch_constructs_chatgpt_and_rejects_other_protocols() {
        let catalog = catalog::Catalog::get();
        let provider = catalog.provider("chatgpt").unwrap();
        let model = provider.models.values().next().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(auth::FileCredentialStore::new(
            directory.path().join("auth.json"),
        ));
        assert!(client_for(provider, model, ClientAuth::ChatGpt(store)).is_ok());
        assert!(matches!(
            client_for(provider, model, ClientAuth::None),
            Err(Error::Credentials(_))
        ));
        for provider in catalog
            .providers()
            .filter(|provider| provider.api != Api::OpenAiCodexResponses)
        {
            let model = provider.models.values().next().unwrap_or(model);
            assert!(matches!(
                client_for(provider, model, ClientAuth::None),
                Err(Error::Unsupported(api)) if api == model.api.unwrap_or(provider.api)
            ));
        }
        let router = catalog.provider("openrouter").unwrap();
        let claude = router
            .models
            .values()
            .find(|model| model.api == Some(Api::AnthropicMessages))
            .unwrap();
        assert!(matches!(
            client_for(router, claude, ClientAuth::None),
            Err(Error::Unsupported(Api::AnthropicMessages))
        ));
    }

    #[test]
    fn inference_jobs_with_tool_schemas_round_trip() {
        let session_id = SessionId::from_ulid(ulid::Ulid::generate());
        let job = InferenceJob {
            provider: "fake".into(),
            session_id,
            step: 7,
            request_id: RequestId::for_step(session_id, 7),
            request: Request {
                system_prompt: "test".into(),
                messages: Vec::new(),
                tools: vec![ToolDefinition {
                    name: "clock".into(),
                    description: "Read the time".into(),
                    parameters: serde_json::json!({"type": "object", "properties": {}}),
                }],
                settings: GenerationSettings::default(),
            },
        };
        let encoded = swarmy_core::encode(&job).unwrap();
        assert_eq!(swarmy_core::decode::<InferenceJob>(&encoded).unwrap(), job);
    }
}
