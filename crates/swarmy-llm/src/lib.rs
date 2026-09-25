//! Shared inference contracts and provider wire clients.

pub mod api;
pub mod auth;
pub mod catalog;
pub mod chatgpt;
pub mod fake;
pub mod quota;
pub mod responses;
pub mod retry;
pub mod selection;

use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
use swarmy_core::{Message, Part, RequestId, SessionId};

use auth::CredentialStore;
use catalog::{Api, ModelInfo, ProviderInfo};

/// Refreshable cloud bearer credentials, supplied by the cloud auth adapter.
pub trait BearerSource: Send + Sync {
    fn token(&self) -> futures::future::BoxFuture<'_, Result<String, Error>>;
}

/// Resolved credentials for a protocol client. Cloud credentials can add variants.
#[derive(Clone)]
#[non_exhaustive]
pub enum ClientAuth {
    None,
    ApiKey(String),
    /// Provider metadata, such as Azure's `resource_name`, alongside an API key.
    ApiKeyWithExtra {
        key: String,
        extra: BTreeMap<String, String>,
    },
    Bearer(String),
    /// Provider metadata, such as Azure's `resource_name`, alongside a bearer token.
    BearerWithExtra {
        token: String,
        extra: BTreeMap<String, String>,
    },
    Vertex {
        project: String,
        location: String,
        source: Arc<dyn BearerSource>,
    },
    ChatGpt(Arc<dyn CredentialStore>),
    Headers(BTreeMap<String, String>),
    Ambient,
    Scripted(Arc<dyn Provider>),
}

/// Construct a client for the catalog's selected wire protocol.
///
/// # Errors
/// Returns `Unsupported` for protocols awaiting implementation, or a credential
/// or HTTP configuration error when constructing a client.
pub fn client_for(
    provider: &ProviderInfo,
    model: &ModelInfo,
    auth: ClientAuth,
) -> Result<Arc<dyn Provider>, Error> {
    let api = model.api.unwrap_or(provider.api);
    // Each protocol implementation owns its dispatch arm.
    match api {
        Api::AnthropicMessages => api::anthropic::client_for(provider, model, auth),
        Api::OpenAiResponses | Api::OpenAiCodexResponses => {
            let endpoint = api::responses::ResponsesEndpoint::from_catalog(provider, model, auth)?;
            Ok(Arc::new(api::responses::ResponsesProvider::new(
                endpoint,
                provider.id.clone(),
                model.clone(),
            )?))
        }
        Api::OpenAiCompletions => Ok(Arc::new(api::completions::CompletionsProvider::new(
            provider, model, auth,
        )?)),
        Api::GoogleGenerativeAi | Api::GoogleVertex => {
            api::gemini::client_for(provider, model, auth)
        }
        Api::BedrockConverse => Ok(Arc::new(api::bedrock::BedrockProvider::new(
            model.clone(),
            auth,
        )?)),
        Api::Fake => match auth {
            ClientAuth::Scripted(client) => Ok(client),
            _ => Err(Error::Unsupported(Api::Fake)),
        },
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

/// Small bus delivery for a request saved in the store under `request_id`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InferenceJobRef {
    pub session_id: SessionId,
    pub step: u64,
    pub request_id: RequestId,
    pub provider: String,
    pub selection: GenerationSettings,
}

impl From<&InferenceJob> for InferenceJobRef {
    fn from(job: &InferenceJob) -> Self {
        Self {
            session_id: job.session_id,
            step: job.step,
            request_id: job.request_id,
            provider: job.provider.clone(),
            selection: job.request.settings.clone(),
        }
    }
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

pub use swarmy_core::TokenUsage;

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
    /// Latest published remaining-quota headers, empty when the provider
    /// publishes nothing. The gateway records these on the auth entry.
    #[serde(default)]
    pub quota_remaining: BTreeMap<String, u64>,
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

    /// Attach session affinity without changing the durable request format.
    fn request_for_session(&self, request: Request, _session_id: SessionId) -> ProviderStream {
        self.request(request)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unknown catalog model: {provider}/{model}")]
    UnknownModel { provider: String, model: String },
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
    #[error("provider returned {status}: {message}")]
    ProviderResponse {
        status: reqwest::StatusCode,
        message: String,
        retry_after: Option<std::time::Duration>,
    },
    #[error("HTTP request failed with retryable status {status}")]
    Retryable {
        status: reqwest::StatusCode,
        retry_after: Option<std::time::Duration>,
    },
    #[error("context overflow: {0}")]
    ContextOverflow(String),
    #[error("invalid provider credentials: {0}")]
    Credentials(&'static str),
    #[error("credential account cannot change")]
    AccountChanged,
    #[error("{0} needs login or an API key; Azure login/refresh requires az on this host")]
    NeedsLogin(String),
    #[error("invalid provider protocol: {0}")]
    Protocol(String),
    #[error("login timed out after 15 minutes")]
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
    async fn catalog_dispatch_requires_credentials_and_rejects_unimplemented_protocols() {
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
        for provider in catalog.providers() {
            let model = provider.models.values().next().unwrap_or(model);
            // Implemented protocols reject missing credentials before building a client.
            if matches!(
                model.api.unwrap_or(provider.api),
                Api::AnthropicMessages
                    | Api::BedrockConverse
                    | Api::OpenAiCompletions
                    | Api::OpenAiResponses
                    | Api::OpenAiCodexResponses
                    | Api::GoogleGenerativeAi
                    | Api::GoogleVertex
            ) {
                assert!(matches!(
                    client_for(provider, model, ClientAuth::None),
                    Err(Error::Credentials(_))
                ));
                continue;
            }
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
            Err(Error::Credentials(_))
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
        let reference = InferenceJobRef::from(&job);
        let encoded = swarmy_core::encode(&reference).unwrap();
        assert_eq!(
            swarmy_core::decode::<InferenceJobRef>(&encoded).unwrap(),
            reference
        );
    }
}
