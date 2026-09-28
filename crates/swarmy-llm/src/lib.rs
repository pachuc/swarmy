//! Shared inference contracts and provider wire clients.

pub mod api;
pub mod auth;
pub mod catalog;
pub mod chatgpt;
pub mod cost;
pub(crate) mod error;
pub mod fake;
pub(crate) mod protocol;
pub mod quota;
pub mod reasoning;
pub mod responses;
pub mod retry;
pub mod selection;
pub mod sse;

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

/// Azure endpoint metadata from a credential record.
#[derive(Clone, Debug, Default)]
pub struct AzureAuth {
    pub resource_name: Option<String>,
    pub base_url: Option<String>,
}

/// Bedrock token and region overrides from a credential record.
#[derive(Clone, Debug, Default)]
pub struct BedrockAuth {
    pub region: Option<String>,
    pub bearer_token: Option<String>,
}

/// Provider-specific auth metadata; wire clients never inspect string keys.
#[derive(Clone, Debug)]
pub enum ProviderAuthExtra {
    Azure(AzureAuth),
    Bedrock(BedrockAuth),
}

impl ProviderAuthExtra {
    fn from_record(provider: &str, extra: &BTreeMap<String, String>) -> Option<Self> {
        match provider {
            "azure" => Some(Self::Azure(AzureAuth {
                resource_name: extra.get("resource_name").cloned(),
                base_url: extra.get("base_url").cloned(),
            })),
            "amazon-bedrock" => Some(Self::Bedrock(BedrockAuth {
                region: extra.get("region").cloned(),
                bearer_token: extra.get("bearer_token").cloned(),
            })),
            _ => None,
        }
    }
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
        extra: ProviderAuthExtra,
    },
    Bearer(String),
    /// Provider metadata, such as Azure's `resource_name`, alongside a bearer token.
    BearerWithExtra {
        token: String,
        extra: ProviderAuthExtra,
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
/// Returns `Unsupported` for protocols awaiting implementation,
/// `NotCompiledIn` when the build disabled the matching provider feature, or
/// a credential or HTTP configuration error when constructing a client.
pub fn client_for(
    provider: &ProviderInfo,
    model: &ModelInfo,
    auth: ClientAuth,
) -> Result<Arc<dyn Provider>, Error> {
    let api = model.api.unwrap_or(provider.api);
    // Each protocol implementation owns its dispatch arm. Arms behind Cargo
    // features stay exhaustive when the feature is off and refuse with a
    // clear error instead of failing to compile.
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
        #[cfg(feature = "gemini")]
        Api::GoogleGenerativeAi | Api::GoogleVertex => {
            api::gemini::client_for(provider, model, auth)
        }
        #[cfg(not(feature = "gemini"))]
        Api::GoogleGenerativeAi | Api::GoogleVertex => {
            Err(Error::NotCompiledIn(format!("{api:?}")))
        }
        #[cfg(feature = "bedrock")]
        Api::BedrockConverse => Ok(Arc::new(api::bedrock::BedrockProvider::new(
            model.clone(),
            auth,
        )?)),
        #[cfg(not(feature = "bedrock"))]
        Api::BedrockConverse => Err(Error::NotCompiledIn(format!("{api:?}"))),
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
    /// Pinned auth entry selected by the session's route, if any.
    #[serde(default)]
    pub entry: Option<String>,
    /// Named route that selected the entry, if any.
    #[serde(default)]
    pub route: Option<String>,
    /// Index into the resolved route for this attempt.
    #[serde(default)]
    pub route_step: u32,
    /// Compaction job marker, independent of its system prompt.
    #[serde(default, with = "swarmy_core::trailing")]
    pub summary: bool,
    /// The second checkpoint of a split turn uses Pi's prefix prompt.
    #[serde(default, with = "swarmy_core::trailing")]
    pub summary_prefix: bool,
    /// Cut chosen before the checkpoint request, reused for the archive.
    #[serde(default, with = "swarmy_core::trailing")]
    pub summary_cut: Option<u64>,
    /// Whether this checkpoint is recovering a failed assistant attempt.
    #[serde(default, with = "swarmy_core::trailing")]
    pub summary_recovery: bool,
}

/// Small bus delivery for a request saved in the store under `request_id`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InferenceJobRef {
    pub session_id: SessionId,
    pub step: u64,
    pub request_id: RequestId,
    pub provider: String,
    pub selection: GenerationSettings,
    #[serde(default)]
    pub entry: Option<String>,
    #[serde(default)]
    pub route: Option<String>,
    #[serde(default)]
    pub route_step: u32,
    #[serde(default, with = "swarmy_core::trailing")]
    pub summary: bool,
    #[serde(default, with = "swarmy_core::trailing")]
    pub summary_prefix: bool,
}

impl From<&InferenceJob> for InferenceJobRef {
    fn from(job: &InferenceJob) -> Self {
        Self {
            session_id: job.session_id,
            step: job.step,
            request_id: job.request_id,
            provider: job.provider.clone(),
            selection: job.request.settings.clone(),
            entry: job.entry.clone(),
            route: job.route.clone(),
            route_step: job.route_step,
            summary: job.summary,
            summary_prefix: job.summary_prefix,
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
    /// Disable provider prompt-cache writes for one-off summarization.
    #[serde(skip)]
    pub no_cache: bool,
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
    /// Requests and tokens are separate dimensions; callers must not combine
    /// them. Resets carry seconds until each window resets.
    #[serde(default)]
    pub quota_remaining: BTreeMap<String, u64>,
    #[serde(default)]
    pub quota_resets: BTreeMap<String, u64>,
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

/// Provider response classification, independent of diagnostic wording.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ProviderFailureReason {
    #[default]
    Other,
    Quota,
}

/// Classify a provider payload at its HTTP boundary, before it becomes diagnostic text.
#[must_use]
pub fn classify_provider_failure(body: &str) -> ProviderFailureReason {
    let text = body.to_ascii_lowercase();
    if ["usage_limit", "usage limit", "rate_limit", "rate limit"]
        .iter()
        .any(|marker| text.contains(marker))
    {
        ProviderFailureReason::Quota
    } else {
        ProviderFailureReason::Other
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unknown catalog model: {provider}/{model}")]
    UnknownModel { provider: String, model: String },
    #[error("unsupported provider API: {0:?}")]
    Unsupported(Api),
    #[error(
        "{0} support was not compiled into this build; rebuild with the matching swarmy-llm feature"
    )]
    NotCompiledIn(String),
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
        reason: ProviderFailureReason,
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
            let api = model.api.unwrap_or(provider.api);
            // Builds without a cloud feature refuse that provider with
            // NotCompiledIn instead of reaching the credential check.
            #[cfg(not(feature = "bedrock"))]
            if matches!(api, Api::BedrockConverse) {
                assert!(matches!(
                    client_for(provider, model, ClientAuth::None),
                    Err(Error::NotCompiledIn(_))
                ));
                continue;
            }
            #[cfg(not(feature = "gemini"))]
            if matches!(api, Api::GoogleGenerativeAi | Api::GoogleVertex) {
                assert!(matches!(
                    client_for(provider, model, ClientAuth::None),
                    Err(Error::NotCompiledIn(_))
                ));
                continue;
            }
            #[cfg(not(feature = "azure"))]
            if provider.id == "azure" {
                assert!(matches!(
                    client_for(provider, model, ClientAuth::None),
                    Err(Error::NotCompiledIn(_))
                ));
                continue;
            }
            // Implemented protocols reject missing credentials before building a client.
            if matches!(
                api,
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

    // Each gated provider reports NotCompiledIn when its feature is off, so a
    // slim build fails with a clear error instead of a compile failure.
    #[test]
    #[cfg(not(feature = "bedrock"))]
    fn bedrock_without_feature_reports_not_compiled_in() {
        let catalog = catalog::Catalog::get();
        let provider = catalog.provider("amazon-bedrock").unwrap();
        let model = provider.models.values().next().unwrap();
        assert!(matches!(
            client_for(provider, model, ClientAuth::None),
            Err(Error::NotCompiledIn(_))
        ));
    }

    #[test]
    #[cfg(not(feature = "gemini"))]
    fn gemini_without_feature_reports_not_compiled_in() {
        let catalog = catalog::Catalog::get();
        let provider = catalog.provider("google").unwrap();
        let model = provider.models.values().next().unwrap();
        assert!(matches!(
            client_for(provider, model, ClientAuth::None),
            Err(Error::NotCompiledIn(_))
        ));
    }

    #[test]
    #[cfg(not(feature = "azure"))]
    fn azure_without_feature_reports_not_compiled_in() {
        let catalog = catalog::Catalog::get();
        let provider = catalog.provider("azure").unwrap();
        let model = provider.models.values().next().unwrap();
        assert!(matches!(
            client_for(provider, model, ClientAuth::None),
            Err(Error::NotCompiledIn(_))
        ));
    }

    #[test]
    fn masters_inference_job_layout_still_decodes() {
        // The request was the fourth field on master. A new field inside it
        // would consume the provider byte and misalign every following field.
        #[derive(Serialize)]
        struct OldRequest {
            system_prompt: String,
            messages: Vec<Message>,
            tools: Vec<ToolDefinition>,
            settings: GenerationSettings,
        }
        #[derive(Serialize)]
        struct OldJob {
            session_id: SessionId,
            step: u64,
            request_id: RequestId,
            request: OldRequest,
            provider: String,
            entry: Option<String>,
            route: Option<String>,
            route_step: u32,
        }
        // Pinned bytes produced by the master layout, not by InferenceJob's
        // current serializer. In particular no cache-policy byte may appear
        // between the request and provider fields.
        const MASTER_BYTES: &[u8] = &[
            1, 26, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
            48, 48, 48, 48, 48, 48, 2, 54, 2, 190, 81, 241, 161, 179, 57, 6, 120, 230, 5, 126, 218,
            209, 215, 135, 22, 114, 63, 152, 144, 227, 33, 190, 107, 177, 117, 253, 99, 66, 171, 3,
            111, 108, 100, 0, 0, 0, 0, 0, 0, 4, 102, 97, 107, 101, 0, 0, 0,
        ];
        let id = SessionId::from_ulid(ulid::Ulid::nil());
        let bytes = swarmy_core::encode(&OldJob {
            session_id: id,
            step: 2,
            request_id: RequestId::for_step(id, 2),
            request: OldRequest {
                system_prompt: "old".into(),
                messages: Vec::new(),
                tools: Vec::new(),
                settings: GenerationSettings::default(),
            },
            provider: "fake".into(),
            entry: None,
            route: None,
            route_step: 0,
        })
        .unwrap();
        assert_eq!(bytes, MASTER_BYTES);
        let job: InferenceJob = swarmy_core::decode(MASTER_BYTES).unwrap();
        assert_eq!(job.request.system_prompt, "old");
        assert_eq!(job.provider, "fake");
        assert!(!job.summary);
        assert!(!job.summary_prefix);
        assert!(!job.request.no_cache);
    }

    #[test]
    fn round_two_job_without_cut_still_decodes() {
        #[derive(Serialize)]
        struct RoundTwoJob {
            session_id: SessionId,
            step: u64,
            request_id: RequestId,
            request: Request,
            provider: String,
            entry: Option<String>,
            route: Option<String>,
            route_step: u32,
            #[serde(with = "swarmy_core::trailing")]
            summary: bool,
            #[serde(with = "swarmy_core::trailing")]
            summary_prefix: bool,
        }
        let session_id = SessionId::from_ulid(ulid::Ulid::nil());
        let bytes = swarmy_core::encode(&RoundTwoJob {
            session_id,
            step: 3,
            request_id: RequestId::for_step(session_id, 3),
            request: Request {
                system_prompt: "checkpoint".into(),
                messages: Vec::new(),
                tools: Vec::new(),
                settings: GenerationSettings::default(),
                no_cache: true,
            },
            provider: "fake".into(),
            entry: None,
            route: None,
            route_step: 0,
            summary: true,
            summary_prefix: false,
        })
        .unwrap();
        let job: InferenceJob = swarmy_core::decode(&bytes).unwrap();
        assert!(job.summary);
        assert!(!job.summary_prefix);
        assert_eq!(job.summary_cut, None);
        assert!(!job.summary_recovery);
    }

    #[test]
    fn inference_jobs_with_tool_schemas_round_trip() {
        let session_id = SessionId::from_ulid(ulid::Ulid::generate());
        let job = InferenceJob {
            summary: false,
            summary_prefix: false,
            summary_cut: None,
            summary_recovery: false,
            provider: "fake".into(),
            entry: Some("primary".into()),
            route: Some("fallback".into()),
            route_step: 1,
            session_id,
            step: 7,
            request_id: RequestId::for_step(session_id, 7),
            request: Request {
                no_cache: false,
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

#[cfg(test)]
mod provider_failure_reason_tests {
    use super::*;

    #[test]
    fn classifies_quota_payload_at_response_boundary() {
        for payload in [
            r#"{"error":{"code":"usage_limit_reached"}}"#,
            r#"{"error":"rate_limit_exceeded"}"#,
            "usage limit reached",
            "rate limit exceeded",
        ] {
            assert_eq!(
                classify_provider_failure(payload),
                ProviderFailureReason::Quota
            );
        }
        assert_eq!(
            classify_provider_failure(r#"{"error":"invalid token"}"#),
            ProviderFailureReason::Other
        );
    }
}
