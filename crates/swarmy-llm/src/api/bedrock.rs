//! Bedrock Converse mapping. The AWS SDK owns signing and event-stream decoding.

use std::{collections::BTreeMap, sync::Arc};

use aws_sdk_bedrockruntime::{
    Client,
    config::{BehaviorVersion, Region, Token, retry::RetryConfig},
    error::ProvideErrorMetadata,
    operation::converse_stream::ConverseStreamInput,
    types::{self as sdk, ContentBlock, ConversationRole, SystemContentBlock},
};
use aws_smithy_types::{Blob, Document, Number};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use swarmy_core::{MessageRole, Part, ToolCallId, ToolResult};
use tokio::sync::OnceCell;

use crate::{
    ClientAuth, Delta, Error, Provider, ProviderStream, ReasoningEffort, Request, Response,
    StopReason, TokenUsage,
    catalog::ModelInfo,
    retry::{RetryPolicy, with_retry},
};

/// A model-scoped client with lazily loaded AWS credentials and configuration.
#[derive(Clone)]
pub struct BedrockProvider {
    model: ModelInfo,
    region: String,
    bearer_token: Option<String>,
    client: Arc<OnceCell<Client>>,
}

impl BedrockProvider {
    /// `Ambient` uses the AWS default credential chain. `Bearer` supplies an
    /// explicit token, and `ApiKeyWithExtra` carries a bearer token as the key
    /// alongside the credential record's `extra` map, such as `region`.
    ///
    /// # Errors
    /// Returns an error for an incompatible credential kind.
    pub fn new(model: ModelInfo, auth: ClientAuth) -> Result<Self, Error> {
        let (token, extra) = match auth {
            ClientAuth::Ambient => (None, BTreeMap::new()),
            ClientAuth::Bearer(token) => (Some(token), BTreeMap::new()),
            ClientAuth::ApiKeyWithExtra { key, extra } => (Some(key), extra),
            _ => {
                return Err(Error::Credentials(
                    "Bedrock requires AWS credentials or a bearer token",
                ));
            }
        };
        let region = resolve_region(
            &model.id,
            extra.get("region").map(String::as_str),
            std::env::var("AWS_REGION").ok().as_deref(),
        );
        let bearer_token = token
            .or_else(|| extra.get("bearer_token").cloned())
            .or_else(|| std::env::var("AWS_BEARER_TOKEN_BEDROCK").ok());
        Ok(Self {
            model,
            region,
            bearer_token,
            client: Arc::new(OnceCell::new()),
        })
    }

    async fn client(&self) -> &Client {
        self.client
            .get_or_init(|| async {
                let mut loader = aws_config::defaults(BehaviorVersion::latest())
                    .region(Region::new(self.region.clone()));
                // Bearer auth must not probe instance metadata or require signing credentials.
                if self.bearer_token.is_some() {
                    loader = loader.no_credentials();
                }
                let shared = loader.load().await;
                let mut config = aws_sdk_bedrockruntime::config::Builder::from(&shared)
                    // The shared retry helper owns the attempt budget.
                    .retry_config(RetryConfig::disabled());
                if let Some(token) = &self.bearer_token {
                    config = config
                        .token_provider(Token::new(token.clone(), None))
                        .auth_scheme_preference(["smithy.api#httpBearerAuth".into()]);
                }
                Client::from_conf(config.build())
            })
            .await
    }
}

impl Provider for BedrockProvider {
    fn request(&self, request: Request) -> ProviderStream {
        let provider = self.clone();
        Box::pin(async_stream::try_stream! {
            let input = request_input(&request, &provider.model)?;
            let client = provider.client().await;
            let (mut output, mut mapper, initial) = with_retry(&RetryPolicy::default(), || async {
                let mut output = client.converse_stream()
                    .set_model_id(input.model_id.clone())
                    .set_system(input.system.clone())
                    .set_messages(input.messages.clone())
                    .set_inference_config(input.inference_config.clone())
                    .set_tool_config(input.tool_config.clone())
                    .set_additional_model_request_fields(input.additional_model_request_fields.clone())
                    .customize()
                    .mutate_request(|request| {
                        request.headers_mut().insert("user-agent", concat!("swarmy/", env!("CARGO_PKG_VERSION")));
                    })
                    .send().await.map_err(|error| request_error(error.as_service_error()))?;
                let mut mapper = StreamMapper::new(provider.model.id.clone());
                // Event-stream exceptions can arrive after the HTTP response. Retry
                // only while no deltas have been exposed to the caller.
                let mut initial = Vec::new();
                while let Some(event) = output.stream.recv().await
                    .map_err(|error| stream_error(error.as_service_error()))? {
                    initial = mapper.event(event)?;
                    if !initial.is_empty() { break; }
                }
                Ok((output, mapper, initial))
            }).await?;
            for delta in initial { yield delta; }
            while let Some(event) = output.stream.recv().await
                .map_err(|error| stream_error(error.as_service_error()))? {
                for delta in mapper.event(event)? {
                    yield delta;
                }
            }
            yield mapper.finish()?;
        })
    }
}

fn resolve_region(model: &str, extra: Option<&str>, environment: Option<&str>) -> String {
    let fields: Vec<_> = model.splitn(6, ':').collect();
    let arn_region = (fields.len() == 6 && fields[0] == "arn" && fields[2] == "bedrock")
        .then(|| fields[3])
        .filter(|region| !region.is_empty());
    arn_region
        .or(extra)
        .or(environment)
        .unwrap_or("us-east-1")
        .into()
}

// Event-stream exception payloads need not include an error code in metadata.
// Classify modeled variants directly and retain their message fields.
fn request_error(
    error: Option<&aws_sdk_bedrockruntime::operation::converse_stream::ConverseStreamError>,
) -> Error {
    use aws_sdk_bedrockruntime::operation::converse_stream::ConverseStreamError as E;
    let Some(error) = error else {
        return Error::ProviderResponse {
            status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
            message: "Bedrock request transport failed".into(),
            retry_after: None,
        };
    };
    let (code, message) = match error {
        E::ThrottlingException(error) => ("ThrottlingException", error.message()),
        E::ServiceUnavailableException(error) => ("ServiceUnavailableException", error.message()),
        E::ModelNotReadyException(error) => ("ModelNotReadyException", error.message()),
        E::ValidationException(error) => ("ValidationException", error.message()),
        E::InternalServerException(error) => ("InternalServerException", error.message()),
        E::ModelTimeoutException(error) => ("ModelTimeoutException", error.message()),
        _ => (error.code().unwrap_or("error"), error.message()),
    };
    service_error(code, message)
}

fn stream_error(error: Option<&sdk::error::ConverseStreamOutputError>) -> Error {
    use sdk::error::ConverseStreamOutputError as E;
    let Some(error) = error else {
        return Error::ProviderResponse {
            status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
            message: "Bedrock event stream transport failed".into(),
            retry_after: None,
        };
    };
    let (code, message) = match error {
        E::ThrottlingException(error) => ("ThrottlingException", error.message()),
        E::ServiceUnavailableException(error) => ("ServiceUnavailableException", error.message()),
        E::ValidationException(error) => ("ValidationException", error.message()),
        E::InternalServerException(error) => ("InternalServerException", error.message()),
        _ => (error.code().unwrap_or("error"), error.message()),
    };
    service_error(code, message)
}

fn service_error(code: &str, message: Option<&str>) -> Error {
    match code {
        "ValidationException"
            if message.is_some_and(|message| message.contains("Input is too long")) =>
        {
            Error::ContextOverflow(message.unwrap_or_default().into())
        }
        "ThrottlingException" | "ModelNotReadyException" => Error::ProviderResponse {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            message: format!("Bedrock {code}: {}", message.unwrap_or("service error")),
            retry_after: None,
        },
        "ServiceUnavailableException" | "InternalServerException" | "ModelTimeoutException" => {
            Error::ProviderResponse {
                status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
                message: format!("Bedrock {code}: {}", message.unwrap_or("service error")),
                retry_after: None,
            }
        }
        _ if code.contains("Expired")
            || message.is_some_and(|m| m.to_ascii_lowercase().contains("expired")) =>
        {
            Error::Credentials(
                "Bedrock console API keys expire after twelve hours and are for development only; use an IAM identity for long-lived use",
            )
        }
        _ => Error::Protocol(format!(
            "Bedrock {code}: {}",
            message.unwrap_or("service error")
        )),
    }
}

fn build_error(error: impl std::fmt::Display) -> Error {
    Error::Protocol(format!("invalid Bedrock request: {error}"))
}

fn document(value: &Value) -> Document {
    match value {
        Value::Null => Document::Null,
        Value::Bool(value) => Document::Bool(*value),
        Value::String(value) => Document::String(value.clone()),
        Value::Number(value) => Document::Number(if let Some(value) = value.as_u64() {
            Number::PosInt(value)
        } else if let Some(value) = value.as_i64() {
            Number::NegInt(value)
        } else {
            Number::Float(value.as_f64().expect("JSON numbers are finite"))
        }),
        Value::Array(values) => Document::Array(values.iter().map(document).collect()),
        Value::Object(values) => Document::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), document(value)))
                .collect(),
        ),
    }
}

fn model_names(model: &ModelInfo) -> impl Iterator<Item = String> {
    [&model.id, &model.name]
        .into_iter()
        .map(|name| name.to_ascii_lowercase().replace([' ', '.', '_', ':'], "-"))
}

fn is_claude(model: &ModelInfo) -> bool {
    model_names(model).any(|name| name.contains("claude"))
}

fn supports_cache(model: &ModelInfo) -> bool {
    is_claude(model)
        && model_names(model).any(|name| {
            name.contains("claude-3-7")
                || [
                    "opus-4", "sonnet-4", "haiku-4", "opus-5", "sonnet-5", "haiku-5", "fable-5",
                ]
                .iter()
                .any(|family| name.contains(family))
        })
}

fn adaptive(model: &ModelInfo) -> bool {
    is_claude(model)
        && model_names(model).any(|name| {
            ["opus", "sonnet", "fable"].iter().any(|family| {
                if name.contains(&format!("{family}-5")) {
                    return true;
                }
                *family != "fable"
                    && name
                        .split_once(&format!("{family}-4-"))
                        .is_some_and(|(_, minor)| {
                            minor
                                .split('-')
                                .next()
                                .and_then(|minor| minor.parse::<u32>().ok())
                                .is_some_and(|minor| minor >= 6)
                        })
            })
        })
}

fn inference(
    request: &Request,
    model: &ModelInfo,
) -> Result<(sdk::InferenceConfiguration, Option<Document>), Error> {
    let limit = model.limit.output.unwrap_or(u64::MAX);
    let mut maximum = request
        .settings
        .max_output_tokens
        .or(model.limit.output)
        .ok_or_else(|| Error::Protocol("Bedrock needs an output token limit".into()))?
        .min(limit);
    let mut temperature = request
        .settings
        .temperature
        .filter(|_| model.compat.supports_temperature() != Some(false));
    let mut fields = None;
    let effort = model
        .clamp_effort(
            request
                .settings
                .reasoning_effort
                .unwrap_or(ReasoningEffort::None),
        )
        .0;
    if is_claude(model) && effort != ReasoningEffort::None {
        temperature = None;
        fields = Some(if adaptive(model) {
            let effort = match effort {
                ReasoningEffort::None | ReasoningEffort::Minimal | ReasoningEffort::Low => "low",
                ReasoningEffort::Medium => "medium",
                ReasoningEffort::High => "high",
                ReasoningEffort::Xhigh => "xhigh",
                ReasoningEffort::Max => "max",
            };
            document(
                &json!({"thinking": {"type": "adaptive", "display": "summarized"}, "output_config": {"effort": effort}}),
            )
        } else {
            let budget = match effort {
                ReasoningEffort::Minimal => 1024,
                ReasoningEffort::Low => 2048,
                ReasoningEffort::Medium => 8192,
                _ => 16384,
            };
            maximum = maximum.saturating_add(budget).min(limit);
            if maximum <= budget {
                return Err(Error::Protocol(
                    "output limit must exceed the thinking budget".into(),
                ));
            }
            document(
                &json!({"thinking": {"type": "enabled", "budget_tokens": budget}, "anthropic_beta": ["interleaved-thinking-2025-05-14"]}),
            )
        });
    }
    if maximum == 0 {
        return Err(Error::Protocol(
            "output token limit must be positive".into(),
        ));
    }
    let maximum = i32::try_from(maximum).map_err(build_error)?;
    let temperature = temperature
        .map(|value| {
            if !value.is_finite() || !(0.0..=1.0).contains(&value) {
                return Err(Error::Protocol(
                    "Bedrock temperature must be between 0 and 1".into(),
                ));
            }
            // Bedrock uses f32 for sampling parameters; the value is bounded above.
            #[allow(clippy::cast_possible_truncation)]
            Ok(value as f32)
        })
        .transpose()?;
    Ok((
        sdk::InferenceConfiguration::builder()
            .max_tokens(maximum)
            .set_temperature(temperature)
            .build(),
        fields,
    ))
}

fn request_input(request: &Request, model: &ModelInfo) -> Result<ConverseStreamInput, Error> {
    if request.settings.model != model.id {
        return Err(Error::Protocol(
            "Bedrock request model differs from the configured model".into(),
        ));
    }
    let mut system = Vec::new();
    if !request.system_prompt.trim().is_empty() {
        system.push(SystemContentBlock::Text(request.system_prompt.clone()));
        if supports_cache(model) {
            system.push(SystemContentBlock::CachePoint(
                sdk::CachePointBlock::builder()
                    .r#type(sdk::CachePointType::Default)
                    .build()
                    .map_err(build_error)?,
            ));
        }
    }
    let (inference, fields) = inference(request, model)?;
    let tools = request
        .tools
        .iter()
        .map(|tool| {
            Ok(sdk::Tool::ToolSpec(
                sdk::ToolSpecification::builder()
                    .name(&tool.name)
                    .description(&tool.description)
                    .input_schema(sdk::ToolInputSchema::Json(document(&tool.parameters)))
                    .build()
                    .map_err(build_error)?,
            ))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let tools = if tools.is_empty() {
        None
    } else {
        Some(
            sdk::ToolConfiguration::builder()
                .set_tools(Some(tools))
                .build()
                .map_err(build_error)?,
        )
    };
    ConverseStreamInput::builder()
        .model_id(&model.id)
        .set_system((!system.is_empty()).then_some(system))
        .set_messages(Some(messages(request, model)?))
        .inference_config(inference)
        .set_tool_config(tools)
        .set_additional_model_request_fields(fields)
        .build()
        .map_err(build_error)
}

fn tool_id(id: &str) -> String {
    if !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        id.into()
    } else {
        // Hashing avoids collisions between foreign IDs that sanitize identically.
        blake3::hash(id.as_bytes()).to_hex().to_string()
    }
}

fn result_block(id: &str, text: String, error: bool) -> Result<ContentBlock, Error> {
    Ok(ContentBlock::ToolResult(
        sdk::ToolResultBlock::builder()
            .tool_use_id(tool_id(id))
            .content(sdk::ToolResultContentBlock::Text(
                if text.trim().is_empty() {
                    "(empty result)".into()
                } else {
                    text
                },
            ))
            .status(if error {
                sdk::ToolResultStatus::Error
            } else {
                sdk::ToolResultStatus::Success
            })
            .build()
            .map_err(build_error)?,
    ))
}

fn reasoning(
    text: &str,
    metadata: &BTreeMap<String, Value>,
    model: &ModelInfo,
) -> Result<Option<ContentBlock>, Error> {
    if let Some(data) = metadata
        .get("bedrock")
        .filter(|data| data["model"].as_str() == Some(&model.id))
    {
        if let Some(redacted) = data["redacted_content"].as_str() {
            let bytes = STANDARD.decode(redacted).map_err(build_error)?;
            return Ok(Some(ContentBlock::ReasoningContent(
                sdk::ReasoningContentBlock::RedactedContent(Blob::new(bytes)),
            )));
        }
        if let Some(signature) = data["signature"]
            .as_str()
            .filter(|signature| !signature.is_empty())
        {
            return Ok(Some(ContentBlock::ReasoningContent(
                sdk::ReasoningContentBlock::ReasoningText(
                    sdk::ReasoningTextBlock::builder()
                        .text(text)
                        .signature(signature)
                        .build()
                        .map_err(build_error)?,
                ),
            )));
        }
    }
    Ok((!text.trim().is_empty()).then(|| ContentBlock::Text(text.into())))
}

fn part_block(part: &Part, model: &ModelInfo) -> Result<Option<ContentBlock>, Error> {
    Ok(match part {
        Part::Image {
            media_type, bytes, ..
        } => {
            let format = match media_type.as_str() {
                "image/png" => sdk::ImageFormat::Png,
                "image/jpeg" => sdk::ImageFormat::Jpeg,
                "image/gif" => sdk::ImageFormat::Gif,
                "image/webp" => sdk::ImageFormat::Webp,
                _ => {
                    return Err(Error::Protocol(format!(
                        "unsupported image media type: {media_type}"
                    )));
                }
            };
            Some(ContentBlock::Image(
                sdk::ImageBlock::builder()
                    .format(format)
                    .source(sdk::ImageSource::Bytes(Blob::new(bytes.clone())))
                    .build()
                    .map_err(build_error)?,
            ))
        }
        Part::Text { text } => (!text.trim().is_empty()).then(|| ContentBlock::Text(text.clone())),
        Part::Reasoning { text, metadata } => reasoning(text, metadata, model)?,
        Part::ToolCall {
            call_id,
            tool,
            input,
        } => Some(ContentBlock::ToolUse(
            sdk::ToolUseBlock::builder()
                .tool_use_id(tool_id(&call_id.0))
                .name(tool)
                .input(document(input))
                .build()
                .map_err(build_error)?,
        )),
        Part::ToolResult { call_id, result } => Some(match result {
            ToolResult::Completed { output, .. } => {
                result_block(&call_id.0, output.clone(), false)?
            }
            ToolResult::Error { error } => result_block(&call_id.0, error.clone(), true)?,
        }),
    })
}

fn messages(request: &Request, model: &ModelInfo) -> Result<Vec<sdk::Message>, Error> {
    let mut turns: Vec<(ConversationRole, Vec<ContentBlock>)> = Vec::new();
    for message in &request.messages {
        let role = if message.role == MessageRole::Assistant {
            ConversationRole::Assistant
        } else {
            ConversationRole::User
        };
        let content = message
            .parts
            .iter()
            .map(|part| part_block(part, model))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        if content.is_empty() {
            continue;
        }
        if let Some((_, blocks)) = turns.last_mut().filter(|(previous, _)| *previous == role) {
            blocks.extend(content);
        } else {
            turns.push((role, content));
        }
    }
    // A tool result must immediately follow the assistant turn that requested it.
    let mut index = 0;
    while index < turns.len() {
        let calls: Vec<_> = turns[index]
            .1
            .iter()
            .filter_map(|block| {
                block
                    .as_tool_use()
                    .ok()
                    .map(|call| call.tool_use_id.clone())
            })
            .collect();
        if !calls.is_empty() {
            if turns
                .get(index + 1)
                .is_none_or(|(role, _)| *role != ConversationRole::User)
            {
                turns.insert(index + 1, (ConversationRole::User, Vec::new()));
            }
            let blocks = &mut turns[index + 1].1;
            for id in calls {
                if !blocks.iter().any(|block| {
                    block
                        .as_tool_result()
                        .is_ok_and(|result| result.tool_use_id == id)
                }) {
                    blocks.push(result_block(&id, "Tool call has no result.".into(), true)?);
                }
            }
            blocks.sort_by_key(|block| !matches!(block, ContentBlock::ToolResult(_)));
        }
        index += 1;
    }
    turns
        .into_iter()
        .map(|(role, content)| {
            sdk::Message::builder()
                .role(role)
                .set_content(Some(content))
                .build()
                .map_err(build_error)
        })
        .collect()
}

#[derive(Default)]
struct ReasoningBlock {
    text: String,
    signature: String,
    redacted: Vec<u8>,
}

enum Block {
    Text(String),
    Reasoning(ReasoningBlock),
    Tool {
        id: String,
        name: String,
        input: String,
    },
}

struct StreamMapper {
    model: String,
    started: bool,
    blocks: BTreeMap<usize, Block>,
    parts: BTreeMap<usize, Part>,
    stop: Option<StopReason>,
    usage: Option<TokenUsage>,
}

impl StreamMapper {
    fn new(model: String) -> Self {
        Self {
            model,
            started: false,
            blocks: BTreeMap::new(),
            parts: BTreeMap::new(),
            stop: None,
            usage: None,
        }
    }

    fn event(&mut self, event: sdk::ConverseStreamOutput) -> Result<Vec<Delta>, Error> {
        use sdk::ConverseStreamOutput as Event;
        let mut deltas = Vec::new();
        match event {
            Event::MessageStart(event) => {
                if self.started || event.role != ConversationRole::Assistant {
                    return Err(protocol("invalid message start"));
                }
                self.started = true;
            }
            Event::ContentBlockStart(event) => {
                let index = self.index(event.content_block_index)?;
                if self.blocks.contains_key(&index) {
                    return Err(protocol("duplicate block start"));
                }
                let Some(start) = event.start else {
                    return Ok(deltas);
                };
                let sdk::ContentBlockStart::ToolUse(tool) = start else {
                    return Err(protocol("unsupported block start"));
                };
                self.blocks.insert(
                    index,
                    Block::Tool {
                        id: tool.tool_use_id,
                        name: tool.name,
                        input: String::new(),
                    },
                );
            }
            Event::ContentBlockDelta(event) => {
                let index = self.index(event.content_block_index)?;
                deltas.extend(self.delta(
                    index,
                    event.delta.ok_or_else(|| protocol("missing block delta"))?,
                )?);
            }
            Event::ContentBlockStop(event) => {
                let index = self.index(event.content_block_index)?;
                let block = self
                    .blocks
                    .remove(&index)
                    .ok_or_else(|| protocol("stop for unknown block"))?;
                let part = self.part(block)?;
                self.parts.insert(index, part.clone());
                deltas.push(Delta::PartDone {
                    output_index: index,
                    part,
                });
            }
            Event::MessageStop(event) => {
                if !self.started || self.stop.is_some() || !self.blocks.is_empty() {
                    return Err(protocol("invalid message stop"));
                }
                self.stop = Some(match event.stop_reason {
                    sdk::StopReason::EndTurn | sdk::StopReason::StopSequence => StopReason::EndTurn,
                    sdk::StopReason::ToolUse => StopReason::ToolCalls,
                    sdk::StopReason::MaxTokens => StopReason::MaxOutputTokens,
                    sdk::StopReason::ContentFiltered | sdk::StopReason::GuardrailIntervened => {
                        StopReason::ContentFilter
                    }
                    reason => StopReason::Incomplete(reason.as_str().into()),
                });
            }
            Event::Metadata(event) => {
                if self.stop.is_none() || self.usage.is_some() {
                    return Err(protocol("invalid metadata event"));
                }
                let usage = event.usage.ok_or_else(|| protocol("missing token usage"))?;
                self.usage = Some(TokenUsage {
                    input_tokens: token_count(usage.input_tokens)?,
                    output_tokens: token_count(usage.output_tokens)?,
                    total_tokens: token_count(usage.total_tokens)?,
                    cached_input_tokens: token_count(usage.cache_read_input_tokens.unwrap_or(0))?,
                    cache_write_input_tokens: token_count(
                        usage.cache_write_input_tokens.unwrap_or(0),
                    )?,
                    reasoning_output_tokens: 0,
                });
            }
            _ => return Err(protocol("unsupported stream event")),
        }
        Ok(deltas)
    }

    fn index(&self, index: i32) -> Result<usize, Error> {
        let index = usize::try_from(index).map_err(|_| protocol("negative block index"))?;
        if !self.started || self.stop.is_some() || self.parts.contains_key(&index) {
            return Err(protocol(
                "block event outside an open message or after block stop",
            ));
        }
        Ok(index)
    }

    fn delta(
        &mut self,
        index: usize,
        delta: sdk::ContentBlockDelta,
    ) -> Result<Option<Delta>, Error> {
        let output_index = index;
        Ok(match delta {
            sdk::ContentBlockDelta::Text(text) => {
                let Block::Text(buffer) = self
                    .blocks
                    .entry(index)
                    .or_insert_with(|| Block::Text(String::new()))
                else {
                    return Err(protocol("text changed block type"));
                };
                buffer.push_str(&text);
                Some(Delta::Text { output_index, text })
            }
            sdk::ContentBlockDelta::ToolUse(tool) => {
                let Some(Block::Tool { input, .. }) = self.blocks.get_mut(&index) else {
                    return Err(protocol("tool input without a tool start"));
                };
                input.push_str(&tool.input);
                Some(Delta::ToolArguments {
                    output_index,
                    arguments: tool.input,
                })
            }
            sdk::ContentBlockDelta::ReasoningContent(delta) => {
                let Block::Reasoning(block) = self
                    .blocks
                    .entry(index)
                    .or_insert_with(|| Block::Reasoning(ReasoningBlock::default()))
                else {
                    return Err(protocol("reasoning changed block type"));
                };
                match delta {
                    sdk::ReasoningContentBlockDelta::Text(text) => {
                        block.text.push_str(&text);
                        Some(Delta::Reasoning { output_index, text })
                    }
                    sdk::ReasoningContentBlockDelta::Signature(signature) => {
                        block.signature.push_str(&signature);
                        None
                    }
                    sdk::ReasoningContentBlockDelta::RedactedContent(bytes) => {
                        block.redacted.extend_from_slice(bytes.as_ref());
                        None
                    }
                    _ => return Err(protocol("unsupported reasoning delta")),
                }
            }
            _ => return Err(protocol("unsupported block delta")),
        })
    }

    fn part(&self, block: Block) -> Result<Part, Error> {
        Ok(match block {
            Block::Text(text) => Part::Text { text },
            Block::Tool { id, name, input } => Part::ToolCall {
                call_id: ToolCallId(id),
                tool: name,
                input: serde_json::from_str(if input.is_empty() { "{}" } else { &input })?,
            },
            Block::Reasoning(block) => {
                let mut data = json!({"model": self.model});
                if !block.signature.is_empty() {
                    data["signature"] = json!(block.signature);
                }
                if !block.redacted.is_empty() {
                    data["redacted_content"] = json!(STANDARD.encode(block.redacted));
                }
                Part::Reasoning {
                    text: block.text,
                    metadata: BTreeMap::from([("bedrock".into(), data)]),
                }
            }
        })
    }

    fn finish(self) -> Result<Delta, Error> {
        Ok(Delta::Completed(Response {
            parts: self.parts.into_values().collect(),
            stop_reason: self
                .stop
                .ok_or_else(|| protocol("stream ended before message stop"))?,
            usage: self
                .usage
                .ok_or_else(|| protocol("stream ended before usage metadata"))?,
            // Bedrock surfaces throttling through SDK retry metadata rather
            // than remaining-quota headers, so nothing is recorded here.
            quota_remaining: std::collections::BTreeMap::new(),
            quota_resets: std::collections::BTreeMap::new(),
        }))
    }
}

fn token_count(value: i32) -> Result<u64, Error> {
    u64::try_from(value).map_err(|_| protocol("negative token usage"))
}

fn protocol(message: &str) -> Error {
    Error::Protocol(format!("Bedrock: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        GenerationSettings, ToolDefinition,
        catalog::{Catalog, ReasoningOptions},
    };
    use futures::TryStreamExt as _;
    use sdk::ConverseStreamOutput as Event;
    use swarmy_core::{Message, MessageId};

    fn model(id: &str) -> ModelInfo {
        let mut model = Catalog::get()
            .provider("amazon-bedrock")
            .unwrap()
            .models
            .values()
            .find(|model| model.id.contains("claude-opus-4-8"))
            .unwrap()
            .clone();
        model.id = id.into();
        model.name = id.into();
        model.compat = crate::catalog::Compat::default();
        model.reasoning = Some(ReasoningOptions::Effort(vec![
            ReasoningEffort::None,
            ReasoningEffort::Minimal,
            ReasoningEffort::Low,
            ReasoningEffort::Medium,
            ReasoningEffort::High,
            ReasoningEffort::Xhigh,
            ReasoningEffort::Max,
        ]));
        model
    }

    fn message(role: MessageRole, parts: Vec<Part>) -> Message {
        Message {
            id: MessageId::from_ulid(ulid::Ulid::nil()),
            role,
            parts,
        }
    }

    fn request(model: &ModelInfo) -> Request {
        Request {
            system_prompt: "Be helpful.".into(),
            messages: vec![message(
                MessageRole::User,
                vec![Part::Text {
                    text: "Hello".into(),
                }],
            )],
            tools: vec![ToolDefinition {
                name: "clock".into(),
                description: "Read time".into(),
                parameters: json!({"type": "object", "properties": {}}),
            }],
            settings: GenerationSettings {
                model: model.id.clone(),
                max_output_tokens: Some(1000),
                temperature: Some(0.5),
                reasoning_effort: None,
            },
        }
    }

    #[test]
    fn image_block_contains_raw_bytes() {
        let model = model("anthropic.claude-3-7-sonnet-v1:0");
        let part = Part::Image {
            media_type: "image/png".into(),
            bytes: vec![1, 2, 3],
            object_key: None,
            detail: None,
        };
        let block = part_block(&part, &model).unwrap().unwrap();
        let ContentBlock::Image(image) = block else {
            panic!("expected image block")
        };
        assert_eq!(image.format, sdk::ImageFormat::Png);
        assert_eq!(
            image.source.unwrap().as_bytes().unwrap().as_ref(),
            &[1, 2, 3]
        );
    }

    #[test]
    fn request_cache_tools_roles_and_inference() {
        for (id, cache) in [
            ("us.anthropic.claude-opus-4-8", true),
            ("eu.anthropic.claude-3-7-sonnet-v1:0", true),
            ("global.anthropic.claude-sonnet-4", true),
            ("anthropic.claude-fable-5", true),
            ("amazon.nova-pro-v1:0", false),
        ] {
            let model = model(id);
            let input = request_input(&request(&model), &model).unwrap();
            assert_eq!(input.model_id.as_deref(), Some(id));
            assert_eq!(input.system().len(), if cache { 2 } else { 1 });
            assert_eq!(input.system()[0].as_text().unwrap(), "Be helpful.");
            if cache {
                assert_eq!(
                    input.system()[1].as_cache_point().unwrap().r#type,
                    sdk::CachePointType::Default
                );
            }
            assert_eq!(input.messages()[0].role, ConversationRole::User);
            let config = input.inference_config.unwrap();
            assert_eq!(config.max_tokens, Some(1000));
            assert_eq!(config.temperature, Some(0.5));
            let tools = input.tool_config.unwrap();
            let spec = tools.tools[0].as_tool_spec().unwrap();
            assert_eq!(spec.name, "clock");
            assert_eq!(
                spec.input_schema.as_ref().unwrap().as_json().unwrap(),
                &document(&json!({"type": "object", "properties": {}}))
            );
        }
    }

    #[test]
    fn adaptive_budget_and_non_claude_thinking() {
        for id in [
            "us.anthropic.claude-opus-4-6-v1",
            "us.anthropic.claude-opus-4-8",
            "claude-sonnet-4-7",
            "claude-opus-5",
            "claude-sonnet-5",
            "claude-fable-5",
        ] {
            let model = model(id);
            for (effort, expected) in [
                (ReasoningEffort::Minimal, "low"),
                (ReasoningEffort::Medium, "medium"),
                (ReasoningEffort::Xhigh, "xhigh"),
                (ReasoningEffort::Max, "max"),
            ] {
                let mut request = request(&model);
                request.settings.reasoning_effort = Some(effort);
                let input = request_input(&request, &model).unwrap();
                assert_eq!(
                    input.additional_model_request_fields,
                    Some(document(
                        &json!({"thinking": {"type": "adaptive", "display": "summarized"}, "output_config": {"effort": expected}})
                    ))
                );
                assert_eq!(
                    input.inference_config.as_ref().unwrap().max_tokens,
                    Some(1000)
                );
                assert!(input.inference_config.unwrap().temperature.is_none());
            }
        }
        let model = model("us.anthropic.claude-sonnet-4-5");
        for (effort, budget) in [
            (ReasoningEffort::Minimal, 1024),
            (ReasoningEffort::Low, 2048),
            (ReasoningEffort::Medium, 8192),
            (ReasoningEffort::High, 16384),
            (ReasoningEffort::Xhigh, 16384),
            (ReasoningEffort::Max, 16384),
        ] {
            let mut request = request(&model);
            request.settings.reasoning_effort = Some(effort);
            let input = request_input(&request, &model).unwrap();
            assert_eq!(
                input.additional_model_request_fields,
                Some(document(
                    &json!({"thinking": {"type": "enabled", "budget_tokens": budget}, "anthropic_beta": ["interleaved-thinking-2025-05-14"]})
                ))
            );
            assert_eq!(
                input.inference_config.unwrap().max_tokens,
                Some(1000 + budget)
            );
        }
        let mut nova = model;
        nova.id = "amazon.nova-pro-v1:0".into();
        nova.name = "Nova".into();
        let mut request = request(&nova);
        request.settings.reasoning_effort = Some(ReasoningEffort::High);
        assert!(
            request_input(&request, &nova)
                .unwrap()
                .additional_model_request_fields
                .is_none()
        );
    }

    #[test]
    fn errors_orphans_and_foreign_call_ids_are_replayed() {
        let model = model("us.anthropic.claude-opus-4-8");
        let mut request = request(&model);
        let call = |id: &str| Part::ToolCall {
            call_id: ToolCallId(id.into()),
            tool: "clock".into(),
            input: json!({"zone": "UTC"}),
        };
        request.messages.push(message(
            MessageRole::Assistant,
            vec![call("foreign/id"), call("missing")],
        ));
        request.messages.push(message(
            MessageRole::System,
            vec![Part::Text {
                text: "Computer rebuilt".into(),
            }],
        ));
        request.messages.push(message(
            MessageRole::Tool,
            vec![Part::ToolResult {
                call_id: ToolCallId("foreign/id".into()),
                result: ToolResult::Error {
                    error: "Interrupted".into(),
                },
            }],
        ));
        let input = request_input(&request, &model).unwrap();
        let messages = input.messages();
        let id = &messages[1].content[0].as_tool_use().unwrap().tool_use_id;
        let result = messages[2].content[0].as_tool_result().unwrap();
        assert_eq!(&result.tool_use_id, id);
        assert_eq!(result.status, Some(sdk::ToolResultStatus::Error));
        assert_eq!(
            messages[2].content[1].as_tool_result().unwrap().tool_use_id,
            "missing"
        );
        assert!(messages[2].content[2].is_text());
        assert_ne!(tool_id("foreign/id"), tool_id("foreign|id"));
        assert_eq!(tool_id(&"x".repeat(100)).len(), 64);
        request.messages.truncate(2);
        assert_eq!(
            request_input(&request, &model).unwrap().messages()[2]
                .content
                .len(),
            2
        );
    }

    #[test]
    fn reasoning_replay_requires_matching_model_and_protocol() {
        let model = model("us.anthropic.claude-opus-4-8");
        for (protocol, source, signed) in [
            ("bedrock", model.id.as_str(), true),
            ("bedrock", "other", false),
            ("anthropic", model.id.as_str(), false),
        ] {
            let metadata = BTreeMap::from([(
                protocol.into(),
                json!({"model": source, "signature": "opaque"}),
            )]);
            let block = reasoning("Thought", &metadata, &model).unwrap().unwrap();
            if signed {
                let text = block
                    .as_reasoning_content()
                    .unwrap()
                    .as_reasoning_text()
                    .unwrap();
                assert_eq!(text.text, "Thought");
                assert_eq!(text.signature.as_deref(), Some("opaque"));
            } else {
                assert_eq!(block.as_text().unwrap(), "Thought");
            }
        }
        let metadata = BTreeMap::from([(
            "bedrock".into(),
            json!({"model": model.id, "redacted_content": STANDARD.encode([0, 255, 42])}),
        )]);
        let block = reasoning("", &metadata, &model).unwrap().unwrap();
        assert_eq!(
            block
                .as_reasoning_content()
                .unwrap()
                .as_redacted_content()
                .unwrap()
                .as_ref(),
            &[0, 255, 42]
        );
        assert!(
            reasoning("", &metadata, &self::model("other"))
                .unwrap()
                .is_none()
        );
    }

    fn start() -> Event {
        Event::MessageStart(
            sdk::MessageStartEvent::builder()
                .role(ConversationRole::Assistant)
                .build()
                .unwrap(),
        )
    }
    fn delta(index: i32, delta: sdk::ContentBlockDelta) -> Event {
        Event::ContentBlockDelta(
            sdk::ContentBlockDeltaEvent::builder()
                .content_block_index(index)
                .delta(delta)
                .build()
                .unwrap(),
        )
    }
    fn block_stop(index: i32) -> Event {
        Event::ContentBlockStop(
            sdk::ContentBlockStopEvent::builder()
                .content_block_index(index)
                .build()
                .unwrap(),
        )
    }
    fn stop(reason: sdk::StopReason) -> Event {
        Event::MessageStop(
            sdk::MessageStopEvent::builder()
                .stop_reason(reason)
                .build()
                .unwrap(),
        )
    }
    fn metadata() -> Event {
        Event::Metadata(
            sdk::ConverseStreamMetadataEvent::builder()
                .usage(
                    sdk::TokenUsage::builder()
                        .input_tokens(30)
                        .output_tokens(5)
                        .total_tokens(35)
                        .cache_read_input_tokens(10)
                        .cache_write_input_tokens(3)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
    }
    fn mapped(events: Vec<Event>) -> Vec<Delta> {
        let mut mapper = StreamMapper::new("model".into());
        let mut deltas: Vec<_> = events
            .into_iter()
            .flat_map(|event| mapper.event(event).unwrap())
            .collect();
        deltas.push(mapper.finish().unwrap());
        deltas
    }
    fn completed(parts: Vec<Part>, stop_reason: StopReason) -> Delta {
        Delta::Completed(Response {
            parts,
            stop_reason,
            quota_remaining: std::collections::BTreeMap::new(),
            quota_resets: std::collections::BTreeMap::new(),
            usage: TokenUsage {
                input_tokens: 30,
                output_tokens: 5,
                total_tokens: 35,
                cached_input_tokens: 10,
                cache_write_input_tokens: 3,
                reasoning_output_tokens: 0,
            },
        })
    }

    #[test]
    fn text_turn_and_stop_reasons_emit_exact_deltas_and_usage() {
        for (reason, expected) in [
            (sdk::StopReason::EndTurn, StopReason::EndTurn),
            (sdk::StopReason::MaxTokens, StopReason::MaxOutputTokens),
            (sdk::StopReason::StopSequence, StopReason::EndTurn),
            (sdk::StopReason::ContentFiltered, StopReason::ContentFilter),
        ] {
            let part = Part::Text {
                text: "Hello 🦀".into(),
            };
            assert_eq!(
                mapped(vec![
                    start(),
                    Event::ContentBlockStart(
                        sdk::ContentBlockStartEvent::builder()
                            .content_block_index(0)
                            .build()
                            .unwrap()
                    ),
                    delta(0, sdk::ContentBlockDelta::Text("Hello ".into())),
                    delta(0, sdk::ContentBlockDelta::Text("🦀".into())),
                    block_stop(0),
                    stop(reason),
                    metadata()
                ]),
                vec![
                    Delta::Text {
                        output_index: 0,
                        text: "Hello ".into()
                    },
                    Delta::Text {
                        output_index: 0,
                        text: "🦀".into()
                    },
                    Delta::PartDone {
                        output_index: 0,
                        part: part.clone()
                    },
                    completed(vec![part], expected)
                ]
            );
        }
    }

    #[test]
    fn tool_turn_accumulates_json() {
        let part = Part::ToolCall {
            call_id: ToolCallId("tool-1".into()),
            tool: "clock".into(),
            input: json!({"zone": "UTC"}),
        };
        let tool_start = Event::ContentBlockStart(
            sdk::ContentBlockStartEvent::builder()
                .content_block_index(2)
                .start(sdk::ContentBlockStart::ToolUse(
                    sdk::ToolUseBlockStart::builder()
                        .tool_use_id("tool-1")
                        .name("clock")
                        .build()
                        .unwrap(),
                ))
                .build()
                .unwrap(),
        );
        let argument = |text: &str| {
            delta(
                2,
                sdk::ContentBlockDelta::ToolUse(
                    sdk::ToolUseBlockDelta::builder()
                        .input(text)
                        .build()
                        .unwrap(),
                ),
            )
        };
        assert_eq!(
            mapped(vec![
                start(),
                tool_start,
                argument("{\"zone\":"),
                argument("\"UTC\"}"),
                block_stop(2),
                stop(sdk::StopReason::ToolUse),
                metadata()
            ]),
            vec![
                Delta::ToolArguments {
                    output_index: 2,
                    arguments: "{\"zone\":".into()
                },
                Delta::ToolArguments {
                    output_index: 2,
                    arguments: "\"UTC\"}".into()
                },
                Delta::PartDone {
                    output_index: 2,
                    part: part.clone()
                },
                completed(vec![part], StopReason::ToolCalls)
            ]
        );
    }

    #[test]
    fn reasoning_signatures_and_redacted_bytes_accumulate() {
        use sdk::ReasoningContentBlockDelta as Reason;
        let reasoning_delta =
            |index, value| delta(index, sdk::ContentBlockDelta::ReasoningContent(value));
        let signed = Part::Reasoning {
            text: "Think".into(),
            metadata: BTreeMap::from([(
                "bedrock".into(),
                json!({"model": "model", "signature": "signature"}),
            )]),
        };
        let redacted = Part::Reasoning {
            text: String::new(),
            metadata: BTreeMap::from([(
                "bedrock".into(),
                json!({"model": "model", "redacted_content": STANDARD.encode([0, 255, 42])}),
            )]),
        };
        assert_eq!(
            mapped(vec![
                start(),
                reasoning_delta(0, Reason::Text("Think".into())),
                reasoning_delta(0, Reason::Signature("sign".into())),
                reasoning_delta(0, Reason::Signature("ature".into())),
                block_stop(0),
                reasoning_delta(1, Reason::RedactedContent(Blob::new([0, 255]))),
                reasoning_delta(1, Reason::RedactedContent(Blob::new([42]))),
                block_stop(1),
                stop(sdk::StopReason::EndTurn),
                metadata()
            ]),
            vec![
                Delta::Reasoning {
                    output_index: 0,
                    text: "Think".into()
                },
                Delta::PartDone {
                    output_index: 0,
                    part: signed.clone()
                },
                Delta::PartDone {
                    output_index: 1,
                    part: redacted.clone()
                },
                completed(vec![signed, redacted], StopReason::EndTurn)
            ]
        );
    }

    #[test]
    fn truncated_and_malformed_streams_fail() {
        assert!(StreamMapper::new("model".into()).finish().is_err());
        let mut mapper = StreamMapper::new("model".into());
        assert!(
            mapper
                .event(delta(0, sdk::ContentBlockDelta::Text("premature".into())))
                .is_err()
        );
        mapper.event(start()).unwrap();
        assert!(mapper.event(block_stop(0)).is_err());
        mapper
            .event(delta(0, sdk::ContentBlockDelta::Text("open".into())))
            .unwrap();
        assert!(mapper.event(stop(sdk::StopReason::EndTurn)).is_err());
        mapper.event(block_stop(0)).unwrap();
        mapper.event(stop(sdk::StopReason::EndTurn)).unwrap();
        assert!(mapper.finish().is_err());
    }

    #[test]
    fn region_precedence_and_dispatch() {
        assert_eq!(
            resolve_region(
                "arn:aws:bedrock:eu-west-1:123:inference-profile/model",
                Some("us-west-2"),
                Some("ap-south-1")
            ),
            "eu-west-1"
        );
        assert_eq!(
            resolve_region(
                "arn:aws-us-gov:bedrock:us-gov-west-1:123:inference-profile/model",
                None,
                None
            ),
            "us-gov-west-1"
        );
        assert_eq!(
            resolve_region(
                "global.anthropic.claude-opus-4-8",
                Some("us-west-2"),
                Some("ap-south-1")
            ),
            "us-west-2"
        );
        assert_eq!(
            resolve_region("model", None, Some("ap-south-1")),
            "ap-south-1"
        );
        assert_eq!(resolve_region("model", None, None), "us-east-1");
        let provider = Catalog::get().provider("amazon-bedrock").unwrap();
        assert!(crate::client_for(provider, &model("model"), ClientAuth::Ambient).is_ok());
        assert!(matches!(
            crate::client_for(provider, &model("model"), ClientAuth::None),
            Err(Error::Credentials(_))
        ));
    }

    #[test]
    fn expired_console_key_points_to_iam() {
        let error = service_error(
            "UnrecognizedClientException",
            Some("The security token has expired"),
        );
        assert!(
            matches!(error, Error::Credentials(message) if message.contains("IAM identity") && message.contains("development only"))
        );
    }

    #[tokio::test]
    async fn sdk_errors_use_shared_retry_and_classify_overflow() {
        use aws_sdk_bedrockruntime::operation::converse_stream::ConverseStreamError;
        let throttled = ConverseStreamError::ThrottlingException(
            sdk::error::ThrottlingException::builder()
                .message("busy")
                .build(),
        );
        let unavailable = ConverseStreamError::ServiceUnavailableException(
            sdk::error::ServiceUnavailableException::builder()
                .message("busy")
                .build(),
        );
        let not_ready = ConverseStreamError::ModelNotReadyException(
            sdk::error::ModelNotReadyException::builder()
                .message("busy")
                .build(),
        );
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_delay: std::time::Duration::ZERO,
            max_delay: std::time::Duration::ZERO,
        };
        for error in [throttled, unavailable, not_ready] {
            let attempts = std::sync::atomic::AtomicUsize::new(0);
            let result: Result<(), Error> = with_retry(&policy, || {
                attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                std::future::ready(Err(request_error(Some(&error))))
            })
            .await;
            assert!(matches!(result, Err(Error::ProviderResponse { .. })));
            assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 3);
        }
        let error = ConverseStreamError::ValidationException(
            sdk::error::ValidationException::builder()
                .message("Input is too long for requested model")
                .build(),
        );
        assert!(matches!(
            request_error(Some(&error)),
            Error::ContextOverflow(_)
        ));
        let error = ConverseStreamError::ValidationException(
            sdk::error::ValidationException::builder()
                .message("Invalid model")
                .build(),
        );
        assert!(matches!(request_error(Some(&error)), Error::Protocol(_)));
    }

    #[tokio::test]
    async fn stream_exceptions_without_metadata_codes_retry() {
        use sdk::error::ConverseStreamOutputError as E;
        let errors = [
            E::ThrottlingException(
                sdk::error::ThrottlingException::builder()
                    .message("busy")
                    .build(),
            ),
            E::ServiceUnavailableException(
                sdk::error::ServiceUnavailableException::builder()
                    .message("busy")
                    .build(),
            ),
        ];
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_delay: std::time::Duration::ZERO,
            max_delay: std::time::Duration::ZERO,
        };
        for error in errors {
            assert!(error.code().is_none());
            let attempts = std::sync::atomic::AtomicUsize::new(0);
            let result: Result<(), Error> = with_retry(&policy, || {
                attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                std::future::ready(Err(stream_error(Some(&error))))
            })
            .await;
            assert!(matches!(result, Err(Error::ProviderResponse { .. })));
            assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 3);
        }
        let error = E::ValidationException(
            sdk::error::ValidationException::builder()
                .message("Input is too long")
                .build(),
        );
        assert!(matches!(
            stream_error(Some(&error)),
            Error::ContextOverflow(_)
        ));
    }

    #[tokio::test]
    async fn bearer_auth_and_user_agent_reach_the_sdk_transport() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{header, method},
        };
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(header("authorization", "Bearer fixture-token"))
            .and(header(
                "user-agent",
                concat!("swarmy/", env!("CARGO_PKG_VERSION")),
            ))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "__type": "ValidationException", "message": "Input is too long"
            })))
            .expect(1)
            .mount(&server)
            .await;
        let model = model("us.anthropic.claude-opus-4-8");
        let mut provider = BedrockProvider::new(
            model.clone(),
            ClientAuth::ApiKeyWithExtra {
                key: "fixture-token".into(),
                extra: BTreeMap::from([("region".into(), "us-west-2".into())]),
            },
        )
        .unwrap();
        let config = provider
            .client()
            .await
            .config()
            .to_builder()
            .endpoint_url(server.uri())
            .build();
        provider.client = Arc::new(OnceCell::new_with(Some(Client::from_conf(config))));
        let error = provider
            .request(request(&model))
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(matches!(error, Error::ContextOverflow(_)), "{error}");
        let requests = server.received_requests().await.unwrap();
        let body: Value = requests[0].body_json().unwrap();
        assert_eq!(body["inferenceConfig"]["maxTokens"], 1000);
        assert!(
            requests[0]
                .url
                .path()
                .contains("us.anthropic.claude-opus-4-8")
        );
        assert!(!requests[0].headers.contains_key("x-amz-security-token"));
    }
}
