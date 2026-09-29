//! Shared transport for direct Responses APIs and the Codex backend.
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures::StreamExt;
use serde_json::Value;
use swarmy_core::SessionId;

use crate::{
    ClientAuth, Error, Provider, ProviderStream, Request,
    auth::{CredentialStore, Credentials, OAuthClient},
    catalog::{Api, Catalog, Compat, ModelInfo, ProviderInfo},
    retry::{RetryPolicy, with_retry},
};

#[derive(Clone)]
pub struct ResponsesEndpoint {
    pub url: String,
    pub auth: ClientAuth,
    pub extra_headers: BTreeMap<String, String>,
    pub codex: bool,
    pub compat: Compat,
}

impl ResponsesEndpoint {
    /// Resolve a catalog endpoint, including Azure credential resource metadata.
    /// Custom base URLs take precedence over Azure's resource-derived URL.
    /// # Errors
    /// Rejects missing credentials or an invalid Azure resource name.
    pub fn from_catalog(
        provider: &ProviderInfo,
        model: &ModelInfo,
        auth: ClientAuth,
    ) -> Result<Self, Error> {
        // Azure shares the Responses wire protocol but needs its own endpoint
        // derivation. Refuse it without the feature so slim builds fail with
        // a clear error instead of reaching misconfigured URLs.
        #[cfg(not(feature = "azure"))]
        if provider.id == "azure" {
            return Err(Error::NotCompiledIn(provider.id.clone()));
        }
        let codex = model.api.unwrap_or(provider.api) == Api::OpenAiCodexResponses;
        if codex && !matches!(auth, ClientAuth::ChatGpt(_)) {
            return Err(Error::Credentials("ChatGPT requires a credential store"));
        }
        if matches!(auth, ClientAuth::None | ClientAuth::Vertex { .. }) {
            return Err(Error::Credentials(
                "Responses requires an API key, bearer token, headers, or a ChatGPT store",
            ));
        }
        let base = model.base_url.as_deref().unwrap_or(&provider.base_url);
        let base = if provider.id == "azure" && base.is_empty() {
            let extra = match &auth {
                ClientAuth::ApiKeyWithExtra {
                    extra: crate::ProviderAuthExtra::Azure(extra),
                    ..
                }
                | ClientAuth::BearerWithExtra {
                    extra: crate::ProviderAuthExtra::Azure(extra),
                    ..
                } => Some(extra),
                _ => None,
            };
            let endpoint = extra
                .and_then(|extra| extra.base_url.as_ref())
                .cloned()
                .or_else(|| {
                    if extra.is_none() {
                        std::env::var("AZURE_OPENAI_BASE_URL").ok()
                    } else {
                        None
                    }
                });
            if let Some(endpoint) = endpoint.filter(|url| !url.is_empty()) {
                let parsed = reqwest::Url::parse(&endpoint)
                    .map_err(|_| Error::Credentials("invalid Azure base URL"))?;
                if parsed.scheme() != "https"
                    || parsed.host_str().is_none()
                    || parsed.username() != ""
                    || parsed.password().is_some()
                    || parsed.query().is_some()
                    || parsed.fragment().is_some()
                {
                    return Err(Error::Credentials("invalid Azure base URL"));
                }
                format!("{}/openai/v1", endpoint.trim_end_matches('/'))
            } else {
                let resource = extra
                    .and_then(|extra| extra.resource_name.as_ref())
                    .cloned()
                    .or_else(|| std::env::var("AZURE_RESOURCE_NAME").ok())
                    .ok_or(Error::Credentials(
                        "Azure requires resource_name or AZURE_RESOURCE_NAME",
                    ))?;
                if resource.is_empty()
                    || !resource
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                {
                    return Err(Error::Credentials("invalid Azure resource name"));
                }
                format!("https://{resource}.openai.azure.com/openai/v1")
            }
        } else {
            base.to_owned()
        };
        let mut extra_headers = BTreeMap::new();
        let auth = match auth {
            ClientAuth::ApiKey(key) | ClientAuth::ApiKeyWithExtra { key, .. }
                if provider.id == "azure" =>
            {
                extra_headers.insert("api-key".into(), key);
                ClientAuth::Headers(BTreeMap::new())
            }
            auth => auth,
        };
        if codex {
            extra_headers.insert("OpenAI-Beta".into(), "responses=experimental".into());
            extra_headers.insert("originator".into(), "swarmy".into());
        }
        Ok(Self {
            url: format!("{}/responses", base.trim_end_matches('/')),
            auth,
            extra_headers,
            codex,
            compat: model.compat.clone(),
        })
    }
}

#[derive(Clone)]
pub struct ResponsesProvider {
    endpoint: ResponsesEndpoint,
    provider_id: String,
    model: Option<ModelInfo>,
    oauth: OAuthClient,
    client: reqwest::Client,
    pub retry_policy: RetryPolicy,
}

impl ResponsesProvider {
    /// # Errors
    /// Returns an error if the HTTP client cannot be configured.
    pub fn new(
        endpoint: ResponsesEndpoint,
        provider_id: String,
        model: ModelInfo,
    ) -> Result<Self, Error> {
        Self::build(endpoint, provider_id, Some(model), OAuthClient::new()?)
    }

    pub(crate) fn codex(
        store: Arc<dyn CredentialStore>,
        base: &str,
        oauth: OAuthClient,
    ) -> Result<Self, Error> {
        let provider = Catalog::get()
            .provider("chatgpt")
            .ok_or(Error::Credentials("missing ChatGPT catalog"))?;
        let model = provider
            .models
            .values()
            .next()
            .ok_or(Error::Credentials("empty ChatGPT catalog"))?;
        let mut endpoint =
            ResponsesEndpoint::from_catalog(provider, model, ClientAuth::ChatGpt(store))?;
        endpoint.url = format!("{}/responses", base.trim_end_matches('/'));
        Self::build(endpoint, provider.id.clone(), None, oauth)
    }

    fn build(
        endpoint: ResponsesEndpoint,
        provider_id: String,
        model: Option<ModelInfo>,
        oauth: OAuthClient,
    ) -> Result<Self, Error> {
        Ok(Self {
            endpoint,
            provider_id,
            model,
            oauth,
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(30))
                .read_timeout(Duration::from_secs(120))
                .build()?,
            retry_policy: RetryPolicy::default(),
        })
    }

    fn stream(&self, request: Request, session: Option<SessionId>) -> ProviderStream {
        let provider = self.clone();
        Box::pin(async_stream::try_stream! {
            let model = provider.model.as_ref().filter(|m| m.id == request.settings.model)
                .or_else(|| Catalog::get().model(&provider.provider_id, &request.settings.model));
            let body = request_json_for(&request, &provider.endpoint, &provider.provider_id, model, session)?;
            let response = provider.send_authenticated(&body).await?;
            // The live Codex backend omits Content-Type; reject only an explicit
            // non-stream type and let the parser validate the body otherwise.
            let content_type = response.headers().get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|v| v.split(';').next().unwrap_or_default().trim().to_ascii_lowercase());
            if let Some(mime) = content_type.filter(|mime| mime != "text/event-stream") {
                Err(Error::Protocol(format!("expected text/event-stream, got {mime}")))?;
            }
            let quota = crate::quota::openai_remaining(response.headers());
            let resets = crate::quota::openai_resets(response.headers());
            let mut bytes = response.bytes_stream();
            let mut parser = ResponsesStream::with_context(&provider.provider_id, &request.settings.model);
            parser.set_quota(quota);
            parser.set_quota_resets(resets);
            while let Some(chunk) = bytes.next().await {
                for delta in parser.push(&chunk?)? { yield delta; }
                if parser.is_completed() { break; }
            }
            parser.finish()?;
        })
    }

    async fn send_authenticated(&self, body: &Value) -> Result<reqwest::Response, Error> {
        // Refresh rotates credentials and must never be retried by the generic
        // request loop. Only inference HTTP requests are safe to repeat here.
        if let ClientAuth::ChatGpt(store) = &self.endpoint.auth {
            let mut credentials = store.load().await?;
            if credentials.needs_refresh() {
                credentials = self.oauth.refresh(store.as_ref(), &credentials).await?;
            }
            match self.send_with_retry(body, Some(&credentials)).await {
                Err(Error::Authentication(_)) => {
                    credentials = self.oauth.refresh(store.as_ref(), &credentials).await?;
                    self.send_with_retry(body, Some(&credentials)).await
                }
                result => result,
            }
        } else {
            self.send_with_retry(body, None).await
        }
    }

    async fn send_with_retry(
        &self,
        body: &Value,
        credentials: Option<&Credentials>,
    ) -> Result<reqwest::Response, Error> {
        with_retry(&self.retry_policy, || async {
            check_response(self.send(body, credentials).await?).await
        })
        .await
    }

    async fn send(
        &self,
        body: &Value,
        credentials: Option<&Credentials>,
    ) -> Result<reqwest::Response, Error> {
        let mut request = self
            .client
            .post(&self.endpoint.url)
            .header("accept", "text/event-stream");
        for (name, value) in &self.endpoint.extra_headers {
            request = request.header(name, value);
        }
        request = match &self.endpoint.auth {
            ClientAuth::ApiKey(key)
            | ClientAuth::Bearer(key)
            | ClientAuth::ApiKeyWithExtra { key, .. }
            | ClientAuth::BearerWithExtra { token: key, .. } => request.bearer_auth(key),
            ClientAuth::Headers(headers) => {
                for (name, value) in headers {
                    request = request.header(name, value);
                }
                request
            }
            ClientAuth::ChatGpt(_) => {
                let credentials =
                    credentials.ok_or(Error::Credentials("missing ChatGPT credentials"))?;
                request
                    .bearer_auth(credentials.access_token())
                    .header("chatgpt-account-id", credentials.account_id())
            }
            ClientAuth::None
            | ClientAuth::Vertex { .. }
            | ClientAuth::Ambient
            | ClientAuth::Scripted(_) => {
                return Err(Error::Credentials(
                    "Responses requires an API key, bearer token, headers, or a ChatGPT store",
                ));
            }
        };
        Ok(request
            .header(
                reqwest::header::USER_AGENT,
                concat!("swarmy/", env!("CARGO_PKG_VERSION")),
            )
            .json(body)
            .send()
            .await?)
    }
}

impl Provider for ResponsesProvider {
    fn request(&self, request: Request) -> ProviderStream {
        self.stream(request, None)
    }

    fn request_for_session(&self, request: Request, session_id: SessionId) -> ProviderStream {
        self.stream(request, Some(session_id))
    }
}

/// Classify a failed status so the retry loop can honor the server's delay.
async fn check_response(response: reqwest::Response) -> Result<reqwest::Response, Error> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let retry_after = crate::retry::retry_after_header(response.headers());
    let body = response.text().await?;
    Err(crate::error::classify_http_failure(
        status,
        &body,
        retry_after,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_overflow_classification() {
        assert!(!crate::error::is_context_overflow(
            "rate limit: too many tokens"
        ));
    }
}

mod wire {
    //! Responses request normalization and incremental stream parsing.
    use crate::sse::{Frame, SseParser};
    use crate::{Delta, Error, Request, Response, StopReason, TokenUsage};
    use crate::{
        ReasoningEffort,
        api::responses::ResponsesEndpoint,
        catalog::{Compat, ModelInfo, ReasoningOptions},
    };
    use base64::Engine as _;
    use serde_json::{Value, json};
    use std::collections::BTreeMap;
    use swarmy_core::{MessageRole, Part, SessionId, ToolCallId, ToolResult};

    struct RequestContext<'a> {
        provider: &'a str,
        model: Option<&'a ModelInfo>,
        compat: &'a Compat,
        codex: bool,
    }

    /// Build a catalog-aware request, with session affinity when available.
    /// # Errors
    /// Rejects uncorrelated tool text and non-finite temperatures.
    pub fn request_json_for(
        request: &Request,
        endpoint: &ResponsesEndpoint,
        provider: &str,
        model: Option<&ModelInfo>,
        session: Option<SessionId>,
    ) -> Result<Value, Error> {
        let context = RequestContext {
            provider,
            model,
            compat: &endpoint.compat,
            codex: endpoint.codex,
        };
        let mut value = build_request(request, Some(&context))?;
        if let Some(session) = session.filter(|_| !request.no_cache) {
            value["prompt_cache_key"] = json!(session.to_string());
        }
        if !request.no_cache
            && context.compat.supports_long_cache_retention() == Some(true)
            && !context.codex
        {
            value["prompt_cache_retention"] = json!("24h");
        }
        Ok(value)
    }

    fn build_input(
        request: &Request,
        context: Option<&RequestContext<'_>>,
    ) -> Result<Vec<Value>, Error> {
        let mut input = Vec::new();
        let developer =
            context.is_none_or(|ctx| ctx.compat.supports_developer_role() == Some(true));
        if context.is_some_and(|ctx| !ctx.codex) && !request.system_prompt.is_empty() {
            input.push(text_item(
                if developer { "developer" } else { "system" },
                "input_text",
                &request.system_prompt,
            ));
        }
        for message in &request.messages {
            for part in &message.parts {
                if let Part::Reasoning { text, metadata } = part {
                    let saved = metadata
                        .get("openai_responses")
                        .or_else(|| metadata.get("chatgpt"));
                    if let Some(saved) = saved {
                        let item = saved.get("item").unwrap_or(saved);
                        let same = context.is_none_or(|ctx| {
                            saved["provider"] == ctx.provider
                                && saved["model"] == request.settings.model
                        });
                        if same && item["type"] == "reasoning" {
                            input.push(item.clone());
                            continue;
                        }
                    }
                    if !text.is_empty() {
                        input.push(text_item("assistant", "output_text", text));
                    }
                    continue;
                }
                input.push(match part {
                Part::Image { media_type, bytes, detail, .. } => json!({"type": "message", "role": "user", "content": [{"type": "input_image", "image_url": format!("data:{media_type};base64,{}", base64::engine::general_purpose::STANDARD.encode(bytes)), "detail": detail.as_deref().unwrap_or("auto") }]}),
                Part::Text { text } | Part::Notice { text, .. } => {
                    let (role, kind) = match message.role {
                        // Harness notices use the backend's developer role. A system
                        // role in input is rejected after a computer rebuild.
                        MessageRole::System => (if developer { "developer" } else { "system" }, "input_text"),
                        MessageRole::User => ("user", "input_text"),
                        MessageRole::Assistant => ("assistant", "output_text"),
                        MessageRole::Tool => return Err(Error::Protocol("tool text requires a tool result call id".into())),
                    };
                    json!({"type": "message", "role": role, "content": [{"type": kind, "text": text}]})
                }
                Part::ToolCall { call_id, tool, input } => json!({"type": "function_call", "call_id": call_id.0, "name": tool, "arguments": serde_json::to_string(input)?}),
                Part::ToolResult { call_id, result } => {
                    let output = match result {
                        ToolResult::Completed { output, .. } => output.clone(),
                        ToolResult::Error { error } => serde_json::to_string(&json!({"error": error}))?,
                    };
                    json!({"type": "function_call_output", "call_id": call_id.0, "output": output})
                }
                Part::Reasoning { .. } => continue,
            });
            }
        }
        if context.is_some() {
            normalize_calls(&mut input);
        }
        Ok(input)
    }

    fn build_request(
        request: &Request,
        context: Option<&RequestContext<'_>>,
    ) -> Result<Value, Error> {
        let input = build_input(request, context)?;
        let tools: Vec<_> = request
        .tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function", "name": tool.name, "description": tool.description,
                "parameters": tool.parameters, "strict": context.is_some_and(|ctx| ctx.compat.supports_strict_mode() == Some(true)) && strict_schema(&tool.parameters),
            })
        })
        .collect();
        let mut value = json!({
            "model": request.settings.model, "instructions": request.system_prompt, "input": input,
            "tools": tools, "tool_choice": "auto", "parallel_tool_calls": true,
            "stream": true, "store": false, "include": ["reasoning.encrypted_content"],
        });
        if context.is_some_and(|ctx| !ctx.codex) {
            value
                .as_object_mut()
                .expect("request is an object")
                .remove("instructions");
        }
        let effort = match context {
        None => request.settings.reasoning_effort,
        Some(ctx) => ctx.model.filter(|model| model.reasoning.is_some()).and_then(|model| {
            let (effort, _) = model.clamp_effort(request.settings.reasoning_effort.unwrap_or(ReasoningEffort::Medium));
            let explicit_none = matches!(&model.reasoning, Some(ReasoningOptions::Effort(efforts)) if efforts.contains(&ReasoningEffort::None));
            (effort != ReasoningEffort::None || explicit_none).then_some(effort)
        }),
    };
        if let Some(effort) = effort {
            value["reasoning"] = json!({"effort": effort, "summary": "auto"});
        }
        if context.is_some_and(|ctx| {
            request.settings.model.starts_with("gpt-5")
                || ctx.model.is_some_and(|model| {
                    // Azure deployment ids can differ from the catalog's model name.
                    model.name.starts_with("GPT-5")
                        || model
                            .family
                            .as_deref()
                            .is_some_and(|family| family.starts_with("gpt-5"))
                })
        }) {
            value["text"] = json!({"verbosity": "low"});
        }
        // The ChatGPT Codex backend rejects `max_output_tokens` with a 400, so
        // a capped request (the worker's context summary) is sent uncapped there.
        if let Some(maximum) = request.settings.max_output_tokens
            && !context.is_some_and(|ctx| ctx.codex)
        {
            value["max_output_tokens"] = json!(maximum);
        }
        if let Some(temperature) = request.settings.temperature {
            if !temperature.is_finite() {
                return Err(Error::Protocol("temperature must be finite".into()));
            }
            value["temperature"] = json!(temperature);
        }
        Ok(value)
    }

    // Strict mode requires every object property to be required. Keep optional
    // parameters and schemas outside this supported subset unchanged and non-strict.
    fn strict_schema(schema: &Value) -> bool {
        let Some(object) = schema.as_object() else {
            return false;
        };
        if object.keys().any(|key| {
            !matches!(
                key.as_str(),
                "type"
                    | "properties"
                    | "required"
                    | "additionalProperties"
                    | "items"
                    | "enum"
                    | "description"
                    | "title"
                    | "anyOf"
            )
        }) {
            return false;
        }
        match schema["type"].as_str() {
            Some("object") => {
                let Some(properties) = schema["properties"].as_object() else {
                    return false;
                };
                schema["additionalProperties"] == false
                    && properties.iter().all(|(name, property)| {
                        schema["required"]
                            .as_array()
                            .is_some_and(|required| required.iter().any(|value| value == name))
                            && strict_schema(property)
                    })
            }
            Some("array") => strict_schema(&schema["items"]),
            Some("string" | "number" | "integer" | "boolean" | "null") => true,
            _ => schema["anyOf"].as_array().is_some_and(|alternatives| {
                !alternatives.is_empty() && alternatives.iter().all(strict_schema)
            }),
        }
    }

    fn text_item(role: &str, kind: &str, text: &str) -> Value {
        json!({"type": "message", "role": role, "content": [{"type": kind, "text": text}]})
    }

    fn normalize_calls(input: &mut Vec<Value>) {
        // Hash unsupported ids so replacements remain stable across calls and results
        // without colliding with another id after punctuation is removed.
        for item in input.iter_mut() {
            if let Some(id) = item["call_id"].as_str() {
                item["call_id"] = json!(crate::protocol::sanitize_tool_id(id, "", 64));
            }
        }
        crate::protocol::repair_tool_results(input, crate::protocol::ToolWire::Responses);
    }

    /// Incremental SSE parser, including CRLF, multiline data, comments, and UTF-8
    /// split across arbitrary network chunks. Unknown event types are ignored.
    #[derive(Default)]
    pub struct ResponsesStream {
        framing: SseParser,
        output: BTreeMap<usize, Vec<Part>>,
        text_content: BTreeMap<usize, BTreeMap<usize, String>>,
        saw_tool_arguments: bool,
        context: Option<(String, String)>,
        completed: bool,
        quota_remaining: BTreeMap<String, u64>,
        quota_resets: BTreeMap<String, u64>,
    }

    impl ResponsesStream {
        /// Record provenance for safe reasoning replay. The default parser retains
        /// the legacy metadata shape for callers of the original standalone codec.
        #[must_use]
        pub fn with_context(provider: &str, model: &str) -> Self {
            Self {
                context: Some((provider.into(), model.into())),
                ..Self::default()
            }
        }

        fn item_parts(&self, item: &Value) -> Result<Vec<Part>, Error> {
            let mut parts = item_parts(item)?;
            if let Some((provider, model)) = &self.context {
                for part in &mut parts {
                    if let Part::Reasoning { metadata, .. } = part {
                        metadata.clear();
                        metadata.insert(
                            "openai_responses".into(),
                            json!({"provider": provider, "model": model, "item": item}),
                        );
                    }
                }
            }
            Ok(parts)
        }
        #[must_use]
        pub const fn is_completed(&self) -> bool {
            self.completed
        }

        /// Capture `OpenAI` remaining-quota headers before streaming starts.
        pub fn set_quota(&mut self, quota: BTreeMap<String, u64>) {
            self.quota_remaining = quota;
        }

        /// Capture reset windows before streaming starts.
        pub fn set_quota_resets(&mut self, resets: BTreeMap<String, u64>) {
            self.quota_resets = resets;
        }

        /// # Errors
        /// Rejects malformed events, provider errors, and events over 8 MiB.
        pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Delta>, Error> {
            let mut deltas = Vec::new();
            for frame in self.framing.push(bytes)? {
                if self.completed {
                    break;
                }
                self.frame(frame, &mut deltas)?;
            }
            Ok(deltas)
        }

        fn frame(&mut self, frame: Frame, deltas: &mut Vec<Delta>) -> Result<(), Error> {
            if let Frame::Data(data) = frame {
                if data == b"[DONE]\n" {
                    return Err(Error::Protocol(
                        "stream ended without response.completed".into(),
                    ));
                }
                self.event(&serde_json::from_slice::<Value>(&data)?, deltas)?;
            }
            Ok(())
        }

        /// # Errors
        /// An EOF before a terminal response is an error, never a partial success.
        pub fn finish(&self) -> Result<(), Error> {
            if self.completed {
                Ok(())
            } else {
                Err(Error::MalformedStream(
                    "stream closed before completion".into(),
                ))
            }
        }

        fn save_text(&mut self, output_index: usize) -> Part {
            let part = Part::Text {
                text: self.text_content[&output_index]
                    .values()
                    .cloned()
                    .collect::<String>(),
            };
            self.output.insert(output_index, vec![part.clone()]);
            part
        }

        fn streamed_parts(&mut self) -> Vec<Part> {
            // Build unfinished text only at completion; copying it on every delta
            // makes long responses unnecessarily expensive.
            let unfinished: Vec<_> = self
                .text_content
                .keys()
                .filter(|index| !self.output.contains_key(index))
                .copied()
                .collect();
            for index in unfinished {
                self.save_text(index);
            }
            std::mem::take(&mut self.output)
                .into_values()
                .flatten()
                .collect()
        }

        fn event(&mut self, event: &Value, deltas: &mut Vec<Delta>) -> Result<(), Error> {
            match string(event, "type")? {
                "response.output_text.delta" | "response.refusal.delta" => {
                    let output_index = index(event)?;
                    let text = string(event, "delta")?.to_owned();
                    let content_index = content_index(event);
                    self.text_content
                        .entry(output_index)
                        .or_default()
                        .entry(content_index)
                        .or_default()
                        .push_str(&text);
                    deltas.push(Delta::Text { output_index, text });
                }
                "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => deltas
                    .push(Delta::Reasoning {
                        output_index: index(event)?,
                        text: string(event, "delta")?.to_owned(),
                    }),
                "response.function_call_arguments.delta" => {
                    self.saw_tool_arguments = true;
                    deltas.push(Delta::ToolArguments {
                        output_index: index(event)?,
                        arguments: string(event, "delta")?.to_owned(),
                    });
                }
                "response.output_text.done" => {
                    let output_index = index(event)?;
                    let content_index = content_index(event);
                    self.text_content
                        .entry(output_index)
                        .or_default()
                        .insert(content_index, string(event, "text")?.to_owned());
                    let part = self.save_text(output_index);
                    deltas.push(Delta::PartDone { output_index, part });
                }
                "response.output_item.done" => {
                    let output_index = index(event)?;
                    let parts = self.item_parts(&event["item"])?;
                    for part in &parts {
                        deltas.push(Delta::PartDone {
                            output_index,
                            part: part.clone(),
                        });
                    }
                    self.output.insert(output_index, parts);
                }
                "response.completed" | "response.incomplete" => {
                    self.complete_event(event, deltas)?;
                }
                "response.failed" => {
                    return Err(crate::error::response_event_error(
                        &event["response"]["error"],
                    ));
                }
                "error" => {
                    return Err(crate::error::response_event_error(
                        event.get("error").unwrap_or(event),
                    ));
                }
                _ => (),
            }
            Ok(())
        }

        fn complete_event(&mut self, event: &Value, deltas: &mut Vec<Delta>) -> Result<(), Error> {
            let response = &event["response"];
            if !response.is_object() {
                return Err(Error::MalformedStream("missing completed response".into()));
            }
            if response["status"] == "failed" {
                return Err(crate::error::response_event_error(&response["error"]));
            }
            // The Codex backend sends an empty output array in the terminal
            // event; the items already collected from output_item.done are
            // authoritative in that case.
            let parts: Vec<Part> = match response["output"].as_array() {
                Some(output) if !output.is_empty() => output
                    .iter()
                    .map(|item| self.item_parts(item))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten()
                    .collect(),
                _ => self.streamed_parts(),
            };
            let incomplete =
                event["type"] == "response.incomplete" || response["status"] == "incomplete";
            if incomplete
                && self.saw_tool_arguments
                && !parts
                    .iter()
                    .any(|part| matches!(part, Part::ToolCall { .. }))
            {
                tracing::debug!(
                    reason = response["incomplete_details"]["reason"]
                        .as_str()
                        .unwrap_or("unknown"),
                    "response.incomplete dropped a partially received tool call"
                );
            }
            let stop_reason = if incomplete {
                match response["incomplete_details"]["reason"]
                    .as_str()
                    .unwrap_or("unknown")
                {
                    "max_output_tokens" => StopReason::MaxOutputTokens,
                    "content_filter" => StopReason::ContentFilter,
                    reason => StopReason::Incomplete(reason.to_owned()),
                }
            } else if parts
                .iter()
                .any(|part| matches!(part, Part::ToolCall { .. }))
            {
                StopReason::ToolCalls
            } else {
                StopReason::EndTurn
            };
            deltas.push(Delta::Completed(Response {
                parts,
                stop_reason,
                usage: usage(&response["usage"]),
                quota_remaining: std::mem::take(&mut self.quota_remaining),
                quota_resets: std::mem::take(&mut self.quota_resets),
            }));
            self.completed = true;
            Ok(())
        }
    }

    fn content_index(event: &Value) -> usize {
        event["content_index"]
            .as_u64()
            .and_then(|i| usize::try_from(i).ok())
            .unwrap_or(0)
    }

    fn index(event: &Value) -> Result<usize, Error> {
        event["output_index"]
            .as_u64()
            .and_then(|i| usize::try_from(i).ok())
            .ok_or_else(|| Error::Protocol("missing output index".into()))
    }

    fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str, Error> {
        value[key]
            .as_str()
            .ok_or_else(|| Error::Protocol(format!("missing string field {key}")))
    }

    fn item_parts(item: &Value) -> Result<Vec<Part>, Error> {
        Ok(match string(item, "type")? {
            "message" => item["content"]
                .as_array()
                .ok_or_else(|| Error::Protocol("missing message content".into()))?
                .iter()
                .map(|content| {
                    let key = if content["type"] == "refusal" {
                        "refusal"
                    } else {
                        "text"
                    };
                    Ok(Part::Text {
                        text: string(content, key)?.to_owned(),
                    })
                })
                .collect::<Result<_, Error>>()?,
            "function_call" => vec![Part::ToolCall {
                call_id: ToolCallId(string(item, "call_id")?.to_owned()),
                tool: string(item, "name")?.to_owned(),
                input: serde_json::from_str(string(item, "arguments")?)?,
            }],
            "reasoning" => vec![Part::Reasoning {
                text: item["summary"]
                    .as_array()
                    .map(|parts| {
                        parts
                            .iter()
                            .filter_map(|part| part["text"].as_str())
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default(),
                metadata: BTreeMap::from([("chatgpt".into(), item.clone())]),
            }],
            kind => return Err(Error::Protocol(format!("unsupported output item {kind}"))),
        })
    }

    fn usage(value: &Value) -> TokenUsage {
        TokenUsage {
            input_tokens: value["input_tokens"].as_u64().unwrap_or(0),
            cached_input_tokens: value["input_tokens_details"]["cached_tokens"]
                .as_u64()
                .unwrap_or(0),
            output_tokens: value["output_tokens"].as_u64().unwrap_or(0),
            reasoning_output_tokens: value["output_tokens_details"]["reasoning_tokens"]
                .as_u64()
                .unwrap_or(0),
            total_tokens: value["total_tokens"].as_u64().unwrap_or(0),
            cache_write_input_tokens: 0,
        }
    }
}
pub use wire::{ResponsesStream, request_json_for};
