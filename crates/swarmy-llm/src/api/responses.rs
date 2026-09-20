//! Shared transport for direct Responses APIs and the Codex backend.
use std::{collections::BTreeMap, sync::Arc, time::Duration, time::SystemTime};

use futures::StreamExt;
use serde_json::Value;
use swarmy_core::SessionId;

use crate::{
    ClientAuth, Error, Provider, ProviderStream, Request,
    auth::{CredentialStore, Credentials, OAuthClient},
    catalog::{Api, Catalog, Compat, ModelInfo, ProviderInfo},
    responses::{SseParser, is_context_overflow, request_json_for},
    retry::{RetryPolicy, retryable, with_retry},
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
                ClientAuth::ApiKeyWithExtra { extra, .. }
                | ClientAuth::BearerWithExtra { extra, .. } => extra.get("resource_name").cloned(),
                _ => None,
            };
            let resource = extra
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
            let mut bytes = response.bytes_stream();
            let mut parser = SseParser::with_context(&provider.provider_id, &request.settings.model);
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
                Err(Error::Status(reqwest::StatusCode::UNAUTHORIZED)) => {
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
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(retry_after);
    let body = response.text().await?;
    if status == reqwest::StatusCode::PAYLOAD_TOO_LARGE || is_context_overflow(&body) {
        return Err(Error::ContextOverflow(body));
    }
    if retryable(status) {
        return Err(Error::Retryable {
            status,
            retry_after,
        });
    }
    Err(Error::Status(status))
}

fn retry_after(value: &str) -> Option<Duration> {
    value
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
        .or_else(|| {
            httpdate::parse_http_date(value)
                .ok()
                .map(|date| date.duration_since(SystemTime::now()).unwrap_or_default())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_accepts_seconds_and_http_dates() {
        assert_eq!(retry_after("12"), Some(Duration::from_secs(12)));
        assert_eq!(
            retry_after("Wed, 21 Oct 2015 07:28:00 GMT"),
            Some(Duration::ZERO)
        );
        assert!(retry_after("Wed, 21 Oct 2099 07:28:00 GMT").is_some());
        assert!(retry_after("invalid").is_none());
        assert!(!is_context_overflow("rate limit: too many tokens"));
    }
}
