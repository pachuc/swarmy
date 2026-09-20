//! `ChatGPT` subscription transport; every request reloads its credential store.
use crate::{
    Error, Provider, ProviderStream, Request,
    auth::{CredentialStore, OAuthClient},
    responses::{SseParser, request_json},
};
use futures::StreamExt;
use std::{sync::Arc, time::Duration};

pub const BACKEND_BASE: &str = "https://chatgpt.com/backend-api/codex";
const USER_AGENT: &str = concat!("swarmy/", env!("CARGO_PKG_VERSION"));

#[derive(Clone)]
pub struct ChatGptProvider {
    store: Arc<dyn CredentialStore>,
    oauth: OAuthClient,
    client: reqwest::Client,
    base: String,
}

impl ChatGptProvider {
    /// # Errors
    /// Returns an error if the HTTP clients cannot be configured.
    pub fn new(store: Arc<dyn CredentialStore>) -> Result<Self, Error> {
        Self::with_endpoints(store, BACKEND_BASE, OAuthClient::new()?)
    }

    /// Override endpoints for a local protocol test server.
    /// # Errors
    /// Returns an error if the HTTP client cannot be configured.
    pub fn with_endpoints(
        store: Arc<dyn CredentialStore>,
        base: &str,
        oauth: OAuthClient,
    ) -> Result<Self, Error> {
        Ok(Self {
            store,
            oauth,
            base: base.trim_end_matches('/').to_owned(),
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(30))
                .read_timeout(Duration::from_secs(120))
                .build()?,
        })
    }
}

impl Provider for ChatGptProvider {
    fn request(&self, request: Request) -> ProviderStream {
        let provider = self.clone();
        Box::pin(async_stream::try_stream! {
            let body = request_json(&request)?;
            let mut credentials = provider.store.load().await?;
            if credentials.needs_refresh() {
                credentials = provider.oauth.refresh(provider.store.as_ref(), &credentials).await?;
            }
            let mut response = provider.send(&body, &credentials).await?;
            if response.status() == reqwest::StatusCode::UNAUTHORIZED {
                credentials = provider.oauth.refresh(provider.store.as_ref(), &credentials).await?;
                response = provider.send(&body, &credentials).await?;
            }
            if !response.status().is_success() { Err(Error::Status(response.status()))?; }
            // The live backend streams events without any Content-Type header, so
            // only an explicit non-stream type is rejected here; the parser rejects
            // bodies that are not event streams.
            let content_type = response.headers().get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|v| v.split(';').next().unwrap_or_default().trim().to_ascii_lowercase());
            if let Some(mime) = content_type.filter(|mime| mime != "text/event-stream") {
                Err(Error::Protocol(format!("expected text/event-stream, got {mime}")))?;
            }
            let mut bytes = response.bytes_stream();
            let mut parser = SseParser::default();
            while let Some(chunk) = bytes.next().await {
                for delta in parser.push(&chunk?)? { yield delta; }
                if parser.is_completed() { break; }
            }
            parser.finish()?;
        })
    }
}

impl ChatGptProvider {
    async fn send(
        &self,
        body: &serde_json::Value,
        credentials: &crate::auth::Credentials,
    ) -> Result<reqwest::Response, Error> {
        Ok(self
            .client
            .post(format!("{}/responses", self.base))
            .bearer_auth(credentials.access_token())
            .header("chatgpt-account-id", credentials.account_id())
            .header("accept", "text/event-stream")
            .header("OpenAI-Beta", "responses=experimental")
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .json(body)
            .send()
            .await?)
    }
}
