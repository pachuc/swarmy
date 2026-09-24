//! HTTP client for the versioned swarmy API. No control-plane service libraries are linked.
use futures_util::{Stream, StreamExt};
use reqwest::{Method, Response, StatusCode};
use serde::{Serialize, de::DeserializeOwned};
use std::{collections::HashMap, pin::Pin, time::Duration};
use swarmy_api_types as api;
use thiserror::Error;
use tokio::sync::watch;

#[derive(Debug, Error)]
pub enum Error {
    #[error("API returned {status}: {body:?}")]
    Api {
        status: StatusCode,
        body: api::ApiError,
    },
    #[error("HTTP request failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("invalid API response: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("invalid stream UTF-8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("invalid base URL: {0}")]
    Url(#[from] url::ParseError),
    #[error("unexpected response status {status}: {body}")]
    Status { status: StatusCode, body: String },
}

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    base: url::Url,
    token: String,
}

impl Client {
    /// The base URL is the server origin, not a `/v1` URL.
    ///
    /// # Errors
    /// Returns an error for an invalid URL.
    pub fn new(base: &str, token: impl Into<String>) -> Result<Self, Error> {
        Ok(Self {
            http: reqwest::Client::new(),
            base: url::Url::parse(base)?,
            token: token.into(),
        })
    }
    fn url(&self, path: &str) -> url::Url {
        self.base
            .join(&format!("v1/{path}"))
            .expect("static API path")
    }
    async fn request<T: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
        query: &[(&str, String)],
    ) -> Result<T, Error> {
        let mut request = self
            .http
            .request(method, self.url(path))
            .bearer_auth(&self.token)
            .query(query);
        if let Some(body) = body {
            request = request.json(body);
        }
        decode(request.send().await?).await
    }
    async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T, Error> {
        self.request::<T, ()>(Method::GET, path, None, query).await
    }
    async fn send<T: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        body: &B,
    ) -> Result<T, Error> {
        self.request(method, path, Some(body), &[]).await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn health(&self) -> Result<serde_json::Value, Error> {
        self.get("health", &[]).await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn openapi(&self) -> Result<serde_json::Value, Error> {
        self.get("openapi.json", &[]).await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn agents(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<api::Agent>, Error> {
        self.get("agents", &page(after, limit)).await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn agent(&self, id: &str) -> Result<api::Agent, Error> {
        self.get(&format!("agents/{}", segment(id)), &[]).await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn create_agent(&self, body: &api::CreateAgent) -> Result<api::Agent, Error> {
        self.send(Method::POST, "agents", body).await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn update_agent(
        &self,
        id: &str,
        body: &api::UpdateAgent,
    ) -> Result<api::Agent, Error> {
        self.send(Method::PATCH, &format!("agents/{}", segment(id)), body)
            .await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn delete_agent(&self, id: &str, key: &str) -> Result<serde_json::Value, Error> {
        self.send(
            Method::DELETE,
            &format!("agents/{}", segment(id)),
            &serde_json::json!({"idempotency_key":key}),
        )
        .await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn sessions(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<api::Session>, Error> {
        self.get("sessions", &page(after, limit)).await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn session(&self, id: &str) -> Result<api::Session, Error> {
        self.get(&format!("sessions/{id}"), &[]).await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn create_session(&self, body: &api::CreateSession) -> Result<api::Session, Error> {
        self.send(Method::POST, "sessions", body).await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn append_message(
        &self,
        id: &str,
        body: &api::AppendMessage,
    ) -> Result<api::AppendedMessage, Error> {
        self.send(Method::POST, &format!("sessions/{id}/messages"), body)
            .await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn interrupt(
        &self,
        id: &str,
        body: &api::InterruptSession,
    ) -> Result<api::InterruptOutcome, Error> {
        self.send(Method::POST, &format!("sessions/{id}/interrupt"), body)
            .await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn close_session(
        &self,
        id: &str,
        body: &api::CloseSession,
    ) -> Result<api::SessionClosed, Error> {
        self.send(Method::DELETE, &format!("sessions/{id}"), body)
            .await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn wait_idle(
        &self,
        id: &str,
        after: u64,
        timeout_ms: u64,
    ) -> Result<api::Session, Error> {
        self.get(
            &format!("sessions/{id}/wait-idle"),
            &[
                ("after", after.to_string()),
                ("timeout_ms", timeout_ms.to_string()),
            ],
        )
        .await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn events(
        &self,
        id: &str,
        after: u64,
        limit: usize,
    ) -> Result<Vec<api::Event>, Error> {
        self.get(
            &format!("sessions/{id}/events"),
            &[("after", after.to_string()), ("limit", limit.to_string())],
        )
        .await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn images(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<api::Image>, Error> {
        self.get("images", &page(after, limit)).await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn image(&self, name: &str, tag: &str) -> Result<api::Image, Error> {
        self.get(&format!("images/{}/{}", segment(name), segment(tag)), &[])
            .await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn models(&self) -> Result<Vec<api::Model>, Error> {
        self.get("models", &[]).await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn search_models(&self, q: &str) -> Result<Vec<api::Model>, Error> {
        self.get("models/search", &[("q", q.into())]).await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn model(&self, provider: &str, model: &str) -> Result<api::Model, Error> {
        self.get(&format!("models/{provider}/{model}"), &[]).await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn providers(&self) -> Result<Vec<api::Provider>, Error> {
        self.get("providers", &[]).await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn credentials(&self) -> Result<Vec<api::Credential>, Error> {
        self.get("credentials", &[]).await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn credential(&self, provider: &str) -> Result<api::Credential, Error> {
        self.get(&format!("credentials/{}", segment(provider)), &[])
            .await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn set_credential(
        &self,
        body: &api::CreateCredential,
    ) -> Result<api::Credential, Error> {
        self.send(Method::POST, "credentials", body).await
    }
    /// Calls the corresponding API route.
    ///
    /// # Errors
    /// Returns an API, transport, or response decoding error.
    pub async fn remove_credential(
        &self,
        provider: &str,
        key: &str,
    ) -> Result<serde_json::Value, Error> {
        self.send(
            Method::DELETE,
            &format!("credentials/{}", segment(provider)),
            &serde_json::json!({"idempotency_key":key}),
        )
        .await
    }
    /// Read a CLI compatibility projection without linking the store.
    /// # Errors
    /// Returns transport, API, or decoding failures.
    pub async fn cli_sessions(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>, Error> {
        self.get("cli/sessions", &page(after, limit)).await
    }
    /// Read a CLI compatibility projection.
    /// # Errors
    /// Returns transport, API, or decoding failures.
    pub async fn cli_session(&self, id: &str) -> Result<serde_json::Value, Error> {
        self.get(&format!("cli/sessions/{}", segment(id)), &[])
            .await
    }
    /// Read a CLI compatibility projection.
    /// # Errors
    /// Returns transport, API, or decoding failures.
    pub async fn cli_agents(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>, Error> {
        self.get("cli/agents", &page(after, limit)).await
    }
    /// Read a CLI compatibility projection.
    /// # Errors
    /// Returns transport, API, or decoding failures.
    pub async fn cli_agent(&self, name: &str) -> Result<serde_json::Value, Error> {
        self.get(&format!("cli/agents/{}", segment(name)), &[])
            .await
    }
    /// Read image metadata used by the CLI.
    /// # Errors
    /// Returns transport, API, or decoding failures.
    pub async fn cli_image(&self, name: &str, tag: &str) -> Result<serde_json::Value, Error> {
        self.get(
            &format!("cli/images/{}/{}", segment(name), segment(tag)),
            &[],
        )
        .await
    }
    /// Read full catalog model rows for CLI rendering.
    /// # Errors
    /// Returns transport, API, or decoding failures.
    pub async fn cli_models(
        &self,
        q: Option<&str>,
        provider: Option<&str>,
        reasoning: bool,
    ) -> Result<Vec<serde_json::Value>, Error> {
        let mut query = vec![("reasoning", reasoning.to_string())];
        if let Some(q) = q {
            query.push(("q", q.into()));
        }
        if let Some(provider) = provider {
            query.push(("provider", provider.into()));
        }
        self.get("cli/models", &query).await
    }
    /// Read full provider rows for CLI rendering.
    /// # Errors
    /// Returns transport, API, or decoding failures.
    pub async fn cli_providers(&self) -> Result<Vec<serde_json::Value>, Error> {
        self.get("cli/providers", &[]).await
    }
    /// Submit a CLI management mutation.
    /// # Errors
    /// Returns transport, API, or decoding failures.
    pub async fn cli_create_agent(
        &self,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        self.send(Method::POST, "cli/agents", body).await
    }
    /// Submit a CLI management mutation.
    /// # Errors
    /// Returns transport, API, or decoding failures.
    pub async fn cli_update_agent(
        &self,
        name: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        self.send(
            Method::PATCH,
            &format!("cli/agents/{}/settings", segment(name)),
            body,
        )
        .await
    }
    /// Read credential metadata in the legacy CLI format.
    /// # Errors
    /// Returns transport, API, or decoding failures.
    pub async fn cli_credentials(&self) -> Result<Vec<serde_json::Value>, Error> {
        self.get("cli/credentials", &[]).await
    }
    /// Read credential metadata in the legacy CLI format.
    /// # Errors
    /// Returns transport, API, or decoding failures.
    pub async fn cli_credential(&self, provider: &str) -> Result<serde_json::Value, Error> {
        self.get(&format!("cli/credentials/{}", segment(provider)), &[])
            .await
    }
    /// Submit an encrypted credential through the API.
    /// # Errors
    /// Returns transport, API, or decoding failures.
    pub async fn cli_set_credential(
        &self,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        self.send(Method::POST, "cli/credentials", body).await
    }
    #[must_use]
    pub fn stream(&self, subscription: api::Subscription) -> EventStream {
        let (changes, _) = watch::channel(subscription.clone());
        EventStream {
            client: self.clone(),
            subscription,
            changes,
            response: None,
            buffer: Vec::new(),
            connection_id: None,
            delay: Duration::from_millis(100),
            retry_floor: Duration::ZERO,
        }
    }
}
fn segment(value: &str) -> String {
    let mut url = url::Url::parse("http://unused/").expect("static origin");
    url.path_segments_mut()
        .expect("hierarchical URL")
        .push(value);
    url.path().trim_start_matches('/').to_owned()
}
fn page(after: Option<&str>, limit: usize) -> Vec<(&'static str, String)> {
    let mut result = vec![("limit", limit.to_string())];
    if let Some(after) = after {
        result.push(("after", after.into()));
    }
    result
}
async fn decode<T: DeserializeOwned>(response: Response) -> Result<T, Error> {
    let status = response.status();
    let bytes = response.bytes().await?;
    if !status.is_success() {
        return Err(match serde_json::from_slice(&bytes) {
            Ok(body) => Error::Api { status, body },
            Err(_) => Error::Status {
                status,
                body: String::from_utf8_lossy(&bytes).into_owned(),
            },
        });
    }
    Ok(serde_json::from_slice(&bytes)?)
}

type ByteStream = Pin<Box<dyn Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>>;
/// Clone this handle to change the subscribed logs while another task awaits events.
#[derive(Clone)]
pub struct SubscriptionHandle(watch::Sender<api::Subscription>);
impl SubscriptionHandle {
    /// Request a change on the open stream. Invalid changes are reported once
    /// by `next` or `next_item`; set a valid subscription to try again.
    pub fn set(&self, subscription: api::Subscription) {
        self.0.send_replace(subscription);
    }
}

/// Durable events advance their log cursor. Live token deltas are not replayable.
#[derive(Debug)]
pub enum StreamItem {
    Event(api::Event),
    TokenDelta {
        log_id: api::LogId,
        payload: api::EventPayload,
    },
}

pub struct EventStream {
    client: Client,
    subscription: api::Subscription,
    changes: watch::Sender<api::Subscription>,
    response: Option<ByteStream>,
    buffer: Vec<u8>,
    connection_id: Option<String>,
    delay: Duration,
    retry_floor: Duration,
}
impl EventStream {
    #[must_use]
    pub fn subscription_handle(&self) -> SubscriptionHandle {
        SubscriptionHandle(self.changes.clone())
    }
    #[must_use]
    pub fn cursors(&self) -> &[api::Cursor] {
        &self.subscription.cursors
    }
    async fn connect(&mut self) -> Result<(), Error> {
        let query = serde_json::to_string(&self.subscription)?;
        let response = self
            .client
            .http
            .get(self.client.url("events"))
            .bearer_auth(&self.client.token)
            .query(&[("subscription", query)])
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(decode::<serde_json::Value>(response).await.unwrap_err());
        }
        self.connection_id = response
            .headers()
            .get("x-swarmy-connection-id")
            .and_then(|h| h.to_str().ok())
            .map(str::to_owned);
        self.response = Some(Box::pin(response.bytes_stream()));
        self.buffer.clear();
        Ok(())
    }
    async fn update(&mut self) -> Result<(), Error> {
        let requested = self.changes.borrow().clone();
        let old: HashMap<_, _> = self
            .subscription
            .cursors
            .iter()
            .map(|c| (log_key(&c.log_id), c.sequence))
            .collect();
        let mut next = requested;
        for cursor in &mut next.cursors {
            if let Some(sequence) = old.get(&log_key(&cursor.log_id)) {
                cursor.sequence = cursor.sequence.max(*sequence);
            }
        }
        if let Some(id) = &self.connection_id {
            let response = self
                .client
                .http
                .put(self.client.url(&format!("events/{id}/subscription")))
                .bearer_auth(&self.client.token)
                .json(&next)
                .send()
                .await?;
            if !response.status().is_success() {
                let failure = decode::<serde_json::Value>(response).await.unwrap_err();
                // A rejected change must not be sent again on the next poll.
                if retryable(&failure) {
                    // A transient rejection may have happened after the update was
                    // applied. Reconnect with delivered cursors and the new selection.
                    self.response = None;
                    self.connection_id = None;
                    self.buffer.clear();
                    self.subscription = next;
                    self.backoff().await;
                    return Ok(());
                }
                self.changes.send_replace(self.subscription.clone());
                return Err(failure);
            }
        }
        self.subscription = next;
        Ok(())
    }
    /// Returns the next durable event. Connection failures are retried indefinitely;
    /// protocol and API errors are returned. Cursor advancement happens only on delivery.
    ///
    /// # Errors
    /// Returns an API or decoding error.
    pub async fn next(&mut self) -> Result<api::Event, Error> {
        loop {
            if let StreamItem::Event(event) = self.next_item().await? {
                return Ok(event);
            }
        }
    }
    /// Receive durable events or opt-in live token deltas.
    ///
    /// # Errors
    /// Returns an API or decoding error.
    pub async fn next_item(&mut self) -> Result<StreamItem, Error> {
        let mut changes = self.changes.subscribe();
        'receive: loop {
            if subscription_changed(&changes.borrow(), &self.subscription) {
                changes.borrow_and_update();
                self.update().await?;
            }
            if self.response.is_none()
                && let Err(error) = self.connect().await
            {
                if !retryable(&error) {
                    return Err(error);
                }
                self.backoff().await;
                continue;
            }
            // Drain complete frames before reading again; one network chunk may contain many events.
            while let Some(end) = self.buffer.windows(2).position(|part| part == b"\n\n") {
                let frame = self.buffer.drain(..end + 2).collect::<Vec<_>>();
                let frame = String::from_utf8(frame)?;
                let parsed = parse_frame(&frame)?;
                if let Some(retry) = parsed.retry {
                    self.retry_floor = retry;
                }
                if let Some(item) = parsed.item {
                    let StreamItem::Event(event) = item else {
                        return Ok(item);
                    };
                    if let Some(cursor) = self
                        .subscription
                        .cursors
                        .iter_mut()
                        .find(|c| c.log_id == event.log_id)
                    {
                        if event.sequence <= cursor.sequence {
                            continue;
                        }
                        if event.sequence != cursor.sequence + 1 {
                            // Discard queued data and request replay from the delivered cursor.
                            self.response = None;
                            self.connection_id = None;
                            self.buffer.clear();
                            self.backoff().await;
                            continue 'receive;
                        }
                        cursor.sequence = event.sequence;
                        self.delay = Duration::from_millis(100);
                        return Ok(StreamItem::Event(event));
                    }
                }
            }
            tokio::select! {
                result = async { if let Some(stream) = self.response.as_mut() { stream.next().await } else { None } } => {
                    if let Some(Ok(bytes)) = result { self.buffer.extend_from_slice(&bytes); }
                    else { self.response = None; self.connection_id = None; self.backoff().await; }
                }
                notification = changes.changed() => {
                    if notification.is_ok() { changes.borrow_and_update(); self.update().await?; }
                }
            }
        }
    }
    /// Reopen after a caller-visible error, retaining delivered cursors.
    pub async fn restart(&mut self) {
        self.response = None;
        self.connection_id = None;
        self.buffer.clear();
        self.backoff().await;
    }
    async fn backoff(&mut self) {
        let base = self.delay.max(self.retry_floor);
        let jitter =
            Duration::from_millis(rand::random_range(0..=base.as_millis().min(1000) as u64));
        tokio::time::sleep(base + jitter).await;
        self.delay = (self.delay * 2).min(Duration::from_secs(5));
    }
}
fn retryable(error: &Error) -> bool {
    match error {
        Error::Api { status, .. } | Error::Status { status, .. } => {
            status.is_server_error() || *status == StatusCode::TOO_MANY_REQUESTS
        }
        _ => true,
    }
}
fn subscription_changed(requested: &api::Subscription, current: &api::Subscription) -> bool {
    requested.token_deltas != current.token_deltas
        || requested.cursors.len() != current.cursors.len()
        || requested
            .cursors
            .iter()
            .zip(&current.cursors)
            .any(|(want, have)| want.log_id != have.log_id || want.sequence > have.sequence)
}
fn log_key(log: &api::LogId) -> String {
    format!("{log:?}")
}
struct ParsedFrame {
    item: Option<StreamItem>,
    retry: Option<Duration>,
}
fn parse_frame(frame: &str) -> Result<ParsedFrame, Error> {
    let mut kind = "";
    let mut data = String::new();
    let mut retry = None;
    for line in frame.lines() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if let Some(value) = line.strip_prefix("event:") {
            kind = value.trim();
        }
        if let Some(value) = line.strip_prefix("retry:") {
            retry = value.trim().parse::<u64>().ok().map(Duration::from_millis);
        }
        if let Some(value) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(value.trim_start());
        }
    }
    let item = match kind {
        "event" => Some(StreamItem::Event(serde_json::from_str(&data)?)),
        "token_delta" => {
            #[derive(serde::Deserialize)]
            struct Delta {
                log_id: api::LogId,
                payload: api::EventPayload,
            }
            let delta: Delta = serde_json::from_str(&data)?;
            Some(StreamItem::TokenDelta {
                log_id: delta.log_id,
                payload: delta.payload,
            })
        }
        _ => None,
    };
    Ok(ParsedFrame { item, retry })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        extract::{Query, State},
        http::StatusCode as HttpStatus,
        response::IntoResponse,
        response::sse::{Event as SseEvent, Sse},
        routing::get,
    };
    use std::{
        convert::Infallible,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    async fn stream_handler(
        State(head): State<Arc<AtomicU64>>,
        Query(query): Query<HashMap<String, String>>,
    ) -> impl axum::response::IntoResponse {
        let sub: api::Subscription = serde_json::from_str(&query["subscription"]).unwrap();
        let start = sub.cursors[0].sequence;
        let end = head.load(Ordering::SeqCst);
        let events = (start + 1..=end).map(move |sequence| {
            let event = api::Event {
                log_id: api::LogId::Session("s".into()),
                sequence,
                payload: api::EventPayload::Idle {
                    session_id: "s".into(),
                },
            };
            Ok::<_, Infallible>(
                SseEvent::default()
                    .event("event")
                    .data(serde_json::to_string(&event).unwrap()),
            )
        });
        Sse::new(futures_util::stream::iter(events))
    }
    fn serve(
        listener: tokio::net::TcpListener,
        head: Arc<AtomicU64>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let app = Router::new()
                .route("/v1/events", get(stream_handler))
                .with_state(head);
            axum::serve(listener, app).await.unwrap();
        })
    }
    #[tokio::test]
    async fn resumes_after_server_restart_without_duplicate() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let head = Arc::new(AtomicU64::new(2));
        let server = serve(listener, head.clone());
        let client = Client::new(&format!("http://{address}"), "token").unwrap();
        let mut stream = client.stream(api::Subscription {
            cursors: vec![api::Cursor {
                log_id: api::LogId::Session("s".into()),
                sequence: 0,
            }],
            token_deltas: false,
        });
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), stream.next())
                .await
                .unwrap()
                .unwrap()
                .sequence,
            1
        );
        assert_eq!(stream.next().await.unwrap().sequence, 2);
        server.abort();
        server.await.unwrap_err();
        head.store(4, Ordering::SeqCst);
        let listener = tokio::net::TcpListener::bind(address).await.unwrap();
        let _server = serve(listener, head);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), stream.next())
                .await
                .unwrap()
                .unwrap()
                .sequence,
            3
        );
        assert_eq!(stream.next().await.unwrap().sequence, 4);
    }
    #[test]
    fn parses_token_delta_without_advancing_cursor() {
        let item = parse_frame("event: token_delta\ndata: {\"log_id\":{\"kind\":\"session\",\"id\":\"s\"},\"payload\":{\"type\":\"token_delta\",\"data\":{\"turn_id\":\"t\",\"position\":0,\"text\":\"hi\"}}}\n\n").unwrap();
        assert!(
            matches!(item.item, Some(StreamItem::TokenDelta { log_id: api::LogId::Session(id), payload: api::EventPayload::TokenDelta { text, .. } }) if id == "s" && text == "hi")
        );
    }
    #[tokio::test]
    async fn updates_subscription_on_open_connection() {
        use axum::{http::HeaderMap, routing::put};
        let (sent, mut received) = tokio::sync::mpsc::channel::<api::Subscription>(1);
        let app = Router::new()
            .route(
                "/v1/events",
                get(|| async {
                    let stream = futures_util::stream::once(async {
                        Ok::<_, Infallible>(SseEvent::default().event("connected").data("{}"))
                    })
                    .chain(futures_util::stream::pending());
                    let mut headers = HeaderMap::new();
                    headers.insert("x-swarmy-connection-id", "connection".parse().unwrap());
                    (headers, Sse::new(stream))
                }),
            )
            .route(
                "/v1/events/connection/subscription",
                put(move |Json(sub): Json<api::Subscription>| {
                    let sent = sent.clone();
                    async move {
                        sent.send(sub).await.unwrap();
                        HttpStatus::NO_CONTENT
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = Client::new(&format!("http://{address}"), "token").unwrap();
        let first = api::Subscription {
            cursors: vec![api::Cursor {
                log_id: api::LogId::Session("s".into()),
                sequence: 0,
            }],
            token_deltas: false,
        };
        let mut stream = client.stream(first);
        let handle = stream.subscription_handle();
        // Poll next so the initial connection has opened before changing it.
        let mut pending = Box::pin(stream.next());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut pending)
                .await
                .is_err()
        );
        drop(pending);
        let updated = api::Subscription {
            cursors: vec![api::Cursor {
                log_id: api::LogId::Session("s".into()),
                sequence: 0,
            }],
            token_deltas: true,
        };
        handle.set(updated.clone());
        let _ = tokio::time::timeout(Duration::from_millis(100), stream.next()).await;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), received.recv())
                .await
                .unwrap()
                .unwrap(),
            updated
        );
    }

    #[tokio::test]
    async fn gap_reconnects_from_delivered_cursor() {
        let attempts = Arc::new(AtomicU64::new(0));
        let app = Router::new().route(
            "/v1/events",
            get({
                let attempts = attempts.clone();
                move || {
                    let attempts = attempts.clone();
                    async move {
                        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                        let numbers = if attempt == 0 { vec![2] } else { vec![1, 2] };
                        let events = numbers.into_iter().map(|sequence| {
                            let event = api::Event {
                                log_id: api::LogId::Session("s".into()),
                                sequence,
                                payload: api::EventPayload::Idle {
                                    session_id: "s".into(),
                                },
                            };
                            Ok::<_, Infallible>(
                                SseEvent::default()
                                    .event("event")
                                    .data(serde_json::to_string(&event).unwrap()),
                            )
                        });
                        Sse::new(futures_util::stream::iter(events))
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = Client::new(&format!("http://{address}"), "token").unwrap();
        let mut stream = client.stream(api::Subscription {
            cursors: vec![api::Cursor {
                log_id: api::LogId::Session("s".into()),
                sequence: 0,
            }],
            token_deltas: false,
        });
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), stream.next())
                .await
                .unwrap()
                .unwrap()
                .sequence,
            1
        );
        assert_eq!(stream.next().await.unwrap().sequence, 2);
        assert!(attempts.load(Ordering::SeqCst) >= 2);
    }
    #[test]
    fn path_segments_are_encoded() {
        assert_eq!(segment("a/b ?"), "a%2Fb%20%3F");
    }
    #[tokio::test]
    async fn non_json_four_xx_keeps_body() {
        let app = Router::new().route(
            "/v1/agents/missing",
            get(|| async { (HttpStatus::BAD_REQUEST, "malformed path") }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = Client::new(&format!("http://{address}"), "token").unwrap();
        let Err(Error::Status { status, body }) = client.agent("missing").await else {
            panic!("expected status")
        };
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, "malformed path");
    }
    #[tokio::test]
    async fn stream_retries_transient_status_and_obeys_retry_floor() {
        let attempts = Arc::new(AtomicU64::new(0));
        let app = Router::new().route(
            "/v1/events",
            get({
                let attempts = attempts.clone();
                move || {
                    let attempts = attempts.clone();
                    async move {
                        if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                            return (HttpStatus::SERVICE_UNAVAILABLE, "unavailable")
                                .into_response();
                        }
                        let event = api::Event {
                            log_id: api::LogId::Session("s".into()),
                            sequence: 1,
                            payload: api::EventPayload::Idle {
                                session_id: "s".into(),
                            },
                        };
                        let events = futures_util::stream::iter([
                            Ok::<_, Infallible>(
                                SseEvent::default()
                                    .event("connected")
                                    .retry(Duration::from_millis(200))
                                    .data("{}"),
                            ),
                            Ok(SseEvent::default()
                                .event("event")
                                .data(serde_json::to_string(&event).unwrap())),
                        ]);
                        Sse::new(events).into_response()
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = Client::new(&format!("http://{address}"), "token").unwrap();
        let mut stream = client.stream(api::Subscription {
            cursors: vec![api::Cursor {
                log_id: api::LogId::Session("s".into()),
                sequence: 0,
            }],
            token_deltas: false,
        });
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), stream.next())
                .await
                .unwrap()
                .unwrap()
                .sequence,
            1
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(stream.retry_floor, Duration::from_millis(200));
    }

    #[tokio::test]
    async fn preserves_error_code_and_provider_text() {
        let app = Router::new().route(
            "/v1/agents/missing",
            get(|| async {
                (
                    HttpStatus::BAD_GATEWAY,
                    Json(api::ApiError {
                        code: "provider_failure".into(),
                        message: "failed".into(),
                        provider_text: Some("original".into()),
                    }),
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = Client::new(&format!("http://{address}"), "token").unwrap();
        let Err(Error::Api { status, body }) = client.agent("missing").await else {
            panic!("expected API error")
        };
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(body.code, "provider_failure");
        assert_eq!(body.provider_text.as_deref(), Some("original"));
    }
}
