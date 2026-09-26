//! A subscription is registered before replay, and the store remains the
//! authority for every durable event after the live handover.
use super::{AppState, error, storage};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::{
    StreamExt,
    stream::{BoxStream, SelectAll},
};
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    sync::Arc,
    time::Duration,
};
use swarmy_api_types::{self as api, LogId, Subscription};
use swarmy_bus::LiveFeed;
use swarmy_core::{LiveTokenDelta, SessionId, TurnEvent};
use swarmy_store::MAX_SCAN_LIMIT;
use tokio::{
    sync::{mpsc, watch},
    time::interval,
};
use ulid::Ulid;

const OUTBOUND_CAPACITY: usize = 64;
const MAX_LOGS: usize = 32;
type ApiError = (StatusCode, Json<api::ApiError>);
type Registry = Arc<std::sync::Mutex<HashMap<String, Connection>>>;

#[derive(Clone)]
pub(crate) struct Connection {
    sender: watch::Sender<Subscription>,
    progress: Arc<std::sync::Mutex<Subscription>>,
}

#[derive(Deserialize)]
pub struct StreamQuery {
    subscription: Option<String>,
}

fn key(log: &LogId) -> String {
    match log {
        LogId::Session(id) => format!("session:{id}"),
        LogId::Channel(id) => format!("channel:{id}"),
        LogId::Timeline(id) => format!("timeline:{id}"),
    }
}
fn session_id(log: &LogId) -> Result<SessionId, ApiError> {
    let LogId::Session(text) = log else {
        return Err(error(StatusCode::BAD_REQUEST, "unsupported_log"));
    };
    text.parse::<Ulid>()
        .map(SessionId::from_ulid)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid_log_id"))
}
/// Timeline cursors name the session whose turn observations are followed.
/// The session must exist, but observations are live-only and never replayed.
fn timeline_id(log: &LogId) -> Result<SessionId, ApiError> {
    let LogId::Timeline(text) = log else {
        return Err(error(StatusCode::BAD_REQUEST, "unsupported_log"));
    };
    text.parse::<Ulid>()
        .map(SessionId::from_ulid)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid_log_id"))
}
async fn validate(state: &AppState, subscription: &Subscription) -> Result<(), ApiError> {
    if subscription.cursors.is_empty() || subscription.cursors.len() > MAX_LOGS {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_subscription"));
    }
    let mut seen = HashSet::new();
    for cursor in &subscription.cursors {
        if !seen.insert(key(&cursor.log_id)) {
            return Err(error(StatusCode::BAD_REQUEST, "duplicate_log"));
        }
        let id = match &cursor.log_id {
            LogId::Timeline(_) => timeline_id(&cursor.log_id)?,
            _ => session_id(&cursor.log_id)?,
        };
        if state
            .store
            .fetch_session(id)
            .await
            .map_err(storage)?
            .is_none()
        {
            return Err(error(StatusCode::NOT_FOUND, "session_not_found"));
        }
    }
    Ok(())
}
fn encode_cursor(subscription: &Subscription) -> Result<String, ApiError> {
    serde_json::to_vec(subscription)
        .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "cursor_encoding"))
}
fn decode_cursor(text: &str) -> Result<Subscription, ApiError> {
    if text.len() > 8192 {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_cursor"));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(text)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid_cursor"))?;
    serde_json::from_slice(&bytes).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid_cursor"))
}

/// GET /v1/events?subscription=<URL-encoded Subscription JSON>.
/// A Last-Event-ID overrides the original set as well as its cursors, so a
/// normal `EventSource` reconnect works after subscription changes. With that
/// header, the subscription query parameter can be omitted.
pub async fn subscribe(
    State(state): State<AppState>,
    Query(query): Query<StreamQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let subscription = if let Some(header) = headers.get("last-event-id") {
        decode_cursor(
            header
                .to_str()
                .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid_cursor"))?,
        )?
    } else {
        serde_json::from_str(
            query
                .subscription
                .as_deref()
                .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid_subscription"))?,
        )
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid_subscription"))?
    };
    validate(&state, &subscription).await?;
    // Install the live subscriptions before headers become visible to a client.
    // Durable records can replay, but a token emitted in this window cannot.
    let initial_feeds = feeds(&state, &subscription)
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "subscription_unavailable"))?;
    let connection_id = Ulid::generate().to_string();
    let (changes, receiver) = watch::channel(subscription.clone());
    let progress = Arc::new(std::sync::Mutex::new(subscription.clone()));
    state
        .stream_connections
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(
            connection_id.clone(),
            Connection {
                sender: changes,
                progress: progress.clone(),
            },
        );
    let guard = ConnectionGuard {
        id: connection_id.clone(),
        registry: state.stream_connections.clone(),
        progress,
    };
    let (sender, mut outbound) = mpsc::channel(OUTBOUND_CAPACITY);
    let initial = Event::default()
        .event("connected")
        .retry(Duration::from_secs(1))
        .id(encode_cursor(&subscription)?)
        .data(serde_json::json!({"connection_id": connection_id}).to_string());
    let _ = sender.try_send(initial);
    tokio::spawn(produce(
        state.clone(),
        receiver,
        sender,
        guard,
        initial_feeds,
    ));
    let body = async_stream::stream! {
        while let Some(event) = outbound.recv().await { yield Ok::<Event, Infallible>(event); }
    };
    let mut response = Sse::new(body)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("heartbeat"),
        )
        .into_response();
    response.headers_mut().insert(
        "x-swarmy-connection-id",
        HeaderValue::from_str(&connection_id)
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "connection_id"))?,
    );
    Ok(response)
}

/// Replace the subscribed logs and token preference without closing the stream.
pub async fn update(
    State(state): State<AppState>,
    Path(connection_id): Path<String>,
    Json(subscription): Json<Subscription>,
) -> Result<StatusCode, ApiError> {
    validate(&state, &subscription).await?;
    let sender = state
        .stream_connections
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&connection_id)
        .cloned()
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "connection_not_found"))?;
    let mut progress = sender
        .progress
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let previous: HashMap<_, _> = progress
        .cursors
        .iter()
        .map(|c| (key(&c.log_id), c.sequence))
        .collect();
    for cursor in &subscription.cursors {
        if previous
            .get(&key(&cursor.log_id))
            .is_some_and(|old| cursor.sequence < *old)
        {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(api::ApiError {
                    code: "cursor_rewind".into(),
                    message: format!("cursor rewind for log {}", key(&cursor.log_id)),
                    provider_text: None,
                }),
            ));
        }
    }
    // The producer may already be delivering another event. Its progress is
    // monotone, so it can safely catch up beyond this requested cursor.
    sender
        .sender
        .send(subscription.clone())
        .map_err(|_| error(StatusCode::NOT_FOUND, "connection_not_found"))?;
    *progress = subscription;
    Ok(StatusCode::NO_CONTENT)
}

struct ConnectionGuard {
    id: String,
    registry: Registry,
    progress: Arc<std::sync::Mutex<Subscription>>,
}
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
}

enum FeedItem {
    Durable(SessionId),
    Token(LogId, LiveTokenDelta),
    Timeline(LogId, u64, TurnEvent),
}
async fn feeds(
    state: &AppState,
    subscription: &Subscription,
) -> Result<SelectAll<BoxStream<'static, FeedItem>>, swarmy_bus::Error> {
    let mut all = SelectAll::new();
    for cursor in &subscription.cursors {
        if matches!(cursor.log_id, LogId::Timeline(_)) {
            let id = timeline_id(&cursor.log_id).expect("validated subscription");
            let log = cursor.log_id.clone();
            // Live-only observations are numbered per connection from the
            // subscribed cursor, so a reconnect resumes without duplicates.
            let mut sequence = cursor.sequence;
            let mut observations = state
                .bus
                .subscribe_live::<TurnEvent>(LiveFeed::TurnTimeline(id))
                .await?;
            all.push(Box::pin(async_stream::stream! {
                while let Some(value) = observations.next().await {
                    if let Ok(event) = value {
                        sequence += 1;
                        yield FeedItem::Timeline(log.clone(), sequence, event);
                    }
                }
            }) as BoxStream<'static, FeedItem>);
            continue;
        }
        let id = session_id(&cursor.log_id).expect("validated subscription");
        let mut events = state
            .bus
            .subscribe_live::<swarmy_core::Event>(LiveFeed::SessionEvents(id))
            .await?;
        all.push(Box::pin(async_stream::stream! {
            while let Some(value) = events.next().await {
                if value.is_ok() { yield FeedItem::Durable(id); }
            }
        }) as BoxStream<'static, FeedItem>);
        if subscription.token_deltas {
            let log = cursor.log_id.clone();
            let mut tokens = state
                .bus
                .subscribe_live::<LiveTokenDelta>(LiveFeed::ApiTokenDeltas(id))
                .await?;
            all.push(Box::pin(async_stream::stream! {
                while let Some(value) = tokens.next().await {
                    if let Ok(value) = value { yield FeedItem::Token(log.clone(), value); }
                }
            }) as BoxStream<'static, FeedItem>);
        }
    }
    Ok(all)
}

async fn deliver(sender: &mpsc::Sender<Event>, event: Event, deadline: Duration) -> bool {
    // A full fixed-size queue backpressures a fast replay briefly. A client
    // that remains slow is disconnected; the first SSE event set its retry hint.
    tokio::time::timeout(deadline, sender.send(event))
        .await
        .is_ok_and(|result| result.is_ok())
}
#[derive(PartialEq, Eq)]
enum Replay {
    Done,
    Updated,
    Failed,
}
async fn catch_up(
    state: &AppState,
    sub: &mut Subscription,
    index: usize,
    sender: &mpsc::Sender<Event>,
    changes: &watch::Receiver<Subscription>,
    progress: &Arc<std::sync::Mutex<Subscription>>,
) -> Replay {
    if matches!(sub.cursors[index].log_id, LogId::Timeline(_)) {
        return Replay::Done;
    }
    let id = session_id(&sub.cursors[index].log_id).expect("validated subscription");
    loop {
        let after = sub.cursors[index].sequence;
        let page = match state.store.read_events(id, after, MAX_SCAN_LIMIT).await {
            Ok(page) => page,
            Err(error) => {
                tracing::warn!(%error, "SSE replay failed");
                return Replay::Failed;
            }
        };
        if page.is_empty() {
            return Replay::Done;
        }
        for record in page {
            if record.seq() <= sub.cursors[index].sequence {
                continue;
            }
            let payload = match serde_json::to_value(&record) {
                Ok(record) => api::EventPayload::StoreRecord { record },
                Err(error) => {
                    tracing::warn!(%error, "SSE event encoding failed");
                    return Replay::Failed;
                }
            };
            // A cursor only advances when the corresponding event has entered
            // the bounded output queue. The client can replay after a disconnect.
            let next = record.seq();
            let mut upcoming = sub.clone();
            upcoming.cursors[index].sequence = next;
            let Ok(id_field) = encode_cursor(&upcoming) else {
                return Replay::Failed;
            };
            let Ok(data) = serde_json::to_string(&api::Event {
                log_id: sub.cursors[index].log_id.clone(),
                sequence: next,
                payload,
            }) else {
                return Replay::Failed;
            };
            if !deliver(
                sender,
                Event::default().event("event").id(id_field).data(data),
                Duration::from_secs(2),
            )
            .await
            {
                return Replay::Failed;
            }
            sub.cursors[index].sequence = next;
            let mut seen = progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(cursor) = seen
                .cursors
                .iter_mut()
                .find(|c| c.log_id == sub.cursors[index].log_id)
            {
                cursor.sequence = cursor.sequence.max(next);
            }
        }
        if changes.has_changed().unwrap_or(false) {
            return Replay::Updated;
        }
    }
}
async fn catch_all(
    state: &AppState,
    sub: &mut Subscription,
    sender: &mpsc::Sender<Event>,
    changes: &watch::Receiver<Subscription>,
    progress: &Arc<std::sync::Mutex<Subscription>>,
) -> Replay {
    for index in 0..sub.cursors.len() {
        match catch_up(state, sub, index, sender, changes, progress).await {
            Replay::Done => {}
            other => return other,
        }
    }
    Replay::Done
}
async fn produce(
    state: AppState,
    mut changes: watch::Receiver<Subscription>,
    sender: mpsc::Sender<Event>,
    guard: ConnectionGuard,
    initial_feeds: SelectAll<BoxStream<'static, FeedItem>>,
) {
    let mut first = Some(initial_feeds);
    let mut current = changes.borrow().clone();
    let mut pending_update = false;
    let mut force_update = false;
    'reconfigure: loop {
        if force_update || changes.has_changed().unwrap_or(false) {
            force_update = false;
            let requested = changes.borrow_and_update().clone();
            current = requested;
            pending_update = true;
            first = None;
        }
        // Register live feeds before reading the log. A notification is only a
        // nudge; rereading the store also repairs a dropped NATS publication.
        let mut live = match if let Some(live) = first.take() {
            Ok(live)
        } else {
            feeds(&state, &current).await
        } {
            Ok(live) => live,
            Err(error) => {
                tracing::warn!(%error, "SSE subscription failed");
                return;
            }
        };
        match catch_all(&state, &mut current, &sender, &changes, &guard.progress).await {
            Replay::Done => {}
            Replay::Updated => continue 'reconfigure,
            Replay::Failed => return,
        }
        if pending_update {
            let Ok(id) = encode_cursor(&current) else {
                return;
            };
            if !deliver(
                &sender,
                Event::default().event("subscription").id(id).data("{}"),
                Duration::from_secs(2),
            )
            .await
            {
                return;
            }
            pending_update = false;
        }
        let mut poll = interval(state.stream_poll_interval);
        poll.tick().await;
        loop {
            tokio::select! {
                () = sender.closed() => return,
                updated = changes.changed() => {
                    if updated.is_err() { return; }
                    force_update = true;
                    continue 'reconfigure;
                }
                _ = poll.tick() => {
                    match catch_all(&state, &mut current, &sender, &changes, &guard.progress).await {
                        Replay::Done => {}
                        Replay::Updated => continue 'reconfigure,
                        Replay::Failed => return,
                    }
                }
                item = live.next() => {
                    match item {
                        Some(FeedItem::Durable(id)) => {
                            if let Some(index) = current.cursors.iter().position(|c| session_id(&c.log_id).ok() == Some(id))
                                {
                                match catch_up(&state, &mut current, index, &sender, &changes, &guard.progress).await {
                                    Replay::Done => {}
                                    Replay::Updated => continue 'reconfigure,
                                    Replay::Failed => return,
                                }
                            }
                        }
                        Some(FeedItem::Token(log, delta)) => {
                            let payload = api::EventPayload::TokenDelta {
                                turn_id: delta.turn_id, position: delta.position, text: delta.text,
                            };
                            let Ok(data) = serde_json::to_string(&serde_json::json!({"log_id": log, "payload": payload})) else { return };
                            if !deliver(&sender, Event::default().event("token_delta").data(data), Duration::from_secs(2)).await { return; }
                        }
                        Some(FeedItem::Timeline(log, sequence, observation)) => {
                            let Ok(record) = serde_json::to_value(&observation) else { return };
                            let Ok(data) = serde_json::to_string(&api::Event {
                                log_id: log,
                                sequence,
                                payload: api::EventPayload::StoreRecord { record },
                            }) else { return };
                            if !deliver(&sender, Event::default().event("event").data(data), Duration::from_secs(2)).await { return; }
                        }
                        None => break,
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use api::Cursor;
    #[tokio::test]
    async fn bounded_writer_refuses_a_slow_client() {
        let (sender, mut receiver) = mpsc::channel(1);
        let deadline = Duration::from_millis(20);
        assert!(deliver(&sender, Event::default().data("first"), deadline).await);
        assert!(!deliver(&sender, Event::default().data("second"), deadline).await);
        drop(sender);
        assert!(receiver.recv().await.is_some());
        assert!(receiver.recv().await.is_none());
    }
    #[test]
    fn cursor_carries_the_whole_subscription() {
        let sub = Subscription {
            cursors: vec![
                Cursor {
                    log_id: LogId::Session("one".into()),
                    sequence: 5,
                },
                Cursor {
                    log_id: LogId::Session("two".into()),
                    sequence: 7,
                },
            ],
            token_deltas: true,
        };
        assert_eq!(decode_cursor(&encode_cursor(&sub).unwrap()).unwrap(), sub);
    }
}
