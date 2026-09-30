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
    ops::ControlFlow,
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
/// How long a slow SSE client gets to drain one event before the stream ends.
const SSE_DELIVERY_TIMEOUT: Duration = Duration::from_secs(2);
type ApiError = (StatusCode, Json<api::ApiError>);
type Registry = Arc<std::sync::Mutex<HashMap<String, Connection>>>;

#[derive(Clone)]
pub(crate) struct Connection {
    sender: watch::Sender<Subscription>,
    progress: Arc<std::sync::Mutex<Subscription>>,
}

#[derive(Deserialize)]
pub(crate) struct StreamQuery {
    subscription: Option<String>,
}

fn key(log: &LogId) -> String {
    match log {
        LogId::Session(id) => format!("session:{id}"),
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
            LogId::Session(_) => session_id(&cursor.log_id)?,
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
pub(crate) async fn subscribe(
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
pub(crate) async fn update(
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
#[derive(Clone, Copy, PartialEq, Eq)]
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
            // A cursor only advances when the corresponding event has entered
            // the bounded output queue. The client can replay after a disconnect.
            let next = record.seq();
            let payload = api::EventPayload::StoreRecord {
                record: api::RecordBody::Event(record),
            };
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
                SSE_DELIVERY_TIMEOUT,
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

/// Apply a pending subscription change before rebuilding feeds.
/// Both `produce` entry points share this instead of repeating the block.
fn apply_subscription(
    changes: &mut watch::Receiver<Subscription>,
    current: &mut Subscription,
    pending_update: &mut bool,
    first: &mut Option<SelectAll<BoxStream<'static, FeedItem>>>,
) {
    *current = changes.borrow_and_update().clone();
    *pending_update = true;
    *first = None;
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
    'reconfigure: loop {
        if changes.has_changed().unwrap_or(false) {
            apply_subscription(&mut changes, &mut current, &mut pending_update, &mut first);
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
        match settle(catch_all(&state, &mut current, &sender, &changes, &guard.progress).await) {
            ControlFlow::Continue(()) => {}
            ControlFlow::Break(Drive::Changed) => continue 'reconfigure,
            ControlFlow::Break(Drive::Rebuild | Drive::End) => return,
        }
        if pending_update {
            let Ok(id) = encode_cursor(&current) else {
                return;
            };
            if !deliver(
                &sender,
                Event::default().event("subscription").id(id).data("{}"),
                SSE_DELIVERY_TIMEOUT,
            )
            .await
            {
                return;
            }
            pending_update = false;
        }
        match drive(
            &state,
            &mut current,
            &sender,
            &mut changes,
            &guard.progress,
            &mut live,
        )
        .await
        {
            // The change notification was consumed inside `drive`; apply the
            // pending subscription before rebuilding feeds.
            Drive::Changed => {
                apply_subscription(&mut changes, &mut current, &mut pending_update, &mut first);
            }
            Drive::Rebuild => {}
            Drive::End => return,
        }
    }
}

/// What the inner drive loop tells `produce` to do next.
enum Drive {
    /// Subscription changed; apply it before rebuilding feeds.
    Changed,
    /// Feeds ended; rebuild them from the top of the loop.
    Rebuild,
    /// Client gone or undeliverable; end the stream.
    End,
}

/// Fold one replay outcome into loop control: done carries on, updated
/// reconfigures, failed ends the stream. Every replay point in `produce`
/// shares this instead of repeating the match.
fn settle(replay: Replay) -> ControlFlow<Drive> {
    match replay {
        Replay::Done => ControlFlow::Continue(()),
        Replay::Updated => ControlFlow::Break(Drive::Changed),
        Replay::Failed => ControlFlow::Break(Drive::End),
    }
}

/// Handle one live-feed item. Done carries on polling; anything else leaves
/// the drive loop with the outcome for `produce`.
async fn on_feed_item(
    state: &AppState,
    current: &mut Subscription,
    sender: &mpsc::Sender<Event>,
    changes: &watch::Receiver<Subscription>,
    progress: &Arc<std::sync::Mutex<Subscription>>,
    item: Option<FeedItem>,
) -> ControlFlow<Drive> {
    match item {
        Some(FeedItem::Durable(id)) => {
            let Some(index) = current
                .cursors
                .iter()
                .position(|c| session_id(&c.log_id).ok() == Some(id))
            else {
                return ControlFlow::Continue(());
            };
            settle(catch_up(state, current, index, sender, changes, progress).await)
        }
        Some(FeedItem::Token(log, delta)) => {
            let payload = api::EventPayload::TokenDelta {
                turn_id: delta.turn_id,
                position: delta.position,
                text: delta.text,
            };
            let Ok(data) =
                serde_json::to_string(&serde_json::json!({"log_id": log, "payload": payload}))
            else {
                return ControlFlow::Break(Drive::End);
            };
            if !deliver(
                sender,
                Event::default().event("token_delta").data(data),
                SSE_DELIVERY_TIMEOUT,
            )
            .await
            {
                return ControlFlow::Break(Drive::End);
            }
            ControlFlow::Continue(())
        }
        Some(FeedItem::Timeline(log, sequence, observation)) => {
            let Ok(data) = serde_json::to_string(&api::Event {
                log_id: log,
                sequence,
                // Timeline observations ride the `store_record`
                // tag so older clients, which decode the
                // record as a value, keep working.
                payload: api::EventPayload::StoreRecord {
                    record: api::RecordBody::Timeline(observation),
                },
            }) else {
                return ControlFlow::Break(Drive::End);
            };
            if !deliver(
                sender,
                Event::default().event("event").data(data),
                SSE_DELIVERY_TIMEOUT,
            )
            .await
            {
                return ControlFlow::Break(Drive::End);
            }
            ControlFlow::Continue(())
        }
        // Feeds ended; rebuild them from the top of the loop.
        None => ControlFlow::Break(Drive::Rebuild),
    }
}

/// Poll the store and live feeds until the subscription changes, the feeds
/// end, or the client goes away.
async fn drive(
    state: &AppState,
    current: &mut Subscription,
    sender: &mpsc::Sender<Event>,
    changes: &mut watch::Receiver<Subscription>,
    progress: &Arc<std::sync::Mutex<Subscription>>,
    live: &mut SelectAll<BoxStream<'static, FeedItem>>,
) -> Drive {
    let mut poll = interval(state.stream_poll_interval);
    poll.tick().await;
    loop {
        tokio::select! {
            () = sender.closed() => return Drive::End,
            updated = changes.changed() => {
                if updated.is_err() { return Drive::End; }
                return Drive::Changed;
            }
            _ = poll.tick() => {
                if let ControlFlow::Break(drive) =
                    settle(catch_all(state, current, sender, changes, progress).await)
                {
                    return drive;
                }
            }
            item = live.next() => {
                if let ControlFlow::Break(drive) =
                    on_feed_item(state, current, sender, changes, progress, item).await
                {
                    return drive;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
