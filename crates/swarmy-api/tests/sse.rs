use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};
use swarmy_api::{AppState, router};
use swarmy_api_types::{self as api, Cursor, LogId, Subscription};
use swarmy_bus::{Bus, Config, LiveFeed};
use swarmy_core::{
    CHUNK_SIZE, ContentHash, Event as StoredEvent, ImageTag, ManifestHeader, ManifestId, Message,
    MessageId, MessageRole, Part, SessionId,
};
use swarmy_store::{Store, blob::MemoryBlobStore};
use tokio::task::JoinHandle;
use ulid::Ulid;

static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();

struct Fixture {
    store: Store,
    bus: Bus,
    client: reqwest::Client,
    base: String,
    server: JoinHandle<Result<(), std::io::Error>>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    async fn new() -> Option<Self> {
        let cluster = std::env::var("SWARMY_FDB_CLUSTER_FILE").ok()?;
        let nats = std::env::var("SWARMY_NATS_URL").ok()?;
        NETWORK.get_or_init(swarmy_store::boot);
        let path = vec!["sse-test".into(), Ulid::generate().to_string()];
        let store = Store::open(
            Some(&cluster),
            Some(&path),
            Arc::new(MemoryBlobStore::default()),
        )
        .await
        .unwrap();
        let manifest = ManifestId::from_ulid(Ulid::generate());
        store
            .put_manifest(
                manifest,
                &ManifestHeader {
                    size: u64::from(CHUNK_SIZE),
                    chunk_size: CHUNK_SIZE,
                    root_hash: ContentHash::ZERO,
                },
            )
            .await
            .unwrap();
        store
            .put_image("fixture", &ImageTag("test".into()), manifest)
            .await
            .unwrap();
        let bus = Bus::connect(&nats, Config::default()).await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let state = AppState::new(
            store.clone(),
            bus.clone(),
            "test-token".into(),
            swarmy_llm::catalog::Catalog::get().clone(),
        );
        let server = tokio::spawn(axum::serve(listener, router(state)).into_future());
        Some(Self {
            store,
            bus,
            client: reqwest::Client::new(),
            base,
            server,
        })
    }
    async fn session(&self, name: &str) -> SessionId {
        let agent = self
            .store
            .create_agent(name, "fixture:test", "", jiff::Timestamp::now())
            .await
            .unwrap();
        let (id, _) = self
            .store
            .open_main_session(agent.agent_id, jiff::Timestamp::now())
            .await
            .unwrap();
        id
    }
    async fn append(&self, id: SessionId, text: &str) -> u64 {
        let previous = self
            .store
            .fetch_session(id)
            .await
            .unwrap()
            .unwrap()
            .head_seq;
        let stored = StoredEvent::MessageAppended {
            seq: 0,
            message: Message {
                id: MessageId::from_ulid(Ulid::generate()),
                role: MessageRole::Assistant,
                parts: vec![Part::Text { text: text.into() }],
            },
        };
        let next = self
            .store
            .append_events(id, previous, std::slice::from_ref(&stored))
            .await
            .unwrap();
        self.bus
            .publish_live(LiveFeed::SessionEvents(id), &stored)
            .await
            .unwrap();
        next
    }
    fn url(&self, subscription: &Subscription) -> String {
        let mut url = reqwest::Url::parse(&format!("{}/v1/events", self.base)).unwrap();
        url.query_pairs_mut().append_pair(
            "subscription",
            &serde_json::to_string(subscription).unwrap(),
        );
        url.into()
    }
    async fn connect(&self, subscription: &Subscription, last_id: Option<&str>) -> SseReader {
        let url = if last_id.is_some() {
            format!("{}/v1/events", self.base)
        } else {
            self.url(subscription)
        };
        let mut request = self.client.get(url).bearer_auth("test-token");
        if let Some(last_id) = last_id {
            request = request.header("last-event-id", last_id);
        }
        let response = request.send().await.unwrap();
        assert!(response.status().is_success(), "{}", response.status());
        SseReader {
            connection_id: response
                .headers()
                .get("x-swarmy-connection-id")
                .unwrap()
                .to_str()
                .unwrap()
                .into(),
            response,
            pending: String::new(),
        }
    }
}
fn subscription(ids: &[SessionId], tokens: bool) -> Subscription {
    Subscription {
        cursors: ids
            .iter()
            .map(|id| Cursor {
                log_id: LogId::Session(id.to_string()),
                sequence: 0,
            })
            .collect(),
        token_deltas: tokens,
    }
}
struct SseReader {
    connection_id: String,
    response: reqwest::Response,
    pending: String,
}
struct SseItem {
    kind: String,
    id: Option<String>,
    data: String,
    retry: Option<String>,
}
impl SseReader {
    async fn next(&mut self) -> SseItem {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Some(end) = self.pending.find("\n\n") {
                    let block = self.pending[..end].to_owned();
                    self.pending.drain(..end + 2);
                    let mut item = SseItem {
                        kind: String::new(),
                        id: None,
                        data: String::new(),
                        retry: None,
                    };
                    for line in block.lines() {
                        if let Some(value) = line.strip_prefix("event: ") {
                            item.kind = value.into();
                        }
                        if let Some(value) = line.strip_prefix("id: ") {
                            item.id = Some(value.into());
                        }
                        if let Some(value) = line.strip_prefix("data: ") {
                            item.data.push_str(value);
                        }
                        if let Some(value) = line.strip_prefix("retry: ") {
                            item.retry = Some(value.into());
                        }
                    }
                    if !item.kind.is_empty() {
                        return item;
                    }
                }
                let bytes = self
                    .response
                    .chunk()
                    .await
                    .unwrap()
                    .expect("SSE closed unexpectedly");
                self.pending
                    .push_str(&String::from_utf8_lossy(&bytes).replace("\r\n", "\n"));
            }
        })
        .await
        .expect("SSE event timeout")
    }
}

#[tokio::test]
async fn replay_live_and_resume_from_full_cursor() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let a = f.session("a").await;
    let b = f.session("b").await;
    f.append(a, "a1").await;
    f.append(b, "b1").await;
    let sub = subscription(&[a, b], false);
    let mut reader = f.connect(&sub, None).await;
    let connected = reader.next().await;
    assert_eq!(connected.kind, "connected");
    assert_eq!(connected.retry.as_deref(), Some("1000"));
    let first = reader.next().await;
    let second = reader.next().await;
    assert_eq!(
        (first.kind.as_str(), second.kind.as_str()),
        ("event", "event")
    );
    let first: api::Event = serde_json::from_str(&first.data).unwrap();
    let second: api::Event = serde_json::from_str(&second.data).unwrap();
    assert_eq!(first.log_id, LogId::Session(a.to_string()));
    assert_eq!(second.log_id, LogId::Session(b.to_string()));
    assert_eq!((first.sequence, second.sequence), (1, 1));
    f.append(a, "a2").await;
    let a_live_item = tokio::time::timeout(Duration::from_millis(500), reader.next())
        .await
        .expect("live feed must deliver without store poll");
    let a_live: api::Event = serde_json::from_str(&a_live_item.data).unwrap();
    assert_eq!(
        (a_live.log_id, a_live.sequence),
        (LogId::Session(a.to_string()), 2)
    );
    let rewind = f
        .client
        .put(format!(
            "{}/v1/events/{}/subscription",
            f.base, reader.connection_id
        ))
        .bearer_auth("test-token")
        .json(&sub)
        .send()
        .await
        .unwrap();
    assert_eq!(rewind.status(), reqwest::StatusCode::BAD_REQUEST);
    let error: api::ApiError = rewind.json().await.unwrap();
    assert_eq!(error.code, "cursor_rewind");
    assert!(error.message.contains(&a.to_string()));
    f.append(b, "b2").await;
    let live = reader.next().await;
    let event: api::Event = serde_json::from_str(&live.data).unwrap();
    assert_eq!(
        (event.log_id, event.sequence),
        (LogId::Session(b.to_string()), 2)
    );
    let last_id = live.id.unwrap();
    drop(reader);
    f.append(a, "a3").await;
    let mut resumed = f.connect(&sub, Some(&last_id)).await;
    assert_eq!(resumed.next().await.kind, "connected");
    let resumed_event: api::Event = serde_json::from_str(&resumed.next().await.data).unwrap();
    assert_eq!(
        (resumed_event.log_id, resumed_event.sequence),
        (LogId::Session(a.to_string()), 3)
    );
}

#[tokio::test]
async fn subscription_changes_and_tokens_are_scoped() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let a = f.session("a").await;
    let b = f.session("b").await;
    let c = f.session("c").await;
    let sub = subscription(&[a], false);
    let mut reader = f.connect(&sub, None).await;
    assert_eq!(reader.next().await.kind, "connected");
    let token = swarmy_core::LiveTokenDelta {
        turn_id: "turn".into(),
        position: 0,
        text: "first".into(),
    };
    f.bus
        .publish_live(LiveFeed::ApiTokenDeltas(a), &token)
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(350), reader.next())
            .await
            .is_err()
    );
    let updated = subscription(&[a, b], true);
    let response = f
        .client
        .put(format!(
            "{}/v1/events/{}/subscription",
            f.base, reader.connection_id
        ))
        .bearer_auth("test-token")
        .json(&updated)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
    let update = reader.next().await;
    assert_eq!(update.kind, "subscription");
    f.bus
        .publish_live(LiveFeed::ApiTokenDeltas(b), &token)
        .await
        .unwrap();
    let item = reader.next().await;
    assert_eq!(item.kind, "token_delta");
    let value: serde_json::Value = serde_json::from_str(&item.data).unwrap();
    assert_eq!(value["log_id"]["id"], b.to_string());
    assert!(value.get("sequence").is_none());
    let payload: api::EventPayload = serde_json::from_value(value["payload"].clone()).unwrap();
    assert!(
        matches!(payload, api::EventPayload::TokenDelta { position: 0, ref text, .. } if text == "first")
    );
    f.bus
        .publish_live(LiveFeed::ApiTokenDeltas(c), &token)
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(350), reader.next())
            .await
            .is_err()
    );
    f.append(b, "b1").await;
    let durable = reader.next().await;
    let last_id = durable.id.clone().unwrap();
    let event: api::Event = serde_json::from_str(&durable.data).unwrap();
    assert_eq!(
        (event.log_id, event.sequence),
        (LogId::Session(b.to_string()), 1)
    );
    drop(reader);
    f.append(b, "b2").await;
    // The original URL lists only A, but the last id remembers the updated set.
    let mut resumed = f.connect(&sub, Some(&last_id)).await;
    assert_eq!(resumed.next().await.kind, "connected");
    let next: api::Event = serde_json::from_str(&resumed.next().await.data).unwrap();
    assert_eq!(
        (next.log_id, next.sequence),
        (LogId::Session(b.to_string()), 2)
    );
}

#[tokio::test]
async fn replay_pages_past_the_output_capacity_without_losing_a_fast_reader() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let id = f.session("large-history").await;
    for index in 0..70 {
        f.append(id, &format!("message-{index}")).await;
    }
    let mut reader = f.connect(&subscription(&[id], false), None).await;
    assert_eq!(reader.next().await.kind, "connected");
    for sequence in 1..=70 {
        let next = reader.next().await;
        assert_eq!(next.kind, "event");
        let event: api::Event = serde_json::from_str(&next.data).unwrap();
        assert_eq!(event.sequence, sequence);
    }
}

#[tokio::test]
async fn slow_http_client_is_closed_and_removed_from_registry() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let id = f.session("slow-client").await;
    let text = "x".repeat(64 * 1024);
    for _ in 0..140 {
        f.append(id, &text).await;
    }
    let reader = f.connect(&subscription(&[id], false), None).await;
    let url = format!("{}/v1/events/{}/subscription", f.base, reader.connection_id);
    // Do not consume the response body: the HTTP transport must exert real
    // backpressure, rather than just a bare channel in the producer test.
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let status = f
                .client
                .put(&url)
                .bearer_auth("test-token")
                .json(&subscription(&[id], false))
                .send()
                .await
                .unwrap()
                .status();
            if status == reqwest::StatusCode::NOT_FOUND {
                break;
            }
            assert!(matches!(
                status,
                reqwest::StatusCode::NO_CONTENT | reqwest::StatusCode::BAD_REQUEST
            ));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("slow client was not disconnected");
    let mut body = reader.response;
    tokio::time::timeout(Duration::from_secs(10), async {
        while body.chunk().await.unwrap().is_some() {}
    })
    .await
    .expect("HTTP stream did not close");
}
