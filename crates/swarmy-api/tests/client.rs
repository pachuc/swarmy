//! Exercise the public client against the real HTTP router and development services.
use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};
use swarmy_api::{AppState, router};
use swarmy_api_types as api;
use swarmy_bus::{Bus, Config, LiveFeed};
use swarmy_client::{Client, Error, StreamItem};
use swarmy_core::{
    CHUNK_SIZE, ContentHash, Event as StoredEvent, ImageTag, LiveTokenDelta, ManifestHeader,
    ManifestId, Message, MessageId, MessageRole, Part, SessionId,
};
use swarmy_store::{Store, blob::MemoryBlobStore};
use ulid::Ulid;

static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();

struct Fixture {
    client: Client,
    store: Store,
    bus: Bus,
    address: std::net::SocketAddr,
    server: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    async fn restart(&mut self) {
        self.server.abort();
        let _ = (&mut self.server).await;
        let listener = tokio::net::TcpListener::bind(self.address).await.unwrap();
        let state = AppState::new(
            self.store.clone(),
            self.bus.clone(),
            "test-token".into(),
            swarmy_llm::catalog::Catalog::get().clone(),
            std::sync::Arc::new(object_store::memory::InMemory::new()),
        );
        self.server = tokio::spawn(axum::serve(listener, router(state)).into_future());
    }
    async fn session(&self, name: &str) -> SessionId {
        let agent = self
            .store
            .create_agent(name, "fixture:test", "", jiff::Timestamp::now())
            .await
            .unwrap();
        self.store
            .open_main_session(agent.agent_id, jiff::Timestamp::now())
            .await
            .unwrap()
            .0
    }
    async fn append(&self, id: SessionId, text: &str) -> u64 {
        let previous = self
            .store
            .fetch_session(id)
            .await
            .unwrap()
            .unwrap()
            .head_seq;
        let event = StoredEvent::MessageAppended {
            seq: 0,
            message: Message {
                id: MessageId::from_ulid(Ulid::generate()),
                role: MessageRole::Assistant,
                parts: vec![Part::Text { text: text.into() }],
            },
        };
        let next = self
            .store
            .append_events(id, previous, std::slice::from_ref(&event))
            .await
            .unwrap();
        self.bus
            .publish_live(LiveFeed::SessionEvents(id), &event)
            .await
            .unwrap();
        next
    }
}

async fn fixture() -> Option<Fixture> {
    let cluster = std::env::var("SWARMY_FDB_CLUSTER_FILE").ok()?;
    let nats = std::env::var("SWARMY_NATS_URL").ok()?;
    NETWORK.get_or_init(swarmy_store::boot);
    let store = Store::open(
        Some(&cluster),
        Some(&["client-api-test".into(), Ulid::generate().to_string()]),
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
    let state = AppState::new(
        store.clone(),
        bus.clone(),
        "test-token".into(),
        swarmy_llm::catalog::Catalog::get().clone(),
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = Client::new(&format!("http://{address}"), "test-token").unwrap();
    let server = tokio::spawn(axum::serve(listener, router(state)).into_future());
    Some(Fixture {
        client,
        store,
        bus,
        address,
        server,
    })
}

async fn assert_agent_routes(client: &Client) {
    let created = client
        .create_agent(&api::CreateAgent {
            idempotency_key: "agent".into(),
            name: "fixture-agent".into(),
            description: "test".into(),
            image: api::ImageRef {
                name: "fixture".into(),
                tag: "test".into(),
            },
            provider: None,
            model: None,
            effort: None,
            system_prompt: None,
            route: None,
        })
        .await
        .unwrap();
    assert_eq!(client.agent("fixture-agent").await.unwrap().id, created.id);
    assert_eq!(client.agents(None, 10).await.unwrap().len(), 1);
    assert_eq!(
        client
            .update_agent(
                "fixture-agent",
                &api::UpdateAgent {
                    idempotency_key: "update".into(),
                    description: Some("updated".into()),
                    provider: None,
                    model: None,
                    effort: None,
                    route: None,
                    system_prompt: None,
                }
            )
            .await
            .unwrap()
            .id,
        created.id
    );
    let named = client
        .create_session(&api::CreateSession {
            idempotency_key: "named".into(),
            agent_id: Some(created.id),
            new: false,
            image: None,
            provider: None,
            model: None,
            effort: None,
            route: None,
        })
        .await
        .unwrap();
    assert!(named.agent_id.is_some());
    assert_eq!(
        client
            .delete_agent("fixture-agent", "delete")
            .await
            .unwrap()["deleted"],
        true
    );
}

async fn assert_service_discovery(client: &Client) {
    let health = client.health().await.unwrap();
    assert!(health.get("version").is_some());
    assert!(health.get("api_version").is_some());
    assert!(client.openapi().await.unwrap().get("openapi").is_some());
}

async fn assert_catalog_and_credentials(client: &Client) {
    let first = &client.models().await.unwrap()[0];
    assert_eq!(
        client
            .model(&first.provider_id, &first.id)
            .await
            .unwrap()
            .id,
        first.id
    );
    assert!(!client.search_models("gpt").await.unwrap().is_empty());
    if swarmy_config::Keyring::load().is_ok() {
        let created = client
            .set_credential(&api::CreateCredential {
                idempotency_key: "credential".into(),
                provider: "fixture-provider".into(),
                kind: api::CredentialKind::ApiKey,
                label: "test".into(),
                secret: "secret".into(),
            })
            .await
            .unwrap();
        assert_eq!(
            client.credential("fixture-provider").await.unwrap(),
            created
        );
        assert!(!client.credentials().await.unwrap().is_empty());
        assert_eq!(
            client
                .remove_credential("fixture-provider", "delete-credential")
                .await
                .unwrap()["deleted"],
            true
        );
    }
}

#[tokio::test]
async fn client_round_trips_real_routes() {
    let Some(f) = fixture().await else {
        return;
    };
    let client = &f.client;
    assert_service_discovery(client).await;
    assert!(!client.providers().await.unwrap().is_empty());
    assert!(!client.models().await.unwrap().is_empty());
    assert_catalog_and_credentials(client).await;
    assert_eq!(client.images(None, 1).await.unwrap().len(), 1);
    assert!(
        client
            .images(Some("fixture:test"), 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert_agent_routes(client).await;
    assert_eq!(
        client.image("fixture", "test").await.unwrap().name,
        "fixture"
    );
    let session = client
        .create_session(&api::CreateSession {
            idempotency_key: "session".into(),
            agent_id: None,
            new: false,
            image: Some(api::ImageRef {
                name: "fixture".into(),
                tag: "test".into(),
            }),
            provider: None,
            model: None,
            effort: None,
            route: None,
        })
        .await
        .unwrap();
    assert_eq!(client.session(&session.id).await.unwrap().id, session.id);
    assert_eq!(client.sessions(None, 10).await.unwrap().len(), 2);
    let append = client
        .append_message(
            &session.id,
            &api::AppendMessage {
                idempotency_key: "append".into(),
                expected_head: session.head_sequence,
                text: "hello".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        client.events(&session.id, 0, 10).await.unwrap()[0].sequence,
        append.sequence
    );
    let replay = client
        .append_message(
            &session.id,
            &api::AppendMessage {
                idempotency_key: "append".into(),
                expected_head: session.head_sequence,
                text: "hello".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(append, replay);
    let Err(Error::Api { body, .. }) = client.session("missing").await else {
        panic!("expected error")
    };
    assert_eq!(body.code, "invalid_id");
    let Err(Error::Api { body, .. }) = client.wait_idle(&session.id, append.sequence, 10).await
    else {
        panic!("expected wait timeout while no scheduler is running")
    };
    assert_eq!(body.code, "wait_timeout");
    let interrupted = client
        .interrupt(
            &session.id,
            &api::InterruptSession {
                idempotency_key: "interrupt".into(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        interrupted.result,
        api::InterruptStatus::Requested | api::InterruptStatus::Finished
    ));
    assert!(
        client
            .close_session(
                &session.id,
                &api::CloseSession {
                    idempotency_key: "close".into()
                }
            )
            .await
            .unwrap()
            .closed
    );
}

fn sub(ids: &[SessionId], tokens: bool) -> api::Subscription {
    api::Subscription {
        cursors: ids
            .iter()
            .map(|id| api::Cursor {
                log_id: api::LogId::Session(id.to_string()),
                sequence: 0,
            })
            .collect(),
        token_deltas: tokens,
    }
}
async fn next(stream: &mut swarmy_client::EventStream) -> api::Event {
    tokio::time::timeout(Duration::from_secs(8), stream.next())
        .await
        .unwrap()
        .unwrap()
}
#[tokio::test]
async fn multiplexed_stream_resumes_and_rejects_rewind() {
    let Some(mut f) = fixture().await else { return };
    let a = f.session("stream-a").await;
    let b = f.session("stream-b").await;
    f.append(a, "a1").await;
    f.append(b, "b1").await;
    let mut stream = f.client.stream(sub(&[a, b], false));
    let first = next(&mut stream).await;
    let second = next(&mut stream).await;
    assert_eq!(
        (first.log_id, first.sequence),
        (api::LogId::Session(a.to_string()), 1)
    );
    assert_eq!(
        (second.log_id, second.sequence),
        (api::LogId::Session(b.to_string()), 1)
    );
    f.append(a, "a2").await;
    assert_eq!(next(&mut stream).await.sequence, 2);
    f.restart().await;
    stream.restart().await;
    f.append(b, "b2").await;
    let resumed = next(&mut stream).await;
    assert_eq!(
        (resumed.log_id, resumed.sequence),
        (api::LogId::Session(b.to_string()), 2)
    );
    f.append(a, "a3").await;
    // Let the server's producer move ahead of the client's delivered cursor.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let handle = stream.subscription_handle();
    handle.set(sub(&[a, b], true));
    let Err(Error::Api { body, .. }) = stream.next_item().await else {
        panic!("expected rewind rejection")
    };
    assert_eq!(body.code, "cursor_rewind");
    // The rejected change is reported once; the old stream remains usable.
    assert_eq!(next(&mut stream).await.sequence, 3);
    let mut desired = api::Subscription {
        cursors: stream.cursors().to_vec(),
        token_deltas: true,
    };
    // The server's progress for both logs is now at the delivered cursors.
    handle.set(desired.clone());
    let bus = f.bus.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        bus.publish_live(
            LiveFeed::ApiTokenDeltas(a),
            &LiveTokenDelta {
                turn_id: "t".into(),
                position: 0,
                text: "hi".into(),
            },
        )
        .await
        .unwrap();
    });
    let item = tokio::time::timeout(Duration::from_secs(8), stream.next_item())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(item, StreamItem::TokenDelta { payload: api::EventPayload::TokenDelta { text, .. }, .. } if text == "hi")
    );
    desired.token_deltas = false;
    handle.set(desired);
    let _ = tokio::time::timeout(Duration::from_millis(200), stream.next_item()).await;
    f.bus
        .publish_live(
            LiveFeed::ApiTokenDeltas(a),
            &LiveTokenDelta {
                turn_id: "t".into(),
                position: 1,
                text: "no".into(),
            },
        )
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(400), stream.next_item())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn token_subscription_is_ready_before_open_returns() {
    let Some(fixture) = fixture().await else {
        return;
    };
    let session = fixture
        .client
        .create_session(&api::CreateSession {
            idempotency_key: Ulid::generate().to_string(),
            agent_id: None,
            new: false,
            image: Some(api::ImageRef {
                name: "fixture".into(),
                tag: "test".into(),
            }),
            provider: None,
            model: None,
            effort: None,
            route: None,
        })
        .await
        .unwrap();
    let mut stream = fixture.client.stream(api::Subscription {
        cursors: vec![api::Cursor {
            log_id: session.log_id,
            sequence: session.head_sequence,
        }],
        token_deltas: true,
    });
    stream.open().await.unwrap();
    let id = SessionId::from_ulid(session.id.parse().unwrap());
    fixture
        .bus
        .publish_live(
            LiveFeed::ApiTokenDeltas(id),
            &LiveTokenDelta {
                turn_id: "turn".into(),
                position: 0,
                text: "first".into(),
            },
        )
        .await
        .unwrap();
    let item = tokio::time::timeout(Duration::from_secs(3), stream.next_item())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(item, StreamItem::TokenDelta { payload: api::EventPayload::TokenDelta { text, .. }, .. } if text == "first")
    );
}
