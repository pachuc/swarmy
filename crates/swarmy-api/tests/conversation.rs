use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};
use swarmy_api::{AppState, router};
use swarmy_api_types::{
    AppendMessage, AppendedMessage, CloseSession, CreateSession, ImageRef, InterruptSession,
    Session,
};
use swarmy_bus::{Bus, Config};
use swarmy_core::{
    CHUNK_SIZE, ContentHash, ImageTag, ManifestHeader, ManifestId, SessionId, SessionState,
};
use swarmy_store::{Store, blob::MemoryBlobStore};
use ulid::Ulid;

static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();

struct Fixture {
    store: Store,
    client: reqwest::Client,
    base: String,
    server: tokio::task::JoinHandle<Result<(), std::io::Error>>,
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
        let path = vec!["conversation-api-test".into(), Ulid::generate().to_string()];
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
        let state = AppState::new(
            store.clone(),
            bus,
            "test-token".into(),
            swarmy_llm::catalog::Catalog::get().clone(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(axum::serve(listener, router(state)).into_future());
        Some(Self {
            store,
            client: reqwest::Client::new(),
            base,
            server,
        })
    }
    async fn create(&self, key: &str, agent_id: Option<String>, new: bool) -> Session {
        let response = self
            .client
            .post(format!("{}/v1/sessions", self.base))
            .bearer_auth("test-token")
            .json(&CreateSession {
                idempotency_key: key.into(),
                agent_id: agent_id.clone(),
                new,
                image: agent_id.is_none().then_some(ImageRef {
                    name: "fixture".into(),
                    tag: "test".into(),
                }),
                provider: None,
                model: None,
                effort: None,
            })
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success(), "{}", response.status());
        response.json().await.unwrap()
    }
}

#[tokio::test]
async fn create_append_replay_wait_interrupt_close() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let session = f.create("ephemeral", None, false).await;
    assert_eq!(session.id, f.create("ephemeral", None, false).await.id);
    let created: ulid::Ulid = session.id.parse().unwrap();
    assert!(
        created
            .timestamp_ms()
            .abs_diff(ulid::Ulid::generate().timestamp_ms())
            < 60_000
    );
    let id: SessionId = SessionId::from_ulid(session.id.parse().unwrap());
    assert!(f.store.session_image(id).await.unwrap().is_some());
    assert_ephemeral_selection(&f).await;
    let body = AppendMessage {
        idempotency_key: "turn-one".into(),
        expected_head: 0,
        text: "hello".into(),
    };
    let append = || {
        f.client
            .post(format!("{}/v1/sessions/{}/messages", f.base, session.id))
            .bearer_auth("test-token")
            .json(&body)
    };
    let first = append().send().await.unwrap();
    assert!(first.status().is_success(), "{}", first.status());
    let first: AppendedMessage = first.json().await.unwrap();
    assert_eq!(first.sequence, 1);
    let replay: AppendedMessage = append().send().await.unwrap().json().await.unwrap();
    assert_eq!(first, replay);
    assert_eq!(f.store.read_events(id, 0, 10).await.unwrap().len(), 1);
    let interrupted = f
        .client
        .post(format!("{}/v1/sessions/{}/interrupt", f.base, session.id))
        .bearer_auth("test-token")
        .json(&InterruptSession {
            idempotency_key: "stop".into(),
        })
        .send()
        .await
        .unwrap();
    assert!(
        interrupted.status().is_success(),
        "{}",
        interrupted.status()
    );
    assert!(f.store.interrupt_requested(id).await.unwrap());
    assert!(f.store.finish_runnable_interrupt(id).await.unwrap());
    let late_replay: AppendedMessage = append().send().await.unwrap().json().await.unwrap();
    assert_eq!(late_replay, first);
    assert_eq!(
        f.store
            .read_events(id, 0, 10)
            .await
            .unwrap()
            .iter()
            .filter(|event| matches!(event, swarmy_core::Event::MessageAppended { .. }))
            .count(),
        1
    );
    let idle = f
        .client
        .get(format!(
            "{}/v1/sessions/{}/wait-idle?after=1&timeout_ms=1000",
            f.base, session.id
        ))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap();
    assert_eq!(idle.status(), reqwest::StatusCode::OK);
    let idle: Session = idle.json().await.unwrap();
    assert_eq!(idle.state, swarmy_api_types::SessionState::Idle);
    let closed = f
        .client
        .delete(format!("{}/v1/sessions/{}", f.base, session.id))
        .bearer_auth("test-token")
        .json(&CloseSession {
            idempotency_key: "close".into(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(closed.status(), reqwest::StatusCode::OK);
    assert_eq!(
        f.store.fetch_session(id).await.unwrap().unwrap().state,
        SessionState::Completed
    );
}

async fn assert_ephemeral_selection(f: &Fixture) {
    let selected = f
        .client
        .post(format!("{}/v1/sessions", f.base))
        .bearer_auth("test-token")
        .json(&CreateSession {
            idempotency_key: "selected".into(),
            agent_id: None,
            new: false,
            image: Some(ImageRef {
                name: "fixture".into(),
                tag: "test".into(),
            }),
            provider: Some("fake".into()),
            model: Some("scripted".into()),
            effort: None,
        })
        .send()
        .await
        .unwrap();
    assert!(selected.status().is_success(), "{}", selected.status());
    let selected: Session = selected.json().await.unwrap();
    let selected_record = f
        .store
        .fetch_session(SessionId::from_ulid(selected.id.parse().unwrap()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected_record.inference.provider.as_deref(), Some("fake"));
    assert_eq!(selected_record.inference.model.as_deref(), Some("scripted"));
}

#[tokio::test]
async fn named_main_and_side_preserve_image_and_selection() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let agent = f
        .store
        .create_agent("named", "fixture:test", "", jiff::Timestamp::now())
        .await
        .unwrap();
    let main = f
        .create("main", Some(agent.agent_id.to_string()), false)
        .await;
    assert_eq!(
        main.id,
        f.create("main-again", Some(agent.agent_id.to_string()), false)
            .await
            .id
    );
    let side = f
        .create("side", Some(agent.agent_id.to_string()), true)
        .await;
    assert_ne!(main.id, side.id);
    let record = f
        .store
        .fetch_session(SessionId::from_ulid(side.id.parse().unwrap()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.agent_id, agent.agent_id);
    assert_eq!(record.inference, swarmy_core::InferenceSelection::default());
    assert!(
        f.store
            .session_image(record.session_id)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn wait_idle_times_out_for_active_session() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let session = f.create("timeout", None, false).await;
    let response = f
        .client
        .get(format!(
            "{}/v1/sessions/{}/wait-idle?after=0&timeout_ms=50",
            f.base, session.id
        ))
        .bearer_auth("test-token")
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::REQUEST_TIMEOUT);
}
