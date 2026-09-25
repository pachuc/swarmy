use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};
use swarmy_api::{AppState, router};
use swarmy_api_types::{
    AppendMessage, AppendedMessage, CloseSession, CreateSession, ImageRef, InterruptOutcome,
    InterruptSession, InterruptStatus, Session, SessionClosed,
};
use swarmy_bus::{Bus, Config, LiveFeed};
use swarmy_core::{
    CHUNK_SIZE, ContentHash, ImageTag, ManifestHeader, ManifestId, SessionId, SessionState,
};
use swarmy_store::{Store, blob::MemoryBlobStore};
use ulid::Ulid;

static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();

struct Fixture {
    store: Store,
    bus: Bus,
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
            bus.clone(),
            "test-token".into(),
            swarmy_llm::catalog::Catalog::get().clone(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(axum::serve(listener, router(state)).into_future());
        Some(Self {
            store,
            bus,
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
    let outcome: InterruptOutcome = interrupted.json().await.unwrap();
    assert_eq!(outcome.result, InterruptStatus::Requested);
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
    assert!(closed.json::<SessionClosed>().await.unwrap().closed);
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

fn ephemeral(key: &str, provider: Option<&str>, model: Option<&str>) -> CreateSession {
    CreateSession {
        idempotency_key: key.into(),
        agent_id: None,
        new: false,
        image: Some(ImageRef {
            name: "fixture".into(),
            tag: "test".into(),
        }),
        provider: provider.map(str::to_owned),
        model: model.map(str::to_owned),
        effort: None,
    }
}

#[tokio::test]
async fn conversation_routes_require_authentication() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let session = f.create("auth", None, false).await;
    let url = format!("{}/v1/sessions/{}", f.base, session.id);
    let requests = [
        f.client
            .post(format!("{}/v1/sessions", f.base))
            .json(&ephemeral("unauth", None, None)),
        f.client
            .post(format!("{url}/messages"))
            .json(&AppendMessage {
                idempotency_key: "a".into(),
                expected_head: 0,
                text: "hi".into(),
            }),
        f.client
            .post(format!("{url}/interrupt"))
            .json(&InterruptSession {
                idempotency_key: "i".into(),
            }),
        f.client.delete(&url).json(&CloseSession {
            idempotency_key: "c".into(),
        }),
        f.client.get(format!("{url}/wait-idle")),
    ];
    for request in requests {
        assert_eq!(
            request.send().await.unwrap().status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
    }
}

#[tokio::test]
async fn invalid_selection_empty_message_and_stale_head_are_distinct() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    for (provider, model) in [
        (Some("unknown-provider"), None),
        (Some("openai"), Some("unknown-model")),
    ] {
        let response = f
            .client
            .post(format!("{}/v1/sessions", f.base))
            .bearer_auth("test-token")
            .json(&ephemeral(&Ulid::generate().to_string(), provider, model))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        let body: swarmy_api_types::ApiError = response.json().await.unwrap();
        assert_eq!(body.code, "invalid_selection");
        assert!(body.message.contains("unknown provider/model"));
    }
    let session = f.create("errors", None, false).await;
    let url = format!("{}/v1/sessions/{}/messages", f.base, session.id);
    let empty = f
        .client
        .post(&url)
        .bearer_auth("test-token")
        .json(&AppendMessage {
            idempotency_key: "empty".into(),
            expected_head: 0,
            text: "  ".into(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(empty.status(), reqwest::StatusCode::BAD_REQUEST);
    let valid = AppendMessage {
        idempotency_key: "first".into(),
        expected_head: 0,
        text: "first".into(),
    };
    assert!(
        f.client
            .post(&url)
            .bearer_auth("test-token")
            .json(&valid)
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    let stale = f
        .client
        .post(&url)
        .bearer_auth("test-token")
        .json(&AppendMessage {
            idempotency_key: "second".into(),
            expected_head: 0,
            text: "second".into(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(stale.status(), reqwest::StatusCode::CONFLICT);
    let body: swarmy_api_types::ApiError = stale.json().await.unwrap();
    assert_eq!(body.code, "stale_head");
    assert!(body.message.contains('1'));
}

#[tokio::test]
async fn close_runnable_session_returns_completed_and_prevents_append() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let session = f.create("close-runnable", None, false).await;
    let id = SessionId::from_ulid(session.id.parse().unwrap());
    let url = format!("{}/v1/sessions/{}", f.base, session.id);
    let first = f
        .client
        .post(format!("{url}/messages"))
        .bearer_auth("test-token")
        .json(&AppendMessage {
            idempotency_key: "first".into(),
            expected_head: 0,
            text: "hello".into(),
        })
        .send()
        .await
        .unwrap();
    assert!(first.status().is_success());
    let closed = f
        .client
        .delete(&url)
        .bearer_auth("test-token")
        .json(&CloseSession {
            idempotency_key: "close".into(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(closed.status(), reqwest::StatusCode::OK);
    assert!(closed.json::<SessionClosed>().await.unwrap().closed);
    assert_eq!(
        f.store.fetch_session(id).await.unwrap().unwrap().state,
        SessionState::Completed
    );
    let later = f
        .client
        .post(format!("{url}/messages"))
        .bearer_auth("test-token")
        .json(&AppendMessage {
            idempotency_key: "later".into(),
            expected_head: 1,
            text: "later".into(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(later.status(), reqwest::StatusCode::CONFLICT);
    let error: swarmy_api_types::ApiError = later.json().await.unwrap();
    assert!(matches!(
        error.code.as_str(),
        "stale_head" | "session_not_idle"
    ));
    let completed: Session = f
        .client
        .get(format!("{url}/wait-idle?timeout_ms=100"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(completed.state, swarmy_api_types::SessionState::Completed);
}

#[tokio::test]
async fn wait_idle_wakes_from_live_transition() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let session = f.create("live-wait", None, false).await;
    let id = SessionId::from_ulid(session.id.parse().unwrap());
    let _ = f
        .client
        .post(format!("{}/v1/sessions/{id}/messages", f.base))
        .bearer_auth("test-token")
        .json(&AppendMessage {
            idempotency_key: "turn".into(),
            expected_head: 0,
            text: "hello".into(),
        })
        .send()
        .await
        .unwrap();
    let url = format!(
        "{}/v1/sessions/{id}/wait-idle?after=1&timeout_ms=2000",
        f.base
    );
    let client = f.client.clone();
    let waiter = tokio::spawn(async move {
        client
            .get(url)
            .bearer_auth("test-token")
            .send()
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    f.store.interrupt_session(id).await.unwrap();
    assert!(f.store.finish_runnable_interrupt(id).await.unwrap());
    let event = swarmy_core::Event::StateChanged {
        seq: 2,
        from: SessionState::Runnable,
        to: SessionState::Idle,
    };
    f.bus
        .publish_live(LiveFeed::SessionEvents(id), &event)
        .await
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn invalid_effort_uses_cli_selection_error() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let response = f
        .client
        .post(format!("{}/v1/sessions", f.base))
        .bearer_auth("test-token")
        .json(&serde_json::json!({
            "idempotency_key": "bad-effort", "image": {"name":"fixture", "tag":"test"},
            "effort": "ultra"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: swarmy_api_types::ApiError = response.json().await.unwrap();
    assert_eq!(body.code, "invalid_selection");
    assert_eq!(
        body.message,
        "reasoning effort must be one of: none, minimal, low, medium, high, xhigh, max"
    );
}

#[tokio::test]
async fn durable_turn_metrics_match_the_session_and_agent_api() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let agent = f
        .store
        .create_agent("metric-agent", "fixture:test", "", jiff::Timestamp::now())
        .await
        .unwrap();
    let (session, _) = f
        .store
        .open_main_session(agent.agent_id, jiff::Timestamp::now())
        .await
        .unwrap();
    let turn = swarmy_core::MessageId::from_ulid(Ulid::generate());
    let request = swarmy_core::RequestId::for_step(session, 1);
    for (stage, ns) in [
        (swarmy_core::TurnStage::Appended, 1_000_000),
        (swarmy_core::TurnStage::InferenceStarted, 2_000_000),
        (swarmy_core::TurnStage::FirstToken, 3_000_000),
        (swarmy_core::TurnStage::InferenceFinished, 5_000_000),
        (swarmy_core::TurnStage::ToolDispatched, 6_000_000),
        (swarmy_core::TurnStage::ToolCompleted, 8_000_000),
        (swarmy_core::TurnStage::Idle, 9_000_000),
    ] {
        f.store
            .record_turn_metric(
                session,
                turn,
                swarmy_store::MetricPatch::Stage(swarmy_core::TurnEvent {
                    session_id: session,
                    turn_id: turn,
                    stage,
                    request_id: Some(request),
                    clock_id: "test-boot".into(),
                    monotonic_ns: ns,
                    unix_ns: i128::from(ns),
                }),
            )
            .await
            .unwrap();
    }
    f.store
        .record_turn_metric(
            session,
            turn,
            swarmy_store::MetricPatch::Inference(swarmy_api_types::InferenceMetric {
                request_id: request.to_string(),
                provider: "fake".into(),
                model: "scripted".into(),
                input_tokens: 12,
                cached_input_tokens: 3,
                output_tokens: 4,
                reasoning_tokens: 1,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    f.store
        .record_turn_metric(
            session,
            turn,
            swarmy_store::MetricPatch::Tool(swarmy_api_types::ToolMetric {
                request_id: request.to_string(),
                name: "bash".into(),
                exit_status: Some(0),
                output_bytes: Some(42),
                started_ns: Some(7_000_000),
                process_wall_ms: Some(0.5),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let direct = f.store.list_turn_metrics(session, None, 10).await.unwrap();
    assert_eq!(direct.len(), 1);
    assert_eq!(direct[0].append_to_first_token_ms, Some(2.0));
    assert_eq!(direct[0].inference_duration_ms, Some(3.0));
    assert_eq!(direct[0].tools[0].name, "bash");
    assert_eq!(direct[0].tools[0].queue_ms, Some(1.0));
    let client = swarmy_client::Client::new(&f.base, "test-token").unwrap();
    assert_eq!(
        client
            .session_metrics(&session.to_string(), None, 10)
            .await
            .unwrap(),
        direct
    );
    let rollup = client.agent_metrics("metric-agent").await.unwrap();
    assert_eq!(rollup.turns, 1);
    assert_eq!(rollup.output_tokens, 4);
    assert!((rollup.latencies["tool_round_trip"].p50_ms - 2.0).abs() < 0.001);
}
