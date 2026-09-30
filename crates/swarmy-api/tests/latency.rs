#![deny(clippy::disallowed_methods)]
//! Latency acceptance against the fake development stack. The fixture
//! registers a metadata-only image and spawns its own scheduler, worker,
//! and gateway, so the tests need no root and no registered image. The
//! fifty-turn p95 comparison stays opt-in behind `SWARMY_API_FAKE_BENCH=1`
//! because a 5 ms comparison is noise on shared runners.
use std::time::{Duration, Instant};
use swarmy_api::{AppState, router};
use swarmy_api_types::{AppendMessage, AppendedMessage, CreateSession, ImageRef, Session};
use swarmy_bus::{Bus, LiveFeed};
use swarmy_core::{
    InferenceSelection, Message, MessageId, MessageRole, Part, SessionId, SessionState,
};
use swarmy_store::{AgentSessionOptions, Store};
use ulid::Ulid;

const TURNS: usize = 50;

struct BenchFixture {
    store: Store,
    bus: Bus,
    client: reqwest::Client,
    base: String,
    api_id: SessionId,
    direct_id: SessionId,
    server: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    resend: Duration,
    // Held for their Drop: the script directory outlives the spawned
    // services, and the guards kill the services even on panic.
    files: tempfile::TempDir,
    children: Vec<swarmy_testkit::ChildGuard>,
}

#[tokio::test]
async fn api_first_fake_token_stays_within_five_ms_of_direct_append() {
    let Some(_) = swarmy_core::test_support::opt_in_env(
        "SWARMY_API_FAKE_BENCH",
        "set SWARMY_API_FAKE_BENCH=1 to run the opt-in API fake benchmark",
    ) else {
        return;
    };
    if swarmy_testkit::require_stack("SWARMY_FDB_CLUSTER_FILE").is_none() {
        return;
    }
    if swarmy_testkit::require_stack("SWARMY_NATS_URL").is_none() {
        return;
    }
    let fixture = setup().await;
    let mut api = Vec::with_capacity(TURNS);
    let mut direct = Vec::with_capacity(TURNS);
    for turn in 0..TURNS {
        api.push(measure(&fixture, fixture.api_id, true, turn).await);
        direct.push(measure(&fixture, fixture.direct_id, false, turn).await);
    }
    api.sort();
    direct.sort();
    let api_p95 = api[47];
    let direct_p95 = direct[47];
    assert!(
        api_p95 <= direct_p95 + Duration::from_millis(5),
        "50-turn append-to-first-token p95: API {api_p95:?}, direct {direct_p95:?}"
    );
    fixture.server.abort();
}

async fn setup() -> BenchFixture {
    swarmy_testkit::boot_fdb();
    // The benchmark reads no host configuration: provider, model, and store
    // location come from compiled defaults over the named stack variables.
    let mut settings = swarmy_testkit::test_settings(&[
        "SWARMY_FDB_CLUSTER_FILE",
        "SWARMY_STORE_DIRECTORY",
        "SWARMY_NATS_URL",
        "SWARMY_MODEL",
    ]);
    assert_eq!(
        settings.selection.provider, "fake",
        "benchmark needs the fake provider stack"
    );
    settings.store.directory = format!("latency-bench-{}", Ulid::generate());
    settings.bus.prefix = format!("latency-bench-{}", Ulid::generate());
    let opened = Store::open_store(&settings).await.unwrap();
    let store = opened.store;
    // Sessions need an image reference but never boot a computer from it,
    // so register the metadata-only fixture image instead of requiring a
    // built image from the environment.
    let image = swarmy_testkit::image(&store).await;
    // The fixture runs its own scheduler, worker, and gateway against its
    // randomized store directory and bus prefix, so no running service ever
    // needs to see its turns. The fake script answers every turn with text.
    let files = tempfile::tempdir().unwrap();
    swarmy_testkit::Script::new("done").write_to(&files.path().join("script.json"));
    let mut children = Vec::new();
    for name in ["swarmy-scheduler", "swarmy-worker", "swarmy-gateway"] {
        children.push(swarmy_testkit::ChildGuard::new(
            tokio::process::Command::new(swarmy_testkit::bin(name))
                .env("SWARMY_PROVIDER", "fake")
                .env("SWARMY_MODEL", settings.selection.model.clone())
                .env("SWARMY_STORE_DIRECTORY", &settings.store.directory)
                .env("SWARMY_BUS_PREFIX", &settings.bus.prefix)
                .env("SWARMY_FAKE_SCRIPT", files.path().join("script.json"))
                .kill_on_drop(true)
                .spawn()
                .unwrap(),
        ));
    }
    let bus = Bus::connect(&settings.bus.nats_url, settings.bus.bus_config().unwrap())
        .await
        .unwrap();
    let state = AppState::new(
        store.clone(),
        bus.clone(),
        "bench-token".into(),
        settings.catalog().unwrap(),
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, router(state)).into_future());
    let client = reqwest::Client::new();
    let (name, tag) = image
        .split_once(':')
        .expect("fixture image must be NAME:TAG");
    let created = client
        .post(format!("{base}/v1/sessions"))
        .bearer_auth("bench-token")
        .json(&CreateSession {
            idempotency_key: Ulid::generate().to_string(),
            agent_id: None,
            new: false,
            image: Some(ImageRef {
                name: name.into(),
                tag: tag.into(),
            }),
            provider: Some("fake".into()),
            model: Some(settings.selection.model.clone()),
            effort: None,
            route: None,
        })
        .send()
        .await
        .unwrap();
    assert!(created.status().is_success(), "{}", created.status());
    let api_session: Session = created.json().await.unwrap();
    let api_id = SessionId::from_ulid(api_session.id.parse().unwrap());
    let direct_id = SessionId::from_ulid(Ulid::generate());
    store
        .create_agent_session(
            direct_id,
            None,
            jiff::Timestamp::now(),
            Some(AgentSessionOptions {
                image: Some(image),
                inference: Some(&InferenceSelection {
                    provider: Some("fake".into()),
                    model: Some(settings.selection.model.clone()),
                    effort: None,
                }),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    BenchFixture {
        store,
        bus,
        client,
        base,
        api_id,
        direct_id,
        server,
        resend: settings.scheduler.resend_interval_ms,
        files,
        children,
    }
}

async fn measure(f: &BenchFixture, id: SessionId, via_api: bool, turn: usize) -> Duration {
    let head = f.store.fetch_session(id).await.unwrap().unwrap().head_seq;
    let mut tokens = f
        .bus
        .subscribe_live::<swarmy_core::LiveTokenDelta>(LiveFeed::ApiTokenDeltas(id))
        .await
        .unwrap();
    let elapsed = if via_api {
        let start = Instant::now();
        let response = f
            .client
            .post(format!("{}/v1/sessions/{id}/messages", f.base))
            .bearer_auth("bench-token")
            .json(&AppendMessage {
                queue: false,
                idempotency_key: format!("turn-{turn}"),
                expected_head: head,
                text: "swarmy bench turn no_tool".into(),
            })
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success(), "{}", response.status());
        let appended: AppendedMessage = response.json().await.unwrap();
        wait_first_token(&mut tokens, &appended.turn_id)
            .await
            .duration_since(start)
    } else {
        let turn_id = MessageId::from_ulid(Ulid::generate());
        let message = Message {
            id: turn_id,
            role: MessageRole::User,
            parts: vec![Part::Text {
                text: "swarmy bench turn no_tool".into(),
            }],
        };
        let start = Instant::now();
        let next = f
            .store
            .append_user_message(id, head, &message)
            .await
            .unwrap();
        f.bus
            .nudge(id, next, Some(turn_id), f.resend, false)
            .await
            .unwrap();
        wait_first_token(&mut tokens, &turn_id.to_string())
            .await
            .duration_since(start)
    };

    swarmy_testkit::eventually("fake turn finishes", Duration::from_secs(30), async || {
        let record = f.store.fetch_session(id).await.unwrap().unwrap();
        (record.state == SessionState::Idle && record.head_seq > head).then_some(())
    })
    .await;
    elapsed
}

/// Drive one scripted no-tool turn on the agent's main session and wait
/// for idle, returning the session and the appended turn id.
async fn drive_agent_turn(
    fixture: &BenchFixture,
    agent: &swarmy_core::AgentRecord,
) -> (SessionId, String) {
    let (session, _) = fixture
        .store
        .open_main_session(agent.agent_id, jiff::Timestamp::now())
        .await
        .unwrap();
    let head = fixture
        .store
        .fetch_session(session)
        .await
        .unwrap()
        .unwrap()
        .head_seq;
    let response = fixture
        .client
        .post(format!("{}/v1/sessions/{session}/messages", fixture.base))
        .bearer_auth("bench-token")
        .json(&AppendMessage {
            queue: false,
            idempotency_key: Ulid::generate().to_string(),
            expected_head: head,
            text: "swarmy bench turn no_tool".into(),
        })
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success(), "{}", response.status());
    let appended: AppendedMessage = response.json().await.unwrap();
    swarmy_testkit::eventually("fake turn finishes", Duration::from_secs(60), async || {
        let record = fixture.store.fetch_session(session).await.unwrap().unwrap();
        (record.state == SessionState::Idle && record.head_seq > head).then_some(())
    })
    .await;
    (session, appended.turn_id)
}

/// Fetch one turn record and assert the first-token stage and its derived
/// latencies landed. The gateway streams `PartDone` deltas for the fake
/// provider; those must start the first-token clock or every derived
/// latency stays null.
async fn assert_first_token_metrics(fixture: &BenchFixture, session: SessionId, turn_id: &str) {
    let metrics: Vec<swarmy_api_types::TurnMetrics> = fixture
        .client
        .get(format!("{}/v1/sessions/{session}/metrics", fixture.base))
        .bearer_auth("bench-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(metrics.len(), 1, "{metrics:?}");
    let turn = &metrics[0];
    assert_eq!(turn.turn_id, turn_id);
    assert!(
        turn.stages.iter().any(|stage| stage.stage == "first_token"),
        "no first-token stage in {:?}",
        turn.stages
            .iter()
            .map(|stage| &stage.stage)
            .collect::<Vec<_>>(),
    );
    assert!(turn.append_to_first_token_ms.is_some(), "{turn:?}");
    assert!(turn.inference_duration_ms.is_some(), "{turn:?}");
    assert!(turn.append_to_idle_ms.is_some(), "{turn:?}");
    assert_eq!(turn.inference.len(), 1);
    assert!(
        turn.inference[0].time_to_first_token_ms.is_some(),
        "{turn:?}"
    );
    assert!(
        turn.inference[0].streaming_duration_ms.is_some(),
        "{turn:?}"
    );
    assert!(
        turn.inference[0].output_tokens_per_second.is_some(),
        "{turn:?}"
    );
}

/// A real fake-provider turn must land a first-token stage and derived
/// latencies in the durable record. The fixture spawns its own scheduler,
/// worker, and gateway against the dev stack, so this runs on every PR.
#[tokio::test]
async fn fake_turn_records_first_token_metrics() {
    if swarmy_testkit::require_stack("SWARMY_FDB_CLUSTER_FILE").is_none() {
        return;
    }
    if swarmy_testkit::require_stack("SWARMY_NATS_URL").is_none() {
        return;
    }
    let fixture = setup().await;
    let image = swarmy_testkit::image(&fixture.store).await;
    let agent = fixture
        .store
        .create_agent(
            &format!("metrics-{}", Ulid::generate()),
            image,
            "",
            jiff::Timestamp::now(),
            None,
        )
        .await
        .unwrap();
    let (session, turn_id) = drive_agent_turn(&fixture, &agent).await;
    assert_first_token_metrics(&fixture, session, &turn_id).await;
    let rollup: swarmy_api_types::AgentMetrics = fixture
        .client
        .get(format!(
            "{}/v1/agents/{}/metrics",
            fixture.base, agent.agent_id
        ))
        .bearer_auth("bench-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(rollup.turns, 1);
    assert!(
        rollup.latencies.contains_key("append_to_idle"),
        "{rollup:?}"
    );
    fixture.server.abort();
}

async fn wait_first_token(
    tokens: &mut swarmy_bus::LiveMessages<swarmy_core::LiveTokenDelta>,
    turn_id: &str,
) -> Instant {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let delta = tokens
                .next()
                .await
                .expect("token feed closed")
                .expect("invalid token feed");
            if delta.turn_id == turn_id {
                return Instant::now();
            }
        }
    })
    .await
    .expect("fake provider did not emit a token")
}
