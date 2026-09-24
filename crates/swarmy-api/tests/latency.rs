//! Run on a real node with a registered image and the fake development stack.
//! The sandbox fleet lacks NBD, so the benchmark is opt-in there.
use std::{
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};
use swarmy_api::{AppState, router};
use swarmy_api_types::{AppendMessage, AppendedMessage, CreateSession, ImageRef, Session};
use swarmy_bus::{Bus, Config, LiveFeed, SubjectToken};
use swarmy_core::{
    InferenceSelection, Message, MessageId, MessageRole, Part, SessionId, SessionState,
};
use swarmy_store::{Store, blob::ObjectBlobStore};
use ulid::Ulid;

static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();
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
}

#[tokio::test]
async fn api_first_fake_token_stays_within_five_ms_of_direct_append() {
    let Ok(image) = std::env::var("SWARMY_TEST_IMAGE") else {
        return;
    };
    if std::env::var("SWARMY_API_FAKE_BENCH").as_deref() != Ok("1") {
        return;
    }
    let fixture = setup(&image).await;
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

async fn setup(image: &str) -> BenchFixture {
    NETWORK.get_or_init(swarmy_store::boot);
    let settings = swarmy_config::Settings::load().unwrap().settings;
    assert_eq!(
        settings.provider, "fake",
        "benchmark needs the fake provider stack"
    );
    let directory: Vec<_> = settings
        .store_directory
        .split('/')
        .map(str::to_owned)
        .collect();
    let store = Store::open(
        Some(&settings.fdb_cluster_file),
        Some(&directory),
        Arc::new(ObjectBlobStore::from_env().unwrap()),
    )
    .await
    .unwrap();
    let bus = Bus::connect(
        &settings.nats_url,
        Config {
            prefix: if settings.bus_prefix.is_empty() {
                None
            } else {
                Some(SubjectToken::new(&settings.bus_prefix).unwrap())
            },
            ack_wait: Duration::from_millis(settings.bus_ack_wait_ms),
            max_deliver: settings.bus_max_deliver,
        },
    )
    .await
    .unwrap();
    let state = AppState::new(
        store.clone(),
        bus.clone(),
        "bench-token".into(),
        settings.catalog().unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, router(state)).into_future());
    let client = reqwest::Client::new();
    let (name, tag) = image
        .split_once(':')
        .expect("SWARMY_TEST_IMAGE must be NAME:TAG");
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
            model: Some(settings.model.clone()),
            effort: None,
        })
        .send()
        .await
        .unwrap();
    assert!(created.status().is_success(), "{}", created.status());
    let api_session: Session = created.json().await.unwrap();
    let api_id = SessionId::from_ulid(api_session.id.parse().unwrap());
    let direct_id = SessionId::from_ulid(Ulid::generate());
    store
        .create_session_with_inference(
            direct_id,
            None,
            Some(image),
            jiff::Timestamp::now(),
            &InferenceSelection {
                provider: Some("fake".into()),
                model: Some(settings.model.clone()),
                effort: None,
            },
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
        resend: Duration::from_millis(settings.scheduler_resend_interval_ms),
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

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let record = f.store.fetch_session(id).await.unwrap().unwrap();
            if record.state == SessionState::Idle && record.head_seq > head {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("fake turn did not finish");
    elapsed
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
