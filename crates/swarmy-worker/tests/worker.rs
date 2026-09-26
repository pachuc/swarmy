#[path = "../../swarmy-store/tests/support/mod.rs"]
mod image_fixture;

use std::{
    collections::{BTreeMap, HashSet},
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use foundationdb::{
    Database,
    directory::{Directory, DirectoryLayer},
};
use futures::FutureExt;
use jiff::Timestamp;
use swarmy_bus::{Bus, Config, LiveFeed, SubjectToken, WorkQueue};
use swarmy_core::{
    AgentId, Event, Message, MessageId, MessageRole, Nudge, Part, RequestId, SessionId,
    SessionRecord, SessionState, ToolCallId, ToolResult,
};
use swarmy_llm::{InferenceJob, Response, StopReason, TokenUsage};
use swarmy_store::{Store, blob::ObjectBlobStore, runnable_partition};
use tempfile::TempDir;
use tokio::{
    process::{Child, Command},
    time::{sleep, timeout},
};
use ulid::Ulid;

const WAIT: Duration = Duration::from_secs(45);

struct Fixture {
    store: Store,
    bus: Bus,
    api_url: String,
    api_token: String,
    prefix: String,
    summarize_at_tokens: u64,
    max_wait_seconds: u64,
    gateway_wait_seconds: u64,
    provider: String,
    model: String,
    nats_url: String,
    files: TempDir,
    children: Vec<Child>,
    snapshots: Mutex<HashSet<String>>,
    keyring: Option<swarmy_config::Keyring>,
}

impl Fixture {
    async fn new() -> Option<Self> {
        let Ok(url) = std::env::var("SWARMY_NATS_URL") else {
            eprintln!("skipping worker integration test: SWARMY_NATS_URL is unset");
            return None;
        };
        Self::new_at(url).await
    }

    async fn new_at(nats_url: String) -> Option<Self> {
        static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();
        for variable in ["SWARMY_FDB_CLUSTER_FILE", "SWARMY_S3_ENDPOINT"] {
            if std::env::var(variable).is_err() {
                eprintln!("skipping worker integration test: {variable} is unset");
                return None;
            }
        }
        NETWORK.get_or_init(swarmy_store::boot);
        let prefix = format!("worker_{}", Ulid::generate());
        let store = Store::open(
            Some(&std::env::var("SWARMY_FDB_CLUSTER_FILE").unwrap()),
            Some(std::slice::from_ref(&prefix)),
            Arc::new(ObjectBlobStore::from_env().unwrap()),
        )
        .await
        .unwrap();
        let bus = Bus::connect(
            &nats_url,
            Config {
                prefix: Some(SubjectToken::new(&prefix).unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        bus.setup(&[]).await.unwrap();
        let api_token = Ulid::generate().to_string();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_url = format!("http://{}", listener.local_addr().unwrap());
        let objects = swarmy_store::blob::ObjectBlobStore::from_env()
            .unwrap()
            .object_store();
        let api = swarmy_api::AppState::new(
            store.clone(),
            bus.clone(),
            api_token.clone(),
            swarmy_llm::catalog::Catalog::get().clone(),
            objects,
        );
        tokio::spawn(axum::serve(listener, swarmy_api::router(api)).into_future());
        Some(Self {
            store,
            bus,
            api_url,
            api_token,
            prefix,
            summarize_at_tokens: 300_000,
            max_wait_seconds: 3600,
            gateway_wait_seconds: 1,
            provider: "fake".into(),
            model: "fake-model".into(),
            nats_url,
            files: TempDir::new().unwrap(),
            children: Vec::new(),
            snapshots: Mutex::default(),
            keyring: None,
        })
    }

    /// Generate a cluster keyring for stored auth entries and pass it to
    /// subsequently started services, so their gateways resolve entry labels.
    fn keyring(&mut self) -> swarmy_config::Keyring {
        let keyring =
            swarmy_config::Keyring::generate_at(&self.files.path().join("keyring")).unwrap();
        self.keyring = Some(keyring.clone());
        keyring
    }

    /// Store an API-key auth entry for the fixture provider.
    async fn put_entry(&self, label: &str) {
        self.put_entry_for(&self.provider.clone(), label).await;
    }

    /// Store an API-key auth entry for an explicit provider id.
    async fn put_entry_for(&self, provider: &str, label: &str) {
        let keyring = self.keyring.clone().expect("call keyring() first");
        self.store
            .credentials(keyring)
            .put_entry(
                swarmy_core::CredentialScope::Cluster,
                provider,
                label,
                &swarmy_core::CredentialRecord {
                    kind: swarmy_core::CredentialKind::ApiKey {
                        key: format!("{label}-key"),
                        extra: BTreeMap::new(),
                    },
                    updated_at: Timestamp::now(),
                },
            )
            .await
            .unwrap();
    }

    /// Store a named route over explicit `provider/label` steps.
    async fn put_route(&self, name: &str, steps: &[(&str, &str)]) {
        self.store
            .put_route(
                name,
                &steps
                    .iter()
                    .map(|(provider, entry)| swarmy_core::RouteStep {
                        provider: (*provider).into(),
                        entry: (*entry).into(),
                        model: None,
                    })
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
    }

    /// Serve two scripted fake providers instead of the default one, with a
    /// catalog model for each. Call before `start`.
    fn fake_pair(&mut self) {
        self.provider = "fake-a".into();
        self.model = "fake-model".into();
    }

    fn script(&self, tools: bool, tool_name: &str) {
        let answer = Response {
            parts: vec![Part::Text {
                text: "The turn is complete.".into(),
            }],
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
            quota_remaining: std::collections::BTreeMap::new(),
            quota_resets: std::collections::BTreeMap::new(),
        };
        let mut responses: BTreeMap<_, _> = (0..100).map(|index| (index, answer.clone())).collect();
        if tools {
            responses.insert(
                0,
                Response {
                    parts: vec![Part::ToolCall {
                        call_id: ToolCallId("clock".into()),
                        tool: tool_name.into(),
                        input: serde_json::json!({}),
                    }],
                    stop_reason: StopReason::ToolCalls,
                    usage: TokenUsage::default(),
                    quota_remaining: std::collections::BTreeMap::new(),
                    quota_resets: std::collections::BTreeMap::new(),
                },
            );
        }
        std::fs::write(
            self.files.path().join("script.json"),
            serde_json::to_vec(&serde_json::json!({"latency_ms": 10, "responses": responses}))
                .unwrap(),
        )
        .unwrap();
    }

    fn rate_limit_script(&self, failures: usize, retry_after_seconds: u64) {
        let answer = Response {
            parts: vec![Part::Text {
                text: "Recovered.".into(),
            }],
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
            quota_remaining: std::collections::BTreeMap::new(),
            quota_resets: std::collections::BTreeMap::new(),
        };
        let responses: BTreeMap<_, _> = (failures..100).map(|index| (index, &answer)).collect();
        let failures: BTreeMap<_, _> = (0..failures).map(|index| (index, serde_json::json!({
            "status": 429, "message": "quota reached", "retry_after_seconds": retry_after_seconds
        }))).collect();
        std::fs::write(
            self.files.path().join("script.json"),
            serde_json::to_vec(&serde_json::json!({"responses": responses, "failures": failures}))
                .unwrap(),
        )
        .unwrap();
    }

    async fn interrupt_from_cli(&self, id: SessionId) {
        let executable =
            std::path::Path::new(env!("CARGO_BIN_EXE_swarmy-worker")).with_file_name("swarmy");
        let output = Command::new(executable)
            .args(["session", "interrupt", &id.to_string()])
            .env("SWARMY_API_URL", &self.api_url)
            .env("SWARMY_API_TOKEN", &self.api_token)
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn start(&mut self, service: &str, kill_point: Option<&str>) -> usize {
        let executable =
            std::path::Path::new(env!("CARGO_BIN_EXE_swarmy-worker")).with_file_name(service);
        assert!(
            executable.exists(),
            "build the workspace binaries before running worker tests"
        );
        let index = self.children.len();
        let log =
            std::fs::File::create(self.files.path().join(format!("service-{index}.log"))).unwrap();
        let mut command = Command::new(executable);
        let pair = self.provider == "fake-a";
        command
            .env("SWARMY_PROVIDER", &self.provider)
            .env("SWARMY_NATS_URL", &self.nats_url)
            .env(
                "SWARMY_MODEL",
                if self.provider == "fake" || pair {
                    self.model.as_str()
                } else {
                    "gpt-5.5"
                },
            )
            .env(
                "SWARMY_CUSTOM_PROVIDERS",
                if pair {
                    r#"{"fake-a":{"api":"Fake","base_url":"fake://a"},"fake-b":{"api":"Fake","base_url":"fake://b"}}"#
                } else {
                    r#"{"openai":{"api":"Fake"}}"#
                },
            )
            .env(
                "SWARMY_PROVIDERS",
                if pair { "fake-a,fake-b" } else { self.provider.as_str() },
            );
        if pair {
            command.env(
                "SWARMY_MODELS",
                r#"[{"provider":"fake-a","id":"fake-model","api":"Fake"},{"provider":"fake-b","id":"fake-model","api":"Fake"}]"#,
            );
        } else {
            command.env_remove("SWARMY_MODELS");
        }
        command
            .env(
                "SWARMY_SUMMARIZE_AT_TOKENS",
                self.summarize_at_tokens.to_string(),
            )
            .env("SWARMY_STORE_DIRECTORY", &self.prefix)
            .env("SWARMY_BUS_PREFIX", &self.prefix)
            .env("SWARMY_WORKER_PARTITIONS", "7")
            .env("SWARMY_SCHEDULER_PARTITIONS", "7")
            .env("SWARMY_SCHEDULER_SCAN_INTERVAL_MS", "50")
            .env("SWARMY_SCHEDULER_RESEND_INTERVAL_MS", "100")
            .env("SWARMY_WORKER_LEASE_MS", "600")
            .env("SWARMY_WORKER_RECOVERY_INTERVAL_MS", "200")
            .env("SWARMY_FAKE_SCRIPT", self.files.path().join("script.json"))
            .env("SWARMY_FAKE_CALL_LOG", self.files.path().join("calls"))
            .env(
                "SWARMY_INFERENCE_MAX_WAIT_SECONDS",
                self.max_wait_seconds.to_string(),
            )
            .env(
                "SWARMY_INFERENCE_GATEWAY_WAIT_SECONDS",
                self.gateway_wait_seconds.to_string(),
            )
            .env("SWARMY_GATEWAY_CONCURRENCY", "1")
            .env("RUST_LOG", "info");
        if self.keyring.is_some() {
            command.env("SWARMY_KEYRING", self.files.path().join("keyring"));
        }
        command
            .env_remove("SWARMY_WORKER_KILL_POINT")
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .kill_on_drop(true);
        if let Some(point) = kill_point {
            command.env("SWARMY_WORKER_KILL_POINT", point);
        }
        self.children.push(command.spawn().unwrap());
        index
    }

    async fn create(&self) -> SessionId {
        self.create_with_provider(None).await
    }

    async fn create_with_route(&self, route: &str) -> SessionId {
        let id = loop {
            let id = SessionId::from_ulid(Ulid::generate());
            if runnable_partition(id) == 7 {
                break id;
            }
        };
        let image = image_fixture::image(&self.store).await;
        self.store
            .create_session_with_route(
                id,
                None,
                Some(image),
                Timestamp::now(),
                &swarmy_core::InferenceSelection::default(),
                Some(route),
            )
            .await
            .unwrap();
        self.user_message(id).await;
        id
    }

    async fn create_named_agent(&self, name: &str) -> AgentId {
        self.store
            .create_agent(
                name,
                image_fixture::image(&self.store).await,
                "",
                Timestamp::now(),
            )
            .await
            .unwrap()
            .agent_id
    }

    async fn set_agent_route(&self, agent: AgentId, route: Option<&str>) {
        self.store
            .set_agent(
                agent,
                &swarmy_core::AgentSettings {
                    route: route.map(str::to_owned),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }

    async fn create_agent_session(&self, agent: AgentId, route: Option<&str>) -> SessionId {
        let id = loop {
            let id = SessionId::from_ulid(Ulid::generate());
            if runnable_partition(id) == 7 {
                break id;
            }
        };
        // Named sessions pin the agent's image instead of taking one.
        self.store
            .create_session_with_route(
                id,
                Some(agent),
                None,
                Timestamp::now(),
                &swarmy_core::InferenceSelection::default(),
                route,
            )
            .await
            .unwrap();
        self.user_message(id).await;
        id
    }

    /// Wait until the gateway advertises a provider, so the first attempt
    /// cannot fail over behind a missing advertisement instead of the
    /// scripted failure the test asserts on.
    async fn gateway_serves(&self, provider: &str) {
        timeout(WAIT, async {
            loop {
                if self.store.gateway_serves(provider).await.unwrap() {
                    break;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
    }

    fn histories(&self) -> Vec<Vec<swarmy_core::Message>> {
        std::fs::read_to_string(self.files.path().join("calls"))
            .unwrap_or_default()
            .lines()
            .map(|line| {
                let entry: serde_json::Value = serde_json::from_str(line).unwrap();
                serde_json::from_value(entry["messages"].clone()).unwrap()
            })
            .collect()
    }

    async fn create_with_provider(&self, provider: Option<&str>) -> SessionId {
        let id = loop {
            let id = SessionId::from_ulid(Ulid::generate());
            if runnable_partition(id) == 7 {
                break id;
            }
        };
        self.store
            .create_session(
                &SessionRecord {
                    interrupt_requested: false,
                    session_id: id,
                    agent_id: AgentId::from_ulid(Ulid::generate()),
                    state: SessionState::Idle,
                    head_seq: 0,
                    snapshot_ref: None,
                    inference: swarmy_core::InferenceSelection {
                        provider: provider.map(str::to_owned),
                        ..Default::default()
                    },
                    kind: swarmy_core::SessionKind::Ephemeral,
                    computer_deleted: false,
                    plan: Vec::new(),
                    route: None,
                    route_step: 0,
                },
                Timestamp::now(),
                image_fixture::image(&self.store).await,
            )
            .await
            .unwrap();
        self.user_message(id).await;
        id
    }

    async fn user_message(&self, id: SessionId) {
        let head = self
            .store
            .fetch_session(id)
            .await
            .unwrap()
            .unwrap()
            .head_seq;
        self.store
            .append_events(
                id,
                head,
                &[Event::MessageAppended {
                    seq: 0,
                    message: Message {
                        id: MessageId::from_ulid(Ulid::generate()),
                        role: MessageRole::User,
                        parts: vec![Part::Text {
                            text: "What time is it?".into(),
                        }],
                    },
                }],
            )
            .await
            .unwrap();
    }

    async fn wake(&self, id: SessionId) {
        timeout(WAIT, async {
            loop {
                if self
                    .bus
                    .request_wake(id, Duration::from_millis(200))
                    .await
                    .is_ok()
                {
                    break;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
    }

    async fn idle(&self, id: SessionId) -> Vec<Event> {
        timeout(WAIT, async {
            loop {
                let session = self.store.fetch_session(id).await.unwrap().unwrap();
                if session.state == SessionState::Idle
                    && let Some(snapshot) = session.snapshot_ref
                {
                    self.snapshots.lock().unwrap().insert(snapshot.object_key);
                    return self.store.read_events(id, 0, 64).await.unwrap();
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("session did not reach Idle")
    }

    fn calls(&self) -> usize {
        std::fs::read_to_string(self.files.path().join("calls"))
            .unwrap_or_default()
            .lines()
            .count()
    }

    async fn cleanup(&mut self) {
        for child in &mut self.children {
            let _ = child.kill().await;
        }
        let blobs = ObjectBlobStore::from_env().unwrap();
        for key in self.snapshots.get_mut().unwrap().drain() {
            blobs.delete(&key).await.unwrap();
        }
        let db = Database::new(Some(&std::env::var("SWARMY_FDB_CLUSTER_FILE").unwrap())).unwrap();
        let path = vec![self.prefix.clone()];
        db.run(|trx, _| {
            let path = &path;
            async move {
                DirectoryLayer::default()
                    .remove_if_exists(&trx, path)
                    .await?;
                Ok(())
            }
        })
        .await
        .unwrap();
        let client = async_nats::connect(&self.nats_url).await.unwrap();
        let context = async_nats::jetstream::new(client);
        for stream in ["INFER_REQ", "SCHED_RUNNABLE", "TOOL_REMOTE", "TOOL_NODE"] {
            context
                .delete_stream(format!("{}_{stream}", self.prefix))
                .await
                .unwrap();
        }
    }
}

async fn run(
    test: impl for<'a> FnOnce(&'a mut Fixture) -> std::pin::Pin<Box<dyn Future<Output = ()> + 'a>>,
) {
    run_at(None, test).await;
}

async fn run_at(
    nats_url: Option<String>,
    test: impl for<'a> FnOnce(&'a mut Fixture) -> std::pin::Pin<Box<dyn Future<Output = ()> + 'a>>,
) {
    let Some(mut fixture) = (if let Some(url) = nats_url {
        Fixture::new_at(url).await
    } else {
        Fixture::new().await
    }) else {
        return;
    };
    let result = AssertUnwindSafe(timeout(Duration::from_secs(90), test(&mut fixture)))
        .catch_unwind()
        .await;
    if !matches!(result, Ok(Ok(()))) {
        for index in 0..fixture.children.len() {
            eprintln!(
                "service {index}: {}",
                std::fs::read_to_string(fixture.files.path().join(format!("service-{index}.log")))
                    .unwrap()
            );
        }
    }
    fixture.cleanup().await;
    match result {
        Ok(result) => result.expect("test timed out"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

fn assert_requests(id: SessionId, events: &[Event], count: usize) {
    let requests: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::InferenceRequested {
                seq,
                request_id,
                step,
            } => {
                assert_eq!(seq, step);
                assert_eq!(*request_id, RequestId::for_step(id, *step));
                Some(*request_id)
            }
            _ => None,
        })
        .collect();
    assert_eq!(requests.len(), count);
    assert_eq!(requests.iter().collect::<HashSet<_>>().len(), count);
}

#[tokio::test]
async fn tool_turn_live_events_and_snapshot_survive_worker_restart() {
    run(|f| {
        Box::pin(async move {
            f.script(true, "get_time");
            f.start("swarmy-scheduler", None);
            f.start("swarmy-gateway", None);
            let worker = f.start("swarmy-worker", None);
            let id = f.create().await;
            let mut live = f
                .bus
                .subscribe_live::<Event>(LiveFeed::SessionEvents(id))
                .await
                .unwrap();
            f.wake(id).await;
            let events = f.idle(id).await;
            let tags: Vec<_> = events
                .iter()
                .map(|event| match event {
                    Event::MessageAppended { message, .. } if message.role == MessageRole::User => {
                        "user"
                    }
                    Event::InferenceRequested { .. } => "request",
                    Event::InferenceCompleted { .. } => "completion",
                    Event::ToolCallRequested { .. } => "tool request",
                    Event::ToolCallCompleted {
                        result: ToolResult::Completed { output, .. },
                        ..
                    } => {
                        output.parse::<Timestamp>().unwrap();
                        "tool completion"
                    }
                    Event::MessageAppended { .. } => "fold",
                    Event::StateChanged { .. } => "state",
                    _ => panic!("unexpected event: {event:?}"),
                })
                .collect();
            assert_eq!(
                tags,
                [
                    "user",
                    "request",
                    "completion",
                    "tool request",
                    "tool completion",
                    "fold",
                    "request",
                    "completion",
                    "state"
                ]
            );
            assert_requests(id, &events, 2);
            let mut received = BTreeMap::new();
            timeout(WAIT, async {
                while received.len() < events.len() {
                    let event = live.next().await.unwrap().unwrap();
                    received.insert(event.seq(), event);
                }
            })
            .await
            .unwrap();
            assert_eq!(received.into_values().collect::<Vec<_>>(), events);
            assert_eq!(f.calls(), 2);
            f.children[worker].kill().await.unwrap();
            f.user_message(id).await;
            f.start("swarmy-worker", None);
            f.wake(id).await;
            let events = f.idle(id).await;
            assert_requests(id, &events, 3);
            assert_eq!(f.calls(), 3);
            let request = events
                .iter()
                .rev()
                .find_map(|event| {
                    if let Event::InferenceRequested { request_id, .. } = event {
                        Some(*request_id)
                    } else {
                        None
                    }
                })
                .unwrap();
            let job: swarmy_llm::InferenceJob =
                f.store.get_inference_input(request).await.unwrap().unwrap();
            assert_eq!(job.request.messages.len(), 5);
        })
    })
    .await;
}

#[tokio::test]
async fn rate_limit_waits_without_a_worker_lease_then_recovers() {
    run(|f| Box::pin(async move {
        f.rate_limit_script(1, 2);
        f.start("swarmy-scheduler", None);
        f.start("swarmy-gateway", None);
        f.start("swarmy-worker", None);
        let id = f.create().await;
        f.wake(id).await;
        let started = std::time::Instant::now();
        timeout(WAIT, async {
            loop {
                if f.store.fetch_session(id).await.unwrap().unwrap().state == SessionState::Sleeping {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        }).await.unwrap();
        assert!(f.store.inference_wait(id).await.unwrap().unwrap().reasons[0].contains("quota reached"));
        let leases = f.store.scan_expired_leases(
            Timestamp::now().checked_add(Duration::from_secs(60)).unwrap(), None, 64
        ).await.unwrap();
        assert!(!leases.iter().any(|(session, _)| *session == id));
        let events = f.idle(id).await;
        assert!(started.elapsed() >= Duration::from_secs(2));
        assert_eq!(f.calls(), 2);
        assert!(events.iter().any(|event| matches!(event, Event::InferenceFailed { retryable: true, .. })));
        assert!(events.iter().any(|event| matches!(event, Event::InferenceCompleted { .. })));
        assert!(!events.iter().any(|event| matches!(event, Event::MessageAppended { message, .. }
            if message.role == MessageRole::System && message.parts.iter().any(|part| matches!(part, Part::Text { text } if text.contains("quota reached"))))));
    })).await;
}

#[tokio::test]
async fn parked_inference_can_be_interrupted_and_followed_by_a_new_turn() {
    run(|f| {
        Box::pin(async move {
            f.rate_limit_script(1, 3600);
            f.start("swarmy-scheduler", None);
            f.start("swarmy-gateway", None);
            f.start("swarmy-worker", None);
            let id = f.create().await;
            f.wake(id).await;
            timeout(WAIT, async {
                while f.store.fetch_session(id).await.unwrap().unwrap().state
                    != SessionState::Sleeping
                {
                    sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            let start = std::time::Instant::now();
            f.interrupt_from_cli(id).await;
            timeout(Duration::from_secs(1), async {
                while f.store.fetch_session(id).await.unwrap().unwrap().state != SessionState::Idle
                {
                    sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            assert!(start.elapsed() < Duration::from_secs(1));
            assert!(f.store.inference_wait(id).await.unwrap().is_none());
            let head = f.store.fetch_session(id).await.unwrap().unwrap().head_seq;
            let last = f.store.read_events(id, head - 1, 1).await.unwrap();
            assert!(
                matches!(&last[0], Event::InferenceFailed { retryable: false, error, .. }
            if error == "interrupted by operator")
            );
            let key = swarmy_store::CredentialKey::provider("fake");
            f.store
                .claim_entry(
                    &key,
                    Timestamp::now()
                        .checked_add(Duration::from_secs(3601))
                        .unwrap(),
                )
                .await
                .unwrap();
            f.store.entry_success(&key).await.unwrap();
            f.user_message(id).await;
            f.wake(id).await;
            let events = f.idle(id).await;
            assert!(
                events
                    .iter()
                    .any(|event| matches!(event, Event::InferenceCompleted { .. }))
            );
        })
    })
    .await;
}

#[tokio::test]
async fn inflight_inference_interrupt_ends_at_next_boundary() {
    run(|f| {
        Box::pin(async move {
            f.script(false, "get_time");
            let script = f.files.path().join("script.json");
            let mut data: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&script).unwrap()).unwrap();
            data["latency_ms"] = 1500.into();
            std::fs::write(&script, serde_json::to_vec(&data).unwrap()).unwrap();
            f.start("swarmy-scheduler", None);
            f.start("swarmy-gateway", None);
            f.start("swarmy-worker", None);
            let id = f.create().await;
            f.wake(id).await;
            timeout(WAIT, async {
                while f.store.fetch_session(id).await.unwrap().unwrap().state
                    != SessionState::WaitingInference
                {
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            f.interrupt_from_cli(id).await;
            assert!(
                f.store
                    .fetch_session(id)
                    .await
                    .unwrap()
                    .unwrap()
                    .interrupt_requested
            );
            timeout(WAIT, async {
                while f.store.fetch_session(id).await.unwrap().unwrap().state != SessionState::Idle
                {
                    sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            let events = f.store.read_events(id, 0, 64).await.unwrap();
            assert!(
                events.iter().any(
                    |event| matches!(event, Event::InferenceFailed { retryable: false, error, .. }
            if error == "interrupted by operator")
                ),
                "events after interruption: {events:?}"
            );
            assert!(
                !f.store
                    .fetch_session(id)
                    .await
                    .unwrap()
                    .unwrap()
                    .interrupt_requested
            );
            assert_eq!(f.calls(), 1);
        })
    })
    .await;
}

#[tokio::test]
async fn missing_gateway_parks_until_it_returns() {
    run(|f| Box::pin(async move {
        f.provider = "openai".into();
        f.script(false, "get_time");
        f.start("swarmy-scheduler", None);
        f.start("swarmy-worker", None);
        let id = f.create().await;
        f.wake(id).await;
        timeout(WAIT, async {
            while f.store.fetch_session(id).await.unwrap().unwrap().state != SessionState::Sleeping {
                sleep(Duration::from_millis(20)).await;
            }
        }).await.unwrap();
        let events = f.store.read_events(id, 0, 64).await.unwrap();
        assert!(events.iter().any(|event| matches!(event,
            Event::InferenceFailed { retryable: true, retry_at: Some(_), error, .. }
            if error.contains("no gateway serves provider openai"))));
        assert!(f.store.inference_wait(id).await.unwrap().is_some());
        f.start("swarmy-gateway", None);
            let events = f.idle(id).await;
            assert!(events.iter().any(|event| matches!(event, Event::InferenceCompleted { .. })));
            assert!(!events.iter().any(|event| matches!(event,
                Event::InferenceFailed { retryable: false, .. })));
        assert!(!events.iter().any(|event| matches!(event,
            Event::MessageAppended { message, .. } if message.role == MessageRole::System
                && message.parts.iter().any(|part| matches!(part, Part::Text { text } if text.contains("no gateway serves"))))));
    })).await;
}

#[tokio::test]
async fn unknown_provider_fails_without_waiting() {
    run(|f| {
        Box::pin(async move {
            f.script(false, "get_time");
            f.start("swarmy-scheduler", None);
            f.start("swarmy-worker", None);
            for provider in ["unknown-provider", "openai"] {
                let id = f.create_with_provider(Some(provider)).await;
                f.wake(id).await;
                let events = f.idle(id).await;
                assert!(events.iter().any(|event| matches!(event,
                Event::InferenceFailed { retryable: false, retry_at: None, error, .. }
                if error.contains(&format!("no gateway serves provider {provider}")))));
                assert!(f.store.inference_wait(id).await.unwrap().is_none());
            }
        })
    })
    .await;
}

#[tokio::test]
async fn missing_gateway_exhausts_wait_budget() {
    run(|f| Box::pin(async move {
        f.provider = "openai".into();
        f.max_wait_seconds = 2;
        f.script(false, "get_time");
        f.start("swarmy-scheduler", None);
        f.start("swarmy-worker", None);
        let id = f.create().await;
        f.wake(id).await;
        let events = f.idle(id).await;
        assert!(events.iter().any(|event| matches!(event,
            Event::InferenceFailed { retryable: false, error, .. }
            if error.contains("inference wait exceeded") && error.contains("no gateway serves provider openai"))));
    })).await;
}

#[tokio::test]
async fn inference_wait_budget_ends_a_turn_with_accumulated_reasons() {
    run(|f| {
        Box::pin(async move {
            f.max_wait_seconds = 3;
            f.rate_limit_script(100, 1);
            f.start("swarmy-scheduler", None);
            f.start("swarmy-gateway", None);
            f.start("swarmy-worker", None);
            let id = f.create().await;
            let started = std::time::Instant::now();
            f.wake(id).await;
            let events = f.idle(id).await;
            assert!(started.elapsed() >= Duration::from_secs(3));
            assert!(events.iter().any(
                |event| matches!(event, Event::InferenceFailed { retryable: false, error, .. }
            if error.contains("inference wait exceeded") && error.contains("quota reached"))
            ));
        })
    })
    .await;
}

#[tokio::test]
async fn first_entry_breaker_fails_over_to_second_entry_implicitly() {
    run(|f| {
        Box::pin(async move {
            f.provider = "openai".into();
            f.keyring();
            f.put_entry("primary").await;
            f.put_entry("backup").await;
            f.rate_limit_script(1, 3600);
            f.start("swarmy-scheduler", None);
            f.start("swarmy-gateway", None);
            f.start("swarmy-worker", None);
            f.gateway_serves("openai").await;
            let first = f.create().await;
            f.wake(first).await;
            // Without a named route the provider's entries form the implicit
            // chain in creation order: the 429 on the primary fails the turn
            // over to the backup instead of parking the session.
            let events = f.idle(first).await;
            assert!(events.iter().any(|event| matches!(
                event,
                Event::InferenceFailed {
                    retryable: true,
                    ..
                }
            )));
            let completed = events.iter().find_map(|event| match event {
                Event::InferenceCompleted {
                    entry,
                    route,
                    route_step,
                    ..
                } => Some((entry.clone(), route.clone(), *route_step)),
                _ => None,
            });
            assert_eq!(
                completed,
                Some((Some("backup".into()), None, Some(1))),
                "the completion names the failover entry and step"
            );
            let primary = swarmy_store::CredentialKey::entry("openai", "primary");
            let backup = swarmy_store::CredentialKey::entry("openai", "backup");
            assert!(
                f.store
                    .entry_open_until(&primary, Timestamp::now())
                    .await
                    .unwrap()
                    .is_some()
            );
            assert!(
                f.store
                    .entry_open_until(&backup, Timestamp::now())
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(f.calls(), 2);
        })
    })
    .await;
}

#[tokio::test]
async fn route_fails_over_across_providers_and_records_entry_and_step() {
    run(|f| {
        Box::pin(async move {
            f.fake_pair();
            f.keyring();
            f.put_entry_for("fake-a", "first").await;
            f.put_entry_for("fake-b", "second").await;
            f.put_route("ab", &[("fake-a", "first"), ("fake-b", "second")])
                .await;
            f.rate_limit_script(1, 1);
            f.start("swarmy-scheduler", None);
            f.start("swarmy-gateway", None);
            f.start("swarmy-worker", None);
            f.gateway_serves("fake-a").await;
            f.gateway_serves("fake-b").await;
            let id = f.create_with_route("ab").await;
            f.wake(id).await;
            // The 429 on the first step fails the turn over to the second
            // provider; the turn completes there without waiting out the retry.
            let events = f.idle(id).await;
            assert_requests(id, &events, 2);
            let completed = events.iter().find_map(|event| match event {
                Event::InferenceCompleted {
                    entry,
                    route,
                    route_step,
                    request_id,
                    ..
                } => Some((entry.clone(), route.clone(), *route_step, *request_id)),
                _ => None,
            });
            let (entry, route, step, request_id) =
                completed.expect("turn completes on the second step");
            assert_eq!(entry.as_deref(), Some("second"));
            assert_eq!(route.as_deref(), Some("ab"));
            assert_eq!(step, Some(1));
            let usage = f
                .store
                .inference_usage_record(request_id)
                .await
                .unwrap()
                .expect("completion records usage");
            assert_eq!(usage.entry.as_deref(), Some("second"));
            assert_eq!(usage.route.as_deref(), Some("ab"));
            assert_eq!(usage.route_step, Some(1));
            assert_eq!(f.calls(), 2);
        })
    })
    .await;
}

#[tokio::test]
async fn route_failover_drops_previous_provider_reasoning() {
    run(|f| {
        Box::pin(async move {
            f.fake_pair();
            f.keyring();
            f.put_entry_for("fake-a", "first").await;
            f.put_entry_for("fake-b", "second").await;
            f.put_route("ab", &[("fake-a", "first"), ("fake-b", "second")])
                .await;
            // The first attempt answers with reasoning and a tool call; the
            // folded attempt hits the rate limit, so the failover request to
            // the second provider must not replay the first provider's
            // reasoning blocks.
            let first = Response {
                parts: vec![
                    Part::Reasoning {
                        text: "Think first.".into(),
                        metadata: BTreeMap::from([(
                            "openai_responses".into(),
                            serde_json::json!({
                                "provider": "fake-a",
                                "model": "fake-model",
                                "item": {"type": "reasoning"},
                            }),
                        )]),
                    },
                    Part::ToolCall {
                        call_id: ToolCallId("clock".into()),
                        tool: "get_time".into(),
                        input: serde_json::json!({}),
                    },
                ],
                stop_reason: StopReason::ToolCalls,
                usage: TokenUsage::default(),
                quota_remaining: BTreeMap::new(),
                quota_resets: BTreeMap::new(),
            };
            let answer = Response {
                parts: vec![Part::Text {
                    text: "The turn is complete.".into(),
                }],
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
                quota_remaining: BTreeMap::new(),
                quota_resets: BTreeMap::new(),
            };
            std::fs::write(
                f.files.path().join("script.json"),
                serde_json::to_vec(&serde_json::json!({
                    "responses": {"0": first, "2": answer, "3": answer},
                    "failures": {"1": {"status": 429, "message": "quota reached", "retry_after_seconds": 1}},
                }))
                .unwrap(),
            )
            .unwrap();
            f.start("swarmy-scheduler", None);
            f.start("swarmy-gateway", None);
            f.start("swarmy-worker", None);
            f.gateway_serves("fake-a").await;
            f.gateway_serves("fake-b").await;
            let id = f.create_with_route("ab").await;
            f.wake(id).await;
            let events = f.idle(id).await;
            assert!(
                events
                    .iter()
                    .any(|event| matches!(event, Event::InferenceCompleted { .. }))
            );
            assert_eq!(f.calls(), 3);
            let histories = f.histories();
            assert_eq!(histories.len(), 3);
            // The folded retry on the first provider keeps its reasoning.
            assert!(
                histories[1].iter().any(|message| message.parts.iter().any(
                    |part| matches!(part, Part::Reasoning { .. })
                )),
                "same-provider retries keep reasoning"
            );
            // The failover request carries the thinking as text, never as a
            // replayable reasoning block from the other provider.
            assert!(
                histories[2].iter().all(|message| message.parts.iter().all(
                    |part| !matches!(part, Part::Reasoning { .. })
                )),
                "failover drops the previous provider's reasoning blocks"
            );
            assert!(
                histories[2].iter().any(|message| message.parts.iter().any(
                    |part| matches!(part, Part::Text { text } if text == "Think first.")
                )),
                "downgraded thinking text is preserved"
            );
        })
    })
    .await;
}

#[tokio::test]
async fn one_step_route_waits_without_touching_other_entries() {
    run(|f| {
        Box::pin(async move {
            f.provider = "openai".into();
            f.keyring();
            f.put_entry("primary").await;
            f.put_entry("backup").await;
            f.put_route("pinned", &[("openai", "primary")]).await;
            // A one-step route pins the agent: with its entry open the
            // session waits instead of failing over within the provider.
            f.store
                .entry_failure(
                    &swarmy_store::CredentialKey::entry("openai", "primary"),
                    Timestamp::now()
                        .checked_add(Duration::from_secs(3600))
                        .unwrap(),
                    "openai/primary: quota reached",
                )
                .await
                .unwrap();
            f.script(false, "get_time");
            f.start("swarmy-scheduler", None);
            f.start("swarmy-gateway", None);
            f.start("swarmy-worker", None);
            f.gateway_serves("openai").await;
            let id = f.create_with_route("pinned").await;
            f.wake(id).await;
            timeout(WAIT, async {
                loop {
                    if f.store.fetch_session(id).await.unwrap().unwrap().state
                        == SessionState::Sleeping
                    {
                        break;
                    }
                    sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            let wait = f.store.inference_wait(id).await.unwrap().unwrap();
            assert!(
                wait.reasons
                    .iter()
                    .any(|reason| reason.contains("openai/primary")),
                "waiting reasons name the pinned entry: {:?}",
                wait.reasons
            );
            assert_eq!(f.calls(), 0, "pinned turns never call another entry");
            assert!(
                f.store
                    .entry_open_until(
                        &swarmy_store::CredentialKey::entry("openai", "backup"),
                        Timestamp::now()
                    )
                    .await
                    .unwrap()
                    .is_none()
            );
        })
    })
    .await;
}

#[tokio::test]
async fn agent_route_assignment_changes_next_turn_entry() {
    run(|f| {
        Box::pin(async move {
            f.provider = "openai".into();
            f.keyring();
            f.put_entry("primary").await;
            f.put_entry("backup").await;
            f.put_route("use-backup", &[("openai", "backup")]).await;
            f.script(false, "get_time");
            f.start("swarmy-scheduler", None);
            f.start("swarmy-gateway", None);
            f.start("swarmy-worker", None);
            f.gateway_serves("openai").await;
            let agent = f.create_named_agent("routed").await;
            let id = f.create_agent_session(agent, None).await;
            f.wake(id).await;
            // Without assignments the implicit chain serves the oldest entry.
            let first = f.idle(id).await;
            assert!(first.iter().any(
                |event| matches!(event, Event::InferenceCompleted { entry: Some(entry), .. } if entry == "primary")
            ));
            // Assigning the agent's route changes which entry the next turn uses.
            f.set_agent_route(agent, Some("use-backup")).await;
            f.user_message(id).await;
            f.wake(id).await;
            let second = f.idle(id).await;
            let entries: Vec<_> = second
                .iter()
                .filter_map(|event| match event {
                    Event::InferenceCompleted { entry, .. } => entry.clone(),
                    _ => None,
                })
                .collect();
            // Both turns' completions stay readable; the second turn serves backup.
            assert_eq!(entries, ["primary", "backup"]);
        })
    })
    .await;
}

#[tokio::test]
async fn session_route_overrides_agent_route() {
    run(|f| {
        Box::pin(async move {
            f.provider = "openai".into();
            f.keyring();
            f.put_entry("primary").await;
            f.put_entry("backup").await;
            f.put_route("use-backup", &[("openai", "backup")]).await;
            f.put_route("use-primary", &[("openai", "primary")]).await;
            f.script(false, "get_time");
            f.start("swarmy-scheduler", None);
            f.start("swarmy-gateway", None);
            f.start("swarmy-worker", None);
            f.gateway_serves("openai").await;
            let agent = f.create_named_agent("routed").await;
            f.set_agent_route(agent, Some("use-backup")).await;
            // The session override wins for that session only; the agent keeps
            // its own assignment for every other session.
            let id = f.create_agent_session(agent, Some("use-primary")).await;
            f.wake(id).await;
            let events = f.idle(id).await;
            let entries: Vec<_> = events
                .iter()
                .filter_map(|event| match event {
                    Event::InferenceCompleted { entry, .. } => entry.clone(),
                    _ => None,
                })
                .collect();
            assert_eq!(entries, ["primary"]);
            let agent_record = f.store.get_agent(agent).await.unwrap().unwrap();
            assert_eq!(agent_record.route.as_deref(), Some("use-backup"));
        })
    })
    .await;
}

#[tokio::test]
async fn all_entries_open_parks_until_earliest_retry() {
    run(|f| {
        Box::pin(async move {
            f.provider = "openai".into();
            f.keyring();
            f.put_entry("primary").await;
            f.put_entry("backup").await;
            f.script(false, "get_time");
            let started = std::time::Instant::now();
            // Gateway-written reasons name the entry; the scheduler parks with
            // them verbatim, which is what session show renders.
            for (label, after) in [
                ("primary", Duration::from_secs(3)),
                ("backup", Duration::from_secs(3600)),
            ] {
                f.store
                    .entry_failure(
                        &swarmy_store::CredentialKey::entry("openai", label),
                        Timestamp::now().checked_add(after).unwrap(),
                        &format!("openai/{label}: quota reached"),
                    )
                    .await
                    .unwrap();
            }
            f.start("swarmy-scheduler", None);
            f.start("swarmy-gateway", None);
            f.start("swarmy-worker", None);
            let id = f.create().await;
            f.wake(id).await;
            // Both entries are open, so the session parks instead of calling.
            // The first park may blame a missing advertisement before the
            // gateway is ready; wait for the entry-named breaker reason.
            timeout(WAIT, async {
                loop {
                    if f.store
                        .inference_wait(id)
                        .await
                        .unwrap()
                        .is_some_and(|wait| {
                            wait.reasons.iter().any(|reason| {
                                reason.contains("openai/primary")
                                    && reason.contains("quota reached")
                            })
                        })
                    {
                        break;
                    }
                    sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            let wait = f.store.inference_wait(id).await.unwrap().unwrap();
            assert!(
                wait.reasons
                    .iter()
                    .any(|reason| reason.contains("openai/primary")
                        && reason.contains("quota reached")),
                "waiting reasons name the entry: {:?}",
                wait.reasons
            );
            assert_eq!(f.calls(), 0);
            // The session wakes at the earlier retry and completes on the
            // primary probe. Waiting for the later retry would time out idle.
            let events = f.idle(id).await;
            assert!(started.elapsed() >= Duration::from_millis(2900));
            assert!(
                events
                    .iter()
                    .any(|event| matches!(event, Event::InferenceCompleted { .. }))
            );
            assert!(
                f.store
                    .entry_open_until(
                        &swarmy_store::CredentialKey::entry("openai", "primary"),
                        Timestamp::now()
                    )
                    .await
                    .unwrap()
                    .is_none()
            );
        })
    })
    .await;
}

#[tokio::test]
async fn twenty_sessions_share_one_open_breaker() {
    run(|f| {
        Box::pin(async move {
            f.rate_limit_script(1, 5);
            f.start("swarmy-scheduler", None);
            f.start("swarmy-gateway", None);
            f.start("swarmy-worker", None);
            let mut ids = Vec::new();
            for _ in 0..20 {
                let id = f.create().await;
                f.wake(id).await;
                ids.push(id);
            }
            timeout(WAIT, async {
                loop {
                    if f.store
                        .entry_open_until(
                            &swarmy_store::CredentialKey::provider("fake"),
                            Timestamp::now(),
                        )
                        .await
                        .unwrap()
                        .is_some()
                    {
                        break;
                    }
                    sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            let calls = f.calls();
            sleep(Duration::from_secs(1)).await;
            assert_eq!(
                f.calls(),
                calls,
                "provider was called while breaker was open"
            );
            for id in ids {
                let events = f.idle(id).await;
                assert!(
                    events
                        .iter()
                        .any(|event| matches!(event, Event::InferenceCompleted { .. }))
                );
            }
            assert_eq!(f.calls(), 21);
        })
    })
    .await;
}

async fn kill_point(point: &'static str) {
    run(|f| {
        Box::pin(async move {
            f.script(true, "get_time");
            f.start("swarmy-scheduler", None);
            f.start("swarmy-gateway", None);
            let worker = f.start("swarmy-worker", Some(point));
            let id = f.create().await;
            f.wake(id).await;
            let status = timeout(WAIT, f.children[worker].wait())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(status.code(), Some(137));
            f.start("swarmy-worker", None);
            let events = f.idle(id).await;
            assert_requests(id, &events, 2);
            assert_eq!(f.calls(), 2);
        })
    })
    .await;
}

#[tokio::test]
async fn recover_after_claim() {
    kill_point("after_claim").await;
}
#[tokio::test]
async fn recover_after_request_event() {
    kill_point("after_request_event").await;
}
#[tokio::test]
async fn recover_before_release() {
    kill_point("before_release").await;
}
#[tokio::test]
async fn recover_unpublished_waiting_inference() {
    kill_point("after_release").await;
}

#[tokio::test]
async fn large_request_dispatches_on_default_nats_limit() {
    if std::env::var("SWARMY_FDB_CLUSTER_FILE").is_err()
        || std::env::var("SWARMY_S3_ENDPOINT").is_err()
    {
        eprintln!("skipping worker integration test: dev stack is unset");
        return;
    }
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let files = TempDir::new().unwrap();
    let mut server = Command::new("nats-server")
        .args([
            "-js",
            "-sd",
            files.path().to_str().unwrap(),
            "-p",
            &port.to_string(),
        ])
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let url = format!("nats://127.0.0.1:{port}");
    timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(client) = async_nats::connect(&url).await {
                assert_eq!(client.max_payload(), 1_048_576);
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    run_at(Some(url), |f| {
        Box::pin(async move {
            f.script(false, "");
            f.start("swarmy-scheduler", None);
            f.start("swarmy-gateway", None);
            f.start("swarmy-worker", None);
            let id = f.create().await;
            let head = f.store.fetch_session(id).await.unwrap().unwrap().head_seq;
            f.store
                .append_events(
                    id,
                    head,
                    &[Event::MessageAppended {
                        seq: 0,
                        message: Message {
                            id: MessageId::from_ulid(Ulid::generate()),
                            role: MessageRole::Tool,
                            parts: vec![Part::Text {
                                text: "x".repeat(1_200_000),
                            }],
                        },
                    }],
                )
                .await
                .unwrap();
            f.wake(id).await;
            let events = f.idle(id).await;
            assert_requests(id, &events, 1);
            let request = events
                .iter()
                .find_map(|event| match event {
                    Event::InferenceRequested { request_id, .. } => Some(*request_id),
                    _ => None,
                })
                .unwrap();
            let job: swarmy_llm::InferenceJob =
                f.store.get_inference_input(request).await.unwrap().unwrap();
            assert!(swarmy_core::encode(&job.request).unwrap().len() > 1_048_576);
            assert_eq!(f.calls(), 1);
        })
    })
    .await;
    server.kill().await.unwrap();
}

#[tokio::test]
async fn permanent_publish_error_ends_turn() {
    if std::env::var("SWARMY_FDB_CLUSTER_FILE").is_err()
        || std::env::var("SWARMY_S3_ENDPOINT").is_err()
    {
        eprintln!("skipping worker integration test: dev stack is unset");
        return;
    }
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let files = TempDir::new().unwrap();
    let config = files.path().join("nats.conf");
    std::fs::write(&config, "max_payload: 1MB\n").unwrap();
    let mut server = Command::new("nats-server")
        .args([
            "-js",
            "-sd",
            files.path().to_str().unwrap(),
            "-p",
            &port.to_string(),
            "-c",
            config.to_str().unwrap(),
        ])
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let url = format!("nats://127.0.0.1:{port}");
    timeout(Duration::from_secs(10), async {
        loop {
            if async_nats::connect(&url).await.is_ok() {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let mut f = Fixture::new_at(url.clone()).await.unwrap();
    f.bus.setup(&[WorkQueue::Runnable(7)]).await.unwrap();
    std::fs::write(&config, "max_payload: 512\n").unwrap();
    assert!(
        Command::new("kill")
            .args(["-HUP", &server.id().unwrap().to_string()])
            .status()
            .await
            .unwrap()
            .success()
    );
    timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(client) = async_nats::connect(&url).await
                && client.max_payload() == 512
            {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    f.model = "long-model-".to_owned() + &"x".repeat(600);
    f.script(false, "");
    f.start("swarmy-scheduler", None);
    f.start("swarmy-worker", None);
    let id = f.create().await;
    let started = std::time::Instant::now();
    f.wake(id).await;
    let events = f.idle(id).await;
    assert!(started.elapsed() < Duration::from_secs(5));
    let failed = events.iter().any(|event| {
        matches!(event, Event::InferenceFailed { retryable: false, error, .. }
            if error.contains("encoded bus message size") && error.contains("512"))
    });
    assert!(failed);
    let request_id = events
        .iter()
        .find_map(|event| match event {
            Event::InferenceRequested { request_id, .. } => Some(*request_id),
            _ => None,
        })
        .unwrap();
    assert!(
        f.store
            .get_inference_request::<swarmy_llm::Request>(request_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(f.calls(), 0);
    f.cleanup().await;
    server.kill().await.unwrap();
}

#[tokio::test]
async fn unknown_worker_tool_becomes_an_error_result() {
    run(|f| Box::pin(async move {
        f.script(true, "missing_tool");
        f.start("swarmy-scheduler", None);
        f.start("swarmy-gateway", None);
        f.start("swarmy-worker", None);
        let id = f.create().await;
        f.wake(id).await;
        let events = f.idle(id).await;
        assert!(events.iter().any(|event| matches!(event, Event::ToolCallCompleted {result: ToolResult::Error {error}, ..} if error.contains("missing_tool"))));
    })).await;
}

#[tokio::test]
async fn competing_workers_claim_each_step_once() {
    run(|f| {
        Box::pin(async move {
            f.script(false, "");
            f.start("swarmy-scheduler", None);
            f.start("swarmy-gateway", None);
            let first = f.start("swarmy-worker", None);
            let second = f.start("swarmy-worker", None);
            let mut sessions = Vec::new();
            for _ in 0..24 {
                let id = f.create().await;
                f.wake(id).await;
                for _ in 0..3 {
                    f.bus
                        .publish_work(&WorkQueue::Runnable(7), &Nudge { session_id: id })
                        .await
                        .unwrap();
                }
                sessions.push(id);
            }
            for id in sessions {
                assert_requests(id, &f.idle(id).await, 1);
            }
            assert_eq!(f.calls(), 24);
            let mut claims = HashSet::new();
            let mut owners = HashSet::new();
            for index in [first, second] {
                let log =
                    std::fs::read_to_string(f.files.path().join(format!("service-{index}.log")))
                        .unwrap();
                for line in log.lines().filter(|line| line.contains("claimed step")) {
                    let field = |name: &str| {
                        line.split_whitespace()
                            .find_map(|word| word.strip_prefix(name))
                            .unwrap()
                            .to_owned()
                    };
                    let key = (field("session_id="), field("step="));
                    assert!(claims.insert(key), "step claimed by both workers: {line}");
                    owners.insert(field("owner="));
                }
            }
            // The gateway commits terminal responses and idle together, so
            // each text turn needs only the inference submission claim.
            assert_eq!(claims.len(), 24);
            assert_eq!(owners.len(), 2);
        })
    })
    .await;
}

#[tokio::test]
async fn fresh_appends_and_gateway_completions_finish_without_a_scheduler() {
    run(|f| {
        Box::pin(async move {
            f.script(false, "");
            f.start("swarmy-gateway", None);
            f.start("swarmy-worker", None);
            let id = f.create().await;
            for index in 0..3 {
                if index > 0 {
                    f.user_message(id).await;
                }
                f.store.wake_session(id, Timestamp::now()).await.unwrap();
                let session = f.store.fetch_session(id).await.unwrap().unwrap();
                f.bus
                    .nudge(
                        id,
                        session.head_seq,
                        f.store.turn_id(id).await.unwrap(),
                        Duration::from_secs(60),
                        false,
                    )
                    .await
                    .unwrap();
                let events = timeout(Duration::from_secs(5), f.idle(id))
                    .await
                    .expect("turn needed a scheduler scan");
                assert_requests(id, &events, index + 1);
            }
            assert_eq!(f.calls(), 3);
        })
    })
    .await;
}

#[tokio::test]
async fn main_summary_atomically_archives_and_links_a_fresh_session() {
    run(|f| Box::pin(async move {
        f.summarize_at_tokens = 100;
        let summary = serde_json::json!({
            "goals": "Fix the tests", "state_of_work": "Parser fixed",
            "open_questions": "Which release?", "facts_to_keep": "Repository is /home/agent/project"
        }).to_string();
        let response = |text: String, input_tokens| Response {
            parts: vec![Part::Text { text }], stop_reason: StopReason::EndTurn,
            usage: TokenUsage { input_tokens, ..Default::default() },
            quota_remaining: std::collections::BTreeMap::new(),
            quota_resets: std::collections::BTreeMap::new(),
        };
        std::fs::write(f.files.path().join("script.json"), serde_json::to_vec(&serde_json::json!({
            "responses": {"0": response("Finished the turn".into(), 101), "1": response(summary.clone(), 120)}
        })).unwrap()).unwrap();
        let image = image_fixture::image(&f.store).await;
        let agent = f.store.create_agent("tommy", image, "", Timestamp::now()).await.unwrap();
        let id = loop {
            let id = SessionId::from_ulid(Ulid::generate());
            if runnable_partition(id) == 7 { break id; }
        };
        f.store.create_session_for_agent(id, Some(agent.agent_id), None, Timestamp::now()).await.unwrap();
        f.store.set_main_session(agent.agent_id, id).await.unwrap();
        f.user_message(id).await;
        f.start("swarmy-scheduler", None);
        f.start("swarmy-worker", None);
        f.start("swarmy-gateway", None);
        f.wake(id).await;
        let new = timeout(WAIT, async {
            loop {
                if let Some(next) = f.store.next_session(id).await.unwrap() { break next; }
                sleep(Duration::from_millis(25)).await;
            }
        }).await.unwrap();
        assert_ne!(id, new);
        assert_eq!(f.store.get_agent(agent.agent_id).await.unwrap().unwrap().main_session, Some(new));
        assert_eq!(f.store.fetch_session(id).await.unwrap().unwrap().state, SessionState::Completed);
        assert_eq!(f.store.previous_session(new).await.unwrap(), Some(id));
        let fresh = f.store.fetch_session(new).await.unwrap().unwrap();
        assert_eq!(fresh.state, SessionState::Idle);
        assert_eq!(fresh.agent_id, agent.agent_id);
        let opening = f.store.read_events(new, 0, 64).await.unwrap();
        let Event::MessageAppended { message, .. } = &opening[0] else { panic!("opening missing") };
        assert_eq!(message.role, MessageRole::System);
        let Part::Text { text } = &message.parts[0] else { panic!("summary missing") };
        assert_eq!(serde_json::from_str::<serde_json::Value>(text.lines().last().unwrap()).unwrap(), serde_json::from_str::<serde_json::Value>(&summary).unwrap());
        assert!(text.contains(&id.to_string()));
        let old = f.store.read_events(id, 0, 64).await.unwrap();
        assert_requests(id, &old, 2);
        assert_eq!(f.calls(), 2);
        assert_eq!(f.store.list_sessions_by_agent(agent.agent_id, None, 64).await.unwrap().len(), 2);
        // A duplicate, unfenced rollover cannot create a third session or move the pointer.
        let stale = swarmy_core::Lease { owner: swarmy_core::LeaseOwnerId::from_ulid(Ulid::generate()), seq: 1, expires_at: Timestamp::now() };
        assert!(f.store.summarize_main_session(id, 0, &stale, message).await.is_err());
        assert_eq!(f.store.get_agent(agent.agent_id).await.unwrap().unwrap().main_session, Some(new));
        assert_eq!(f.store.list_sessions_by_agent(agent.agent_id, None, 64).await.unwrap().len(), 2);
    })).await;
}

#[tokio::test]
async fn route_skips_an_expired_entry_for_the_next_step() {
    run(|f| {
        Box::pin(async move {
            f.fake_pair();
            f.keyring();
            // The subscription entry is stored but expired, so expansion
            // skips it exactly like a missing label; the turn serves the
            // key without ever failing on the dead entry.
            f.store
                .credentials(f.keyring.clone().expect("call keyring() first"))
                .put_entry(
                    swarmy_core::CredentialScope::Cluster,
                    "fake-a",
                    "sub",
                    &swarmy_core::CredentialRecord {
                        kind: swarmy_core::CredentialKind::OAuth {
                            access: "stale-access".into(),
                            refresh: "stale-refresh".into(),
                            expires_at: Timestamp::now()
                                .checked_sub(Duration::from_secs(60))
                                .unwrap(),
                            extra: BTreeMap::new(),
                        },
                        updated_at: Timestamp::now(),
                    },
                )
                .await
                .unwrap();
            f.put_entry_for("fake-b", "key").await;
            f.put_route("sub-then-key", &[("fake-a", "sub"), ("fake-b", "key")])
                .await;
            f.script(false, "get_time");
            f.start("swarmy-scheduler", None);
            f.start("swarmy-gateway", None);
            f.start("swarmy-worker", None);
            f.gateway_serves("fake-a").await;
            f.gateway_serves("fake-b").await;
            let id = f.create_with_route("sub-then-key").await;
            f.wake(id).await;
            let events = f.idle(id).await;
            assert!(
                events
                    .iter()
                    .all(|event| !matches!(event, Event::InferenceFailed { .. })),
                "the skipped entry never fails the turn"
            );
            let completed = events.iter().find_map(|event| match event {
                Event::InferenceCompleted {
                    entry,
                    route,
                    route_step,
                    ..
                } => Some((entry.clone(), route.clone(), *route_step)),
                _ => None,
            });
            assert_eq!(
                completed,
                Some((Some("key".into()), Some("sub-then-key".into()), Some(0))),
                "the turn serves the next usable step"
            );
            assert_eq!(f.calls(), 1);
        })
    })
    .await;
}

#[tokio::test]
async fn failover_survives_worker_restart_without_second_advance() {
    run(|f| {
        Box::pin(async move {
            f.provider = "openai".into();
            f.keyring();
            f.put_entry("first").await;
            f.put_entry("second").await;
            f.put_route("ab", &[("openai", "first"), ("openai", "second")])
                .await;
            // The first attempt fails with a long retry, so any second
            // advance would wrap to the open first entry and park the
            // session instead of completing.
            f.rate_limit_script(1, 3600);
            f.start("swarmy-scheduler", None);
            f.start("swarmy-gateway", None);
            // The first worker dies right after handing attempt one to the
            // gateway path; its lease lapses and the replacement recovers
            // the turn from the durable outbox.
            f.start("swarmy-worker", Some("after_release"));
            f.gateway_serves("openai").await;
            let id = f.create_with_route("ab").await;
            f.wake(id).await;
            // Attempt one is durable and its worker is dead before the
            // replacement starts: the kill fires synchronously after the
            // submit, so a durable request means the death already
            // happened, with a short grace for the event publish.
            timeout(WAIT, async {
                loop {
                    let session = f.store.fetch_session(id).await.unwrap().unwrap();
                    if session.state == SessionState::WaitingInference {
                        break;
                    }
                    if f.store
                        .read_events(id, 0, 64)
                        .await
                        .unwrap()
                        .iter()
                        .any(|event| matches!(event, Event::InferenceRequested { .. }))
                    {
                        break;
                    }
                    sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .unwrap();
            sleep(Duration::from_secs(3)).await;
            // The second worker recovers attempt one, records the 429 as a
            // failover to the second step, and dies with the step lease
            // still held, before it can submit attempt two.
            f.start("swarmy-worker", Some("after_advance"));
            timeout(WAIT, async {
                loop {
                    if f.store.fetch_session(id).await.unwrap().unwrap().route_step == 1 {
                        break;
                    }
                    sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .unwrap();
            // The replacement resumes after the failover: the handled
            // failure advances nothing again and parks nothing behind the
            // in-flight successor, so the turn completes on the second
            // entry instead of sleeping behind the first entry's retry.
            sleep(Duration::from_secs(3)).await;
            f.start("swarmy-worker", None);
            let events = f.idle(id).await;
            let completed = events.iter().find_map(|event| match event {
                Event::InferenceCompleted {
                    entry,
                    route,
                    route_step,
                    ..
                } => Some((entry.clone(), route.clone(), *route_step)),
                _ => None,
            });
            assert_eq!(
                completed,
                Some((Some("second".into()), Some("ab".into()), Some(1))),
                "the turn fails over exactly once across the restarts"
            );
            assert_eq!(f.calls(), 2);
            // A second advance would have wrapped to the open first entry
            // and parked behind its hour-long retry instead of completing,
            // so reaching Idle on the second entry proves the resumed
            // worker neither advanced again nor parked the successors. The
            // stored chain position is intentionally left unread here: the
            // gateway's wait cleanup lands just after the idle event the
            // test waits on, so asserting it would race the cleanup.
            let record = f.store.fetch_session(id).await.unwrap().unwrap();
            assert_eq!(
                record.state,
                SessionState::Idle,
                "no park behind the failover"
            );
        })
    })
    .await;
}

#[tokio::test]
async fn unrouted_ephemeral_first_attempt_skips_route_resolution() {
    run(|f| {
        Box::pin(async move {
            f.provider = "openai".into();
            f.keyring();
            f.put_entry("primary").await;
            f.put_entry("backup").await;
            f.script(false, "get_time");
            f.start("swarmy-scheduler", None);
            f.start("swarmy-gateway", None);
            f.start("swarmy-worker", None);
            f.gateway_serves("openai").await;
            let id = f.create().await;
            f.wake(id).await;
            let events = f.idle(id).await;
            // The gateway pool serves the oldest entry; the turn completes
            // without any failover.
            assert!(events.iter().any(|event| matches!(
                event,
                Event::InferenceCompleted {
                    entry: Some(entry),
                    ..
                } if entry == "primary"
            )));
            // The worker skipped the snapshot read for this first attempt,
            // so the stored job carries no pinned entry or step: the gateway
            // pool chose the entry.
            let request_id = events.iter().find_map(|event| match event {
                Event::InferenceRequested { request_id, .. } => Some(*request_id),
                _ => None,
            });
            let job: InferenceJob = f
                .store
                .get_inference_input(request_id.expect("one request"))
                .await
                .unwrap()
                .expect("stored job");
            assert_eq!(job.entry, None);
            assert_eq!(job.route, None);
            assert_eq!(job.route_step, 0);
            assert_eq!(f.calls(), 1);
        })
    })
    .await;
}
