#![deny(clippy::disallowed_methods)]
use std::{
    collections::{BTreeMap, HashSet},
    future::Future,
    os::unix::process::ExitStatusExt,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
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
    SessionRecord, SessionState, ToolCallId, ToolResult, ignore_best_effort,
};
use swarmy_llm::{Response, StopReason, TokenUsage};
use swarmy_store::{AgentSessionOptions, Store, blob::ObjectBlobStore, runnable_partition};
use tempfile::TempDir;
use tokio::{
    process::{Child, Command},
    time::timeout,
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
        let url = swarmy_testkit::require_stack("SWARMY_NATS_URL")?;
        Self::new_at(url).await
    }

    async fn new_at(nats_url: String) -> Option<Self> {
        for variable in ["SWARMY_FDB_CLUSTER_FILE", "SWARMY_S3_ENDPOINT"] {
            swarmy_testkit::require_stack(variable)?;
        }
        swarmy_testkit::boot_fdb();
        let prefix = format!("worker_{}", Ulid::generate());
        let store = Store::open(
            Some(std::path::Path::new(
                &swarmy_testkit::require_stack("SWARMY_FDB_CLUSTER_FILE").unwrap(),
            )),
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
        let objects = ObjectBlobStore::from_env().unwrap().object_store();
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
                    bookkeeping: swarmy_core::CredentialBookkeeping::default(),
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
        let mut script = swarmy_testkit::Script::new("The turn is complete.").latency_ms(10);
        if tools {
            script = script.tool_call(0, "clock", tool_name);
        }
        script.write_to(&self.files.path().join("script.json"));
    }

    fn rate_limit_script(&self, failures: usize, retry_after_seconds: u64) {
        swarmy_testkit::Script::new("Recovered.")
            .rate_limited(failures, retry_after_seconds)
            .write_to(&self.files.path().join("script.json"));
    }

    async fn interrupt_from_cli(&self, id: SessionId) {
        let executable = swarmy_testkit::bin("swarmy");
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
        let executable = swarmy_testkit::bin(service);
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

    /// Wait until a spawned service exits, so kill-point tests synchronize on
    /// the death itself instead of sleeping a fixed grace for it.
    async fn wait_exit(&mut self, index: usize) {
        swarmy_testkit::eventually("service exits", WAIT, async || {
            self.children[index].try_wait().unwrap().map(|_| ())
        })
        .await;
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
        let image = swarmy_testkit::image(&self.store).await;
        self.store
            .create_agent_session(
                id,
                None,
                Timestamp::now(),
                Some(AgentSessionOptions {
                    image: Some(image),
                    inference: Some(&swarmy_core::InferenceSelection::default()),
                    route: Some(route),
                }),
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
        swarmy_testkit::eventually("gateway advertises provider", WAIT, async || {
            self.store
                .gateway_serves(provider)
                .await
                .unwrap()
                .then_some(())
        })
        .await;
    }

    /// Wait until a session reaches `state`, naming the expectation so a
    /// timeout points at the missing transition instead of a bare deadline.
    async fn wait_state(&self, id: SessionId, state: SessionState) {
        let label: &'static str = match state {
            SessionState::Sleeping => "session sleeps",
            SessionState::Idle => "session idles",
            SessionState::WaitingInference => "session waits for inference",
            SessionState::Completed => "session completes",
            _ => "session reaches state",
        };
        swarmy_testkit::eventually(label, WAIT, async || {
            (self.store.fetch_session(id).await.unwrap().unwrap().state == state).then_some(())
        })
        .await;
    }

    fn histories(&self) -> Vec<Vec<Message>> {
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
                swarmy_testkit::image(&self.store).await,
            )
            .await
            .unwrap();
        self.user_message(id).await;
        id
    }

    async fn compactable_user_message(&self, id: SessionId) {
        let head = self
            .store
            .fetch_session(id)
            .await
            .unwrap()
            .unwrap()
            .head_seq;
        let mut history = vec![Event::MessageAppended {
            seq: 0,
            message: Message {
                id: MessageId::from_ulid(Ulid::generate()),
                role: MessageRole::User,
                parts: vec![Part::Text {
                    text: "Previous task".into(),
                }],
            },
        }];
        for _ in 0..30 {
            history.push(Event::MessageAppended {
                seq: 0,
                message: Message {
                    id: MessageId::from_ulid(Ulid::generate()),
                    role: MessageRole::Assistant,
                    parts: vec![Part::Text {
                        text: "x".repeat(3_000),
                    }],
                },
            });
        }
        self.store.append_events(id, head, &history).await.unwrap();
        self.user_message(id).await;
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
        swarmy_testkit::eventually("scheduler accepts wake", WAIT, async || {
            self.bus
                .request_wake(id, Duration::from_millis(200))
                .await
                .ok()
        })
        .await;
    }

    async fn idle(&self, id: SessionId) -> Vec<Event> {
        swarmy_testkit::eventually("session reaches Idle with snapshot", WAIT, async || {
            let session = self.store.fetch_session(id).await.unwrap().unwrap();
            if session.state == SessionState::Idle
                && let Some(snapshot) = session.snapshot_ref
            {
                self.snapshots.lock().unwrap().insert(snapshot.object_key);
                return Some(self.store.read_events(id, 0, 64).await.unwrap());
            }
            None
        })
        .await
    }

    fn calls(&self) -> usize {
        std::fs::read_to_string(self.files.path().join("calls"))
            .unwrap_or_default()
            .lines()
            .count()
    }

    async fn cleanup(&mut self) {
        for child in &mut self.children {
            ignore_best_effort(child.kill().await, "kill child process");
        }
        let blobs = ObjectBlobStore::from_env().unwrap();
        for key in self.snapshots.get_mut().unwrap().drain() {
            blobs.delete(&key).await.unwrap();
        }
        let db = Database::new(Some(
            &swarmy_testkit::require_stack("SWARMY_FDB_CLUSTER_FILE").unwrap(),
        ))
        .unwrap();
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
        for stream in ["INFER_REQ", "SCHED_RUNNABLE", "TOOL_NODE"] {
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
        f.wait_state(id, SessionState::Sleeping).await;
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
            f.wait_state(id, SessionState::Sleeping).await;
            let start = std::time::Instant::now();
            f.interrupt_from_cli(id).await;
            swarmy_testkit::eventually(
                "interrupt idles session",
                Duration::from_secs(1),
                async || {
                    (f.store.fetch_session(id).await.unwrap().unwrap().state == SessionState::Idle)
                        .then_some(())
                },
            )
            .await;
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
            f.wait_state(id, SessionState::WaitingInference).await;
            f.interrupt_from_cli(id).await;
            assert!(
                f.store
                    .fetch_session(id)
                    .await
                    .unwrap()
                    .unwrap()
                    .interrupt_requested
            );
            f.wait_state(id, SessionState::Idle).await;
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
        f.wait_state(id, SessionState::Sleeping).await;
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
                Event::InferenceCompleted { completion, .. } => Some((
                    completion.entry.clone(),
                    completion.route.clone(),
                    completion.route_step,
                )),
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
                    completion,
                    request_id,
                    ..
                } => Some((
                    completion.entry.clone(),
                    completion.route.clone(),
                    completion.route_step,
                    *request_id,
                )),
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

/// Wait until the session's inference wait carries a reason containing both
/// fragments, then return the wait.
async fn wait_for_wait_reason(
    f: &Fixture,
    id: SessionId,
    first: &str,
    second: &str,
) -> swarmy_store::InferenceWait {
    swarmy_testkit::eventually("breaker names the entry", WAIT, async || {
        f.store.inference_wait(id).await.unwrap().filter(|wait| {
            wait.reasons
                .iter()
                .any(|reason| reason.contains(first) && reason.contains(second))
        })
    })
    .await
}

/// Extract one `name=value` field from a claimed-step log line.
fn log_field(line: &str, name: &str) -> String {
    line.split_whitespace()
        .find_map(|word| word.strip_prefix(name))
        .unwrap()
        .to_owned()
}

/// Check one old-session event for the expected summary request shape,
/// recording which summary kind was seen.
async fn check_summary_request(
    f: &Fixture,
    event: &Event,
    found_prefix: &mut bool,
    found_history: &mut bool,
) {
    if let Event::InferenceRequested { request_id, .. } = event {
        let job: swarmy_llm::InferenceJob = f
            .store
            .get_inference_input(*request_id)
            .await
            .unwrap()
            .unwrap();
        if !job.summary {
            return;
        }
        let Part::Text { text } = &job.request.messages[0].parts[0] else {
            panic!("summary prompt missing")
        };
        if job.summary_prefix {
            *found_prefix = true;
            assert!(job.request.settings.max_output_tokens.unwrap() <= 8192);
            assert!(text.starts_with("# Conversation\n"));
            assert!(text.contains(swarmy_harness::TURN_PREFIX_SUMMARIZATION_PROMPT));
        } else {
            *found_history = true;
            assert!(text.contains("Previous task"));
            assert!(!text.contains("What time is it?"));
        }
    }
}

/// Wait until attempt one is durable: the session waits for inference or
/// the request event is already in the log.
async fn wait_for_durable_request(f: &Fixture, id: SessionId) {
    swarmy_testkit::eventually("attempt one is durable", WAIT, async || {
        let session = f.store.fetch_session(id).await.unwrap().unwrap();
        if session.state == SessionState::WaitingInference {
            return Some(());
        }
        f.store
            .read_events(id, 0, 64)
            .await
            .unwrap()
            .iter()
            .any(|event| matches!(event, Event::InferenceRequested { .. }))
            .then_some(())
    })
    .await;
}

/// Wait until the session's route step advances to the expected step.
async fn wait_for_route_step(f: &Fixture, id: SessionId, step: u32) {
    swarmy_testkit::eventually("route advances to the expected step", WAIT, async || {
        (f.store.fetch_session(id).await.unwrap().unwrap().route_step == step).then_some(())
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
            let wait = wait_for_wait_reason(f, id, "openai/primary", "quota reached").await;
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
async fn recover_at_each_kill_point() {
    run(|f| {
        Box::pin(async move {
            f.script(true, "get_time");
            f.start("swarmy-scheduler", None);
            // One fixture for every kill point: the scheduler stays up while
            // each iteration restarts the gateway (its scripted responses
            // are indexed by its own call count, so a fresh gateway serves
            // the tool call and the final answer for every point), kills
            // its worker, recovers the turn on a replacement, then stops
            // all three so the next point starts without competition.
            for point in [
                "after_claim",
                "after_request_event",
                "before_release",
                "after_release",
            ] {
                let calls_before = f.calls();
                let gateway = f.start("swarmy-gateway", None);
                let worker = f.start("swarmy-worker", Some(point));
                let id = f.create().await;
                f.wake(id).await;
                let status = timeout(WAIT, f.children[worker].wait())
                    .await
                    .unwrap()
                    .unwrap();
                assert!(
                    status.code() == Some(137) || status.signal() == Some(9),
                    "kill point {point}: {status}"
                );
                let replacement = f.start("swarmy-worker", None);
                let events = f.idle(id).await;
                assert_requests(id, &events, 2);
                assert_eq!(f.calls() - calls_before, 2, "kill point {point}");
                for index in [worker, replacement, gateway] {
                    ignore_best_effort(f.children[index].kill().await, "kill child process");
                    ignore_best_effort(f.children[index].wait().await, "reap child process");
                }
            }
        })
    })
    .await;
}

#[tokio::test]
async fn large_request_dispatches_on_default_nats_limit() {
    if swarmy_testkit::require_stack("SWARMY_FDB_CLUSTER_FILE").is_none()
        || swarmy_testkit::require_stack("SWARMY_S3_ENDPOINT").is_none()
    {
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
    let client = swarmy_testkit::eventually(
        "embedded nats accepts connections",
        Duration::from_secs(10),
        async || async_nats::connect(&url).await.ok(),
    )
    .await;
    assert_eq!(client.max_payload(), 1_048_576);
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
    if swarmy_testkit::require_stack("SWARMY_FDB_CLUSTER_FILE").is_none()
        || swarmy_testkit::require_stack("SWARMY_S3_ENDPOINT").is_none()
    {
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
    swarmy_testkit::eventually(
        "embedded nats accepts connections",
        Duration::from_secs(10),
        async || async_nats::connect(&url).await.ok().map(|_| ()),
    )
    .await;
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
    swarmy_testkit::eventually(
        "reloaded nats advertises the new payload limit",
        Duration::from_secs(10),
        async || {
            if let Ok(client) = async_nats::connect(&url).await
                && client.max_payload() == 512
            {
                return Some(());
            }
            None
        },
    )
    .await;
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
                    let key = (log_field(line, "session_id="), log_field(line, "step="));
                    assert!(claims.insert(key), "step claimed by both workers: {line}");
                    owners.insert(log_field(line, "owner="));
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
        let summary = "## Goal
Finish the task

## Progress
### Done
- [x] Handler done

## Next Steps
1. Verify".to_owned();
        let response = |text: String, input_tokens| Response {
            parts: vec![Part::Text { text }], stop_reason: StopReason::EndTurn,
            usage: TokenUsage { input_tokens, ..Default::default() },
            quota_remaining: BTreeMap::new(),
            quota_resets: BTreeMap::new(),
        };
        std::fs::write(f.files.path().join("script.json"), serde_json::to_vec(&serde_json::json!({
            "responses": {"0": response("Finished the turn".into(), 101), "1": response(summary.clone(), 120)}
        })).unwrap()).unwrap();
        let image = swarmy_testkit::image(&f.store).await;
        let agent = f.store.create_agent("tommy", image, "", Timestamp::now(), None).await.unwrap();
        let id = loop {
            let id = SessionId::from_ulid(Ulid::generate());
            if runnable_partition(id) == 7 { break id; }
        };
        f.store.create_agent_session(id, Some(agent.agent_id), Timestamp::now(), None).await.unwrap();
        f.store.set_main_session(agent.agent_id, id).await.unwrap();
        f.compactable_user_message(id).await;
        f.start("swarmy-scheduler", None);
        f.start("swarmy-worker", None);
        f.start("swarmy-gateway", None);
        f.wake(id).await;
        let new = wait_successor(f, id).await;
        assert_ne!(id, new);
        assert_eq!(f.store.get_agent(agent.agent_id).await.unwrap().unwrap().main_session, Some(new));
        assert_eq!(f.store.fetch_session(id).await.unwrap().unwrap().state, SessionState::Completed);
        assert_eq!(f.store.previous_session(new).await.unwrap(), Some(id));
        let fresh = f.store.fetch_session(new).await.unwrap().unwrap();
        assert_eq!(fresh.state, SessionState::Idle);
        assert_eq!(fresh.agent_id, agent.agent_id);
        let opening = f.store.read_events(new, 0, 64).await.unwrap();
        let Event::MessageAppended { message, .. } = &opening[0] else { panic!("opening missing") };
        assert_eq!(message.role, MessageRole::User);
        let Part::Text { text } = &message.parts[0] else { panic!("summary missing") };
        assert!(text.contains(&summary));
        let old = f.store.read_events(id, 0, 64).await.unwrap();
        assert_requests(id, &old, 2);
        assert_eq!(f.calls(), 2);
        assert_eq!(f.store.list_sessions_by_agent(agent.agent_id, None, 64).await.unwrap().len(), 2);
        // A duplicate, unfenced rollover cannot create a third session or move the pointer.
        let stale = swarmy_core::Lease { owner: swarmy_core::LeaseOwnerId::from_ulid(Ulid::generate()), seq: 1, expires_at: Timestamp::now() };
        assert!(f.store.summarize_main_session(id, 0, &stale, message, &[]).await.is_err());
        assert_eq!(f.store.get_agent(agent.agent_id).await.unwrap().unwrap().main_session, Some(new));
        assert_eq!(f.store.list_sessions_by_agent(agent.agent_id, None, 64).await.unwrap().len(), 2);
    })).await;
}

#[tokio::test]
async fn context_overflow_compacts_and_retries_once() {
    check_overflow_recovery(false).await;
}

#[tokio::test]
async fn second_context_overflow_ends_the_turn() {
    check_overflow_recovery(true).await;
}

async fn check_overflow_recovery(second_overflow: bool) {
    run(|f| Box::pin(async move {
        let summary = "## Goal\nFinish the work\n\n## Next Steps\n1. Retry";
        let mut failures = serde_json::json!({"0": {"status": 400, "message": "context overflow"}});
        if second_overflow {
            failures["2"] = serde_json::json!({"status": 400, "message": "context overflow again"});
        }
        std::fs::write(f.files.path().join("script.json"), serde_json::to_vec(&serde_json::json!({
            "responses": {"1": side_response(summary.into(), 20), "2": side_response("Recovered".into(), 20)},
            "failures": failures
        })).unwrap()).unwrap();
        let image = swarmy_testkit::image(&f.store).await;
        let agent = f.store.create_agent("overflow-agent", image, "", Timestamp::now(), None).await.unwrap();
        let id = side_id();
        f.store.create_agent_session(id, Some(agent.agent_id), Timestamp::now(), None).await.unwrap();
        f.compactable_user_message(id).await;
        f.start("swarmy-scheduler", None);
        f.start("swarmy-worker", None);
        f.start("swarmy-gateway", None);
        f.wake(id).await;
        let successor = wait_successor(f, id).await;
        let events = f.idle(successor).await;
        assert_eq!(f.calls(), 3, "overflow, summary, and one retried request");
        assert_eq!(f.store.previous_session(successor).await.unwrap(), Some(id));
        assert!(f.store.next_session(successor).await.unwrap().is_none());
        assert_eq!(events.iter().filter(|event| matches!(event, Event::InferenceFailed { .. })).count(), usize::from(second_overflow));
        if second_overflow {
            assert!(successor_messages(&events).iter().any(|message| message.parts.iter().any(|part| matches!(part, Part::Text { text } if text == "Context overflow recovery failed after one compact-and-retry attempt. Try reducing context or switching to a larger-context model."))));
        } else {
            assert!(events.iter().any(|event| matches!(event, Event::InferenceCompleted { completion, .. } if completion.message.parts.iter().any(|part| matches!(part, Part::Text { text } if text == "Recovered")))));
        }
    })).await;
}

#[tokio::test]
async fn clean_tool_completion_reenables_overflow_recovery() {
    run(|f| Box::pin(async move {
        let summary = "## Goal\nFinish the work";
        let mut large_tool = side_tool_response("clock-next", 20, false);
        large_tool.parts.push(Part::Reasoning {
            text: "thinking ".repeat(13_000),
            metadata: BTreeMap::new(),
        });
        std::fs::write(f.files.path().join("script.json"), serde_json::to_vec(&serde_json::json!({
            "responses": {
                "1": side_response(summary.into(), 20),
                "2": side_tool_response("clock-after-retry", 20, false),
                "3": large_tool,
                "5": side_response(summary.into(), 20),
                "6": side_response("## Original Request\nContinue".into(), 20),
                "7": side_response("Recovered again".into(), 20)
            },
            "failures": {
                "0": {"status": 400, "message": "context overflow"},
                "4": {"status": 400, "message": "context overflow after tool"}
            }
        })).unwrap()).unwrap();
        let image = swarmy_testkit::image(&f.store).await;
        let agent = f.store.create_agent("clean-reset", image, "", Timestamp::now(), None).await.unwrap();
        let id = side_id();
        f.store.create_agent_session(id, Some(agent.agent_id), Timestamp::now(), None).await.unwrap();
        f.compactable_user_message(id).await;
        f.start("swarmy-scheduler", None);
        f.start("swarmy-worker", None);
        f.start("swarmy-gateway", None);
        f.wake(id).await;
        let first = wait_successor(f, id).await;
        let second = wait_successor(f, first).await;
        let events = f.idle(second).await;
        assert_eq!(f.calls(), 8, "a clean tool reply resets the recovery guard");
        assert!(events.iter().any(|event| matches!(event, Event::InferenceCompleted { completion, .. } if completion.message.parts.iter().any(|part| matches!(part, Part::Text { text } if text == "Recovered again")))));
    })).await;
}

#[tokio::test]
async fn new_user_turn_reenables_overflow_recovery() {
    run(|f| Box::pin(async move {
        let summary = "## Goal\nKeep working";
        std::fs::write(f.files.path().join("script.json"), serde_json::to_vec(&serde_json::json!({
            "responses": {
                "1": side_response(summary.into(), 20),
                "2": side_response("First recovered reply".into(), 20),
                "4": side_response(summary.into(), 20),
                "5": side_response("Second recovered reply".into(), 20)
            },
            "failures": {
                "0": {"status": 400, "message": "context overflow"},
                "3": {"status": 400, "message": "context overflow"}
            }
        })).unwrap()).unwrap();
        let image = swarmy_testkit::image(&f.store).await;
        let agent = f.store.create_agent("reset-recovery", image, "", Timestamp::now(), None).await.unwrap();
        let id = side_id();
        f.store.create_agent_session(id, Some(agent.agent_id), Timestamp::now(), None).await.unwrap();
        f.compactable_user_message(id).await;
        f.start("swarmy-scheduler", None);
        f.start("swarmy-worker", None);
        f.start("swarmy-gateway", None);
        f.wake(id).await;
        let first = wait_successor(f, id).await;
        f.idle(first).await;
        let head = f.store.fetch_session(first).await.unwrap().unwrap().head_seq;
        f.store.append_events(first, head, &[Event::MessageAppended {
            seq: 0,
            message: Message {
                id: MessageId::from_ulid(Ulid::generate()),
                role: MessageRole::User,
                parts: vec![Part::Text { text: "Next request ".repeat(9_000) }],
            },
        }]).await.unwrap();
        f.wake(first).await;
        let second = wait_successor(f, first).await;
        let events = f.idle(second).await;
        assert_eq!(f.calls(), 6, "each turn has one overflow, summary, and retry");
        assert!(successor_messages(&events).iter().any(|message| message.parts.iter().any(|part| matches!(part, Part::Text { text } if text == "Second recovered reply"))));
    })).await;
}

#[tokio::test]
async fn early_length_stop_compacts_without_replaying_truncated_reply() {
    run(|f| Box::pin(async move {
        f.provider = "openai".into();
        f.keyring();
        f.put_entry("primary").await;
        let mut truncated = side_response("TRUNCATED_ATTEMPT".into(), 20);
        truncated.stop_reason = StopReason::MaxOutputTokens;
        truncated.usage.output_tokens = 1;
        let summary = "## Goal\nFinish the work\n\n## Next Steps\n- Retry";
        std::fs::write(f.files.path().join("script.json"), serde_json::to_vec(&serde_json::json!({
            "responses": {"0": truncated, "1": side_response(summary.into(), 20), "2": side_response("Recovered".into(), 20)}
        })).unwrap()).unwrap();
        let image = swarmy_testkit::image(&f.store).await;
        let agent = f.store.create_agent("length-agent", image, "", Timestamp::now(), None).await.unwrap();
        let id = side_id();
        f.store.create_agent_session(id, Some(agent.agent_id), Timestamp::now(), None).await.unwrap();
        f.compactable_user_message(id).await;
        f.start("swarmy-scheduler", None);
        f.start("swarmy-worker", None);
        f.start("swarmy-gateway", None);
        f.wake(id).await;
        let successor = wait_successor(f, id).await;
        let events = f.idle(successor).await;
        assert_eq!(f.calls(), 3);
        assert!(read_all_events(f, id).await.iter().any(|event| matches!(event, Event::InferenceCompleted { completion, .. } if completion.message.parts.iter().any(|part| matches!(part, Part::Text { text } if text == "TRUNCATED_ATTEMPT")))));
        assert!(!successor_messages(&events).iter().any(|message| message.parts.iter().any(|part| matches!(part, Part::Text { text } if text.contains("TRUNCATED_ATTEMPT")))));
        assert!(events.iter().any(|event| matches!(event, Event::InferenceCompleted { completion, .. } if completion.message.parts.iter().any(|part| matches!(part, Part::Text { text } if text == "Recovered")))));
    })).await;
}

#[tokio::test]
async fn second_length_stop_fails_with_notice() {
    run(|f| Box::pin(async move {
        f.provider = "openai".into();
        f.keyring();
        f.put_entry("primary").await;
        let mut truncated = side_response("TRUNCATED_ATTEMPT".into(), 20);
        truncated.parts.push(Part::ToolCall {
            call_id: ToolCallId("first".into()),
            tool: "get_time".into(),
            input: serde_json::json!({}),
        });
        truncated.parts.push(Part::ToolCall {
            call_id: ToolCallId("second".into()),
            tool: "read".into(),
            input: serde_json::json!({"path": "incomplete"}),
        });
        truncated.stop_reason = StopReason::MaxOutputTokens;
        truncated.usage.output_tokens = 1;
        std::fs::write(f.files.path().join("script.json"), serde_json::to_vec(&serde_json::json!({
            "responses": {"0": truncated, "1": side_response("## Goal\nRetry".into(), 20), "2": truncated, "3": side_response("After retry limit".into(), 20)}
        })).unwrap()).unwrap();
        let image = swarmy_testkit::image(&f.store).await;
        let agent = f.store.create_agent("length-twice", image, "", Timestamp::now(), None).await.unwrap();
        let id = side_id();
        f.store.create_agent_session(id, Some(agent.agent_id), Timestamp::now(), None).await.unwrap();
        f.compactable_user_message(id).await;
        f.start("swarmy-scheduler", None);
        f.start("swarmy-worker", None);
        f.start("swarmy-gateway", None);
        f.wake(id).await;
        let successor = wait_successor(f, id).await;
        let events = f.idle(successor).await;
        assert_eq!(f.calls(), 3);
        assert!(successor_messages(&events).iter().any(|message| message.parts.iter().any(|part| matches!(part, Part::Text { text } if text == "Truncated response recovery failed after one compact-and-retry attempt."))));
        assert_eq!(successor_messages(&events).iter().filter(|message| message.parts.iter().any(|part| matches!(part, Part::Text { text } if text == "TRUNCATED_ATTEMPT"))).count(), 1);
        let results: Vec<_> = events.iter().filter_map(|event| match event {
            Event::MessageAppended { message, .. } if message.role == MessageRole::Tool =>
                message.parts.first(),
            _ => None,
        }).filter_map(|part| match part {
            Part::ToolResult { call_id, result: ToolResult::Error { error } } =>
                Some((call_id.0.as_str(), error.as_str())),
            _ => None,
        }).collect();
        assert_eq!(results, [
            ("first", "Tool call \"get_time\" was not executed: the response hit the output token limit, so its arguments may be truncated. Re-issue the tool call with complete arguments."),
            ("second", "Tool call \"read\" was not executed: the response hit the output token limit, so its arguments may be truncated. Re-issue the tool call with complete arguments."),
        ]);
        // An idle wake must not dispatch the truncated calls.
        f.wake(successor).await;
        f.idle(successor).await;
        assert_eq!(f.calls(), 3);
        f.user_message(successor).await;
        f.wake(successor).await;
        f.idle(successor).await;
        let prompt = f.histories().pop().unwrap();
        assert!(prompt.iter().any(|message| message.parts.iter().any(|part| matches!(part, Part::Text { text } if text == "TRUNCATED_ATTEMPT"))));
        for (id, error) in results {
            assert!(prompt.iter().any(|message| message.parts.iter().any(|part|
                matches!(part, Part::ToolResult { call_id, result: ToolResult::Error { error: actual } }
                    if call_id.0 == id && actual == error))));
        }
    })).await;
}

#[tokio::test]
async fn refused_recovery_summary_ends_turn_and_omits_truncated_reply() {
    run(|f| Box::pin(async move {
        f.provider = "openai".into();
        f.keyring();
        f.put_entry("primary").await;
        let mut truncated = side_response("UNUSABLE_TRUNCATION".into(), 20);
        truncated.stop_reason = StopReason::MaxOutputTokens;
        truncated.usage.output_tokens = 1;
        let mut refused = side_response("REFUSED_CHECKPOINT".into(), 20);
        refused.stop_reason = StopReason::MaxOutputTokens;
        std::fs::write(f.files.path().join("script.json"), serde_json::to_vec(&serde_json::json!({
            "responses": {"0": truncated, "1": refused, "2": side_response("Next answer".into(), 20)}
        })).unwrap()).unwrap();
        let image = swarmy_testkit::image(&f.store).await;
        let agent = f.store.create_agent("refused-recovery", image, "", Timestamp::now(), None).await.unwrap();
        let id = side_id();
        f.store.create_agent_session(id, Some(agent.agent_id), Timestamp::now(), None).await.unwrap();
        f.compactable_user_message(id).await;
        f.start("swarmy-scheduler", None);
        f.start("swarmy-worker", None);
        f.start("swarmy-gateway", None);
        f.wake(id).await;
        f.idle(id).await;
        assert_eq!(f.calls(), 2);
        assert!(f.store.next_session(id).await.unwrap().is_none());
        f.wake(id).await;
        f.idle(id).await;
        assert_eq!(f.calls(), 2, "idle wake must not reissue refused checkpoint");
        f.user_message(id).await;
        f.wake(id).await;
        f.idle(id).await;
        assert_eq!(f.calls(), 3);
        let prompt = f.histories().pop().unwrap();
        assert!(!prompt.iter().any(|message| message.parts.iter().any(|part| matches!(part, Part::Text { text } if text == "UNUSABLE_TRUNCATION" || text == "REFUSED_CHECKPOINT"))));
    })).await;
}

async fn seed_no_head_history(f: &Fixture, id: SessionId) {
    let call_id = ToolCallId("large".into());
    f.store
        .append_events(
            id,
            0,
            &[
                Event::MessageAppended {
                    seq: 0,
                    message: Message {
                        id: MessageId::from_ulid(Ulid::generate()),
                        role: MessageRole::User,
                        parts: vec![Part::Text {
                            text: format!(
                                "{}## Goal\nContinue{}",
                                swarmy_harness::COMPACTION_SUMMARY_PREFIX,
                                swarmy_harness::COMPACTION_SUMMARY_SUFFIX
                            ),
                        }],
                    },
                },
                Event::MessageAppended {
                    seq: 0,
                    message: Message {
                        id: MessageId::from_ulid(Ulid::generate()),
                        role: MessageRole::Assistant,
                        parts: vec![Part::ToolCall {
                            call_id: call_id.clone(),
                            tool: "get_time".into(),
                            input: serde_json::json!({}),
                        }],
                    },
                },
                Event::MessageAppended {
                    seq: 0,
                    message: Message {
                        id: MessageId::from_ulid(Ulid::generate()),
                        role: MessageRole::Tool,
                        parts: vec![Part::ToolResult {
                            call_id,
                            result: ToolResult::Completed {
                                output: "x".repeat(128 * 1024),
                                title: String::new(),
                                metadata: BTreeMap::new(),
                            },
                        }],
                    },
                },
            ],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn no_head_to_compact_omits_truncated_tool_attempt() {
    run(|f| {
        Box::pin(async move {
            f.provider = "openai".into();
            f.keyring();
            f.put_entry("primary").await;
            let mut truncated = side_response("partial".into(), 20);
            truncated.parts = vec![Part::ToolCall {
                call_id: ToolCallId("abandoned".into()),
                tool: "get_time".into(),
                input: serde_json::json!({}),
            }];
            truncated.stop_reason = StopReason::MaxOutputTokens;
            truncated.usage.output_tokens = 1;
            std::fs::write(
                f.files.path().join("script.json"),
                serde_json::to_vec(&serde_json::json!({
                    "responses": {"0": truncated, "1": side_response("Next answer".into(), 20)}
                }))
                .unwrap(),
            )
            .unwrap();
            let image = swarmy_testkit::image(&f.store).await;
            let agent = f
                .store
                .create_agent("no-head-recovery", image, "", Timestamp::now(), None)
                .await
                .unwrap();
            let id = side_id();
            f.store
                .create_agent_session(id, Some(agent.agent_id), Timestamp::now(), None)
                .await
                .unwrap();
            seed_no_head_history(f, id).await;
            f.start("swarmy-scheduler", None);
            f.start("swarmy-worker", None);
            f.start("swarmy-gateway", None);
            f.wake(id).await;
            let events = f.idle(id).await;
            assert_eq!(f.calls(), 1);
            assert!(f.store.next_session(id).await.unwrap().is_none());
            assert!(
                !successor_messages(&events)
                    .iter()
                    .any(|message| message.parts.iter().any(|part| match part {
                        Part::Text { text } => text.contains("recovery could not compact"),
                        _ => false,
                    }))
            );
            f.wake(id).await;
            f.idle(id).await;
            assert_eq!(
                f.calls(),
                1,
                "an idle wake must not retry an omitted attempt"
            );
            f.user_message(id).await;
            f.wake(id).await;
            f.idle(id).await;
            assert_eq!(f.calls(), 2);
            let prompt = f.histories().pop().unwrap();
            assert!(!prompt.iter().any(|message| message.parts.iter().any(
                |part| matches!(part, Part::ToolCall { call_id, .. } if call_id.0 == "abandoned")
            )));
        })
    })
    .await;
}

#[tokio::test]
async fn no_head_recovery_survives_crash_before_release() {
    run(|f| {
        Box::pin(async move {
            f.provider = "openai".into();
            f.keyring();
            f.put_entry("primary").await;
            let mut truncated = side_response("partial".into(), 20);
            truncated.parts = vec![Part::ToolCall {
                call_id: ToolCallId("abandoned".into()),
                tool: "get_time".into(),
                input: serde_json::json!({}),
            }];
            truncated.stop_reason = StopReason::MaxOutputTokens;
            truncated.usage.output_tokens = 1;
            std::fs::write(
                f.files.path().join("script.json"),
                serde_json::to_vec(&serde_json::json!({
                    "responses": {"0": truncated, "1": side_response("unexpected".into(), 20)}
                }))
                .unwrap(),
            )
            .unwrap();
            let image = swarmy_testkit::image(&f.store).await;
            let agent = f
                .store
                .create_agent("no-head-crash", image, "", Timestamp::now(), None)
                .await
                .unwrap();
            let id = side_id();
            f.store
                .create_agent_session(id, Some(agent.agent_id), Timestamp::now(), None)
                .await
                .unwrap();
            seed_no_head_history(f, id).await;
            f.start("swarmy-scheduler", None);
            let worker = f.start("swarmy-worker", Some("before_release"));
            f.start("swarmy-gateway", None);
            f.wake(id).await;
            let status = timeout(WAIT, f.children[worker].wait())
                .await
                .unwrap()
                .unwrap();
            assert!(
                status.code() == Some(137) || status.signal() == Some(9),
                "{status}"
            );
            f.start("swarmy-worker", None);
            let events = f.idle(id).await;
            assert_eq!(f.calls(), 1, "replacement must not issue inference");
            assert!(f.store.next_session(id).await.unwrap().is_none());
            // The archived event remains durable, but the idle snapshot omits it.
            assert!(events.iter().any(|event| matches!(event,
                Event::InferenceCompleted { completion, .. } if completion.message.parts.iter().any(
                    |part| matches!(part, Part::ToolCall { call_id, .. } if call_id.0 == "abandoned")
                )
            )));
            f.wake(id).await;
            f.idle(id).await;
            assert_eq!(f.calls(), 1);
            f.user_message(id).await;
            f.wake(id).await;
            f.idle(id).await;
            assert_eq!(f.calls(), 2);
            let prompt = f.histories().pop().unwrap();
            assert!(!prompt.iter().any(|message| message.parts.iter().any(
                |part| matches!(part, Part::ToolCall { call_id, .. } if call_id.0 == "abandoned")
            )));
        })
    })
    .await;
}

#[tokio::test]
async fn empty_successful_summary_rolls_over() {
    run(|f| Box::pin(async move {
        f.summarize_at_tokens = 100;
        std::fs::write(f.files.path().join("script.json"), serde_json::to_vec(&serde_json::json!({
            "responses": {"0": side_response("Original answer".into(), 101), "1": side_response(String::new(), 20)}
        })).unwrap()).unwrap();
        let image = swarmy_testkit::image(&f.store).await;
        let agent = f.store.create_agent("empty-summary", image, "", Timestamp::now(), None).await.unwrap();
        let id = side_id();
        f.store.create_agent_session(id, Some(agent.agent_id), Timestamp::now(), None).await.unwrap();
        f.compactable_user_message(id).await;
        f.start("swarmy-scheduler", None);
        f.start("swarmy-worker", None);
        f.start("swarmy-gateway", None);
        f.wake(id).await;
        let next = wait_successor(f, id).await;
        assert_ne!(next, id);
        assert_eq!(f.calls(), 2);
    })).await;
}

#[tokio::test]
async fn length_stopped_summary_preserves_session() {
    rejected_summary_preserves_session("## Goal\nPartial", StopReason::MaxOutputTokens).await;
}

async fn rejected_summary_preserves_session(summary: &'static str, stop_reason: StopReason) {
    run(|f| Box::pin(async move {
        f.summarize_at_tokens = 100;
        let mut summary_response = side_response(summary.into(), 20);
        summary_response.stop_reason = stop_reason;
        std::fs::write(f.files.path().join("script.json"), serde_json::to_vec(&serde_json::json!({
            "responses": {"0": side_response("Original answer".into(), 101), "1": summary_response,
                "2": side_response("Next answer".into(), 20)}
        })).unwrap()).unwrap();
        let image = swarmy_testkit::image(&f.store).await;
        let agent = f.store.create_agent("rejected-summary", image, "", Timestamp::now(), None).await.unwrap();
        let id = side_id();
        f.store.create_agent_session(id, Some(agent.agent_id), Timestamp::now(), None).await.unwrap();
        f.compactable_user_message(id).await;
        f.start("swarmy-scheduler", None);
        f.start("swarmy-worker", None);
        f.start("swarmy-gateway", None);
        f.wake(id).await;
        let events = f.idle(id).await;
        assert_eq!(f.calls(), 2);
        assert!(f.store.next_session(id).await.unwrap().is_none());
        assert!(events.iter().any(|event| matches!(event, Event::InferenceCompleted { completion, .. } if completion.message.parts.iter().any(|part| matches!(part, Part::Text { text } if text == "Original answer")))));
        f.user_message(id).await;
        f.wake(id).await;
        f.idle(id).await;
        assert_eq!(f.calls(), 3);
        let next_prompt = f.histories().pop().expect("next inference prompt");
        assert!(next_prompt.iter().any(|message| message.parts.iter().any(|part| matches!(part, Part::Text { text } if text == "Original answer"))));
        assert!(!next_prompt.iter().any(|message| message.parts.iter().any(|part| matches!(part, Part::Text { text } if text == summary))));
    })).await;
}

fn side_id() -> SessionId {
    loop {
        let id = SessionId::from_ulid(Ulid::generate());
        if runnable_partition(id) == 7 {
            return id;
        }
    }
}

fn side_response(text: String, input_tokens: u64) -> Response {
    Response {
        parts: vec![Part::Text { text }],
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage {
            input_tokens,
            ..Default::default()
        },
        quota_remaining: BTreeMap::new(),
        quota_resets: BTreeMap::new(),
    }
}

async fn wait_successor(fixture: &Fixture, id: SessionId) -> SessionId {
    swarmy_testkit::eventually("successor session appears", WAIT, async || {
        fixture.store.next_session(id).await.unwrap()
    })
    .await
}

#[tokio::test]
async fn side_summary_archives_with_tail_and_continues_small() {
    run(|f| {
        Box::pin(async move {
            f.summarize_at_tokens = 100;
            let summary = "## Goal
Finish the task

## Progress
### Done
- [x] Handler done

## Next Steps
1. Verify"
                .to_owned();
            write_direct_trigger_script(f, &summary);
            let image = swarmy_testkit::image(&f.store).await;
            let agent = f
                .store
                .create_agent("sidekick", image, "", Timestamp::now(), None)
                .await
                .unwrap();
            let id = side_id();
            f.store
                .create_agent_session(id, Some(agent.agent_id), Timestamp::now(), None)
                .await
                .unwrap();
            f.compactable_user_message(id).await;
            f.start("swarmy-scheduler", None);
            f.start("swarmy-worker", None);
            f.start("swarmy-gateway", None);
            f.wake(id).await;
            let new = wait_successor(f, id).await;
            assert_ne!(id, new);
            check_side_successor(f, &agent.agent_id, id, new, &summary).await;
            f.user_message(new).await;
            f.wake(new).await;
            let fresh_events = f.idle(new).await;
            assert!(
                fresh_events
                    .iter()
                    .any(|event| matches!(event, Event::InferenceCompleted { .. }))
            );
            let request = fresh_events
                .iter()
                .rev()
                .find_map(|event| match event {
                    Event::InferenceRequested { request_id, .. } => Some(*request_id),
                    _ => None,
                })
                .unwrap();
            let job: swarmy_llm::InferenceJob =
                f.store.get_inference_input(request).await.unwrap().unwrap();
            assert!(job.request.messages.len() <= 35);
        })
    })
    .await;
}

fn write_direct_trigger_script(fixture: &Fixture, summary: &str) {
    let responses = serde_json::json!({
        "0": side_response("Still working".into(), 150),
        "1": side_response(summary.to_owned(), 120),
        "2": side_response("Done in the successor".into(), 12),
        "3": side_response("Done in the successor".into(), 12),
    });
    std::fs::write(
        fixture.files.path().join("script.json"),
        serde_json::to_vec(&serde_json::json!({ "responses": responses })).unwrap(),
    )
    .unwrap();
}

async fn check_side_successor(
    f: &mut Fixture,
    agent: &AgentId,
    id: SessionId,
    new: SessionId,
    summary: &str,
) {
    assert_eq!(
        f.store
            .get_agent(*agent)
            .await
            .unwrap()
            .unwrap()
            .main_session,
        None
    );
    assert_eq!(
        f.store.fetch_session(id).await.unwrap().unwrap().state,
        SessionState::Completed
    );
    assert_eq!(f.store.previous_session(new).await.unwrap(), Some(id));
    // A chat-shaped rollover replays to end-of-turn and idles again. The
    // archival wakes the successor runnable first, so poll until it idles
    // instead of asserting on the transient runnable state.
    let fresh = swarmy_testkit::eventually("successor idles", WAIT, async || {
        let fresh = f.store.fetch_session(new).await.unwrap().unwrap();
        (fresh.state == SessionState::Idle).then_some(fresh)
    })
    .await;
    assert_eq!(fresh.state, SessionState::Idle);
    assert_eq!(fresh.agent_id, *agent);
    let opening = f.store.read_events(new, 0, 64).await.unwrap();
    let Event::MessageAppended { message, .. } = &opening[0] else {
        panic!("opening missing")
    };
    assert_eq!(message.role, MessageRole::User);
    let Part::Text { text } = &message.parts[0] else {
        panic!("summary missing")
    };
    assert!(text.contains(summary));
    assert!(opening.len() >= 2);
}

fn side_tool_response(call: &str, input_tokens: u64, reasoning: bool) -> Response {
    let mut parts = Vec::new();
    if reasoning {
        parts.push(Part::Reasoning {
            text: format!("thinking about {call}"),
            metadata: BTreeMap::new(),
        });
    }
    parts.push(Part::ToolCall {
        call_id: ToolCallId(call.into()),
        tool: "get_time".into(),
        input: serde_json::json!({}),
    });
    Response {
        parts,
        stop_reason: StopReason::ToolCalls,
        usage: TokenUsage {
            input_tokens,
            ..Default::default()
        },
        quota_remaining: BTreeMap::new(),
        quota_resets: BTreeMap::new(),
    }
}

fn write_fleet_side_script(fixture: &Fixture, summary: &str, split_turn: bool) {
    // Forty tool rounds with input usage ramping past the 6000-token
    // threshold at round 39, so the summary request lands mid-turn at call
    // 40. The summary response reports usage above the compaction threshold so the
    // gateway sends it down the slow path to archival. The remaining rounds
    // run small in the successor and finish the task there.
    let mut responses = serde_json::Map::new();
    for round in 0..40_u64 {
        let mut response =
            side_tool_response(&format!("clock-{round}"), 150 + round * 150, round % 5 == 0);
        if split_turn {
            response.parts.push(Part::Reasoning {
                text: "working ".repeat(500),
                metadata: BTreeMap::new(),
            });
        }
        responses.insert(round.to_string(), serde_json::to_value(response).unwrap());
    }
    if split_turn {
        responses.insert(
            "40".into(),
            serde_json::to_value(side_response("## Goal\nEarlier work".into(), 6200)).unwrap(),
        );
        responses.insert(
            "41".into(),
            serde_json::to_value(side_response(summary.to_owned(), 6200)).unwrap(),
        );
    } else {
        responses.insert(
            "40".into(),
            serde_json::to_value(side_response(summary.to_owned(), 6200)).unwrap(),
        );
    }
    let offset = u64::from(split_turn);
    for round in 41..45_u64 {
        let response = side_tool_response(&format!("clock-{round}"), 12, round % 5 == 0);
        responses.insert(
            (round + offset).to_string(),
            serde_json::to_value(response).unwrap(),
        );
    }
    responses.insert(
        (45 + offset).to_string(),
        serde_json::to_value(side_response("Finished; nothing remains.".into(), 12)).unwrap(),
    );
    std::fs::write(
        fixture.files.path().join("script.json"),
        serde_json::to_vec(&serde_json::json!({ "responses": responses })).unwrap(),
    )
    .unwrap();
}

async fn read_all_events(fixture: &Fixture, id: SessionId) -> Vec<Event> {
    let mut events = Vec::new();
    loop {
        let after = events.last().map_or(0, Event::seq);
        let page = fixture.store.read_events(id, after, 64).await.unwrap();
        if page.is_empty() {
            break;
        }
        events.extend(page);
    }
    events
}

fn successor_messages(events: &[Event]) -> Vec<Message> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::MessageAppended { message, .. } => Some(message.clone()),
            Event::InferenceCompleted { completion, .. } => Some(completion.message.clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn side_summary_mid_turn_keeps_tool_pairs_and_continues() {
    run(|f| {
        Box::pin(async move {
            f.summarize_at_tokens = 5999;
            let summary = "## Goal
Finish the task

## Progress
### Done
- [x] Handler done

## Next Steps
1. Verify"
                .to_owned();
            write_fleet_side_script(f, &summary, false);
            let image = swarmy_testkit::image(&f.store).await;
            let agent = f
                .store
                .create_agent("sidekick", image, "", Timestamp::now(), None)
                .await
                .unwrap();
            let id = side_id();
            f.store
                .create_agent_session(id, Some(agent.agent_id), Timestamp::now(), None)
                .await
                .unwrap();
            f.compactable_user_message(id).await;
            f.start("swarmy-scheduler", None);
            f.start("swarmy-worker", None);
            f.start("swarmy-gateway", None);
            f.wake(id).await;
            // Usage crosses the threshold mid-turn, so the successor is
            // archived out of the tool loop rather than at turn end. The turn
            // continues in the successor until the scripted final answer.
            let new = wait_successor(f, id).await;
            assert_ne!(id, new);
            // Wait for the continued task, then read the whole log: the
            // successor holds the carried tail plus new rounds, past the
            // first page.
            f.idle(new).await;
            let new_events = read_all_events(f, new).await;
            assert_mid_turn_links(f, id, new).await;
            let messages = successor_messages(&new_events);
            assert_successor_opening(&new_events, &summary);
            assert_successor_request_small(f, &new_events).await;
            assert_continued_tool_pairs(&messages);
        })
    })
    .await;
}

#[tokio::test]
async fn refused_split_prefix_reply_is_not_in_next_prompt() {
    run(|f| Box::pin(async move {
        f.summarize_at_tokens = 5999;
        write_fleet_side_script(f, "REFUSED_PREFIX_REPLY", true);
        let path = f.files.path().join("script.json");
        let mut script: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let mut refused = side_response("REFUSED_PREFIX_REPLY".into(), 6200);
        refused.stop_reason = StopReason::MaxOutputTokens;
        script["responses"]["41"] = serde_json::to_value(refused).unwrap();
        script["responses"]["42"] = serde_json::to_value(side_response("Next answer".into(), 20)).unwrap();
        std::fs::write(&path, serde_json::to_vec(&script).unwrap()).unwrap();
        let image = swarmy_testkit::image(&f.store).await;
        let agent = f.store.create_agent("refused-prefix", image, "", Timestamp::now(), None).await.unwrap();
        let id = side_id();
        f.store.create_agent_session(id, Some(agent.agent_id), Timestamp::now(), None).await.unwrap();
        f.compactable_user_message(id).await;
        f.start("swarmy-scheduler", None);
        f.start("swarmy-worker", None);
        f.start("swarmy-gateway", None);
        f.wake(id).await;
        f.idle(id).await;
        assert_eq!(f.calls(), 42);
        assert!(f.store.next_session(id).await.unwrap().is_none());
        f.user_message(id).await;
        f.wake(id).await;
        f.idle(id).await;
        let prompt = f.histories().pop().unwrap();
        assert!(!prompt.iter().any(|message| message.parts.iter().any(|part| matches!(part, Part::Text { text } if text == "## Goal\nEarlier work" || text == "REFUSED_PREFIX_REPLY"))));
    })).await;
}

#[tokio::test]
async fn split_turn_prefix_summary_keeps_later_tool_rounds() {
    run(|f| Box::pin(async move {
        f.summarize_at_tokens = 5999;
        let prefix = "## Original Request\nFinish the task\n\n## Progress So Far\n- Tools used\n\n## Context Needed to Continue\n- Verify";
        write_fleet_side_script(f, prefix, true);
        let image = swarmy_testkit::image(&f.store).await;
        let agent = f.store.create_agent("split-turn", image, "", Timestamp::now(), None).await.unwrap();
        let id = side_id();
        f.store.create_agent_session(id, Some(agent.agent_id), Timestamp::now(), None).await.unwrap();
        f.compactable_user_message(id).await;
        f.start("swarmy-scheduler", None);
        f.start("swarmy-worker", None);
        f.start("swarmy-gateway", None);
        f.wake(id).await;
        let successor = wait_successor(f, id).await;
        f.idle(successor).await;
        let old = read_all_events(f, id).await;
        let summary_id = old.iter().find_map(|event| match event {
            Event::InferenceRequested { request_id, .. } => Some(*request_id),
            _ => None,
        });
        let mut found_prefix = false;
        let mut found_history = false;
        for event in &old {
            check_summary_request(f, event, &mut found_prefix, &mut found_history).await;
        }
        assert!(summary_id.is_some() && found_history && found_prefix);
        let events = read_all_events(f, successor).await;
        let Event::MessageAppended { message, .. } = &events[0] else { panic!("opening missing") };
        assert!(matches!(&message.parts[0], Part::Text { text } if text.contains("**Turn Context (split turn):**") && text.contains(prefix) && text.contains("## Goal\nEarlier work")));
        assert_continued_tool_pairs(&successor_messages(&events));
    })).await;
}

#[tokio::test]
async fn side_summary_markdown_continues() {
    run(|f| {
        Box::pin(async move {
            f.summarize_at_tokens = 5999;
            let summary =
                "## Goal\nFinish the routes task\n\n## Progress\n### Done\n- [x] Handler done";
            write_fleet_side_script(f, summary, false);
            let image = swarmy_testkit::image(&f.store).await;
            let agent = f
                .store
                .create_agent("sidekick", image, "", Timestamp::now(), None)
                .await
                .unwrap();
            let id = side_id();
            f.store
                .create_agent_session(id, Some(agent.agent_id), Timestamp::now(), None)
                .await
                .unwrap();
            f.compactable_user_message(id).await;
            f.start("swarmy-scheduler", None);
            f.start("swarmy-worker", None);
            f.start("swarmy-gateway", None);
            f.wake(id).await;
            let new = wait_successor(f, id).await;
            f.idle(new).await;
            assert_mid_turn_links(f, id, new).await;
            let events = read_all_events(f, new).await;
            assert_successor_opening(&events, summary);
        })
    })
    .await;
}

async fn assert_mid_turn_links(fixture: &Fixture, id: SessionId, new: SessionId) {
    assert_eq!(
        fixture
            .store
            .fetch_session(id)
            .await
            .unwrap()
            .unwrap()
            .state,
        SessionState::Completed
    );
    assert_eq!(fixture.store.next_session(id).await.unwrap(), Some(new));
    assert_eq!(fixture.store.previous_session(new).await.unwrap(), Some(id));
    // A mid-task rollover wakes the successor runnable so the turn continues
    // without waiting for input; it may idle or lease briefly when observed.
    let fresh = fixture.store.fetch_session(new).await.unwrap().unwrap();
    assert!(
        matches!(
            fresh.state,
            SessionState::Idle | SessionState::Runnable | SessionState::Leased
        ),
        "unexpected mid-turn successor state {:?}",
        fresh.state
    );
}

fn assert_successor_opening(new_events: &[Event], summary: &str) {
    // The successor opens with the summary as a user message.
    let Event::MessageAppended { message, .. } = &new_events[0] else {
        panic!("opening missing")
    };
    assert_eq!(message.role, MessageRole::User);
    let Part::Text { text } = &message.parts[0] else {
        panic!("summary missing")
    };
    assert_eq!(
        text,
        &format!(
            "{}{}{}",
            "The conversation history before this point was compacted into the following summary:\n\n<summary>\n",
            &format!("No prior history.\n\n---\n\n**Turn Context (split turn):**\n\n{summary}"),
            "\n</summary>"
        )
    );
}

async fn assert_successor_request_small(fixture: &Fixture, new_events: &[Event]) {
    // The successor keeps approximately 20k recent context tokens,
    // even when a fixture overrides the usage trigger to 6k.
    let request = new_events
        .iter()
        .find_map(|event| match event {
            Event::InferenceRequested { request_id, .. } => Some(*request_id),
            _ => None,
        })
        .unwrap();
    let job: swarmy_llm::InferenceJob = fixture
        .store
        .get_inference_input(request)
        .await
        .unwrap()
        .unwrap();
    let request_chars = serde_json::to_string(&job.request.messages).unwrap().len();
    assert!(
        request_chars / 4 < 25_000,
        "successor request too large: {request_chars} chars"
    );
}

fn assert_continued_tool_pairs(messages: &[Message]) {
    // The continued task really ran in the successor: several tool rounds
    // completed after the rollover, every call kept its result, and reasoning
    // crossed the boundary inside its assistant message.
    let calls: Vec<_> = messages
        .iter()
        .flat_map(|message| &message.parts)
        .filter_map(|part| match part {
            Part::ToolCall { call_id, .. } => Some(call_id.0.clone()),
            _ => None,
        })
        .collect();
    assert!(calls.len() >= 4, "expected continued tool rounds");
    let results: Vec<_> = messages
        .iter()
        .flat_map(|message| &message.parts)
        .filter_map(|part| match part {
            Part::ToolResult { call_id, .. } => Some(call_id.0.clone()),
            _ => None,
        })
        .collect();
    for call in &calls {
        assert!(
            results.contains(call),
            "tool call {call} lost its result across the summary"
        );
    }
    assert!(
        messages
            .iter()
            .filter(|message| message.role == MessageRole::Assistant)
            .flat_map(|message| &message.parts)
            .any(|part| matches!(part, Part::Reasoning { .. })),
        "reasoning must cross the boundary with its message"
    );
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
            let first = f.start("swarmy-worker", Some("after_release"));
            f.gateway_serves("openai").await;
            let id = f.create_with_route("ab").await;
            f.wake(id).await;
            // Attempt one is durable and its worker is dead before the
            // replacement starts: the kill fires synchronously after the
            // submit, so a durable request means the death already
            // happened, with a short grace for the event publish.
            wait_for_durable_request(f, id).await;
            f.wait_exit(first).await;
            // The second worker recovers attempt one, records the 429 as a
            // failover to the second step, and dies with the step lease
            // still held, before it can submit attempt two.
            let second = f.start("swarmy-worker", Some("after_advance"));
            wait_for_route_step(f, id, 1).await;
            // The replacement resumes after the failover: the handled
            // failure advances nothing again and parks nothing behind the
            // in-flight successor, so the turn completes on the second
            // entry instead of sleeping behind the first entry's retry.
            f.wait_exit(second).await;
            f.start("swarmy-worker", None);
            let events = f.idle(id).await;
            let completed = events.iter().find_map(|event| match event {
                Event::InferenceCompleted { completion, .. } => Some((
                    completion.entry.clone(),
                    completion.route.clone(),
                    completion.route_step,
                )),
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
