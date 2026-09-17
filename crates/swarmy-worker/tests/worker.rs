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
use swarmy_llm::{Response, StopReason, TokenUsage};
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
    prefix: String,
    files: TempDir,
    children: Vec<Child>,
    snapshots: Mutex<HashSet<String>>,
}

impl Fixture {
    async fn new() -> Option<Self> {
        static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();
        for variable in [
            "SWARMY_FDB_CLUSTER_FILE",
            "SWARMY_NATS_URL",
            "SWARMY_S3_ENDPOINT",
        ] {
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
            &std::env::var("SWARMY_NATS_URL").unwrap(),
            Config {
                prefix: Some(SubjectToken::new(&prefix).unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        bus.setup(&[]).await.unwrap();
        Some(Self {
            store,
            bus,
            prefix,
            files: TempDir::new().unwrap(),
            children: Vec::new(),
            snapshots: Mutex::default(),
        })
    }

    fn script(&self, tools: bool, tool_name: &str) {
        let answer = Response {
            parts: vec![Part::Text {
                text: "The turn is complete.".into(),
            }],
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
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
        command
            .env("SWARMY_PROVIDER", "fake")
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
            .env("RUST_LOG", "info")
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
        let id = loop {
            let id = SessionId::from_ulid(Ulid::generate());
            if runnable_partition(id) == 7 {
                break id;
            }
        };
        self.store
            .create_session(
                &SessionRecord {
                    session_id: id,
                    agent_id: AgentId::from_ulid(Ulid::generate()),
                    state: SessionState::Idle,
                    head_seq: 0,
                    snapshot_ref: None,
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
        let client = async_nats::connect(std::env::var("SWARMY_NATS_URL").unwrap())
            .await
            .unwrap();
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
    let Some(mut fixture) = Fixture::new().await else {
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
