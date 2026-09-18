#[path = "session/agent_settings.rs"]
mod agent_settings;

#[path = "../../swarmy-store/tests/support/mod.rs"]
mod image_fixture;

#[path = "session/agents.rs"]
mod agents;

#[path = "session/chat.rs"]
mod chat;

use std::{
    future::Future,
    panic::AssertUnwindSafe,
    process::Stdio,
    sync::{Arc, OnceLock},
    time::Duration,
};

use foundationdb::{
    Database,
    directory::{Directory, DirectoryLayer},
};
use futures_util::{FutureExt, StreamExt};
use jiff::Timestamp;
use swarmy_bus::{Bus, Config, LiveFeed, SubjectToken};
use swarmy_core::{
    Event, LeaseOwnerId, Message, MessageId, MessageRole, Nudge, Part, RequestId, SessionId,
    SessionState, ToolCallId, ToolCallRecord, ToolResult, WakeReply, decode,
};
use swarmy_llm::Delta;
use swarmy_store::{Store, blob::MemoryBlobStore};
use tokio::{
    process::Command,
    time::{Instant, sleep, timeout},
};
use ulid::Ulid;

const WAIT: Duration = Duration::from_secs(15);

#[derive(Clone)]
struct Fixture {
    store: Store,
    bus: Bus,
    cluster: String,
    directory: String,
    prefix: String,
    url: String,
}

impl Fixture {
    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_swarmy"));
        command
            .args(args)
            .env("SWARMY_FDB_CLUSTER_FILE", &self.cluster)
            .env("SWARMY_NATS_URL", &self.url)
            .env("SWARMY_STORE_DIRECTORY", &self.directory)
            .env("SWARMY_BUS_PREFIX", &self.prefix)
            .env("SWARMY_DEFAULT_IMAGE", "fixture:test")
            .env("TOKIO_WORKER_THREADS", "2")
            .stdin(Stdio::null())
            .kill_on_drop(true);
        command
    }

    async fn output(&self, args: &[&str]) -> std::process::Output {
        timeout(WAIT, self.command(args).output())
            .await
            .expect("CLI hung")
            .unwrap()
    }

    async fn cleanup(&self) {
        let db = Database::new(Some(&self.cluster)).unwrap();
        db.run(|trx, _| async move {
            DirectoryLayer::default()
                .remove_if_exists(&trx, std::slice::from_ref(&self.directory))
                .await?;
            Ok(())
        })
        .await
        .unwrap();
        let admin = async_nats::connect(&self.url).await.unwrap();
        let context = async_nats::jetstream::new(admin);
        for stream in ["INFER_REQ", "SCHED_RUNNABLE", "TOOL_REMOTE", "TOOL_NODE"] {
            if let Err(error) = context
                .delete_stream(format!("{}_{stream}", self.prefix))
                .await
            {
                assert!(
                    matches!(error.kind(), async_nats::jetstream::context::DeleteStreamErrorKind::JetStream(ref error) if error.code() == 404),
                    "stream cleanup failed: {error}"
                );
            }
        }
    }
}

async fn run<F: Future<Output = ()>>(test: impl FnOnce(Fixture) -> F) {
    static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();
    let (Ok(cluster), Ok(url)) = (
        std::env::var("SWARMY_FDB_CLUSTER_FILE"),
        std::env::var("SWARMY_NATS_URL"),
    ) else {
        eprintln!(
            "skipping CLI integration test: SWARMY_FDB_CLUSTER_FILE or SWARMY_NATS_URL is unset"
        );
        return;
    };
    NETWORK.get_or_init(swarmy_store::boot);
    let prefix = Ulid::generate().to_string();
    let directory = format!("cli-test-{prefix}");
    let fixture = Fixture {
        store: Store::open(
            Some(&cluster),
            Some(std::slice::from_ref(&directory)),
            Arc::new(MemoryBlobStore::default()),
        )
        .await
        .unwrap(),
        bus: Bus::connect(
            &url,
            Config {
                prefix: Some(SubjectToken::new(&prefix).unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
        cluster,
        directory,
        prefix,
        url,
    };
    image_fixture::image(&fixture.store).await;
    let result = AssertUnwindSafe(test(fixture.clone())).catch_unwind().await;
    fixture.cleanup().await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

fn assistant() -> Event {
    Event::MessageAppended {
        seq: 0,
        message: Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: MessageRole::Assistant,
            parts: vec![Part::Text {
                text: "scripted answer".into(),
            }],
        },
    }
}

async fn worker(fixture: &Fixture, id: SessionId, live: bool) {
    let session = fixture.store.fetch_session(id).await.unwrap().unwrap();
    assert_eq!(session.state, SessionState::Runnable);
    assert_eq!(
        fixture.store.session_image(id).await.unwrap(),
        fixture
            .store
            .get_image("fixture", &swarmy_core::ImageTag("test".into()))
            .await
            .unwrap()
    );
    let prompt = fixture.store.read_events(id, 0, 1).await.unwrap();
    assert!(matches!(&prompt[0], Event::MessageAppended { message, .. }
        if message.role == MessageRole::User && message.parts == [Part::Text { text: "hello".into() }]));
    fixture
        .store
        .wake_session(id, Timestamp::now())
        .await
        .unwrap();
    let lease = fixture
        .store
        .claim_lease(
            id,
            LeaseOwnerId::from_ulid(Ulid::generate()),
            Timestamp::now()
                .checked_add(Duration::from_secs(30))
                .unwrap(),
        )
        .await
        .unwrap();
    if live {
        for text in ["scripted ", "answer"] {
            fixture
                .bus
                .publish_live(
                    LiveFeed::ModelDeltas(id),
                    &Delta::Text {
                        output_index: 0,
                        text: text.into(),
                    },
                )
                .await
                .unwrap();
        }
    }
    let request_id = RequestId::for_step(id, lease.seq);
    let call_id = ToolCallId("clock".into());
    let mut events = vec![
        assistant(),
        Event::ToolCallRequested {
            seq: 0,
            request_id,
            call: ToolCallRecord {
                call_id: call_id.clone(),
                tool: "get_time".into(),
                arguments: serde_json::json!({}),
                result: None,
            },
        },
        Event::ToolCallCompleted {
            seq: 0,
            request_id,
            call_id,
            result: ToolResult::Error {
                error: "one\ntwo".into(),
            },
        },
    ];
    if live {
        events.push(Event::StateChanged {
            seq: 0,
            from: SessionState::Leased,
            to: SessionState::Idle,
        });
    }
    fixture.store.append_events(id, 1, &events).await.unwrap();
    fixture
        .store
        .set_state(id, SessionState::Idle, Some(&lease), Timestamp::now())
        .await
        .unwrap();
    if live {
        for event in fixture.store.read_events(id, 1, 64).await.unwrap() {
            fixture
                .bus
                .publish_live(LiveFeed::SessionEvents(id), &event)
                .await
                .unwrap();
        }
    }
}

async fn serve(fixture: &Fixture, live: bool) -> tokio::task::JoinHandle<()> {
    let mut messages = nudges(fixture).await;
    let server = fixture.clone();
    tokio::spawn(async move {
        while let Some(message) = messages.next().await {
            let nudge: Nudge = decode(&message.payload).unwrap();
            worker(&server, nudge.session_id, live).await;
        }
    })
}

async fn nudges(fixture: &Fixture) -> async_nats::Subscriber {
    fixture.bus.setup(&[]).await.unwrap();
    let client = async_nats::connect(&fixture.url).await.unwrap();
    let messages = client
        .subscribe(format!("{}.sched.runnable.*", fixture.prefix))
        .await
        .unwrap();
    client.flush().await.unwrap();
    messages
}

async fn wait_for_scheduler(fixture: &Fixture) {
    timeout(WAIT, async {
        loop {
            if matches!(
                fixture
                    .bus
                    .request_wake(
                        SessionId::from_ulid(Ulid::generate()),
                        Duration::from_millis(100)
                    )
                    .await,
                Ok(WakeReply::NotFound)
            ) {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn idle_event_enables_input_without_polling_and_history_still_paginates() {
    run(|fixture| async move {
        let server = serve(&fixture, true).await;
        let start = Instant::now();
        let output = fixture.output(&["run", "hello"]).await;
        assert!(
            start.elapsed() < Duration::from_secs(3),
            "waited for store poll"
        );
        server.abort();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        let lines: Vec<_> = text.lines().collect();
        assert_eq!(lines.len(), 3, "{text}");
        assert_eq!(lines[0], "scripted answer");
        assert!(lines[1].starts_with("Tool call "));
        assert!(lines[2].starts_with("Tool result "));
        let session = fixture
            .store
            .list_sessions(None, 1)
            .await
            .unwrap()
            .remove(0);
        // Cross multiple store pages for both inspection commands.
        for _ in 0..2 {
            let head = fixture
                .store
                .fetch_session(session.session_id)
                .await
                .unwrap()
                .unwrap()
                .head_seq;
            fixture
                .store
                .append_events(session.session_id, head, &vec![assistant(); 64])
                .await
                .unwrap();
        }
        for _ in 0..65 {
            let mut record = session.clone();
            record.session_id = SessionId::from_ulid(Ulid::generate());
            record.head_seq = 0;
            fixture
                .store
                .create_session(
                    &record,
                    Timestamp::now(),
                    image_fixture::image(&fixture.store).await,
                )
                .await
                .unwrap();
        }
        let shown = fixture
            .output(&["session", "show", &session.session_id.to_string(), "--json"])
            .await;
        assert!(shown.status.success());
        let events: Vec<Event> = String::from_utf8(shown.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(events.len(), 133);
        for (index, event) in events.iter().enumerate() {
            assert_eq!(event.seq(), u64::try_from(index).unwrap() + 1);
        }
        let listed = fixture.output(&["--json", "session", "list"]).await;
        assert!(listed.status.success());
        let sessions: Vec<swarmy_core::SessionRecord> = String::from_utf8(listed.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(sessions.len(), 66);
        assert!(sessions.iter().any(|s| s.session_id == session.session_id));
        assert!(
            sessions
                .windows(2)
                .all(|pair| pair[0].session_id < pair[1].session_id)
        );
    })
    .await;
}

#[tokio::test]
async fn run_recovers_without_live_publications_or_idle_event() {
    run(|fixture| async move {
        let server = serve(&fixture, false).await;
        let output = fixture.output(&["run", "hello"]).await;
        server.abort();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8(output.stdout)
                .unwrap()
                .starts_with("scripted answer\n")
        );
    })
    .await;
}

#[tokio::test]
async fn run_json_emits_only_machine_readable_records() {
    run(|fixture| async move {
        let server = serve(&fixture, true).await;
        let output = fixture.output(&["run", "hello", "--json"]).await;
        server.abort();
        assert!(output.status.success());
        let rows: Vec<serde_json::Value> = String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(rows[0]["event"], "session_created");
        assert!(
            rows.iter()
                .any(|row| row["delta"]["Text"]["text"] == "scripted ")
        );
        assert_eq!(rows.last().unwrap()["value"]["state_changed"]["to"], "idle");
    })
    .await;
}

#[tokio::test]
async fn append_nudges_without_any_scheduler_and_leaves_durable_recovery_work() {
    run(|fixture| async move {
        let mut messages = nudges(&fixture).await;
        let mut child = fixture
            .command(&["run", "hello"])
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let message = timeout(Duration::from_secs(3), messages.next())
            .await
            .unwrap()
            .unwrap();
        let nudge: Nudge = decode(&message.payload).unwrap();
        let session = fixture
            .store
            .fetch_session(nudge.session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session.state, SessionState::Runnable);
        assert_eq!(session.head_seq, 1);
        assert!(message.subject.ends_with(&format!(
            ".{}",
            swarmy_store::runnable_partition(nudge.session_id)
        )));
        child.kill().await.unwrap();
    })
    .await;
}

async fn serve_until_claim(
    fixture: &Fixture,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::mpsc::Receiver<SessionId>,
) {
    let mut messages = nudges(fixture).await;
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let service = tokio::spawn(async move {
        while let Some(message) = messages.next().await {
            let nudge: Nudge = decode(&message.payload).unwrap();
            sender.send(nudge.session_id).await.unwrap();
        }
    });
    (service, receiver)
}

#[tokio::test]
async fn text_is_flushed_before_the_turn_finishes() {
    use tokio::io::AsyncReadExt;

    run(|fixture| async move {
        let (service, mut receiver) = serve_until_claim(&fixture).await;
        let mut child = fixture
            .command(&["run", "hello"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let id = timeout(WAIT, receiver.recv()).await.unwrap().unwrap();
        let lease = fixture
            .store
            .claim_lease(
                id,
                LeaseOwnerId::from_ulid(Ulid::generate()),
                Timestamp::now()
                    .checked_add(Duration::from_secs(30))
                    .unwrap(),
            )
            .await
            .unwrap();
        fixture
            .bus
            .publish_live(
                LiveFeed::ModelDeltas(id),
                &Delta::Text {
                    output_index: 0,
                    text: "scripted ".into(),
                },
            )
            .await
            .unwrap();
        let mut prefix = [0; 9];
        timeout(WAIT, child.stdout.as_mut().unwrap().read_exact(&mut prefix))
            .await
            .expect("text was buffered until completion")
            .unwrap();
        assert_eq!(&prefix, b"scripted ");
        assert!(child.try_wait().unwrap().is_none());
        fixture
            .store
            .append_events(
                id,
                1,
                &[
                    assistant(),
                    Event::StateChanged {
                        seq: 0,
                        from: SessionState::Leased,
                        to: SessionState::Idle,
                    },
                ],
            )
            .await
            .unwrap();
        fixture
            .store
            .set_state(id, SessionState::Idle, Some(&lease), Timestamp::now())
            .await
            .unwrap();
        for event in fixture.store.read_events(id, 1, 64).await.unwrap() {
            fixture
                .bus
                .publish_live(LiveFeed::SessionEvents(id), &event)
                .await
                .unwrap();
        }
        let output = timeout(WAIT, child.wait_with_output())
            .await
            .unwrap()
            .unwrap();
        service.abort();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.stdout, b"answer\n");
    })
    .await;
}

#[tokio::test]
async fn run_requires_default_image_and_explicit_image_overrides_it() {
    run(|fixture| async move {
        let missing = fixture
            .command(&["run", "hello"])
            .env("SWARMY_DEFAULT_IMAGE", "")
            .output()
            .await
            .unwrap();
        assert!(!missing.status.success());
        let error = String::from_utf8_lossy(&missing.stderr);
        assert!(
            error.contains("default_image") && error.contains("SWARMY_DEFAULT_IMAGE"),
            "{error}"
        );
        assert!(
            fixture
                .store
                .list_sessions(None, 64)
                .await
                .unwrap()
                .is_empty()
        );
        let server = serve(&fixture, true).await;
        let output = timeout(
            WAIT,
            fixture
                .command(&["run", "hello", "--image", "fixture:test"])
                .env("SWARMY_DEFAULT_IMAGE", "unregistered:default")
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        server.abort();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    })
    .await;
}

#[tokio::test]
async fn run_rejects_unknown_images_before_creating_a_session() {
    run(|fixture| async move {
        let output = fixture
            .output(&["run", "hello", "--image", "missing:tag"])
            .await;
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(
            error.contains("missing:tag") && error.contains("registered images: fixture:test"),
            "{error}"
        );
        assert!(
            fixture
                .store
                .list_sessions(None, 64)
                .await
                .unwrap()
                .is_empty()
        );
    })
    .await;
}
