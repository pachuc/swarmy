//! Exercise the actual terminal client with a PTY and a terminal emulator.
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    path::PathBuf,
};

use portable_pty::{CommandBuilder, MasterPty, PtySize, native_pty_system};
use tokio::sync::mpsc;

use super::*;

struct Terminal {
    child: Box<dyn portable_pty::Child + Send + Sync>,
    _master: Box<dyn MasterPty + Send>,
    writer: Box<dyn Write + Send>,
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
    parser: vt100::Parser,
    queries: Vec<u8>,
}

impl Terminal {
    fn open(fixture: &Fixture, id: Option<SessionId>) -> Self {
        Self::with_image(fixture, id, None, "fixture:test")
    }

    fn with_image(
        fixture: &Fixture,
        id: Option<SessionId>,
        image: Option<&str>,
        default: &str,
    ) -> Self {
        Self::with_agent(fixture, id, image, default, None, false)
    }

    fn with_agent(
        fixture: &Fixture,
        id: Option<SessionId>,
        image: Option<&str>,
        default: &str,
        agent: Option<&str>,
        new: bool,
    ) -> Self {
        let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_swarmy"));
        command.arg("chat");
        if new {
            command.arg("--new");
        }
        if let Some(agent) = agent {
            command.args(["--agent", agent]);
        }
        if let Some(image) = image {
            command.args(["--image", image]);
        }
        if let Some(id) = id {
            command.arg(id.to_string());
        }
        Self::command(fixture, command, default)
    }

    fn command(fixture: &Fixture, mut command: CommandBuilder, default: &str) -> Self {
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: 30,
                cols: 140,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        command.env("SWARMY_FDB_CLUSTER_FILE", &fixture.cluster);
        command.env("SWARMY_NATS_URL", &fixture.url);
        command.env("SWARMY_STORE_DIRECTORY", &fixture.directory);
        command.env("SWARMY_BUS_PREFIX", &fixture.prefix);
        command.env("SWARMY_PROVIDER", "fake");
        command.env("SWARMY_DEFAULT_IMAGE", default);
        command.env("SWARMY_API_URL", &fixture.api_url);
        command.env("SWARMY_API_TOKEN", &fixture.api_token);
        command.env("TERM", "xterm-256color");
        command.env("TOKIO_WORKER_THREADS", "2");
        let child = pair.slave.spawn_command(command).unwrap();
        drop(pair.slave);
        let mut reader = pair.master.try_clone_reader().unwrap();
        let writer = pair.master.take_writer().unwrap();
        let (sender, receiver) = mpsc::unbounded_channel();
        std::thread::spawn(move || {
            let mut bytes = [0; 8192];
            while let Ok(count) = reader.read(&mut bytes) {
                if count == 0 || sender.send(bytes[..count].to_vec()).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            _master: pair.master,
            writer,
            receiver,
            parser: vt100::Parser::new(30, 140, 0),
            queries: Vec::new(),
        }
    }

    async fn new_session(fixture: &Fixture) -> Self {
        let mut terminal = Self::open(fixture, None);
        terminal
            .screen(|screen| screen.contains("New session"))
            .await;
        terminal.type_text("\r");
        assert!(terminal.ready().await.contains("ephemeral |"));
        terminal
    }

    fn type_text(&mut self, text: &str) {
        self.writer.write_all(text.as_bytes()).unwrap();
        self.writer.flush().unwrap();
    }

    fn process(&mut self, bytes: &[u8]) {
        self.parser.process(bytes);
        self.queries.extend_from_slice(bytes);
        if self.queries.windows(4).any(|bytes| bytes == b"\x1b[6n") {
            let (row, column) = self.parser.screen().cursor_position();
            self.type_text(&format!("\x1b[{};{}R", row + 1, column + 1));
        }
        let keep = self.queries.len().saturating_sub(3);
        self.queries.drain(..keep);
    }

    async fn screen(&mut self, predicate: impl Fn(&str) -> bool) -> String {
        timeout(WAIT, async {
            loop {
                let screen = self.parser.screen().contents();
                if predicate(&screen) {
                    return screen;
                }
                let bytes = self.receiver.recv().await.unwrap_or_else(|| {
                    panic!(
                        "terminal closed unexpectedly:\n{}",
                        self.parser.screen().contents()
                    )
                });
                self.process(&bytes);
            }
        })
        .await
        .unwrap_or_else(|_| panic!("terminal timed out:\n{}", self.parser.screen().contents()))
    }

    async fn ready(&mut self) -> String {
        self.screen(|screen| screen.contains("Enter: send")).await
    }

    async fn exit(&mut self, success: bool) {
        timeout(WAIT, async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    assert_eq!(status.success(), success);
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
            while let Some(bytes) = self.receiver.recv().await {
                self.process(&bytes);
            }
        })
        .await
        .expect("chat did not exit");
        assert!(
            !self.parser.screen().alternate_screen(),
            "chat did not restore terminal"
        );
    }
}

impl Drop for Terminal {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Services {
    files: tempfile::TempDir,
    children: Vec<tokio::process::Child>,
    bin: PathBuf,
}

impl Services {
    async fn start(fixture: &Fixture) -> Self {
        let build = Command::new("cargo")
            .args([
                "build",
                "--locked",
                "-p",
                "swarmy-scheduler",
                "-p",
                "swarmy-worker",
                "-p",
                "swarmy-gateway",
            ])
            .output()
            .await
            .unwrap();
        assert!(
            build.status.success(),
            "{}",
            String::from_utf8_lossy(&build.stderr)
        );
        let files = tempfile::tempdir().unwrap();
        std::fs::write(files.path().join("script.json"), r#"{
            "latency_ms": 600,
            "request_based": {"steps": 8, "tool_steps": [0], "final_answer": "Scripted conversation reply."}
        }"#).unwrap();
        let mut services = Self {
            files,
            children: Vec::new(),
            bin: PathBuf::from(env!("CARGO_BIN_EXE_swarmy"))
                .parent()
                .unwrap()
                .to_owned(),
        };
        for name in ["scheduler", "worker", "gateway"] {
            services.launch(fixture, name);
        }
        wait_for_scheduler(fixture).await;
        services
    }

    fn launch(&mut self, fixture: &Fixture, name: &str) {
        let log = std::fs::File::create(self.files.path().join(format!("{name}.log"))).unwrap();
        let mut command = Command::new(self.bin.join(format!("swarmy-{name}")));
        command
            .env("SWARMY_FDB_CLUSTER_FILE", &fixture.cluster)
            .env("SWARMY_NATS_URL", &fixture.url)
            .env("SWARMY_STORE_DIRECTORY", &fixture.directory)
            .env("SWARMY_BUS_PREFIX", &fixture.prefix)
            .env("SWARMY_PROVIDER", "fake")
            .env("SWARMY_FAKE_SCRIPT", self.files.path().join("script.json"))
            .env("SWARMY_FAKE_CALL_LOG", self.files.path().join("calls"))
            .env("SWARMY_SCHEDULER_SCAN_INTERVAL_MS", "50")
            .env("SWARMY_SCHEDULER_RESEND_INTERVAL_MS", "100")
            .env("SWARMY_WORKER_LEASE_MS", "600")
            .env("SWARMY_WORKER_RECOVERY_INTERVAL_MS", "100")
            .env("SWARMY_BUS_ACK_WAIT_MS", "900")
            .env("TOKIO_WORKER_THREADS", "2")
            .env_remove("SWARMY_WORKER_KILL_POINT")
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .kill_on_drop(true);
        self.children.push(command.spawn().unwrap());
    }
}

async fn session_id(fixture: &Fixture) -> SessionId {
    fixture.store.list_sessions(None, 1).await.unwrap()[0].session_id
}

async fn idle(fixture: &Fixture, id: SessionId) {
    timeout(WAIT, async {
        loop {
            if fixture
                .store
                .fetch_session(id)
                .await
                .unwrap()
                .unwrap()
                .state
                == SessionState::Idle
            {
                break;
            }
            sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("session did not finish while client was closed");
}

async fn assert_user_order(fixture: &Fixture, id: SessionId) {
    let shown = fixture
        .output(&["session", "show", &id.to_string(), "--json"])
        .await;
    assert!(shown.status.success());
    let shown = String::from_utf8(shown.stdout).unwrap();
    // The selection line precedes the usage line.
    let mut lines = shown.lines().skip(1);
    let usage: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    assert_eq!(usage["cost_dollars"], "0.0000");
    assert!(usage["session_usage"]["usage"]["input_tokens"].is_u64());
    let events: Vec<Event> = lines
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let users: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::MessageAppended { message, .. } if message.role == MessageRole::User => {
                Some(message.parts.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        users,
        vec![
            vec![Part::Text {
                text: "What time is it?".into()
            }],
            vec![Part::Text {
                text: "Remember the first turn?".into()
            }]
        ]
    );
}

#[tokio::test]
async fn chat_converses_resumes_and_survives_worker_and_gateway_death() {
    run(|fixture| async move {
        let mut services = Services::start(&fixture).await;
        let mut terminal = Terminal::new_session(&fixture).await;
        let id = session_id(&fixture).await;
        terminal.type_text("What time is it?\r");
        terminal
            .screen(|screen| screen.contains("Tool: get_time") && screen.contains("Result:"))
            .await;
        let screen = terminal
            .screen(|screen| {
                screen.contains("Agent: Scripted conversation reply.")
                    && screen.contains("input locked")
            })
            .await;
        assert!(screen.find("Result:").unwrap() < screen.find("Agent:").unwrap());
        terminal.ready().await;
        terminal.type_text("Remember the first turn?\r");
        terminal
            .screen(|screen| {
                screen
                    .matches("Agent: Scripted conversation reply.")
                    .count()
                    == 2
            })
            .await;
        terminal.ready().await;
        assert_user_order(&fixture, id).await;
        terminal.type_text("Finish while I am gone.\r");
        terminal
            .screen(|screen| {
                screen
                    .matches("Agent: Scripted conversation reply.")
                    .count()
                    == 3
                    && screen.contains("input locked")
            })
            .await;
        terminal.type_text("\x1b");
        terminal.exit(true).await;
        idle(&fixture, id).await;
        let mut terminal = Terminal::open(&fixture, Some(id));
        let screen = terminal.ready().await;
        assert_eq!(
            screen
                .matches("Agent: Scripted conversation reply.")
                .count(),
            3
        );
        assert!(screen.contains("Tool: get_time") && screen.contains("Result:"));
        for (index, name, count) in [(1, "worker", 4), (2, "gateway", 5)] {
            terminal.type_text(&format!("Recover after {name} exits.\r"));
            terminal
                .screen(|screen| {
                    screen
                        .matches("Agent: Scripted conversation reply.")
                        .count()
                        == count
                        && screen.contains("input locked")
                })
                .await;
            services.children[index].kill().await.unwrap();
            assert!(terminal.child.try_wait().unwrap().is_none());
            services.launch(&fixture, name);
            let screen = terminal.ready().await;
            assert_eq!(
                screen
                    .matches("Agent: Scripted conversation reply.")
                    .count(),
                count
            );
            assert!(screen.contains("Idle | fake"));
        }
        terminal.type_text("\x03");
        terminal.exit(true).await;
        // The picker reads first messages and resumes the selected history.
        let mut terminal = Terminal::open(&fixture, None);
        terminal
            .screen(|screen| screen.contains("What time is it?"))
            .await;
        terminal.type_text("\x1b[B\r");
        let screen = terminal.ready().await;
        assert!(screen.contains(&id.to_string()));
        terminal.type_text("\x1b");
        terminal.exit(true).await;
        for child in &mut services.children {
            let _ = child.kill().await;
        }
    })
    .await;
}

#[tokio::test]
async fn chat_enables_input_from_idle_events_and_recovers_missed_events() {
    for live in [true, false] {
        run(|fixture| async move {
            let server = serve(&fixture, live).await;
            let mut terminal = Terminal::new_session(&fixture).await;
            let start = Instant::now();
            terminal.type_text("hello\r");
            let screen = terminal
                .screen(|screen| {
                    screen.contains("Agent: scripted answer") && screen.contains("Enter: send")
                })
                .await;
            if live {
                assert!(
                    start.elapsed() < Duration::from_secs(3),
                    "idle waited for a store poll"
                );
            } else {
                assert!(
                    start.elapsed() >= Duration::from_secs(3),
                    "test did not exercise fallback polling"
                );
            }
            assert_eq!(screen.matches("Agent: scripted answer").count(), 1);
            terminal.type_text("\x1b");
            terminal.exit(true).await;
            server.abort();
        })
        .await;
    }
}

#[tokio::test]
async fn chat_shows_pending_tools_and_incremental_text_before_commit() {
    run(|fixture| async move {
        let (service, mut receiver) = serve_until_claim(&fixture).await;
        let mut terminal = Terminal::new_session(&fixture).await;
        terminal.type_text("hello\r");
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
        let request_id = RequestId::for_step(id, lease.seq);
        let call_id = ToolCallId("clock".into());
        fixture
            .store
            .append_events(
                id,
                1,
                &[Event::ToolCallRequested {
                    seq: 0,
                    request_id,
                    call: ToolCallRecord {
                        call_id: call_id.clone(),
                        tool: "get_time".into(),
                        arguments: serde_json::json!({}),
                        result: None,
                    },
                }],
            )
            .await
            .unwrap();
        terminal
            .screen(|screen| screen.contains("Tool: get_time [clock] {} (running)"))
            .await;
        fixture
            .store
            .append_events(
                id,
                2,
                &[Event::ToolCallCompleted {
                    seq: 0,
                    request_id,
                    call_id,
                    result: ToolResult::Completed {
                        output: "noon".into(),
                        title: "time".into(),
                        metadata: BTreeMap::default(),
                    },
                }],
            )
            .await
            .unwrap();
        let screen = terminal
            .screen(|screen| screen.contains("Result: noon"))
            .await;
        assert!(!screen.contains("Agent:"));
        for (text, expected) in [
            ("scripted ", "Agent: scripted"),
            ("answer", "Agent: scripted answer"),
        ] {
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
            fixture
                .publish_token(id, text, if text == "scripted " { 0 } else { 9 })
                .await;
            let screen = terminal.screen(|screen| screen.contains(expected)).await;
            assert!(screen.contains("input locked"));
        }
        fixture
            .store
            .append_events(id, 3, &[assistant()])
            .await
            .unwrap();
        fixture
            .store
            .set_state(id, SessionState::Idle, Some(&lease), Timestamp::now())
            .await
            .unwrap();
        let screen = terminal.ready().await;
        assert_eq!(screen.matches("Agent: scripted answer").count(), 1);
        terminal.type_text("\x1b");
        terminal.exit(true).await;
        service.abort();
    })
    .await;
}

#[tokio::test]
async fn chat_uses_server_default_image_and_explicit_image_overrides_it() {
    run(|fixture| async move {
        let mut terminal = Terminal::with_image(&fixture, None, None, "");
        terminal
            .screen(|screen| screen.contains("New session"))
            .await;
        terminal.type_text("\r");
        terminal.ready().await;
        let default = session_id(&fixture).await;
        assert_eq!(
            fixture.store.session_image(default).await.unwrap(),
            fixture
                .store
                .get_image("fixture", &swarmy_core::ImageTag("test".into()))
                .await
                .unwrap()
        );
        terminal.type_text("\x1b");
        terminal.exit(true).await;
        let mut terminal =
            Terminal::with_image(&fixture, None, Some("fixture:test"), "unregistered:default");
        terminal
            .screen(|screen| screen.contains("New session"))
            .await;
        terminal.type_text("\r");
        terminal.ready().await;
        let id = fixture
            .store
            .list_sessions(Some(default), 64)
            .await
            .unwrap()[0]
            .session_id;
        assert_eq!(
            fixture.store.session_image(id).await.unwrap(),
            fixture
                .store
                .get_image("fixture", &swarmy_core::ImageTag("test".into()))
                .await
                .unwrap()
        );
        terminal.type_text("\x1b");
        terminal.exit(true).await;
    })
    .await;
}

#[tokio::test]
async fn chat_uses_default_image_without_a_node() {
    run(|fixture| async move {
        let mut terminal = Terminal::new_session(&fixture).await;
        let id = session_id(&fixture).await;
        assert_eq!(
            fixture.store.session_image(id).await.unwrap(),
            fixture
                .store
                .get_image("fixture", &swarmy_core::ImageTag("test".into()))
                .await
                .unwrap()
        );
        terminal.type_text("\x1b");
        terminal.exit(true).await;
        let mut resumed = Terminal::with_image(&fixture, Some(id), None, "");
        resumed.ready().await;
        resumed.type_text("\x1b");
        resumed.exit(true).await;
    })
    .await;
}

// A failed assertion must still let swarmyd unmount and detach its volumes.
struct Node {
    child: std::process::Child,
    root: PathBuf,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = std::process::Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status();
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while matches!(self.child.try_wait(), Ok(None)) {
            if std::time::Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        // Also clean an interrupted boot whose node could not shut down normally.
        if let Ok(bundles) = std::fs::read_dir(self.root.join("bundles")) {
            for bundle in bundles.flatten() {
                let _ = std::process::Command::new("runc")
                    .arg("--root")
                    .arg(self.root.join("runc"))
                    .args(["delete", "--force"])
                    .arg(bundle.file_name())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
                let _ = std::process::Command::new("umount")
                    .arg(bundle.path().join("rootfs"))
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
                if let Ok(device) = std::fs::read_to_string(bundle.path().join("device"))
                    && device
                        .trim()
                        .strip_prefix("/dev/nbd")
                        .is_some_and(|suffix| suffix.parse::<u32>().is_ok())
                {
                    let _ =
                        swarmy_volume::kernel::cleanup_stale(std::path::Path::new(device.trim()));
                }
            }
        }
    }
}

#[tokio::test]
async fn root_chat_default_image_executes_pwd() {
    if std::process::Command::new("id")
        .arg("-u")
        .output()
        .unwrap()
        .stdout
        != b"0\n"
    {
        eprintln!("skipping root chat test: run the built test with sudo");
        return;
    }
    let Ok(image) = std::env::var("SWARMY_TEST_IMAGE") else {
        eprintln!("skipping root chat test: SWARMY_TEST_IMAGE is unset");
        return;
    };
    run(|fixture| async move {
        let (services, _node) = root_services(&fixture, &image, r#"{
            "request_based": {"steps": 2, "tool_steps": [0], "bash_command": "pwd", "final_answer": "pwd completed"}
        }"#).await;
        let manifest = fixture.store.get_image("fixture", &swarmy_core::ImageTag("test".into())).await.unwrap().unwrap();
        let mut terminal = Terminal::new_session(&fixture).await;
        let id = session_id(&fixture).await;
        assert_eq!(fixture.store.session_image(id).await.unwrap(), Some(manifest));
        let session = fixture.store.fetch_session(id).await.unwrap().unwrap();
        assert!(fixture.store.get_by_agent(session.agent_id).await.unwrap().is_none());
        terminal.type_text("Run pwd\r");
        timeout(Duration::from_secs(120), async {
            loop {
                let events = fixture.store.read_events(id, 0, 64).await.unwrap();
                if let Some(result) = events.iter().find_map(|event| match event {
                    Event::ToolCallCompleted { result, .. } => Some(result),
                    _ => None,
                }) {
                    let ToolResult::Completed { output, .. } = result else { panic!("pwd failed: {result:?}"); };
                    let result: swarmy_core::BashResult = serde_json::from_str(output).unwrap();
                    assert_eq!(result.exit_code, 0);
                    assert!(result.stdout.trim().starts_with('/'), "pwd output: {}", result.stdout);
                    assert!(!result.timed_out);
                    break;
                }
                sleep(Duration::from_millis(100)).await;
            }
        }).await.unwrap_or_else(|_| panic!("pwd did not finish: {}", std::fs::read_to_string(services.files.path().join("node.log")).unwrap()));
        assert!(fixture.store.get_by_agent(session.agent_id).await.unwrap().is_some());
        assert!(fixture.store.get_volume(swarmy_core::VolumeId::from_ulid(session.agent_id.as_ulid())).await.unwrap().is_some());
        terminal.type_text("\x1b");
        terminal.exit(true).await;
    }).await;
}

async fn root_services(fixture: &Fixture, image: &str, script: &str) -> (Services, Node) {
    let settings = swarmy_config::Settings::load().unwrap().settings;
    let images = Store::open(
        Some(&settings.fdb_cluster_file),
        Some(
            &settings
                .store_directory
                .split('/')
                .map(str::to_owned)
                .collect::<Vec<_>>(),
        ),
        Arc::new(MemoryBlobStore::default()),
    )
    .await
    .unwrap();
    let (name, tag) = image.split_once(':').unwrap();
    let manifest = images
        .get_image(name, &swarmy_core::ImageTag(tag.into()))
        .await
        .unwrap()
        .expect("build SWARMY_TEST_IMAGE first");
    fixture
        .store
        .put_manifest(
            manifest,
            &images.get_manifest(manifest).await.unwrap().unwrap(),
        )
        .await
        .unwrap();
    fixture
        .store
        .put_image("fixture", &swarmy_core::ImageTag("test".into()), manifest)
        .await
        .unwrap();
    let files = tempfile::tempdir().unwrap();
    std::fs::create_dir(files.path().join(".swarmy")).unwrap();
    std::fs::write(files.path().join(".swarmy/config.toml"), "").unwrap();
    std::fs::write(files.path().join("script.json"), script).unwrap();
    let mut services = Services {
        files,
        children: Vec::new(),
        bin: PathBuf::from(env!("CARGO_BIN_EXE_swarmy"))
            .parent()
            .unwrap()
            .to_owned(),
    };
    for name in ["scheduler", "worker", "gateway"] {
        services.launch(fixture, name);
    }
    wait_for_scheduler(fixture).await;
    let log = std::fs::File::create(services.files.path().join("node.log")).unwrap();
    let node = Node {
        root: services.files.path().join(".swarmy/node"),
        child: std::process::Command::new(services.bin.join("swarmyd"))
            .current_dir(services.files.path())
            .env("SWARMY_STATE_DIR", services.files.path().join(".swarmy"))
            .env("SWARMY_FDB_CLUSTER_FILE", &fixture.cluster)
            .env("SWARMY_STORE_DIRECTORY", &fixture.directory)
            .env("SWARMY_BUS_PREFIX", &fixture.prefix)
            .env("SWARMY_NATS_URL", &fixture.url)
            .env("TOKIO_WORKER_THREADS", "2")
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap(),
    };

    (services, node)
}

#[path = "chat_named.rs"]
mod named;

#[tokio::test]
async fn header_shows_persisted_provider_model_and_effort() {
    run(|fixture| async move {
        let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_swarmy"));
        command.args(["chat", "--model", "openai/gpt-5.5", "--effort", "max"]);
        let mut terminal = Terminal::command(&fixture, command, "fixture:test");
        let screen = terminal.ready().await;
        assert!(screen.contains("openai/gpt-5.5 max"), "{screen}");
        terminal.type_text("\u{1b}");
        terminal.exit(true).await;
    })
    .await;
}
