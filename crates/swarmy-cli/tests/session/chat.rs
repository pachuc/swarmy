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
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: 30,
                cols: 140,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_swarmy"));
        command.arg("chat");
        if let Some(id) = id {
            command.arg(id.to_string());
        }
        command.env("SWARMY_FDB_CLUSTER_FILE", &fixture.cluster);
        command.env("SWARMY_NATS_URL", &fixture.url);
        command.env("SWARMY_STORE_DIRECTORY", &fixture.directory);
        command.env("SWARMY_BUS_PREFIX", &fixture.prefix);
        command.env("SWARMY_PROVIDER", "fake");
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
        terminal.ready().await;
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
    let events: Vec<Event> = String::from_utf8(shown.stdout)
        .unwrap()
        .lines()
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
async fn chat_names_missing_scheduler_and_restores_terminal() {
    run(|fixture| async move {
        let mut terminal = Terminal::new_session(&fixture).await;
        let start = Instant::now();
        terminal.type_text("hello\r");
        terminal.exit(false).await;
        assert!(start.elapsed() < Duration::from_secs(5));
        assert!(terminal.parser.screen().contents().contains("scheduler"));
        // The append succeeded before the failed wake. Resume retries that wake.
        let id = session_id(&fixture).await;
        let server = serve(&fixture, false).await;
        let mut terminal = Terminal::open(&fixture, Some(id));
        let screen = terminal.ready().await;
        assert!(screen.contains("Agent: scripted answer"));
        terminal.type_text("\x1b");
        terminal.exit(true).await;
        server.abort();
    })
    .await;
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
