//! Exercise the actual terminal client with a PTY and a terminal emulator.
use std::{
    collections::BTreeMap,
    fmt::Write as FmtWrite,
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
    captured: Vec<u8>,
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
        let mut command = CommandBuilder::new(super::cli_bin::swarmy());
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
            captured: Vec::new(),
        }
    }

    async fn new_session(fixture: &Fixture, diagnostics: Diagnostics<'_>) -> Self {
        let mut terminal = Self::open(fixture, None);
        terminal
            .screen_diagnosed(|screen| screen.contains("New session"), WAIT, diagnostics)
            .await;
        terminal.type_text("\r");
        assert!(terminal.ready(diagnostics).await.contains("ephemeral |"));
        terminal
    }

    fn type_text(&mut self, text: &str) {
        self.writer.write_all(text.as_bytes()).unwrap();
        self.writer.flush().unwrap();
    }

    fn process(&mut self, bytes: &[u8]) {
        self.parser.process(bytes);
        self.captured.extend_from_slice(bytes);
        self.queries.extend_from_slice(bytes);
        if self.queries.windows(4).any(|bytes| bytes == b"\x1b[6n") {
            let (row, column) = self.parser.screen().cursor_position();
            self.type_text(&format!("\x1b[{};{}R", row + 1, column + 1));
        }
        let keep = self.queries.len().saturating_sub(3);
        self.queries.drain(..keep);
    }

    /// Wait for a screen predicate with an explicit budget, printing the
    /// screen, the last twenty lines of each service log, and the session's
    /// event log when the budget expires. Steps that never wait on a model
    /// turn or a service restart pass the short [`WAIT`] budget; turn and
    /// restart waits pass [`turn_budget`] or [`service_budget`].
    async fn screen_diagnosed(
        &mut self,
        predicate: impl Fn(&str) -> bool,
        budget: Duration,
        diagnostics: Diagnostics<'_>,
    ) -> String {
        let result = timeout(budget, async {
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
        .await;
        if let Ok(screen) = result {
            screen
        } else {
            let report = diagnostics.report().await;
            panic!(
                "terminal timed out after {budget:?}:\n{}\n{report}",
                self.parser.screen().contents()
            );
        }
    }

    /// Wait for the client to report idle input with the short client budget,
    /// for steps where no model turn runs.
    async fn ready(&mut self, diagnostics: Diagnostics<'_>) -> String {
        self.screen_diagnosed(|screen| screen.contains("Enter: send"), WAIT, diagnostics)
            .await
    }

    /// Wait for the client to report idle input with a model-turn budget and
    /// timeout diagnostics, for use after sending a message.
    async fn ready_after_turn(&mut self, diagnostics: Diagnostics<'_>) -> String {
        self.screen_diagnosed(
            |screen| screen.contains("Enter: send"),
            turn_budget(),
            diagnostics,
        )
        .await
    }

    /// Wait for the client to report idle input with a service-restart budget
    /// and timeout diagnostics, for use after relaunching a service.
    async fn ready_after_restart(&mut self, diagnostics: Diagnostics<'_>) -> String {
        self.screen_diagnosed(
            |screen| screen.contains("Enter: send"),
            service_budget(),
            diagnostics,
        )
        .await
    }

    async fn exit(&mut self, success: bool) {
        timeout(WAIT, async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    // Drain the remaining PTY output first: the client prints
                    // its error to stderr after leaving the alternate screen,
                    // and the PTY merges it into this capture.
                    while let Some(bytes) = self.receiver.recv().await {
                        self.process(&bytes);
                    }
                    if status.success() != success {
                        let mut tail = self
                            .captured
                            .iter()
                            .rev()
                            .take(4096)
                            .copied()
                            .collect::<Vec<_>>();
                        tail.reverse();
                        panic!(
                            "chat exit status {} (expected success={success}); screen:\n{}\npty tail:\n{}",
                            status.exit_code(),
                            self.parser.screen().contents(),
                            String::from_utf8_lossy(&tail),
                        );
                    }
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

/// Context for diagnosing a timed-out wait: the last twenty lines of each
/// service log plus the session's event log, so a slow-runner timeout can be
/// diagnosed without a rerun. Services are absent for tests that drive an
/// in-process fake worker; the session is absent before the first session
/// exists.
#[derive(Clone, Copy)]
struct Diagnostics<'a> {
    fixture: &'a Fixture,
    services: Option<&'a Services>,
    session: Option<SessionId>,
}

impl Diagnostics<'_> {
    async fn report(&self) -> String {
        let mut report = String::new();
        if let Some(services) = self.services {
            report.push_str(&services.log_tails());
        }
        report.push_str(&session_events(self.fixture, self.session).await);
        report
    }
}

/// Best-effort summary of the session's event log for timeout diagnostics.
/// Never panics: a slow database yields an error line instead of events.
/// Prints the last 64 events so the tail of a long test is what gets shown.
async fn session_events(fixture: &Fixture, session: Option<SessionId>) -> String {
    let Some(id) = session else {
        return "session events: (no session yet)\n".into();
    };
    let events = timeout(Duration::from_secs(10), read_last_events(fixture, id)).await;
    match events {
        Ok(Ok(events)) => {
            let mut summary = format!("session events for {id} (last {} events):\n", events.len());
            for event in events {
                let _ = writeln!(summary, "  seq {} {}", event.seq(), event_name(&event));
            }
            summary
        }
        Ok(Err(error)) => format!("session events for {id}: read failed: {error}\n"),
        Err(_) => format!("session events for {id}: read timed out\n"),
    }
}

/// Read up to the last 64 events of a session, starting from the head
/// sequence minus 64 so long tests stay within the store scan limit.
async fn read_last_events(fixture: &Fixture, id: SessionId) -> Result<Vec<Event>, String> {
    let session = fixture
        .store
        .fetch_session(id)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "session row missing".to_string())?;
    fixture
        .store
        .read_events(id, session.head_seq.saturating_sub(64), 64)
        .await
        .map_err(|error| error.to_string())
}

/// Short variant name for an event, enough to tell where a turn stalled.
fn event_name(event: &Event) -> &'static str {
    match event {
        Event::MessageAppended { .. } => "message_appended",
        Event::ToolCallRequested { .. } => "tool_call_requested",
        Event::ToolCallCompleted { .. } => "tool_call_completed",
        Event::InferenceRequested { .. } => "inference_requested",
        Event::InferenceCompleted { .. } => "inference_completed",
        Event::InferenceFailed { .. } => "inference_failed",
        Event::StateChanged { .. } => "state_changed",
        Event::SnapshotWritten { .. } => "snapshot_written",
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
            bin: super::cli_bin::swarmy().parent().unwrap().to_owned(),
        };
        for name in ["scheduler", "worker", "gateway"] {
            services.launch(fixture, name);
        }
        wait_for_scheduler(fixture).await;
        services
    }

    fn launch(&mut self, fixture: &Fixture, name: &str) {
        // Append so relaunching a service after a kill keeps the pre-kill
        // history that a restart timeout needs for diagnosis.
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.files.path().join(format!("{name}.log")))
            .unwrap();
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

    /// Last twenty lines of each service log for timeout diagnostics. Lines
    /// are truncated to 500 characters: some startup lines embed hundreds of
    /// partition ids and would otherwise flood the failure output.
    fn log_tails(&self) -> String {
        let mut tails = String::new();
        for name in ["scheduler", "worker", "gateway"] {
            let _ = writeln!(tails, "--- {name}.log (last 20 lines) ---");
            match std::fs::read_to_string(self.files.path().join(format!("{name}.log"))) {
                Ok(contents) => {
                    let lines: Vec<&str> = contents.lines().collect();
                    let start = lines.len().saturating_sub(20);
                    if lines.is_empty() {
                        tails.push_str("(empty)\n");
                    }
                    for line in &lines[start..] {
                        let truncated: String = line.chars().take(500).collect();
                        if truncated.len() < line.len() {
                            let _ = writeln!(tails, "{truncated}… (truncated)");
                        } else {
                            tails.push_str(line);
                            tails.push('\n');
                        }
                    }
                }
                Err(error) => {
                    let _ = writeln!(tails, "(unreadable: {error})");
                }
            }
        }
        tails
    }
}

/// Look up the session the client just created. Creation runs no model turn,
/// so the short client budget applies.
async fn session_id(fixture: &Fixture) -> SessionId {
    let deadline = Instant::now() + WAIT;
    loop {
        let sessions = list_sessions_tolerant(fixture, deadline).await;
        if let Some(session) = sessions.first() {
            return session.session_id;
        }
        assert!(
            Instant::now() < deadline,
            "no session appeared within {budget:?}",
            budget = WAIT
        );
        sleep(Duration::from_millis(50)).await;
    }
}

/// Wait for a session to reach idle with an explicit budget, printing timeout
/// diagnostics on expiry. Retryable database timeouts keep polling so the
/// outer timeout below wins with diagnostics instead of panicking first.
async fn idle(fixture: &Fixture, services: Option<&Services>, id: SessionId, budget: Duration) {
    let result = timeout(budget, async {
        loop {
            match fixture.store.fetch_session(id).await {
                Ok(session)
                    if session
                        .as_ref()
                        .is_some_and(|session| session.state == SessionState::Idle) =>
                {
                    break;
                }
                Err(error) if is_retryable_store_error(&error) => {}
                Err(error) => panic!("session fetch failed for {id}: {error}"),
                Ok(_) => {}
            }
            sleep(Duration::from_millis(30)).await;
        }
    })
    .await;
    if result.is_err() {
        let diagnostics = Diagnostics {
            fixture,
            services,
            session: Some(id),
        };
        panic!(
            "session {id} did not finish within {budget:?} while client was closed:\n{}",
            diagnostics.report().await
        );
    }
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

/// Shorthand for timeout diagnostics on a live-services turn.
fn diag<'a>(
    fixture: &'a Fixture,
    services: &'a Services,
    session: Option<SessionId>,
) -> Diagnostics<'a> {
    Diagnostics {
        fixture,
        services: Some(services),
        session,
    }
}

/// Wait for the reply count to reach `count` with a model-turn budget and
/// timeout diagnostics, without waiting for the ready prompt. Use this when
/// the client is closed right after the reply: the following `idle()` proves
/// the session finishes.
async fn await_reply(terminal: &mut Terminal, diagnostics: Diagnostics<'_>, count: usize) {
    terminal
        .screen_diagnosed(
            |screen| {
                screen
                    .matches("Agent: Scripted conversation reply.")
                    .count()
                    == count
            },
            turn_budget(),
            diagnostics,
        )
        .await;
}

/// Wait for the reply count to reach `count` and for input to unlock, with a
/// model-turn budget and timeout diagnostics.
async fn await_reply_count(terminal: &mut Terminal, diagnostics: Diagnostics<'_>, count: usize) {
    await_reply(terminal, diagnostics, count).await;
    terminal.ready_after_turn(diagnostics).await;
}

/// Drive one recovery round: get a reply, kill `name`, relaunch it, and wait
/// for input to unlock with a restart budget.
async fn recover_service(
    terminal: &mut Terminal,
    fixture: &Fixture,
    services: &mut Services,
    id: SessionId,
    index: usize,
    name: &str,
    count: usize,
) {
    terminal.type_text(&format!("Recover after {name} exits.\r"));
    terminal
        .screen_diagnosed(
            |screen| {
                screen
                    .matches("Agent: Scripted conversation reply.")
                    .count()
                    == count
            },
            turn_budget(),
            diag(fixture, services, Some(id)),
        )
        .await;
    services.children[index].kill().await.unwrap();
    assert!(terminal.child.try_wait().unwrap().is_none());
    services.launch(fixture, name);
    let screen = terminal
        .ready_after_restart(diag(fixture, services, Some(id)))
        .await;
    assert_eq!(
        screen
            .matches("Agent: Scripted conversation reply.")
            .count(),
        count
    );
    assert!(screen.contains("Idle | fake"));
}

#[tokio::test]
async fn chat_converses_resumes_and_survives_worker_and_gateway_death() {
    run(|fixture| async move {
        let mut services = Services::start(&fixture).await;
        let mut terminal = Terminal::new_session(
            &fixture,
            Diagnostics {
                fixture: &fixture,
                services: Some(&services),
                session: None,
            },
        )
        .await;
        let id = session_id(&fixture).await;
        let diagnostics = Diagnostics {
            fixture: &fixture,
            services: Some(&services),
            session: Some(id),
        };
        terminal.type_text("What time is it?\r");
        terminal
            .screen_diagnosed(
                |screen| screen.contains("Tool: get_time") && screen.contains("Result:"),
                turn_budget(),
                diagnostics,
            )
            .await;
        // Wait for the reply only. The "input locked" status is transient and
        // the client may render the reply and the idle state in one frame, so
        // requiring both together raced on slow runners (as with the wait
        // below, which was relaxed for the same reason).
        let screen = terminal
            .screen_diagnosed(
                |screen| screen.contains("Agent: Scripted conversation reply."),
                turn_budget(),
                diagnostics,
            )
            .await;
        assert!(screen.find("Result:").unwrap() < screen.find("Agent:").unwrap());
        terminal.ready_after_turn(diagnostics).await;
        terminal.type_text("Remember the first turn?\r");
        await_reply_count(&mut terminal, diagnostics, 2).await;
        assert_user_order(&fixture, id).await;
        // Wait for the reply only, then exit without waiting for the ready
        // prompt: the idle() below proves the session finishes with the
        // client closed.
        terminal.type_text("Finish while I am gone.\r");
        await_reply(&mut terminal, diagnostics, 3).await;
        terminal.type_text("\x1b");
        terminal.exit(true).await;
        idle(&fixture, Some(&services), id, turn_budget()).await;
        let mut terminal = Terminal::open(&fixture, Some(id));
        let screen = terminal.ready(diagnostics).await;
        assert_eq!(
            screen
                .matches("Agent: Scripted conversation reply.")
                .count(),
            3
        );
        assert!(screen.contains("Tool: get_time") && screen.contains("Result:"));
        for (index, name, count) in [(1, "worker", 4), (2, "gateway", 5)] {
            recover_service(
                &mut terminal,
                &fixture,
                &mut services,
                id,
                index,
                name,
                count,
            )
            .await;
        }
        terminal.type_text("\x03");
        terminal.exit(true).await;
        // The picker reads first messages and resumes the selected history.
        let mut terminal = Terminal::open(&fixture, None);
        let picker = Diagnostics {
            fixture: &fixture,
            services: Some(&services),
            session: None,
        };
        terminal
            .screen_diagnosed(|screen| screen.contains("What time is it?"), WAIT, picker)
            .await;
        terminal.type_text("\x1b[B\r");
        let screen = terminal.ready(picker).await;
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
            let diagnostics = Diagnostics {
                fixture: &fixture,
                services: None,
                session: None,
            };
            let mut terminal = Terminal::new_session(&fixture, diagnostics).await;
            let start = Instant::now();
            terminal.type_text("hello\r");
            let screen = terminal
                .screen_diagnosed(
                    |screen| {
                        screen.contains("Agent: scripted answer") && screen.contains("Enter: send")
                    },
                    WAIT,
                    diagnostics,
                )
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

/// Publish one incremental text delta and wait for the preview line.
async fn stream_preview_text(
    terminal: &mut Terminal,
    fixture: &Fixture,
    id: SessionId,
    text: &str,
    expected: &str,
) {
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
    let screen = terminal
        .screen_diagnosed(
            |screen| screen.contains(expected),
            WAIT,
            Diagnostics {
                fixture,
                services: None,
                session: Some(id),
            },
        )
        .await;
    assert!(screen.contains("input locked"));
}

/// Append a pending tool call and wait for the running indicator.
async fn request_tool_preview(
    terminal: &mut Terminal,
    fixture: &Fixture,
    id: SessionId,
    request_id: RequestId,
    call_id: &ToolCallId,
) {
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
        .screen_diagnosed(
            |screen| screen.contains("Tool: get_time [clock] {} (running)"),
            WAIT,
            Diagnostics {
                fixture,
                services: None,
                session: Some(id),
            },
        )
        .await;
}

/// Complete the preview tool call and wait for the result line.
async fn complete_tool_preview(
    terminal: &mut Terminal,
    fixture: &Fixture,
    id: SessionId,
    request_id: RequestId,
    call_id: ToolCallId,
) {
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
        .screen_diagnosed(
            |screen| screen.contains("Result: noon"),
            WAIT,
            Diagnostics {
                fixture,
                services: None,
                session: Some(id),
            },
        )
        .await;
    assert!(!screen.contains("Agent:"));
}

#[tokio::test]
async fn chat_shows_pending_tools_and_incremental_text_before_commit() {
    run(|fixture| async move {
        let (service, mut receiver) = serve_until_claim(&fixture).await;
        let mut terminal = Terminal::new_session(
            &fixture,
            Diagnostics {
                fixture: &fixture,
                services: None,
                session: None,
            },
        )
        .await;
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
        request_tool_preview(&mut terminal, &fixture, id, request_id, &call_id).await;
        complete_tool_preview(&mut terminal, &fixture, id, request_id, call_id).await;
        for (text, expected) in [
            ("scripted ", "Agent: scripted"),
            ("answer", "Agent: scripted answer"),
        ] {
            stream_preview_text(&mut terminal, &fixture, id, text, expected).await;
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
        let screen = terminal
            .ready(Diagnostics {
                fixture: &fixture,
                services: None,
                session: Some(id),
            })
            .await;
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
        let diagnostics = Diagnostics {
            fixture: &fixture,
            services: None,
            session: None,
        };
        let mut terminal = Terminal::with_image(&fixture, None, None, "");
        terminal
            .screen_diagnosed(|screen| screen.contains("New session"), WAIT, diagnostics)
            .await;
        terminal.type_text("\r");
        terminal.ready(diagnostics).await;
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
            .screen_diagnosed(|screen| screen.contains("New session"), WAIT, diagnostics)
            .await;
        terminal.type_text("\r");
        terminal.ready(diagnostics).await;
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
        let diagnostics = Diagnostics {
            fixture: &fixture,
            services: None,
            session: None,
        };
        let mut terminal = Terminal::new_session(&fixture, diagnostics).await;
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
        resumed.ready(diagnostics).await;
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
        let mut terminal = Terminal::new_session(
            &fixture,
            Diagnostics {
                fixture: &fixture,
                services: Some(&services),
                session: None,
            },
        )
        .await;
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
        bin: super::cli_bin::swarmy().parent().unwrap().to_owned(),
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
        let mut command = CommandBuilder::new(super::cli_bin::swarmy());
        command.args(["chat", "--model", "openai/gpt-5.5", "--effort", "max"]);
        let mut terminal = Terminal::command(&fixture, command, "fixture:test");
        let screen = terminal
            .ready(Diagnostics {
                fixture: &fixture,
                services: None,
                session: None,
            })
            .await;
        assert!(screen.contains("openai/gpt-5.5 max"), "{screen}");
        terminal.type_text("\u{1b}");
        terminal.exit(true).await;
    })
    .await;
}
