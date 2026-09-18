use super::*;

#[tokio::test]
async fn named_chat_header_and_notices_identify_the_session() {
    run(|fixture| async move {
        let agent = fixture
            .store
            .create_agent("tommy", "fixture:test", "", Timestamp::now())
            .await
            .unwrap();
        let mut first = Terminal::with_agent(&fixture, None, None, "", Some("tommy"), false);
        assert!(first.ready().await.contains("tommy |"));
        let first_id = session_id(&fixture).await;
        first.type_text("\x1b");
        first.exit(true).await;
        let mut first = Terminal::with_agent(&fixture, None, None, "", Some("tommy"), false);
        assert!(first.ready().await.contains(&first_id.to_string()));
        assert_eq!(
            fixture
                .store
                .list_sessions_by_agent(agent.agent_id, None, 64)
                .await
                .unwrap()
                .len(),
            1
        );

        let mut second = Terminal::with_agent(
            &fixture,
            None,
            None,
            "",
            Some(&agent.agent_id.to_string()),
            true,
        );
        assert!(second.ready().await.contains("tommy |"));
        fixture
            .store
            .append_events(
                first_id,
                0,
                &[Event::MessageAppended {
                    seq: 0,
                    message: Message {
                        id: MessageId::from_ulid(Ulid::generate()),
                        role: MessageRole::System,
                        parts: vec![Part::Text {
                            text: "Computer recovered".into(),
                        }],
                    },
                }],
            )
            .await
            .unwrap();
        let screen = first
            .screen(|screen| screen.contains("Computer recovered"))
            .await;
        assert!(screen.contains(&format!("System [session {first_id}]:")));
        first.type_text("\x1b");
        first.exit(true).await;
        second.type_text("\x1b");
        second.exit(true).await;
        let sessions = fixture
            .store
            .list_sessions_by_agent(agent.agent_id, None, 64)
            .await
            .unwrap();
        assert_eq!(sessions.len(), 2);
        assert!(sessions.iter().all(|session| !session.computer_deleted));
    })
    .await;
}

#[tokio::test]
async fn root_named_chats_share_a_background_process_and_delete() {
    if std::process::Command::new("id")
        .arg("-u")
        .output()
        .unwrap()
        .stdout
        != b"0\n"
    {
        eprintln!("skipping root named chat test: run the built test with sudo");
        return;
    }
    let Ok(image) = std::env::var("SWARMY_TEST_IMAGE") else {
        eprintln!("skipping root named chat test: SWARMY_TEST_IMAGE is unset");
        return;
    };
    run(|fixture| async move {
        let (services, _node) = root_services(&fixture, &image, r#"{
            "request_by_prompt": {
                "start": {"steps": 2, "tool_steps": [0], "bash_command": "nohup sleep 600 >/background.log 2>&1 </dev/null & echo $! >/background.pid; cat /background.pid", "final_answer": "Started background process"},
                "inspect": {"steps": 2, "tool_steps": [0], "bash_command": "kill -0 $(cat /background.pid) && echo shared-process-alive", "final_answer": "Inspected background process"}
            }
        }"#).await;
        let created = fixture.output(&["agent", "create", "tommy", "--json"]).await;
        assert!(created.status.success(), "{}", String::from_utf8_lossy(&created.stderr));
        let agent: swarmy_core::AgentRecord = serde_json::from_slice(&created.stdout).unwrap();
        let mut first = Terminal::with_agent(&fixture, None, None, "", Some("tommy"), false);
        first.ready().await;
        let first_id = session_id(&fixture).await;
        let mut second = Terminal::with_agent(&fixture, None, None, "", Some(&agent.agent_id.to_string()), true);
        second.ready().await;
        let sessions = fixture.store.list_sessions_by_agent(agent.agent_id, None, 64).await.unwrap();
        assert_eq!(sessions.len(), 2);
        let second_id = sessions.iter().find(|session| session.session_id != first_id).unwrap().session_id;
        first.type_text("start\r");
        let output = bash_result(&fixture, first_id, &services).await;
        assert!(output.trim().parse::<u32>().is_ok(), "{output}");
        first.screen(|screen| screen.contains("Started background process") && screen.contains("Enter: send")).await;
        let placement = fixture.store.get_by_agent(agent.agent_id).await.unwrap().unwrap();
        second.type_text("inspect\r");
        assert_eq!(bash_result(&fixture, second_id, &services).await.trim(), "shared-process-alive");
        second.screen(|screen| screen.contains("Inspected background process") && screen.contains("Enter: send")).await;
        assert_eq!(fixture.store.get_by_agent(agent.agent_id).await.unwrap().unwrap().epoch, placement.epoch);
        let show = fixture.output(&["agent", "show", "tommy", "--json"]).await;
        assert!(show.status.success());
        let shown: serde_json::Value = serde_json::from_slice(&show.stdout).unwrap();
        assert_eq!(shown["placement"]["node_id"], placement.node_id.to_string());
        assert_eq!(shown["placement"]["epoch"], placement.epoch);
        assert!(shown["last_snapshot_at"].is_string());
        assert!(shown["last_snapshot_age_seconds"].is_number());
        first.type_text("\x1b");
        first.exit(true).await;
        second.type_text("\x1b");
        second.exit(true).await;
        assert!(fixture.output(&["agent", "delete", "tommy", "--yes"]).await.status.success());
        assert!(fixture.store.get_agent(agent.agent_id).await.unwrap().is_none());
        assert!(fixture.store.get_by_agent(agent.agent_id).await.unwrap().is_none());
        assert!(fixture.store.get_volume(swarmy_core::VolumeId::from_ulid(agent.agent_id.as_ulid())).await.unwrap().is_none());
        for session in sessions {
            assert!(fixture.store.fetch_session(session.session_id).await.unwrap().unwrap().computer_deleted);
        }
    }).await;
}

async fn bash_result(fixture: &Fixture, id: SessionId, services: &Services) -> String {
    timeout(Duration::from_secs(120), async {
        loop {
            let events = fixture.store.read_events(id, 0, 64).await.unwrap();
            if let Some(result) = events.iter().find_map(|event| match event {
                Event::ToolCallCompleted { result, .. } => Some(result),
                _ => None,
            }) {
                let ToolResult::Completed { output, .. } = result else {
                    panic!("bash failed: {result:?}");
                };
                let result: swarmy_core::BashResult = serde_json::from_str(output).unwrap();
                assert_eq!(result.exit_code, 0, "{}", result.stderr);
                assert!(!result.timed_out);
                return result.stdout;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "bash did not finish: {}",
            std::fs::read_to_string(services.files.path().join("node.log")).unwrap()
        )
    })
}

#[tokio::test]
async fn agent_delete_confirms_and_cancels_in_a_terminal() {
    run(|fixture| async move {
        let agent = fixture
            .store
            .create_agent("tommy", "fixture:test", "", Timestamp::now())
            .await
            .unwrap();
        for (answer, deleted) in [("n\r", false), ("yes\r", true)] {
            let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_swarmy"));
            command.args(["agent", "delete", "tommy", "--json"]);
            let mut terminal = Terminal::command(&fixture, command, "fixture:test");
            terminal
                .screen(|screen| screen.contains("Delete agent tommy and its computer? [y/N]"))
                .await;
            terminal.type_text(answer);
            terminal.exit(deleted).await;
            assert_eq!(
                fixture
                    .store
                    .get_agent(agent.agent_id)
                    .await
                    .unwrap()
                    .is_none(),
                deleted
            );
            let screen = terminal.parser.screen().contents();
            if deleted {
                assert!(screen.contains("agent_deleted"));
            } else {
                assert!(screen.contains("deletion cancelled"));
            }
        }
    })
    .await;
}

#[tokio::test]
async fn open_chat_follows_a_summarized_main_with_a_notice() {
    run(|fixture| async move {
        let agent = fixture.store.create_agent("tommy", "fixture:test", "", Timestamp::now()).await.unwrap();
        let mut chat = Terminal::with_agent(&fixture, None, None, "", Some("tommy"), false);
        chat.ready().await;
        let old = fixture.store.get_agent(agent.agent_id).await.unwrap().unwrap().main_session.unwrap();
        fixture.store.wake_session(old, Timestamp::now()).await.unwrap();
        let (lease, session, _) = fixture.store.claim_step(old, LeaseOwnerId::from_ulid(Ulid::generate()), Timestamp::now().checked_add(Duration::from_secs(30)).unwrap()).await.unwrap();
        let (new, event) = fixture.store.summarize_main_session(old, session.head_seq, &lease, &Message {
            id: MessageId::from_ulid(Ulid::generate()), role: MessageRole::System,
            parts: vec![Part::Text { text: "Goals: fix parser. State: tests pass. Open questions: release date. Facts: project path.".into() }],
        }).await.unwrap();
        fixture.bus.publish_live(LiveFeed::SessionEvents(old), &event).await.unwrap();
        let screen = chat.screen(|screen| screen.contains("Conversation summarized.") && screen.contains(&new.to_string()) && screen.contains("Enter: send")).await;
        assert!(screen.contains("archived;"));
        chat.type_text("Continue the work\r");
        timeout(WAIT, async {
            loop {
                if fixture.store.read_events(new, 0, 64).await.unwrap().iter().any(|event| matches!(event,
                    Event::MessageAppended { message, .. } if message.role == MessageRole::User)) { break; }
                sleep(Duration::from_millis(25)).await;
            }
        }).await.unwrap();
        assert_eq!(fixture.store.fetch_session(old).await.unwrap().unwrap().state, SessionState::Completed);
        let listing = fixture.output(&["session", "list", "--json"]).await;
        assert!(listing.status.success());
        let listing = String::from_utf8(listing.stdout).unwrap();
        let archived: serde_json::Value = listing.lines().map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap()).find(|value| value["session_id"] == old.to_string()).unwrap();
        assert_eq!(archived["archived"], true);
        assert_eq!(archived["next_session"], new.to_string());
        assert!(fixture.output(&["session", "show", &old.to_string(), "--json"]).await.status.success());
        chat.type_text("\x1b");
        chat.exit(true).await;
    }).await;
}

#[tokio::test]
async fn root_memory_written_by_tools_is_in_the_next_turn_and_capped() {
    if std::process::Command::new("id")
        .arg("-u")
        .output()
        .unwrap()
        .stdout
        != b"0\n"
    {
        eprintln!("skipping root memory test: run the built test with sudo");
        return;
    }
    let Ok(image) = std::env::var("SWARMY_TEST_IMAGE") else {
        eprintln!("skipping root memory test: SWARMY_TEST_IMAGE is unset");
        return;
    };
    run(|fixture| async move {
        let (services, _node) = root_services(&fixture, &image, r#"{
            "request_by_prompt": {
                "save": {"steps": 2, "tool_steps": [0], "bash_command": "mkdir -p /home/agent/memory; printf 'Remember: the launch code is violet.' > /home/agent/memory/a.txt; head -c 40000 /dev/zero | tr '\\0' x > /home/agent/memory/z.txt", "final_answer": "Saved memory"},
                "remember": {"steps": 1, "tool_steps": [], "final_answer": "Read memory"},
                "update": {"steps": 2, "tool_steps": [0], "bash_command": "printf 'Remember: the launch code is orange.' > /home/agent/memory/a.txt", "final_answer": "Updated memory"}
            }
        }"#).await;
        fixture.store.create_agent("tommy", "fixture:test", "", Timestamp::now()).await.unwrap();
        let mut chat = Terminal::with_agent(&fixture, None, None, "", Some("tommy"), false);
        chat.ready().await;
        let id = session_id(&fixture).await;
        chat.type_text("save\r");
        bash_result(&fixture, id, &services).await;
        chat.screen(|screen| screen.contains("Saved memory") && screen.contains("Enter: send")).await;
        let agent = fixture.store.fetch_session(id).await.unwrap().unwrap().agent_id;
        let volume = swarmy_core::VolumeId::from_ulid(agent.as_ulid());
        let manifest = fixture.store.get_volume(volume).await.unwrap().unwrap().head_manifest;
        let memory_reads = || std::fs::read_to_string(services.files.path().join("node.log")).unwrap().matches("reading agent memory with sandbox exec").count();
        assert_eq!(memory_reads(), 1);
        for (prompt, answer, fact) in [("remember", "Read memory", "violet"), ("update", "Updated memory", "orange")] {
            let before = fixture.store.fetch_session(id).await.unwrap().unwrap().head_seq;
            chat.type_text(&format!("{prompt}\r"));
            chat.screen(|screen| screen.contains(answer) && screen.contains("Enter: send")).await;
            let events = fixture.store.read_events(id, before, 64).await.unwrap();
            let request = events.iter().rev().find_map(|event| match event {
                Event::InferenceRequested { request_id, .. } => Some(*request_id), _ => None,
            }).unwrap();
            let job = fixture.store.get_inference_input::<swarmy_llm::InferenceJob>(request).await.unwrap().unwrap();
            assert!(job.request.system_prompt.contains(&format!("launch code is {fact}")));
            assert!(job.request.system_prompt.contains("Agent memory truncated at memory_max_bytes"));
            assert!(job.request.system_prompt.find("a.txt").unwrap() < job.request.system_prompt.find("z.txt").unwrap());
            assert!(job.request.system_prompt.len() < 34000);
            assert_eq!(memory_reads(), if prompt == "remember" { 1 } else { 2 });
        }
        assert_eq!(fixture.store.get_volume(volume).await.unwrap().unwrap().head_manifest, manifest, "memory updates must not require a checkpoint");
        chat.type_text("\x1b");
        chat.exit(true).await;
    }).await;
}
