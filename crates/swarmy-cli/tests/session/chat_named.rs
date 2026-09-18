use super::*;

#[tokio::test]
async fn named_chat_header_and_notices_identify_the_session() {
    run(|fixture| async move {
        let agent = fixture
            .store
            .create_agent("tommy", "fixture:test", "", Timestamp::now())
            .await
            .unwrap();
        let mut first = Terminal::with_agent(&fixture, None, None, "", Some("tommy"));
        assert!(first.ready().await.contains("tommy |"));
        let first_id = session_id(&fixture).await;
        let mut second =
            Terminal::with_agent(&fixture, None, None, "", Some(&agent.agent_id.to_string()));
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
        let mut first = Terminal::with_agent(&fixture, None, None, "", Some("tommy"));
        first.ready().await;
        let first_id = session_id(&fixture).await;
        let mut second = Terminal::with_agent(&fixture, None, None, "", Some(&agent.agent_id.to_string()));
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
