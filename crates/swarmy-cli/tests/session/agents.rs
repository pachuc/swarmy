use super::*;
use swarmy_core::{AgentRecord, SessionKind};

fn success(output: std::process::Output) -> String {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[tokio::test]
async fn agent_commands_and_session_lifetimes() {
    run(|fixture| async move {
        let agent = create_agents(&fixture).await;
        let first = fixture
            .store
            .create_session_for_agent(
                SessionId::from_ulid(Ulid::generate()),
                Some(agent.agent_id),
                None,
                Timestamp::now(),
            )
            .await
            .unwrap();
        let second = fixture
            .store
            .create_session_for_agent(
                SessionId::from_ulid(Ulid::generate()),
                Some(agent.agent_id),
                None,
                Timestamp::now(),
            )
            .await
            .unwrap();
        let ephemeral = fixture
            .store
            .create_session_for_agent(
                SessionId::from_ulid(Ulid::generate()),
                None,
                Some("fixture:test"),
                Timestamp::now(),
            )
            .await
            .unwrap();
        fixture
            .store
            .set_main_session(agent.agent_id, first.session_id)
            .await
            .unwrap();
        inspect_agents(&fixture, &agent, first.session_id, second.session_id).await;
        success(
            fixture
                .output(&["session", "close", &second.session_id.to_string()])
                .await,
        );
        assert!(
            !fixture
                .store
                .fetch_session(second.session_id)
                .await
                .unwrap()
                .unwrap()
                .computer_deleted
        );
        close_and_delete(&fixture, &agent, first.session_id, ephemeral.session_id).await;
    })
    .await;
}

async fn create_agents(fixture: &Fixture) -> AgentRecord {
    for args in [
        vec!["agent", "create", "bad name"],
        vec!["agent", "create", "../bad"],
        vec!["agent", "create", ""],
    ] {
        let output = fixture.output(&args).await;
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("agent names"));
    }
    let output = fixture
        .output(&["agent", "create", "missing", "--image", "missing:tag"])
        .await;
    assert!(!output.status.success());
    assert!(
        fixture
            .store
            .list_agents(None, 64)
            .await
            .unwrap()
            .is_empty()
    );
    let text = success(
        fixture
            .output(&["agent", "create", "tommy", "--description", "Build things"])
            .await,
    );
    assert!(text.contains("Created agent tommy") && text.contains("image=fixture:test"));
    let agent = fixture
        .store
        .get_agent_by_name("tommy")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(agent.description, "Build things");
    assert!(
        !fixture
            .output(&["agent", "create", "tommy"])
            .await
            .status
            .success()
    );
    let created: AgentRecord = serde_json::from_str(&success(
        fixture
            .output(&[
                "agent",
                "create",
                "second",
                "--image",
                "fixture:test",
                "--json",
            ])
            .await,
    ))
    .unwrap();
    assert_eq!(created.name, "second");
    agent
}

async fn inspect_agents(
    fixture: &Fixture,
    agent: &AgentRecord,
    first: SessionId,
    second: SessionId,
) {
    let text = success(fixture.output(&["agent", "ls"]).await);
    assert!(
        text.contains("tommy")
            && text.contains("sessions=2")
            && text.contains("node=-")
            && text.contains("created=")
    );
    let listed = success(fixture.output(&["agent", "ls", "--json"]).await);
    let rows: Vec<serde_json::Value> = listed
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["session_count"], 2);
    assert!(rows[0]["node_id"].is_null());
    let text = success(fixture.output(&["agent", "show", "tommy"]).await);
    for expected in [
        "Build things",
        &format!("main_session={first}"),
        &format!("session={first} state=Idle computer_deleted=false main=true"),
        &format!("session={second} state=Idle computer_deleted=false main=false"),
        "placement_epoch=-",
        "sandbox_state=unknown",
        "last_snapshot=-",
        "age_seconds=-",
        &first.to_string(),
        &second.to_string(),
        "state=Idle",
    ] {
        assert!(text.contains(expected), "missing {expected}: {text}");
    }
    let shown: serde_json::Value = serde_json::from_str(&success(
        fixture
            .output(&["agent", "show", &agent.agent_id.to_string(), "--json"])
            .await,
    ))
    .unwrap();
    assert_eq!(shown["main_session"], first.to_string());
    assert_eq!(shown["sessions"].as_array().unwrap().len(), 2);
    assert_eq!(shown["sessions"][0]["state"], "idle");
    assert!(shown["last_snapshot_at"].is_null());
    assert!(
        !fixture
            .output(&["agent", "show", "absent"])
            .await
            .status
            .success()
    );
    let text = success(fixture.output(&["session", "ls"]).await);
    assert!(text.contains("kind=named agent=tommy") && text.contains("kind=ephemeral agent=-"));
    let text = success(fixture.output(&["session", "ls", "--json"]).await);
    let rows: Vec<serde_json::Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows[0]["agent_name"], "tommy");
    assert_eq!(rows[0]["main"], true);
    assert_eq!(rows[1]["main"], false);
    assert_eq!(rows[2]["main"], false);
    assert_eq!(
        rows[0]["kind"]["named"]["agent_id"],
        agent.agent_id.to_string()
    );
}

async fn close_and_delete(
    fixture: &Fixture,
    agent: &AgentRecord,
    first: SessionId,
    ephemeral: SessionId,
) {
    for json in [false, true] {
        let mut args = vec!["session", "close"];
        let id = first.to_string();
        args.push(&id);
        if json {
            args.push("--json");
        }
        let output = fixture.output(&args).await;
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("agent delete"));
    }
    let id = ephemeral.to_string();
    assert_eq!(
        success(fixture.output(&["session", "close", &id]).await).trim(),
        format!("Closed session {id}")
    );
    let closed: serde_json::Value = serde_json::from_str(&success(
        fixture.output(&["session", "close", &id, "--json"]).await,
    ))
    .unwrap();
    assert_eq!(closed["event"], "session_closed");
    let record = fixture
        .store
        .fetch_session(ephemeral)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, SessionState::Completed);
    assert!(record.computer_deleted);
    let refused = fixture.output(&["agent", "delete", "tommy"]).await;
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("--yes"));
    assert!(
        fixture
            .store
            .get_agent(agent.agent_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        success(fixture.output(&["agent", "delete", "tommy", "--yes"]).await)
            .contains("Deleted agent tommy")
    );
    let deleted: serde_json::Value = serde_json::from_str(&success(
        fixture
            .output(&["agent", "delete", "second", "--yes", "--json"])
            .await,
    ))
    .unwrap();
    assert_eq!(deleted["event"], "agent_deleted");
    let retained = fixture.store.fetch_session(first).await.unwrap().unwrap();
    assert!(retained.computer_deleted);
    assert_eq!(
        retained.kind,
        SessionKind::Named {
            agent_id: agent.agent_id
        }
    );
    assert!(success(fixture.output(&["agent", "ls", "--json"]).await).is_empty());
}

#[tokio::test]
async fn named_run_resolves_names_and_ids_without_a_default_image() {
    run(|fixture| async move {
        let agent = fixture
            .store
            .create_agent("tommy", "fixture:test", "", Timestamp::now())
            .await
            .unwrap();
        let service = serve(&fixture, true).await;
        for (name, json) in [
            ("tommy".to_owned(), false),
            (agent.agent_id.to_string(), true),
        ] {
            let mut command = fixture.command(&["run", "hello", "--agent", &name]);
            if json {
                command.arg("--json");
            }
            let text = success(
                timeout(WAIT, command.env("SWARMY_DEFAULT_IMAGE", "").output())
                    .await
                    .unwrap()
                    .unwrap(),
            );
            if json {
                let row: serde_json::Value =
                    serde_json::from_str(text.lines().next().unwrap()).unwrap();
                assert_eq!(row["agent_name"], "tommy");
            } else {
                assert!(text.contains("scripted answer"));
            }
        }
        service.abort();
        let sessions = fixture
            .store
            .list_sessions_by_agent(agent.agent_id, None, 64)
            .await
            .unwrap();
        assert_eq!(sessions.len(), 1);
        assert!(sessions.iter().all(|s| s.kind
            == SessionKind::Named {
                agent_id: agent.agent_id
            }));
        for command in ["chat", "run"] {
            let mut args = vec![command];
            if command == "run" {
                args.push("hello");
            }
            args.extend(["--agent", "tommy", "--image", "fixture:test"]);
            let output = fixture.output(&args).await;
            assert!(!output.status.success());
            let error = String::from_utf8_lossy(&output.stderr);
            assert!(
                error.contains("--image")
                    && error.contains("--agent")
                    && error.contains("cannot be used")
            );
        }
        assert!(
            !fixture
                .output(&["run", "hello", "--agent", "missing"])
                .await
                .status
                .success()
        );
        assert_eq!(
            fixture.store.list_sessions(None, 64).await.unwrap().len(),
            1
        );
    })
    .await;
}

#[tokio::test]
async fn json_chat_reads_prompts_and_retains_named_sessions_on_eof() {
    use tokio::io::AsyncWriteExt;
    run(|fixture| async move {
        let agent = fixture
            .store
            .create_agent("tommy", "fixture:test", "", Timestamp::now())
            .await
            .unwrap();
        let service = serve(&fixture, true).await;
        let mut child = fixture
            .command(&["chat", "--agent", "tommy", "--json"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(b"hello\n")
            .await
            .unwrap();
        let text = success(
            timeout(WAIT, child.wait_with_output())
                .await
                .unwrap()
                .unwrap(),
        );
        service.abort();
        let rows: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(rows[0]["agent_name"], "tommy");
        assert!(rows.iter().any(|row| row["event"] == "session_event"));
        let records = fixture
            .store
            .list_sessions_by_agent(agent.agent_id, None, 64)
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            fixture
                .store
                .get_agent(agent.agent_id)
                .await
                .unwrap()
                .unwrap()
                .main_session,
            Some(records[0].session_id)
        );
        assert!(!records[0].computer_deleted);
        assert_eq!(records[0].state, SessionState::Idle);
    })
    .await;
}

#[test]
fn agent_options_validate_before_connecting_to_services() {
    for binary in [
        env!("CARGO_BIN_EXE_swarmy"),
        env!("CARGO_BIN_EXE_swarmy-session"),
    ] {
        for args in [
            vec![
                "run",
                "hello",
                "--agent",
                "tommy",
                "--image",
                "fixture:test",
            ],
            vec!["chat", "--agent", "tommy", "--image", "fixture:test"],
            vec!["chat", "01ARZ3NDEKTSV4RRFFQ69G5FAV", "--agent", "tommy"],
            vec!["chat", "--new"],
            vec!["run", "hello", "--new"],
            vec![
                "chat",
                "01ARZ3NDEKTSV4RRFFQ69G5FAV",
                "--agent",
                "tommy",
                "--new",
            ],
            vec!["agent", "create", "invalid/name"],
            vec!["agent", "create", ""],
        ] {
            let output = std::process::Command::new(binary)
                .args(args)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(2),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        for args in [
            vec!["agent", "create", "tommy"],
            vec!["agent", "ls"],
            vec!["agent", "show", "tommy"],
            vec!["agent", "delete", "tommy"],
            vec!["session", "ls"],
            vec!["session", "close", "01ARZ3NDEKTSV4RRFFQ69G5FAV"],
            vec!["chat", "--agent", "tommy"],
            vec!["run", "hello", "--agent", "tommy"],
        ] {
            let output = std::process::Command::new(binary)
                .args(args)
                .args(["--json", "--remote", "demo", "--help"])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let text = String::from_utf8(output.stdout).unwrap();
            assert!(text.contains("--json") && text.contains("--remote"));
        }
    }
}

#[tokio::test]
async fn agent_listing_and_session_counts_cross_store_pages() {
    run(|fixture| async move {
        let mut agents = Vec::new();
        for index in 0..66 {
            agents.push(
                fixture
                    .store
                    .create_agent(
                        &format!("agent-{index}"),
                        "fixture:test",
                        "",
                        Timestamp::now(),
                    )
                    .await
                    .unwrap(),
            );
        }
        for _ in 0..66 {
            fixture
                .store
                .create_session_for_agent(
                    SessionId::from_ulid(Ulid::generate()),
                    Some(agents[0].agent_id),
                    None,
                    Timestamp::now(),
                )
                .await
                .unwrap();
        }
        let listed = success(fixture.output(&["agent", "ls", "--json"]).await);
        let rows: Vec<serde_json::Value> = listed
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(rows.len(), 66);
        assert_eq!(rows[0]["session_count"], 66);
        assert_eq!(rows.last().unwrap()["name"], "agent-65");
        let shown: serde_json::Value = serde_json::from_str(&success(
            fixture
                .output(&["agent", "show", "agent-0", "--json"])
                .await,
        ))
        .unwrap();
        assert_eq!(shown["sessions"].as_array().unwrap().len(), 66);
    })
    .await;
}

#[tokio::test]
async fn agent_show_reports_placement_and_committed_snapshot() {
    use swarmy_core::{NodeCapacity, NodeId, NodeRecord, NodeRole, VolumeId};
    run(|fixture| async move {
        let agent = fixture
            .store
            .create_agent("placed", "fixture:test", "", Timestamp::now())
            .await
            .unwrap();
        let node = NodeId::from_ulid(Ulid::generate());
        fixture
            .store
            .put_node(&NodeRecord {
                node_id: node,
                roles: vec![NodeRole::Sandbox],
                capacity: NodeCapacity {
                    cpu_millis: 1000,
                    memory_bytes: 1024,
                    disk_bytes: 1024,
                    sandboxes: 1,
                },
                last_heartbeat: Timestamp::now(),
                cached_images: vec![],
            })
            .await
            .unwrap();
        let placement = fixture
            .store
            .place(
                agent.agent_id,
                node,
                Timestamp::now()
                    .checked_add(Duration::from_secs(60))
                    .unwrap(),
            )
            .await
            .unwrap();
        fixture
            .store
            .create_volume(
                VolumeId::from_ulid(agent.agent_id.as_ulid()),
                agent.image.manifest_id,
            )
            .await
            .unwrap();
        let text = success(fixture.output(&["agent", "show", "placed"]).await);
        assert!(text.contains(&format!("node={node}")));
        assert!(text.contains(&format!("placement_epoch={}", placement.epoch)));
        assert!(!text.contains("last_snapshot=-"));
        let shown: serde_json::Value = serde_json::from_str(&success(
            fixture.output(&["agent", "show", "placed", "--json"]).await,
        ))
        .unwrap();
        assert_eq!(shown["placement"]["node_id"], node.to_string());
        assert_eq!(shown["placement"]["epoch"], placement.epoch);
        assert!(shown["last_snapshot_at"].is_string());
        assert!(shown["last_snapshot_age_seconds"].is_number());
        // Placement must not be presented as an observed running sandbox.
        assert_eq!(shown["sandbox_state"], "unknown");
        check_call_status(&fixture, &placement).await;
    })
    .await;
}

#[tokio::test]
async fn new_commands_use_the_selected_remote_profile() {
    run(|fixture| async move {
        let files = tempfile::tempdir().unwrap();
        std::fs::create_dir(files.path().join("remote")).unwrap();
        let profile = swarmy_config::RemoteProfile {
            name: "fixture".into(),
            socket_path: files.path().join("unused.sock"),
            pid: 0,
            ports: swarmy_config::RemotePorts::default(),
            remote_ports: swarmy_config::RemotePorts::default(),
            fdb_cluster_file: fixture.cluster.clone().into(),
            nats_url: fixture.url.clone(),
            s3_endpoint: swarmy_config::Settings::load()
                .unwrap()
                .settings
                .s3_endpoint,
            default_image: Some("fixture:test".into()),
        };
        std::fs::write(
            files.path().join("remote/fixture.profile.json"),
            serde_json::to_vec(&profile).unwrap(),
        )
        .unwrap();
        let ephemeral = fixture
            .store
            .create_session_for_agent(
                SessionId::from_ulid(Ulid::generate()),
                None,
                Some("fixture:test"),
                Timestamp::now(),
            )
            .await
            .unwrap();
        let id = ephemeral.session_id.to_string();
        let service = serve(&fixture, true).await;
        for args in [
            vec!["agent", "create", "remote-agent"],
            vec!["agent", "ls"],
            vec!["agent", "show", "remote-agent"],
            vec!["chat", "--agent", "remote-agent"],
            vec!["run", "hello", "--agent", "remote-agent"],
            vec!["session", "ls"],
            vec!["session", "close", &id],
            vec!["agent", "delete", "remote-agent", "--yes"],
        ] {
            let mut command = fixture.command(&args);
            command
                .args(["--remote", "fixture", "--json"])
                .env("SWARMY_STATE_DIR", files.path())
                .env(
                    "SWARMY_FDB_CLUSTER_FILE",
                    files.path().join("missing.cluster"),
                )
                .env("SWARMY_NATS_URL", "nats://127.0.0.1:1")
                .env("SWARMY_DEFAULT_IMAGE", "missing:default");
            let text = success(timeout(WAIT, command.output()).await.unwrap().unwrap());
            assert!(!text.is_empty());
            for line in text.lines() {
                serde_json::from_str::<serde_json::Value>(line).unwrap();
            }
        }
        service.abort();
        assert_eq!(
            fixture.store.list_sessions(None, 64).await.unwrap().len(),
            2
        );
        assert!(
            fixture
                .store
                .list_agents(None, 64)
                .await
                .unwrap()
                .is_empty()
        );
    })
    .await;
}

async fn check_call_status(fixture: &Fixture, placement: &swarmy_core::PlacementRecord) {
    let mut status = swarmy_core::AgentCallStatus {
        agent_id: placement.agent_id,
        node_id: placement.node_id,
        epoch: placement.epoch,
        holder_session_id: Some(SessionId::from_ulid(Ulid::generate())),
        queued_calls: 2,
        observed_at: Timestamp::now(),
        expires_at: placement.expires_at,
    };
    for expected in ["busy", "idle"] {
        fixture.store.put_agent_call_status(&status).await.unwrap();
        let shown: serde_json::Value = serde_json::from_str(&success(
            fixture.output(&["agent", "show", "placed", "--json"]).await,
        ))
        .unwrap();
        assert_eq!(shown["sandbox_state"], expected);
        assert_eq!(shown["call_status"], serde_json::to_value(&status).unwrap());
        let text = success(fixture.output(&["agent", "show", "placed"]).await);
        for field in [
            format!("sandbox_state={expected}"),
            format!("queued_calls={}", status.queued_calls),
            format!("observed_at={}", status.observed_at),
            format!("expires_at={}", status.expires_at),
            format!(
                "call_holder={}",
                status
                    .holder_session_id
                    .map_or_else(|| "-".into(), |id| id.to_string())
            ),
        ] {
            assert!(text.contains(&field), "missing {field}: {text}");
        }
        status.holder_session_id = None;
        status.queued_calls = 0;
        status.observed_at = Timestamp::now();
    }
    // A once-idle observation must not survive expiry or placement release.
    status.expires_at = Timestamp::now()
        .checked_sub(Duration::from_secs(1))
        .unwrap();
    fixture.store.put_agent_call_status(&status).await.unwrap();
    assert_unknown_call_status(fixture).await;
    status.expires_at = placement.expires_at;
    fixture.store.put_agent_call_status(&status).await.unwrap();
    fixture.store.release(placement).await.unwrap();
    assert_unknown_call_status(fixture).await;
}

async fn assert_unknown_call_status(fixture: &Fixture) {
    let shown: serde_json::Value = serde_json::from_str(&success(
        fixture.output(&["agent", "show", "placed", "--json"]).await,
    ))
    .unwrap();
    assert_eq!(shown["sandbox_state"], "unknown");
    assert!(shown["call_status"].is_null());
    assert_eq!(
        shown["sandbox_state_reason"],
        "no current node call observation"
    );
}

#[tokio::test]
async fn named_chat_resumes_main_and_new_preserves_the_pointer() {
    run(|fixture| async move {
        let agent = fixture
            .store
            .create_agent("tommy", "fixture:test", "", Timestamp::now())
            .await
            .unwrap();
        let mut main = None;
        for (new, expected) in [
            (true, "session_created"),
            (false, "session_created"),
            (false, "session_opened"),
            (true, "session_created"),
        ] {
            let mut args = vec!["chat", "--agent", "tommy", "--json"];
            if new {
                args.push("--new");
            }
            let text = success(fixture.output(&args).await);
            let row: serde_json::Value =
                serde_json::from_str(text.lines().next().unwrap()).unwrap();
            assert_eq!(row["event"], expected);
            let id = SessionId::from_ulid(row["session_id"].as_str().unwrap().parse().unwrap());
            if new {
                assert_ne!(main, Some(id));
            } else if let Some(main) = main {
                assert_eq!(id, main);
            } else {
                main = Some(id);
            }
            assert_eq!(
                fixture
                    .store
                    .get_agent(agent.agent_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .main_session,
                main
            );
        }
        let service = serve(&fixture, true).await;
        for new in [false, true] {
            let mut args = vec!["run", "hello", "--agent", "tommy", "--json"];
            if new {
                args.push("--new");
            }
            let text = success(fixture.output(&args).await);
            let row: serde_json::Value =
                serde_json::from_str(text.lines().next().unwrap()).unwrap();
            assert_eq!(
                row["event"],
                if new {
                    "session_created"
                } else {
                    "session_opened"
                }
            );
            assert_eq!(row["session_id"] == main.unwrap().to_string(), !new);
        }
        service.abort();
        assert_eq!(
            fixture
                .store
                .get_agent(agent.agent_id)
                .await
                .unwrap()
                .unwrap()
                .main_session,
            main
        );
        let text = success(fixture.output(&["session", "ls"]).await);
        assert_eq!(
            text.lines()
                .filter(|line| line.contains("main=true"))
                .count(),
            1
        );
        assert!(text.lines().any(
            |line| line.starts_with(&main.unwrap().to_string()) && line.ends_with("main=true")
        ));
    })
    .await;
}
