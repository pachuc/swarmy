use super::*;
use swarmy_core::{AgentRecord, ReasoningEffort, SessionKind};
use swarmy_store::AgentSessionOptions;

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
            .create_agent_session(
                SessionId::from_ulid(Ulid::generate()),
                Some(agent.agent_id),
                Timestamp::now(),
                None,
            )
            .await
            .unwrap();
        let second = fixture
            .store
            .create_agent_session(
                SessionId::from_ulid(Ulid::generate()),
                Some(agent.agent_id),
                Timestamp::now(),
                None,
            )
            .await
            .unwrap();
        let ephemeral = fixture
            .store
            .create_agent_session(
                SessionId::from_ulid(Ulid::generate()),
                None,
                Timestamp::now(),
                Some(AgentSessionOptions {
                    image: Some("fixture:test"),
                    ..Default::default()
                }),
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
    // The expected projection is assembled from the fixture's store records, not the API.
    let expected_agent = serde_json::json!({
        "agent_id": agent.agent_id, "name": agent.name, "description": agent.description,
        "main_session": first, "session_count": 2, "sandbox_state": "unknown",
        "last_snapshot_at": null, "node_id": null,
    });
    let actual_agent = serde_json::json!({
        "agent_id": shown["agent_id"], "name": shown["name"],
        "description": shown["description"], "main_session": shown["main_session"],
        "session_count": shown["session_count"], "sandbox_state": shown["sandbox_state"],
        "last_snapshot_at": shown["last_snapshot_at"], "node_id": shown["node_id"],
    });
    assert_eq!(actual_agent, expected_agent);

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
    let stored_session = fixture.store.fetch_session(first).await.unwrap().unwrap();
    let expected_session = serde_json::json!({
        "id": first, "state": stored_session.state,
        "head_sequence": stored_session.head_seq, "agent_name": "tommy",
        "main": true, "archived": false,
        "resolved": {"provider": "fake", "model": "scripted", "effort": "medium"},
    });
    let actual_session = serde_json::json!({
        "id": rows[0]["id"], "state": rows[0]["state"],
        "head_sequence": rows[0]["head_sequence"], "agent_name": rows[0]["agent_name"],
        "main": rows[0]["main"], "archived": rows[0]["archived"],
        "resolved": rows[0]["resolved"],
    });
    assert_eq!(actual_session, expected_session);

    assert_eq!(rows[1]["main"], false);
    assert_eq!(rows[2]["main"], false);
    assert_eq!(rows[0]["kind"], "named");
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
            .create_agent("tommy", "fixture:test", "", Timestamp::now(), None)
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
            .create_agent("tommy", "fixture:test", "", Timestamp::now(), None)
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
    // Conversation commands run through the API client, so only `swarmy`
    // validates their flag combinations. `swarmy-session` keeps the
    // management commands it still serves.
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
    ] {
        let output = std::process::Command::new(super::cli_bin::bin("swarmy"))
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
    for binary in [super::cli_bin::bin("swarmy")] {
        let binary = binary.as_os_str();
        for args in [
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
    for args in [
        vec!["chat", "--agent", "tommy"],
        vec!["run", "hello", "--agent", "tommy"],
    ] {
        let output = std::process::Command::new(super::cli_bin::bin("swarmy"))
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
                        None,
                    )
                    .await
                    .unwrap(),
            );
        }
        for _ in 0..66 {
            fixture
                .store
                .create_agent_session(
                    SessionId::from_ulid(Ulid::generate()),
                    Some(agents[0].agent_id),
                    Timestamp::now(),
                    None,
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
            .create_agent("placed", "fixture:test", "", Timestamp::now(), None)
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
                    memory_bytes: 1024 * 1024 * 1024,
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
            api_url: Some(fixture.api_url.clone()),
            api_token: Some(fixture.api_token.clone()),
            s3_bucket: None,
            s3_region: None,
            default_image: Some("fixture:test".into()),
        };
        std::fs::write(
            files.path().join("remote/fixture.profile.json"),
            serde_json::to_vec(&profile).unwrap(),
        )
        .unwrap();
        let ephemeral = fixture
            .store
            .create_agent_session(
                SessionId::from_ulid(Ulid::generate()),
                None,
                Timestamp::now(),
                Some(AgentSessionOptions {
                    image: Some("fixture:test"),
                    ..Default::default()
                }),
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
async fn github_tokens_are_private_rotatable_and_clearable() {
    run(|fixture| async move {
        let token = "test_github_private_initial";
        for json in [false, true] {
            let name = if json { "private-json" } else { "private" };
            let mut args = vec!["agent", "create", name, "--github-token", token];
            if json {
                args.push("--json");
            }
            assert!(!success(fixture.output(&args).await).contains(token));
            let agent = fixture
                .store
                .get_agent_by_name(name)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                fixture
                    .store
                    .agent_github_token(agent.agent_id)
                    .await
                    .unwrap()
                    .as_deref(),
                Some(token)
            );
            for command in ["show", "ls"] {
                let mut args = vec!["agent", command];
                if command == "show" {
                    args.push(name);
                }
                if json {
                    args.push("--json");
                }
                let text = success(fixture.output(&args).await);
                assert!(!text.contains(token));
                assert!(!text.contains("github_token"));
            }
            let rotated = "test_github_private_rotated";
            assert!(
                !success(
                    fixture
                        .output(&["agent", "set", name, "--github-token", rotated, "--json"])
                        .await
                )
                .contains(rotated)
            );
            assert_eq!(
                fixture
                    .store
                    .agent_github_token(agent.agent_id)
                    .await
                    .unwrap()
                    .as_deref(),
                Some(rotated)
            );
            success(
                fixture
                    .output(&["agent", "set", name, "--clear-github-token"])
                    .await,
            );
            assert!(
                fixture
                    .store
                    .agent_github_token(agent.agent_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            // Invalid values must not reach diagnostics or partially create records.
            let invalid = "private\ninvalid";
            let output = fixture
                .output(&["agent", "set", name, "--github-token", invalid])
                .await;
            assert!(!output.status.success());
            assert!(!String::from_utf8_lossy(&output.stderr).contains(invalid));
            assert!(
                fixture
                    .store
                    .agent_github_token(agent.agent_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            success(
                fixture
                    .output(&["agent", "set", name, "--github-token", rotated])
                    .await,
            );
            success(fixture.output(&["agent", "delete", name, "--yes"]).await);
            assert!(
                fixture
                    .store
                    .agent_github_token(agent.agent_id)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    })
    .await;
}

#[tokio::test]
async fn named_chat_resumes_main_and_new_preserves_the_pointer() {
    run(|fixture| async move {
        let agent = fixture
            .store
            .create_agent("tommy", "fixture:test", "", Timestamp::now(), None)
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
        assert!(
            text.lines()
                .any(|line| line.starts_with(&main.unwrap().to_string())
                    && line.contains(" main=true previous_session="))
        );
    })
    .await;
}
#[tokio::test]
async fn create_set_and_show_inference_settings_in_text_and_json() {
    run(|fixture| async move {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("prompt.txt");
        let prompt = "  Review this code.\nKeep the trailing newline.\n";
        std::fs::write(&path, prompt).unwrap();
        for json in [false, true] {
            let name = if json { "json-agent" } else { "text-agent" };
            let mut args = vec![
                "agent",
                "create",
                name,
                "--provider",
                "openai",
                "--model",
                "gpt-5.5",
                "--effort",
                "high",
            ];
            if json {
                args.extend(["--system-prompt-file", path.to_str().unwrap(), "--json"]);
            } else {
                args.extend(["--system-prompt", prompt]);
            }
            let output = success(fixture.output(&args).await);
            assert_output(
                &output,
                json,
                "Created",
                prompt,
                "gpt-5.5",
                ReasoningEffort::High,
            );
            assert_show(&fixture, name, prompt, "gpt-5.5", ReasoningEffort::High).await;
            for (flag, value, expected_prompt, expected_model, effort) in [
                (
                    "--model",
                    "gpt-5.4",
                    prompt,
                    "gpt-5.4",
                    ReasoningEffort::High,
                ),
                ("--effort", "none", prompt, "gpt-5.4", ReasoningEffort::None),
                ("--system-prompt", "", "", "gpt-5.4", ReasoningEffort::None),
                (
                    "--system-prompt-file",
                    path.to_str().unwrap(),
                    prompt,
                    "gpt-5.4",
                    ReasoningEffort::None,
                ),
            ] {
                let mut args = vec!["agent", "set", name, flag, value];
                if json {
                    args.push("--json");
                }
                let output = success(fixture.output(&args).await);
                assert_output(
                    &output,
                    json,
                    "Updated",
                    expected_prompt,
                    expected_model,
                    effort,
                );
                assert_show(&fixture, name, expected_prompt, expected_model, effort).await;
            }
        }
        assert_default_output(&fixture).await;
    })
    .await;
}

fn assert_output(
    text: &str,
    json: bool,
    verb: &str,
    prompt: &str,
    model: &str,
    effort: ReasoningEffort,
) {
    if json {
        let agent: AgentRecord = serde_json::from_str(text).unwrap();
        assert_eq!(agent.system_prompt.as_deref(), Some(prompt));
        assert_eq!(agent.model.as_deref(), Some(model));
        assert_eq!(agent.reasoning_effort, Some(effort));
    } else {
        assert!(text.contains(&format!("{verb} agent")));
        for expected in [
            format!("system_prompt={prompt}"),
            format!("model={model}"),
            format!("reasoning_effort={effort}"),
        ] {
            assert!(text.contains(&expected), "missing {expected}: {text}");
        }
    }
}

async fn assert_show(
    fixture: &Fixture,
    name: &str,
    prompt: &str,
    model: &str,
    effort: ReasoningEffort,
) {
    let text = success(fixture.output(&["agent", "show", name]).await);
    for expected in [
        format!("system_prompt={prompt}"),
        format!("model={model}"),
        format!("reasoning_effort={effort}"),
    ] {
        assert!(text.contains(&expected), "missing {expected}: {text}");
    }
    let text = success(fixture.output(&["agent", "show", name, "--json"]).await);
    assert_output(&text, true, "", prompt, model, effort);
}

#[tokio::test]
async fn invalid_agent_settings_do_not_change_records() {
    run(|fixture| async move {
        let agent: AgentRecord = serde_json::from_str(&success(
            fixture
                .output(&["agent", "create", "original", "--json"])
                .await,
        ))
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("missing.txt");
        let invalid_utf8 = directory.path().join("invalid.txt");
        std::fs::write(&invalid_utf8, [0xff]).unwrap();
        for command in ["create", "set"] {
            let name = if command == "create" {
                "invalid"
            } else {
                "original"
            };
            for flags in [
                vec!["--effort", "unknown"],
                vec!["--effort", "HIGH"],
                vec![
                    "--system-prompt",
                    "inline",
                    "--system-prompt-file",
                    missing.to_str().unwrap(),
                ],
                vec!["--system-prompt-file", missing.to_str().unwrap()],
                vec!["--system-prompt-file", invalid_utf8.to_str().unwrap()],
            ] {
                let mut args = vec!["agent", command, name, "--model", "should-not-stick"];
                args.extend(flags);
                let output = fixture.output(&args).await;
                assert!(!output.status.success(), "accepted {args:?}");
            }
        }
        assert!(
            !fixture
                .output(&["agent", "set", "original"])
                .await
                .status
                .success()
        );
        assert!(
            !fixture
                .output(&["agent", "set", "missing", "--model", "new"])
                .await
                .status
                .success()
        );
        assert_eq!(
            fixture.store.get_agent(agent.agent_id).await.unwrap(),
            Some(agent)
        );
        assert!(
            fixture
                .store
                .get_agent_by_name("invalid")
                .await
                .unwrap()
                .is_none()
        );
    })
    .await;
}

async fn assert_default_output(fixture: &Fixture) {
    let created: AgentRecord = serde_json::from_str(&success(
        fixture
            .output(&["agent", "create", "defaults", "--json"])
            .await,
    ))
    .unwrap();
    assert!(
        created.system_prompt.is_none()
            && created.model.is_none()
            && created.reasoning_effort.is_none()
    );
    let text = success(fixture.output(&["agent", "show", "defaults"]).await);
    for field in ["system_prompt", "model", "reasoning_effort"] {
        assert!(text.contains(&format!("{field}=(stack default)")));
    }
    let shown: serde_json::Value = serde_json::from_str(&success(
        fixture
            .output(&["agent", "show", "defaults", "--json"])
            .await,
    ))
    .unwrap();
    for field in ["system_prompt", "model", "reasoning_effort"] {
        assert!(shown[field].is_null());
    }
}

#[tokio::test]
async fn provider_selection_and_explicit_resets_are_durable() {
    run(|fixture| async move {
        success(
            fixture
                .output(&[
                    "agent",
                    "create",
                    "tommy",
                    "--provider",
                    "openrouter",
                    "--model",
                    "anthropic/claude-sonnet-4-6",
                    "--effort",
                    "max",
                ])
                .await,
        );
        let read = || async {
            serde_json::from_str::<AgentRecord>(&success(
                fixture.output(&["agent", "show", "tommy", "--json"]).await,
            ))
            .unwrap()
        };
        let agent = read().await;
        assert_eq!(agent.provider.as_deref(), Some("openrouter"));
        assert_eq!(agent.model.as_deref(), Some("anthropic/claude-sonnet-4.6"));
        assert_eq!(agent.reasoning_effort, Some(ReasoningEffort::Max));
        success(
            fixture
                .output(&["agent", "set", "tommy", "--model", "default"])
                .await,
        );
        assert!(read().await.model.is_none());
        success(
            fixture
                .output(&[
                    "agent",
                    "set",
                    "tommy",
                    "--provider",
                    "default",
                    "--effort",
                    "default",
                ])
                .await,
        );
        let agent = read().await;
        assert!(agent.provider.is_none() && agent.reasoning_effort.is_none());
    })
    .await;
}

#[tokio::test]
async fn ephemeral_selection_is_stored_and_invalid_flags_are_rejected() {
    run(|fixture| async move {
        let server = serve(&fixture, true).await;
        success(
            fixture
                .output(&[
                    "run",
                    "hello",
                    "--model",
                    "openai/gpt-5.5",
                    "--effort",
                    "max",
                ])
                .await,
        );
        let session = fixture
            .store
            .list_sessions(None, 1)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(session.inference.provider.as_deref(), Some("openai"));
        assert_eq!(session.inference.model.as_deref(), Some("gpt-5.5"));
        assert_eq!(session.inference.effort, Some(ReasoningEffort::Max));
        for args in [
            vec![
                "run",
                "hello",
                "--provider",
                "anthropic",
                "--model",
                "openai/gpt-5.5",
            ],
            vec![
                "run",
                "hello",
                "--provider",
                "openai",
                "--model",
                "nonexistent",
            ],
            vec!["run", "hello", "--agent", "tommy", "--effort", "max"],
            vec!["run", "hello", "--effort", "bogus"],
        ] {
            let output = fixture.output(&args).await;
            assert!(!output.status.success(), "accepted {args:?}");
            let error = String::from_utf8_lossy(&output.stderr);
            if args.contains(&"nonexistent") {
                assert!(error.contains("closest matches:"), "{error}");
            }
            if args.contains(&"bogus") {
                assert!(error.contains("max"), "{error}");
            }
        }
        let id = session.session_id.to_string();
        assert!(
            !fixture
                .output(&["chat", &id, "--model", "openai/gpt-5.5"])
                .await
                .status
                .success()
        );
        let listing = success(fixture.output(&["session", "ls"]).await);
        assert!(listing.contains("openai/gpt-5.5"));
        server.abort();
    })
    .await;
}
