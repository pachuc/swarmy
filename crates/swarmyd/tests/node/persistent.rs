use super::*;
use swarmy_bus::{Bus, Config, SubjectToken, WorkQueue};
use swarmy_core::{
    BashArguments, Event, LeaseOwnerId, RequestId, SessionId, SessionRecord, SessionState,
    ToolCallId, ToolCallRecord, ToolJob, ToolResult,
};

async fn dispatch(
    store: &Store,
    bus: &Bus,
    node: NodeId,
    agent: AgentId,
    command: &str,
) -> ToolJob {
    dispatch_arguments(
        store,
        bus,
        node,
        agent,
        swarmy_core::SandboxArguments::Bash(BashArguments {
            command: command.into(),
            timeout_ms: 120_000,
        }),
    )
    .await
}

async fn dispatch_arguments(
    store: &Store,
    bus: &Bus,
    node: NodeId,
    agent: AgentId,
    arguments: swarmy_core::SandboxArguments,
) -> ToolJob {
    let session = SessionRecord {
        session_id: SessionId::from_ulid(ulid::Ulid::generate()),
        agent_id: agent,
        state: SessionState::Idle,
        head_seq: 0,
        snapshot_ref: None,
    };
    let id = session.session_id;
    store
        .create_session(&session, jiff::Timestamp::now())
        .await
        .unwrap();
    store
        .set_session_image(id, "persistent", &ImageTag("test".into()))
        .await
        .unwrap();
    store
        .wake_session(id, jiff::Timestamp::now())
        .await
        .unwrap();
    let lease = store
        .claim_lease(
            id,
            LeaseOwnerId::from_ulid(ulid::Ulid::generate()),
            jiff::Timestamp::now()
                .checked_add(Duration::from_secs(30))
                .unwrap(),
        )
        .await
        .unwrap();
    let job = ToolJob {
        session_id: id,
        request_id: RequestId::for_step(id, 1),
        call_id: ToolCallId(arguments.name().into()),
        step: 1,
        arguments,
    };
    store
        .append_events_leased(
            id,
            0,
            &[Event::ToolCallRequested {
                seq: 0,
                request_id: job.request_id,
                call: ToolCallRecord {
                    call_id: job.call_id.clone(),
                    tool: job.arguments.name().into(),
                    arguments: job.arguments.parameters(),
                    result: None,
                },
            }],
            &lease,
            jiff::Timestamp::now(),
        )
        .await
        .unwrap();
    store
        .dispatch_tool_jobs(id, &lease, std::slice::from_ref(&job))
        .await
        .unwrap();
    bus.publish_work(&WorkQueue::NodeTools(node), &job)
        .await
        .unwrap();
    job
}

async fn completed(
    store: &Store,
    job: &ToolJob,
) -> std::collections::BTreeMap<String, serde_json::Value> {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            if store.tool_completed(job.request_id).await.unwrap() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("persistent tool did not complete");
    let events = store.read_events(job.session_id, 1, 10).await.unwrap();
    let Event::ToolCallCompleted {
        result: ToolResult::Completed { metadata, .. },
        ..
    } = &events[0]
    else {
        panic!("missing bash output")
    };
    assert_eq!(metadata["exit_code"], 0, "{metadata:?}");
    metadata.clone()
}

fn device(node: &Node, agent: AgentId) -> PathBuf {
    PathBuf::from(
        std::fs::read_to_string(
            node.root
                .path()
                .join(format!(".swarmy/node/bundles/{agent}/device")),
        )
        .unwrap(),
    )
}

fn absent(node: &Node, agent: AgentId, device: &Path) {
    assert!(
        !node
            .root
            .path()
            .join(format!(".swarmy/node/runc/{agent}"))
            .exists()
    );
    assert!(
        !node
            .root
            .path()
            .join(format!(".swarmy/node/bundles/{agent}"))
            .exists()
    );
    let name = device.file_name().unwrap().to_str().unwrap();
    assert!(!Path::new(&format!("/sys/block/{name}/pid")).exists());
    assert_eq!(
        std::fs::read_to_string(format!("/sys/block/{name}/size"))
            .unwrap()
            .trim(),
        "0"
    );
}

async fn evicted(store: &Store, agent: AgentId) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while store.get_by_agent(agent).await.unwrap().is_some() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("idle placement was not released");
}

async fn start(settings: swarmy_config::Settings, store: &Store, base: ManifestId) -> (Node, Bus) {
    let mut settings = settings;
    settings.sandbox_idle_seconds = std::num::NonZeroU64::new(2).unwrap();
    settings.placement_lease_seconds = std::num::NonZeroU64::new(3).unwrap();
    settings.bus_prefix = format!("persistent-{}", ulid::Ulid::generate());
    let bus = Bus::connect(
        &settings.nats_url,
        Config {
            prefix: Some(SubjectToken::new(&settings.bus_prefix).unwrap()),
            ..Config::default()
        },
    )
    .await
    .unwrap();
    let mut node = Node::new(settings);
    node.start();
    node.ready(store, jiff::Timestamp::UNIX_EPOCH).await;
    bus.setup(&[WorkQueue::NodeTools(node.id)]).await.unwrap();
    store
        .put_image("persistent", &ImageTag("test".into()), base)
        .await
        .unwrap();
    (node, bus)
}

pub async fn run(settings: swarmy_config::Settings, store: &Store, base: ManifestId) {
    let (mut node, bus) = start(settings, store, base).await;
    managed_tools(&node, store, &bus).await;
    let agent = AgentId::from_ulid(ulid::Ulid::generate());
    let volume = VolumeId::from_ulid(agent.as_ulid());
    let job = dispatch(
        store,
        &bus,
        node.id,
        agent,
        "sleep 300 >/dev/null 2>&1 & echo $! >/background.pid; echo durable >/persistent; sleep 5",
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let first = store.get_by_agent(agent).await.unwrap().unwrap();
    completed(store, &job).await;
    let renewed = store.get_by_agent(agent).await.unwrap().unwrap();
    assert_eq!(first.epoch, renewed.epoch);
    assert!(renewed.expires_at > first.expires_at);
    let path = device(&node, agent);
    let job = dispatch(store, &bus, node.id, agent, "kill -0 $(cat /background.pid) && dd if=/dev/urandom of=/dirty bs=1M count=64 status=none && sync").await;
    completed(store, &job).await;
    let before = store
        .get_volume(volume)
        .await
        .unwrap()
        .unwrap()
        .head_manifest;
    assert_eq!(before, base);
    let started = std::time::Instant::now();
    let job = dispatch(store, &bus, node.id, agent, "true").await;
    completed(store, &job).await;
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "trivial call took {elapsed:?}"
    );
    assert_eq!(
        store
            .get_volume(volume)
            .await
            .unwrap()
            .unwrap()
            .head_manifest,
        before
    );
    eprintln!(
        "persistent acceptance: background process survived another session, lease renewed, trivial call with 64 MiB dirty returned in {elapsed:?} without publication"
    );
    evicted(store, agent).await;
    absent(&node, agent, &path);
    let checkpoint = store.get_volume(volume).await.unwrap().unwrap();
    assert_ne!(checkpoint.head_manifest, before);
    assert!(checkpoint.writer_lease.is_none());
    let job = dispatch(
        store,
        &bus,
        node.id,
        agent,
        "test $(cat /persistent) = durable && test -s /dirty",
    )
    .await;
    completed(store, &job).await;
    let placement = store.get_by_agent(agent).await.unwrap().unwrap();
    assert!(placement.epoch > renewed.epoch);
    assert_eq!(
        placement.last_change_reason,
        swarmy_core::PlacementChangeReason::Eviction
    );
    eprintln!(
        "persistent acceptance: idle eviction published checkpoint, removed container/device, released placement, and rehydrated files with eviction reason"
    );
    crash(&mut node, store, &bus, agent).await;
    takeover(&mut node, store, &bus, agent, true).await;
    takeover(
        &mut node,
        store,
        &bus,
        AgentId::from_ulid(ulid::Ulid::generate()),
        false,
    )
    .await;
    graceful(&mut node, store, &bus, base).await;
}

async fn crash(node: &mut Node, store: &Store, bus: &Bus, agent: AgentId) {
    let job = dispatch(
        store,
        bus,
        node.id,
        agent,
        "touch /uncommitted; sync; sleep 100",
    )
    .await;
    written(node, agent, "uncommitted").await;
    assert!(!store.tool_completed(job.request_id).await.unwrap());
    let path = device(node, agent);
    let volume = VolumeId::from_ulid(agent.as_ulid());
    let head = store.get_volume(volume).await.unwrap().unwrap();
    node.kill();
    node.start();
    node.ready(store, jiff::Timestamp::now()).await;
    absent(node, agent, &path);
    let wait = head
        .writer_lease
        .unwrap()
        .expires_at
        .duration_since(jiff::Timestamp::now())
        .as_secs()
        .max(0)
        .unsigned_abs()
        + 2;
    eprintln!(
        "persistent crash acceptance: startup removed orphan container/device; waiting {wait}s for crashed writer lease"
    );
    tokio::time::sleep(Duration::from_secs(wait)).await;
    let job = dispatch(
        store,
        bus,
        node.id,
        agent,
        "test ! -e /uncommitted && test $(cat /persistent) = durable",
    )
    .await;
    completed(store, &job).await;
    assert!(store.get_by_agent(agent).await.unwrap().unwrap().epoch > 1);
    eprintln!(
        "persistent crash acceptance: restarted node hosted from the last checkpoint after SIGKILL mid-call"
    );
}

async fn takeover(node: &mut Node, store: &Store, bus: &Bus, agent: AgentId, during_call: bool) {
    let active = dispatch(
        store,
        bus,
        node.id,
        agent,
        if during_call {
            "touch /takeover-started; sleep 100"
        } else {
            "touch /takeover-started"
        },
    )
    .await;
    written(node, agent, "takeover-started").await;
    if !during_call {
        completed(store, &active).await;
    }
    let placement = store.get_by_agent(agent).await.unwrap().unwrap();
    let path = device(node, agent);
    let pid = node.child.as_ref().unwrap().id().to_string();
    assert!(
        Command::new("kill")
            .args(["-STOP", &pid])
            .status()
            .unwrap()
            .success()
    );
    tokio::time::sleep(Duration::from_secs(4)).await;
    let mut other = store.get_node(node.id).await.unwrap().unwrap();
    other.node_id = NodeId::from_ulid(ulid::Ulid::generate());
    store.put_node(&other).await.unwrap();
    let replacement = store
        .take_over(
            &placement,
            other.node_id,
            jiff::Timestamp::now()
                .checked_add(Duration::from_secs(30))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        Command::new("kill")
            .args(["-CONT", &pid])
            .status()
            .unwrap()
            .success()
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    absent(node, agent, &path);
    assert_eq!(
        store.tool_completed(active.request_id).await.unwrap(),
        !during_call
    );
    let job = dispatch(store, bus, node.id, agent, "touch /must-not-run").await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!store.tool_completed(job.request_id).await.unwrap());
    assert_eq!(
        store.get_by_agent(agent).await.unwrap().unwrap(),
        replacement
    );
    let log = std::fs::read_to_string(node.root.path().join("node.log")).unwrap();
    assert!(
        log.contains("agent is placed on another node"),
        "wrong-node refusal must explain the placement: {log}"
    );
    eprintln!(
        "persistent takeover acceptance: old container/device stopped and wrong-node call refused after epoch takeover"
    );
}

async fn graceful(node: &mut Node, store: &Store, bus: &Bus, base: ManifestId) {
    let agent = AgentId::from_ulid(ulid::Ulid::generate());
    let job = dispatch(
        store,
        bus,
        node.id,
        agent,
        "echo shutdown >/shutdown-marker; sync; sleep 100",
    )
    .await;
    written(node, agent, "shutdown-marker").await;
    assert!(!store.tool_completed(job.request_id).await.unwrap());
    let path = device(node, agent);
    node.stop().await;
    absent(node, agent, &path);
    assert!(store.get_by_agent(agent).await.unwrap().is_none());
    let volume = store
        .get_volume(VolumeId::from_ulid(agent.as_ulid()))
        .await
        .unwrap()
        .unwrap();
    assert!(volume.writer_lease.is_none());
    assert_ne!(volume.head_manifest, base);
    node.start();
    node.ready(store, jiff::Timestamp::now()).await;
    let job = dispatch(
        store,
        bus,
        node.id,
        agent,
        "test $(cat /shutdown-marker) = shutdown",
    )
    .await;
    completed(store, &job).await;
    node.stop().await;
    assert!(store.get_by_agent(agent).await.unwrap().is_none());
    eprintln!(
        "persistent shutdown acceptance: SIGTERM during a call checkpointed and detached; restart recovered the file"
    );
}

async fn written(node: &Node, agent: AgentId, name: &str) {
    let path = node
        .root
        .path()
        .join(format!(".swarmy/node/bundles/{agent}/rootfs/{name}"));
    tokio::time::timeout(Duration::from_secs(15), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("command did not reach its file write");
}

async fn tool_result(store: &Store, job: &ToolJob) -> ToolResult {
    tokio::time::timeout(Duration::from_secs(45), async {
        while !store.tool_completed(job.request_id).await.unwrap() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("tool did not complete");
    let events = store.read_events(job.session_id, 1, 10).await.unwrap();
    let Event::ToolCallCompleted { result, .. } = &events[0] else {
        panic!("missing result");
    };
    result.clone()
}

async fn invoke(
    node: &Node,
    store: &Store,
    bus: &Bus,
    agent: AgentId,
    name: &str,
    arguments: serde_json::Value,
) -> serde_json::Value {
    let job = dispatch_arguments(
        store,
        bus,
        node.id,
        agent,
        swarmy_core::SandboxArguments::parse(name, arguments).unwrap(),
    )
    .await;
    let ToolResult::Completed { output, .. } = tool_result(store, &job).await else {
        panic!("{name} failed");
    };
    serde_json::from_str(&output).unwrap()
}

async fn managed_tools(node: &Node, store: &Store, bus: &Bus) {
    use serde_json::json;
    let agent = AgentId::from_ulid(ulid::Ulid::generate());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let started = invoke(
        node,
        store,
        bus,
        agent,
        "process_start",
        json!({"command": format!("exec python3 -u -m http.server {port} --bind 127.0.0.1")}),
    )
    .await;
    let id = started["process_id"].clone();
    assert!(
        started["log_path"]
            .as_str()
            .unwrap()
            .starts_with("/var/lib/swarmy/processes/")
    );
    let fetched = invoke(node, store, bus, agent, "bash", json!({"command":format!("curl --retry 10 --retry-connrefused --retry-delay 1 -fsS http://127.0.0.1:{port}/ >/dev/null")})).await;
    assert_eq!(fetched["exit_code"], 0);
    let placement = store.get_by_agent(agent).await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        store.get_by_agent(agent).await.unwrap().unwrap().epoch,
        placement.epoch,
        "managed processes prevent idle eviction"
    );
    let listed = invoke(node, store, bus, agent, "process_list", json!({})).await;
    assert_eq!(listed[0]["process_id"], id);
    assert_eq!(listed[0]["status"], "running");
    let log = invoke(
        node,
        store,
        bus,
        agent,
        "process_log",
        json!({"process_id":id}),
    )
    .await;
    assert!(log["output"].as_str().unwrap().contains("GET /"));
    let job = dispatch_arguments(
        store,
        bus,
        node.id,
        agent,
        swarmy_core::SandboxArguments::parse(
            "bash",
            json!({"command":"sleep 300 & wait", "timeout_ms":200}),
        )
        .unwrap(),
    )
    .await;
    assert!(
        matches!(tool_result(store, &job).await, ToolResult::Error { error } if error.contains("timed out"))
    );
    let fetched = invoke(
        node,
        store,
        bus,
        agent,
        "bash",
        json!({"command":format!("curl -fsS http://127.0.0.1:{port}/ >/dev/null")}),
    )
    .await;
    assert_eq!(
        fetched["exit_code"], 0,
        "timeout must leave HTTP server alive"
    );
    check_log_and_checkpoint(node, store, bus, agent, &started).await;
    stop_and_rebuild(node, store, bus, agent, id, port).await;
}

async fn check_log_and_checkpoint(
    node: &Node,
    store: &Store,
    bus: &Bus,
    agent: AgentId,
    started: &serde_json::Value,
) {
    use serde_json::json;
    let id = &started["process_id"];
    let filled = invoke(node, store, bus, agent, "bash", json!({"command":format!("head -c 70000 /dev/zero | tr '\\0' x > {}", started["log_path"].as_str().unwrap())})).await;
    assert_eq!(filled["exit_code"], 0);
    let tail = invoke(
        node,
        store,
        bus,
        agent,
        "process_log",
        json!({"process_id":id}),
    )
    .await;
    assert_eq!(tail["output"].as_str().unwrap().len(), 65536);
    assert_eq!(tail["truncated"], true);
    let snapshot = invoke(node, store, bus, agent, "checkpoint", json!({})).await;
    let volume = VolumeId::from_ulid(agent.as_ulid());
    assert_eq!(
        snapshot["manifest_id"],
        json!(
            store
                .get_volume(volume)
                .await
                .unwrap()
                .unwrap()
                .head_manifest
        )
    );
}

async fn stop_and_rebuild(
    node: &Node,
    store: &Store,
    bus: &Bus,
    agent: AgentId,
    id: serde_json::Value,
    port: u16,
) {
    use serde_json::json;
    invoke(
        node,
        store,
        bus,
        agent,
        "process_stop",
        json!({"process_id":id}),
    )
    .await;
    let listed = invoke(node, store, bus, agent, "process_list", json!({})).await;
    assert_eq!(listed[0]["status"], "exited");
    let fetched = invoke(
        node,
        store,
        bus,
        agent,
        "bash",
        json!({"command":format!("curl --max-time 1 -fsS http://127.0.0.1:{port}/ >/dev/null")}),
    )
    .await;
    assert_ne!(fetched["exit_code"], 0);
    eprintln!(
        "managed tools passed: background HTTP, later bash, list, logs, idle protection, isolated timeout, checkpoint head, and stop"
    );
    evicted(store, agent).await;
    let job = dispatch_arguments(
        store,
        bus,
        node.id,
        agent,
        swarmy_core::SandboxArguments::parse("process_log", json!({"process_id":id})).unwrap(),
    )
    .await;
    assert!(
        matches!(tool_result(store, &job).await, ToolResult::Error { error } if error.contains("sandbox restarted"))
    );
    evicted(store, agent).await;
}
