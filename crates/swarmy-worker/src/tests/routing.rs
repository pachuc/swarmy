use super::*;
use swarmy_bus::WorkMessage;
use swarmy_core::{
    BashResult, CHUNK_SIZE, ContentHash, ImageTag, LeaseOwnerId, ManifestHeader, ManifestId,
    NodeCapacity, NodeId, NodeRecord, NodeRole, PlacedToolClaim, PlacementChangeReason,
    PlacementRecord, ToolJob, ToolResult,
};
use swarmy_store::StoreError;

struct Fixture {
    store: Store,
    bus: Bus,
    worker: Worker,
    cluster: String,
    url: String,
    prefix: String,
    agent: AgentId,
    nodes: [NodeId; 2],
    manifest: ManifestId,
}

impl Fixture {
    async fn new() -> Option<Self> {
        let (Ok(cluster), Ok(url)) = (
            std::env::var("SWARMY_FDB_CLUSTER_FILE"),
            std::env::var("SWARMY_NATS_URL"),
        ) else {
            eprintln!("skipping routing test: FoundationDB or NATS environment is unset");
            return None;
        };
        NETWORK.get_or_init(swarmy_store::boot);
        let prefix = format!("routing_{}", Ulid::generate());
        let mut config = config(cluster.clone(), url.clone(), &prefix, Arc::default());
        config.harness.tools.register(Box::new(swarmy_tools::Bash));
        config.partitions = (0..256).collect();
        config.bus.ack_wait = Duration::from_millis(200);
        let blobs = Arc::new(MemoryBlobStore::default());
        let store = Store::open(Some(&cluster), Some(&config.directory), blobs.clone())
            .await
            .unwrap();
        let bus = Bus::connect(&url, config.bus.clone()).await.unwrap();
        bus.setup(&[]).await.unwrap();
        let nodes = [
            NodeId::from_ulid(Ulid::from_parts(1, 1)),
            NodeId::from_ulid(Ulid::from_parts(1, 2)),
        ];
        for node in nodes {
            store
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
                    cached_images: Vec::new(),
                })
                .await
                .unwrap();
        }
        let manifest = ManifestId::from_ulid(Ulid::from_parts(1_789_545_600_000, 1));
        store
            .put_manifest(
                manifest,
                &ManifestHeader {
                    size: u64::from(CHUNK_SIZE),
                    chunk_size: CHUNK_SIZE,
                    root_hash: ContentHash::ZERO,
                },
            )
            .await
            .unwrap();
        store
            .put_image("routing", &ImageTag("test".into()), manifest)
            .await
            .unwrap();
        let worker = Worker::new(store.clone(), bus.clone(), blobs, config);
        Some(Self {
            store,
            bus,
            worker,
            cluster,
            url,
            prefix,
            agent: AgentId::from_ulid(Ulid::generate()),
            nodes,
            manifest,
        })
    }

    async fn session(&self) -> SessionId {
        let id = SessionId::from_ulid(Ulid::generate());
        self.store
            .create_session(
                &SessionRecord {
                    session_id: id,
                    agent_id: self.agent,
                    state: SessionState::Idle,
                    head_seq: 0,
                    snapshot_ref: None,
                    kind: swarmy_core::SessionKind::Ephemeral,
                    computer_deleted: false,
                },
                Timestamp::now(),
                "routing:test",
            )
            .await
            .unwrap();
        self.request(id).await;
        id
    }

    async fn request(&self, id: SessionId) {
        let head = self
            .store
            .fetch_session(id)
            .await
            .unwrap()
            .unwrap()
            .head_seq;
        let request_id = self
            .store
            .read_events(id, 0, 64)
            .await
            .unwrap()
            .iter()
            .rev()
            .find_map(|event| match event {
                Event::InferenceRequested { request_id, .. } => Some(*request_id),
                _ => None,
            })
            .unwrap_or_else(|| RequestId::for_step(id, head + 1));
        self.store
            .append_events(
                id,
                head,
                &[Event::InferenceCompleted {
                    seq: 0,
                    request_id,
                    message: Message {
                        id: MessageId::from_ulid(Ulid::generate()),
                        role: MessageRole::Assistant,
                        parts: vec![Part::ToolCall {
                            call_id: ToolCallId(format!("shell-{head}")),
                            tool: "bash".into(),
                            input: json!({"command": "echo hello"}),
                        }],
                    },
                }],
            )
            .await
            .unwrap();
        self.store
            .set_state(id, SessionState::Runnable, None, Timestamp::now())
            .await
            .unwrap();
    }

    async fn step(&self, id: SessionId) {
        let queue = WorkQueue::Runnable(swarmy_store::runnable_partition(id));
        let mut messages = self.bus.consume::<Nudge>(&queue).await.unwrap();
        self.bus
            .publish_work(&queue, &Nudge { session_id: id })
            .await
            .unwrap();
        let message = timeout(Duration::from_secs(5), messages.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        self.worker.handle(&message).await.unwrap();
    }

    async fn delivery(&self, node: NodeId) -> WorkMessage<ToolJob> {
        let mut messages = self
            .bus
            .consume::<ToolJob>(&WorkQueue::NodeTools(node))
            .await
            .unwrap();
        timeout(Duration::from_secs(5), messages.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    }

    async fn claim(&self, job: ToolJob, placement: PlacementRecord) -> PlacedToolClaim {
        self.store.claim_placement(&placement).await.unwrap();
        let claim = PlacedToolClaim {
            job,
            owner: LeaseOwnerId::from_ulid(Ulid::generate()),
            placement,
            expires_at: Timestamp::now()
                .checked_add(Duration::from_secs(30))
                .unwrap(),
        };
        assert!(self.store.claim_placed_tool(&claim).await.unwrap());
        claim
    }

    async fn complete(&self, claim: &PlacedToolClaim) -> swarmy_store::Result<()> {
        let head = self
            .store
            .fetch_session(claim.job.session_id)
            .await
            .unwrap()
            .unwrap()
            .head_seq;
        self.store
            .complete_placed_tool(
                claim,
                head,
                &BashResult {
                    stdout: "hello".into(),
                    stderr: String::new(),
                    exit_code: 0,
                    timed_out: false,
                    manifest_id: self.manifest,
                }
                .tool_result(),
            )
            .await
    }

    async fn cleanup(self) {
        cleanup(&self.cluster, &self.url, &self.prefix).await;
    }
}

#[tokio::test]
async fn two_nodes_share_agent_placement_across_sessions_and_skip_full_nodes() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    // Fill the first candidate to verify transactional capacity admission.
    f.store
        .place(
            AgentId::from_ulid(Ulid::generate()),
            f.nodes[0],
            Timestamp::now()
                .checked_add(Duration::from_secs(30))
                .unwrap(),
        )
        .await
        .unwrap();
    let id = f.session().await;
    f.step(id).await;
    let placement = f.store.get_by_agent(f.agent).await.unwrap().unwrap();
    assert_eq!(placement.node_id, f.nodes[1]);
    f.store.agent_volume(id, &placement).await.unwrap();
    let delivery = f.delivery(f.nodes[1]).await;
    assert_eq!(delivery.value.session_id, id);
    let claim = f.claim(delivery.value.clone(), placement.clone()).await;
    f.complete(&claim).await.unwrap();
    delivery.acknowledge().await.unwrap();
    let second = f.session().await;
    f.step(second).await;
    let delivery = f.delivery(f.nodes[1]).await;
    assert_eq!(delivery.value.session_id, second);
    assert_eq!(
        f.store.get_by_agent(f.agent).await.unwrap().unwrap().epoch,
        placement.epoch
    );
    let claim = f.claim(delivery.value.clone(), placement).await;
    f.complete(&claim).await.unwrap();
    delivery.acknowledge().await.unwrap();
    assert!(
        !f.store
            .read_events(second, 0, 64)
            .await
            .unwrap()
            .iter()
            .any(is_notice)
    );
    f.cleanup().await;
}

fn is_notice(event: &Event) -> bool {
    matches!(event, Event::MessageAppended { message, .. } if message.role == MessageRole::System)
}

#[tokio::test]
async fn recovery_waits_for_writer_lease_before_granting_new_epoch() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let placement = f
        .store
        .place(
            f.agent,
            f.nodes[0],
            // Leases long enough that setup and the first step cannot outlive
            // them on a slow machine; the sleeps below then expire each in turn.
            Timestamp::now()
                .checked_add(Duration::from_millis(2000))
                .unwrap(),
        )
        .await
        .unwrap();
    let id = f.session().await;
    f.store.claim_placement(&placement).await.unwrap();
    let volume = f.store.agent_volume(id, &placement).await.unwrap();
    let now = Timestamp::now();
    f.store
        .acquire_writer_lease(
            volume,
            LeaseOwnerId::from_ulid(f.nodes[0].as_ulid()),
            now,
            now.checked_add(Duration::from_millis(4000)).unwrap(),
        )
        .await
        .unwrap();
    f.step(id).await;
    let delivery = f.delivery(f.nodes[0]).await;
    sleep(Duration::from_millis(2100)).await;
    f.worker.recover_tools().await.unwrap();
    assert_eq!(
        f.store.get_by_agent(f.agent).await.unwrap(),
        Some(placement.clone())
    );
    assert!(
        !f.store
            .tool_completed(delivery.value.request_id)
            .await
            .unwrap()
    );
    sleep(Duration::from_millis(2200)).await;
    f.worker.recover_tools().await.unwrap();
    let current = f.store.get_by_agent(f.agent).await.unwrap().unwrap();
    assert_eq!(current.node_id, f.nodes[1]);
    assert_eq!(current.epoch, placement.epoch + 1);
    assert!(
        f.store
            .tool_completed(delivery.value.request_id)
            .await
            .unwrap()
    );
    let next = f.session().await;
    f.step(next).await;
    let delivery = f.delivery(current.node_id).await;
    let claim = f.claim(delivery.value.clone(), current).await;
    f.complete(&claim).await.unwrap();
    delivery.acknowledge().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
async fn expired_lease_moves_next_call_and_eviction_has_distinct_durable_notice() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let old = f
        .store
        .place(
            f.agent,
            f.nodes[0],
            // Long enough that the session and volume setup below cannot
            // outlive the lease on a slow machine; the sleep then expires it.
            Timestamp::now()
                .checked_add(Duration::from_millis(2000))
                .unwrap(),
        )
        .await
        .unwrap();
    let id = f.session().await;
    f.store.claim_placement(&old).await.unwrap();
    f.store.agent_volume(id, &old).await.unwrap();
    sleep(Duration::from_millis(2100)).await;
    f.step(id).await;
    let placement = f.store.get_by_agent(f.agent).await.unwrap().unwrap();
    assert_eq!(placement.node_id, f.nodes[1]);
    assert!(placement.epoch > old.epoch);
    let delivery = f.delivery(f.nodes[1]).await;
    let claim = f.claim(delivery.value.clone(), placement.clone()).await;
    f.complete(&claim).await.unwrap();
    delivery.acknowledge().await.unwrap();
    f.store.release(&placement).await.unwrap();
    let next = f.session().await;
    f.step(next).await;
    let placement = f.store.get_by_agent(f.agent).await.unwrap().unwrap();
    assert!(placement.epoch > claim.placement.epoch);
    let delivery = f.delivery(placement.node_id).await;
    // Repeated scans must neither lose the notice nor duplicate it.
    f.worker.recover_tools().await.unwrap();
    let events = f.store.read_events(next, 0, 64).await.unwrap();
    assert_eq!(events.iter().filter(|e| is_notice(e)).count(), 1);
    assert!(events.iter().any(|e| matches!(e, Event::MessageAppended { message, .. } if message.parts.iter().any(|part| matches!(part, Part::Text { text } if text.contains("evicted while idle") && text.contains("final checkpoint"))))));
    let claim = f.claim(delivery.value.clone(), placement).await;
    f.complete(&claim).await.unwrap();
    delivery.acknowledge().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
async fn node_lost_mid_call_fails_once_and_delayed_retry_has_no_second_notice() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let placement = f
        .store
        .place(
            f.agent,
            f.nodes[0],
            // Long enough that setup cannot outlive the lease on a slow machine.
            Timestamp::now()
                .checked_add(Duration::from_millis(2000))
                .unwrap(),
        )
        .await
        .unwrap();
    let id = f.session().await;
    let checkpoint = ManifestId::from_ulid(Ulid::from_parts(1_789_545_900_000, 1));
    f.store
        .put_manifest(
            checkpoint,
            &f.store.get_manifest(f.manifest).await.unwrap().unwrap(),
        )
        .await
        .unwrap();
    f.store
        .create_volume(
            swarmy_core::VolumeId::from_ulid(f.agent.as_ulid()),
            checkpoint,
        )
        .await
        .unwrap();
    f.store.agent_volume(id, &placement).await.unwrap();
    f.step(id).await;
    let delivery = f.delivery(f.nodes[0]).await;
    let claim = f.claim(delivery.value.clone(), placement.clone()).await;
    // Leave the first delivery unacknowledged, as a dead node would.
    sleep(Duration::from_millis(2100)).await;
    let redelivery = f.delivery(f.nodes[0]).await;
    assert!(redelivery.delivery_count().unwrap() > 1);
    let current = f
        .store
        .take_over(
            &placement,
            f.nodes[1],
            Timestamp::now()
                .checked_add(Duration::from_secs(2))
                .unwrap(),
        )
        .await
        .unwrap();
    let stale_job = PlacedToolClaim {
        placement: current.clone(),
        ..claim.clone()
    };
    assert!(matches!(
        f.store.claim_placed_tool(&stale_job).await,
        Err(StoreError::LeaseMismatch)
    ));
    f.worker.recover_tools().await.unwrap();
    assert_eq!(current.node_id, f.nodes[1]);
    assert_eq!(placement.epoch, 1);
    assert_eq!(current.epoch, 2);
    assert_eq!(current.last_change_reason, PlacementChangeReason::Failure);
    assert!(matches!(
        f.complete(&claim).await,
        Err(StoreError::LeaseMismatch)
    ));
    // Even a fresh claim at the new node cannot replay the completed old call.
    let new_claim = PlacedToolClaim {
        placement: current.clone(),
        ..claim.clone()
    };
    assert!(!f.store.claim_placed_tool(&new_claim).await.unwrap());
    f.worker.recover_tools().await.unwrap();
    assert_failure_notice(&f, id, &current, checkpoint).await;
    redelivery.acknowledge().await.unwrap();
    // No node ever claimed epoch 2. A later user retry must be able to start
    // epoch 3 without describing another computer loss.
    sleep(Duration::from_millis(2100)).await;
    f.request(id).await;
    f.step(id).await;
    let retry = f.store.get_by_agent(f.agent).await.unwrap().unwrap();
    assert_eq!(retry.epoch, 3);
    assert_eq!(retry.last_change_reason, PlacementChangeReason::Unstarted);
    assert_eq!(
        f.store.placement_failure_estimate(&retry).await.unwrap(),
        None
    );
    let delivery = f.delivery(retry.node_id).await;
    let claim = f.claim(delivery.value.clone(), retry).await;
    f.complete(&claim).await.unwrap();
    delivery.acknowledge().await.unwrap();
    f.worker.recover_tools().await.unwrap();
    let events = f.store.read_events(id, 0, 64).await.unwrap();
    assert_eq!(events.iter().filter(|event| is_notice(event)).count(), 1);
    assert!(events.iter().any(|event| matches!(event,
        Event::ToolCallCompleted { request_id, result: ToolResult::Completed { .. }, .. }
        if *request_id == claim.job.request_id)));
    f.cleanup().await;
}

async fn assert_failure_notice(
    f: &Fixture,
    id: SessionId,
    current: &PlacementRecord,
    checkpoint: ManifestId,
) {
    let events = f.store.read_events(id, 0, 64).await.unwrap();
    assert_eq!(events.iter().filter(|e| is_notice(e)).count(), 1);
    let snapshot =
        Timestamp::from_millisecond(i64::try_from(checkpoint.as_ulid().timestamp_ms()).unwrap())
            .unwrap();
    let explanation = swarmy_core::computer_rebuilt_message(
        current.last_change_reason,
        snapshot,
        current.last_changed_at,
        f.store.placement_failure_estimate(current).await.unwrap(),
    )
    .unwrap();
    let notice_seq = events.iter().find(|e| is_notice(e)).unwrap().seq();
    let failed: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            Event::ToolCallCompleted {
                seq,
                result: ToolResult::Error { error },
                ..
            } => Some((*seq, error)),
            _ => None,
        })
        .collect();
    assert_eq!(failed, vec![(notice_seq + 1, &explanation)]);
    assert_recovery_prompt(f, id, &explanation).await;
}

async fn assert_recovery_prompt(f: &Fixture, id: SessionId, explanation: &str) {
    f.step(id).await;
    let events = f.store.read_events(id, 0, 64).await.unwrap();
    let request = events
        .iter()
        .find_map(|event| match event {
            Event::InferenceRequested { request_id, .. } => Some(*request_id),
            _ => None,
        })
        .unwrap();
    let job: swarmy_llm::InferenceJob =
        f.store.get_inference_input(request).await.unwrap().unwrap();
    assert!(
        job.request
            .messages
            .iter()
            .any(|m| m.role == MessageRole::System
                && m.parts
                    == vec![Part::Text {
                        text: explanation.to_owned()
                    }])
    );
    assert!(job.request.messages.iter().any(|m| m.role == MessageRole::Tool && m.parts.iter().any(|p| matches!(p, Part::ToolResult { result: ToolResult::Error { error }, .. } if error == explanation))));
}

#[tokio::test]
async fn unclaimed_dispatch_expires_without_a_rebuild_notice_or_stuck_job() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let old = f
        .store
        .place(
            f.agent,
            f.nodes[0],
            Timestamp::now()
                .checked_add(Duration::from_secs(2))
                .unwrap(),
        )
        .await
        .unwrap();
    let id = f.session().await;
    f.step(id).await;
    let delivery = f.delivery(old.node_id).await;
    // The durable dispatch exists, but no node claimed the placement or the call.
    sleep(Duration::from_millis(2100)).await;
    f.worker.recover_tools().await.unwrap();
    f.worker.recover_tools().await.unwrap();
    let current = f.store.get_by_agent(f.agent).await.unwrap().unwrap();
    assert_eq!(current.epoch, 2);
    assert_eq!(current.last_change_reason, PlacementChangeReason::Unstarted);
    assert!(
        f.store
            .tool_completed(delivery.value.request_id)
            .await
            .unwrap()
    );
    let events = f.store.read_events(id, 0, 64).await.unwrap();
    assert!(!events.iter().any(is_notice));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event,
        Event::ToolCallCompleted { result: ToolResult::Error { error }, .. }
        if error.contains("placement expired")))
            .count(),
        1
    );
    delivery.acknowledge().await.unwrap();
    // Fold the failed call and let the worker request the retry inference.
    f.step(id).await;
    f.request(id).await;
    f.step(id).await;
    let delivery = f.delivery(current.node_id).await;
    let claim = f.claim(delivery.value.clone(), current).await;
    f.complete(&claim).await.unwrap();
    delivery.acknowledge().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
async fn cached_placement_keeps_observed_expiry_and_invalidates_on_release() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let cache = crate::placement::Cache::default();
    let duration = Duration::from_secs(30);
    let old = cache
        .resolve(&fixture.store, fixture.agent, duration)
        .await
        .unwrap();
    let renewed = fixture
        .store
        .renew(&old, old.expires_at.checked_add(duration).unwrap())
        .await
        .unwrap();
    // A cached route does not borrow a renewal it has not observed.
    assert_eq!(
        cache
            .resolve(&fixture.store, fixture.agent, duration)
            .await
            .unwrap(),
        old
    );
    fixture.store.release(&renewed).await.unwrap();
    assert!(fixture.store.validate_placement(&old).await.is_err());
    cache.invalidate(fixture.agent).await;
    let replacement = cache
        .resolve(&fixture.store, fixture.agent, duration)
        .await
        .unwrap();
    assert!(replacement.epoch > old.epoch);
    fixture.store.release(&replacement).await.unwrap();
    let short = fixture
        .store
        .place(
            fixture.agent,
            fixture.nodes[0],
            Timestamp::now()
                .checked_add(Duration::from_millis(100))
                .unwrap(),
        )
        .await
        .unwrap();
    cache.invalidate(fixture.agent).await;
    assert_eq!(
        cache
            .resolve(&fixture.store, fixture.agent, duration)
            .await
            .unwrap(),
        short
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    let after_expiry = cache
        .resolve(&fixture.store, fixture.agent, duration)
        .await
        .unwrap();
    assert!(after_expiry.epoch > short.epoch);
    fixture.cleanup().await;
}

#[tokio::test]
async fn deleted_computer_fails_pending_sandbox_call_without_replacement() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let id = f.session().await;
    f.step(id).await;
    let placement = f.store.get_by_agent(f.agent).await.unwrap().unwrap();
    let delivery = f.delivery(placement.node_id).await;
    let claim = f.claim(delivery.value.clone(), placement.clone()).await;
    f.store.delete_computer(f.agent).await.unwrap();
    assert!(matches!(
        f.complete(&claim).await,
        Err(StoreError::ComputerDeleted)
    ));
    f.worker.recover_tools().await.unwrap();
    f.worker.recover_tools().await.unwrap();
    assert!(f.store.get_by_agent(f.agent).await.unwrap().is_none());
    assert!(f.store.scan_tool_jobs(None, 64).await.unwrap().is_empty());
    let events = f.store.read_events(id, 0, 64).await.unwrap();
    let errors: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::ToolCallCompleted {
                result: ToolResult::Error { error },
                ..
            } => Some(error.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        errors,
        ["This session's computer has been deleted. Create a new session to run tools."]
    );
    assert_eq!(
        f.store.fetch_session(id).await.unwrap().unwrap().state,
        SessionState::Runnable
    );
    f.cleanup().await;
}
