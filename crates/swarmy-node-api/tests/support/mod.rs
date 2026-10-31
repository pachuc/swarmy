#![deny(clippy::disallowed_methods)]
use axum::{Router, middleware, response::IntoResponse};
use jiff::Timestamp;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use swarmy_core::{NodeCapacity, NodeId, NodeRecord, NodeRole};
use swarmy_node_protocol::NodeAdvert;
use swarmy_store::{Store, blob::MemoryBlobStore};
use ulid::Ulid;
pub(crate) struct Fixture {
    pub(crate) store: Store,
    pub(crate) clock: Arc<Mutex<Timestamp>>,
    pub(crate) _guard: swarmy_testkit::StackGuard,
}
pub(crate) async fn fixture(tag: &str) -> Option<Fixture> {
    let stack = swarmy_testkit::Stack::load(tag)?;
    let (store, guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
    let clock = Arc::new(Mutex::new(Timestamp::now()));
    let copy = clock.clone();
    Some(Fixture {
        store: store.with_clock(move || *copy.lock().unwrap()),
        clock,
        _guard: guard,
    })
}
pub(crate) async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{addr}")
}
pub(crate) fn drop_first_response(
    router: Router,
    path: &'static str,
) -> (Router, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    let copy = count.clone();
    let wrapped = router.layer(middleware::from_fn(
        move |request: axum::extract::Request, next: middleware::Next| {
            let count = copy.clone();
            async move {
                let selected = request.uri().path() == path;
                let answer = next.run(request).await;
                if selected
                    && count.fetch_add(1, Ordering::SeqCst) == 0
                    && answer.status().is_success()
                {
                    axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response()
                } else {
                    answer
                }
            }
        },
    ));
    (wrapped, count)
}
pub(crate) async fn node(store: &Store, sandboxes: u32) -> NodeRecord {
    let record = NodeRecord {
        node_id: NodeId::from_ulid(Ulid::generate()),
        roles: vec![NodeRole::Sandbox, NodeRole::Volume],
        capacity: NodeCapacity {
            cpu_millis: 64_000,
            memory_bytes: 1 << 40,
            disk_bytes: 1 << 40,
            sandboxes,
        },
        last_heartbeat: store.now(),
        cached_images: Vec::new(),
        gpu: None,
    };
    store.put_node(&record).await.unwrap();
    record
}
pub(crate) fn advert(record: &NodeRecord) -> NodeAdvert {
    NodeAdvert {
        roles: record.roles.clone(),
        capacity: record.capacity.clone(),
        gpu: record.gpu.clone(),
    }
}

/// One node with a live placement, tool claim and volume writer.
#[allow(
    dead_code,
    reason = "the shared fixture is used by the sync integration target only"
)]
pub(crate) struct Hosted {
    pub(crate) node: NodeRecord,
    pub(crate) token: swarmy_core::NodeToken,
    pub(crate) session: swarmy_core::SessionId,
    pub(crate) placement: swarmy_core::PlacementRecord,
    pub(crate) claim: swarmy_core::PlacedToolClaim,
    pub(crate) volume: swarmy_core::VolumeId,
    pub(crate) lease: swarmy_core::Lease,
}
#[allow(
    dead_code,
    reason = "the shared fixture is used by the sync integration target only"
)]
pub(crate) async fn hosted(store: &Store) -> Hosted {
    use std::time::Duration;
    use swarmy_core::{
        AgentId, BashArguments, CHUNK_SIZE, ContentHash, Event, ImageTag, LeaseOwnerId,
        ManifestHeader, ManifestId, PlacedToolClaim, RequestId, SandboxArguments, SessionKind,
        SessionRecord, SessionState, ToolCallId, ToolCallRecord, ToolJob,
    };
    let session = swarmy_core::SessionId::from_ulid(Ulid::generate());
    let agent = AgentId::from_ulid(Ulid::generate());
    let image = ManifestId::from_ulid(Ulid::generate());
    store
        .put_manifest(
            image,
            &ManifestHeader {
                size: u64::from(CHUNK_SIZE),
                chunk_size: CHUNK_SIZE,
                root_hash: ContentHash([1; 32]),
            },
        )
        .await
        .unwrap();
    store
        .put_image("base", &ImageTag("test".into()), image, None)
        .await
        .unwrap();
    store
        .create_session(
            &SessionRecord {
                interrupt_requested: false,
                session_id: session,
                agent_id: agent,
                state: SessionState::Idle,
                head_seq: 0,
                snapshot_ref: None,
                inference: swarmy_core::InferenceSelection::default(),
                route: None,
                route_step: 0,
                kind: SessionKind::Ephemeral,
                computer_deleted: false,
                plan: Vec::new(),
            },
            Timestamp::now(),
            "base:test",
        )
        .await
        .unwrap();
    let node = node(store, 1).await;
    let now = Timestamp::now();
    let placement = store
        .place(
            agent,
            node.node_id,
            now.checked_add(Duration::from_secs(30)).unwrap(),
        )
        .await
        .unwrap();
    store.wake_session(session, now).await.unwrap();
    let session_lease = store
        .claim_lease(
            session,
            LeaseOwnerId::from_ulid(Ulid::generate()),
            now.checked_add(Duration::from_secs(30)).unwrap(),
        )
        .await
        .unwrap();
    let job = ToolJob {
        session_id: session,
        step: 1,
        request_id: RequestId::for_step(session, 1),
        call_id: ToolCallId("bash-1".into()),
        arguments: SandboxArguments::Bash(BashArguments {
            command: "echo test".into(),
            timeout_ms: 1000,
            yield_seconds: 10,
            output_budget_bytes: 32768,
        }),
    };
    let event = Event::ToolCallRequested {
        seq: 0,
        request_id: job.request_id,
        call: ToolCallRecord {
            call_id: job.call_id.clone(),
            tool: "bash".into(),
            arguments: job.arguments.parameters(),
            result: None,
        },
    };
    store
        .append_events_leased(session, 0, &[event], &session_lease, now)
        .await
        .unwrap();
    store
        .dispatch_placed_tool_jobs(session, &session_lease, &[job.clone()], &placement)
        .await
        .unwrap();
    store.claim_placement(&placement).await.unwrap();
    let claim = PlacedToolClaim {
        job,
        owner: LeaseOwnerId::from_ulid(Ulid::generate()),
        placement: placement.clone(),
        expires_at: now.checked_add(Duration::from_secs(30)).unwrap(),
    };
    assert!(store.claim_placed_tool(&claim).await.unwrap());
    let volume = store.agent_volume(session, &placement).await.unwrap();
    let lease = store
        .acquire_writer_lease(
            volume,
            LeaseOwnerId::from(node.node_id),
            now.checked_add(Duration::from_secs(60)).unwrap(),
        )
        .await
        .unwrap();
    let token = store.issue_node_credential(node.node_id).await.unwrap();
    Hosted {
        node,
        token,
        session,
        placement,
        claim,
        volume,
        lease,
    }
}
#[allow(
    dead_code,
    reason = "the shared fixture is used by the sync integration target only"
)]
pub(crate) fn request(hosted: &Hosted) -> swarmy_node_protocol::SyncRequest {
    use swarmy_node_protocol::{HostedPlacement, SyncRequest, WriterToken};
    SyncRequest {
        node: advert(&hosted.node),
        placements: vec![HostedPlacement {
            placement: hosted.placement.clone(),
            holder_session_id: Some(hosted.session),
            queued_calls: 2,
        }],
        claims: vec![hosted.claim.clone()],
        writers: vec![WriterToken {
            volume: hosted.volume,
            lease: hosted.lease.clone(),
        }],
        scratch: Vec::new(),
        scratch_evicted: Vec::new(),
        metrics: Vec::new(),
    }
}
