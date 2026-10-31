#![deny(clippy::disallowed_methods)]
use axum::{Router, middleware, response::IntoResponse};
use jiff::Timestamp;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use swarmy_bus::{Bus, Config, SubjectToken};
use swarmy_core::{NodeCapacity, NodeId, NodeRecord, NodeRole};
use swarmy_node_protocol::NodeAdvert;
use swarmy_store::{Store, blob::MemoryBlobStore};
use ulid::Ulid;
pub(crate) struct Fixture {
    pub(crate) store: Store,
    pub(crate) bus: Bus,
    #[allow(
        dead_code,
        reason = "only clock-driven integration targets advance the fixture clock"
    )]
    pub(crate) clock: Arc<Mutex<Timestamp>>,
    pub(crate) _guard: swarmy_testkit::StackGuard,
}
pub(crate) async fn fixture(tag: &str) -> Option<Fixture> {
    let stack = swarmy_testkit::Stack::load(tag)?;
    let (store, guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
    let clock = Arc::new(Mutex::new(Timestamp::now()));
    let copy = clock.clone();
    let bus = Bus::connect(
        &stack.nats_url,
        Config {
            prefix: Some(SubjectToken::new(&stack.prefix).unwrap()),
            ..Config::default()
        },
    )
    .await
    .unwrap();
    bus.setup(&[]).await.unwrap();
    Some(Fixture {
        bus,
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
#[allow(
    dead_code,
    reason = "only retry integration targets use the response-drop helper"
)]
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

#[allow(
    dead_code,
    reason = "the shared fixture is used by the sync integration target only"
)]
async fn hosted_session(store: &Store) -> (swarmy_core::SessionId, swarmy_core::AgentId) {
    use swarmy_core::{
        AgentId, CHUNK_SIZE, ContentHash, ImageTag, ManifestHeader, ManifestId, SessionKind,
        SessionRecord, SessionState,
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
    (session, agent)
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
/// One node with two dispatched jobs and a claimed placement, before volume binding.
#[allow(dead_code, reason = "the fixture is shared across integration targets")]
pub(crate) struct Dispatched {
    pub(crate) node: NodeRecord,
    pub(crate) token: swarmy_core::NodeToken,
    pub(crate) session: swarmy_core::SessionId,
    pub(crate) agent: swarmy_core::AgentId,
    pub(crate) turn: swarmy_core::MessageId,
    pub(crate) jobs: Vec<swarmy_core::ToolJob>,
    pub(crate) placement: swarmy_core::PlacementRecord,
}
#[allow(dead_code, reason = "the fixture is shared across integration targets")]
pub(crate) async fn dispatched(store: &Store) -> Dispatched {
    use std::time::Duration;
    use swarmy_core::{
        BashArguments, Event, LeaseOwnerId, Message, MessageId, MessageRole, Part, RequestId,
        SandboxArguments, ToolCallId, ToolCallRecord, ToolJob,
    };
    let (session, agent) = hosted_session(store).await;
    let node = node(store, 1).await;
    let now = store.now();
    let placement = store
        .place(
            agent,
            node.node_id,
            now.checked_add(Duration::from_secs(30)).unwrap(),
        )
        .await
        .unwrap();
    let turn = MessageId::from_ulid(Ulid::generate());
    store
        .append_user_message(
            session,
            0,
            &Message {
                id: turn,
                role: MessageRole::User,
                parts: vec![Part::Text { text: "run".into() }],
            },
        )
        .await
        .unwrap();
    let session_lease = store
        .claim_lease(
            session,
            LeaseOwnerId::from_ulid(Ulid::generate()),
            now.checked_add(Duration::from_secs(30)).unwrap(),
        )
        .await
        .unwrap();
    let jobs: Vec<ToolJob> = (2..=3)
        .map(|step| ToolJob {
            session_id: session,
            step,
            request_id: RequestId::for_step(session, step),
            call_id: ToolCallId(format!("bash-{step}")),
            arguments: SandboxArguments::Bash(BashArguments {
                command: "echo test".into(),
                timeout_ms: 1000,
                yield_seconds: 10,
                output_budget_bytes: 32768,
            }),
        })
        .collect();
    let events: Vec<Event> = jobs
        .iter()
        .map(|job| Event::ToolCallRequested {
            seq: 0,
            request_id: job.request_id,
            call: ToolCallRecord {
                call_id: job.call_id.clone(),
                tool: "bash".into(),
                arguments: job.arguments.parameters(),
                result: None,
            },
        })
        .collect();
    store
        .append_events_leased(session, 1, &events, &session_lease, now)
        .await
        .unwrap();
    store
        .dispatch_placed_tool_jobs(session, &session_lease, &jobs, &placement)
        .await
        .unwrap();
    store.claim_placement(&placement).await.unwrap();
    let token = store.issue_node_credential(node.node_id).await.unwrap();
    Dispatched {
        node,
        token,
        session,
        agent,
        turn,
        jobs,
        placement,
    }
}
#[allow(dead_code, reason = "the fixture is shared across integration targets")]
pub(crate) async fn hosted(store: &Store) -> Hosted {
    use std::time::Duration;
    use swarmy_core::{LeaseOwnerId, PlacedToolClaim};
    let dispatched = dispatched(store).await;
    let now = store.now();
    let claim = PlacedToolClaim {
        job: dispatched.jobs[0].clone(),
        owner: LeaseOwnerId::from_ulid(Ulid::generate()),
        placement: dispatched.placement.clone(),
        expires_at: now.checked_add(Duration::from_secs(30)).unwrap(),
    };
    assert!(store.claim_placed_tool(&claim).await.unwrap());
    let volume = store
        .agent_volume(dispatched.session, &dispatched.placement)
        .await
        .unwrap();
    let lease = store
        .acquire_writer_lease(
            volume,
            LeaseOwnerId::from(dispatched.node.node_id),
            now.checked_add(Duration::from_secs(60)).unwrap(),
        )
        .await
        .unwrap();
    Hosted {
        node: dispatched.node,
        token: dispatched.token,
        session: dispatched.session,
        placement: dispatched.placement,
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

#[allow(
    dead_code,
    reason = "shared helper is used by the hosting integration targets"
)]
pub(crate) fn policy() -> swarmy_node_api::NodeApiPolicy {
    swarmy_node_api::NodeApiPolicy {
        placement_lease: std::time::Duration::from_secs(30),
        snapshot_retention: std::num::NonZeroUsize::new(3).unwrap(),
        resend_interval: std::time::Duration::from_secs(5),
    }
}
#[allow(
    dead_code,
    reason = "shared helper is used by different integration targets"
)]
pub(crate) fn state(f: &Fixture) -> swarmy_node_api::NodeApiState {
    swarmy_node_api::NodeApiState::new(f.store.clone(), f.bus.clone(), policy())
}
#[allow(
    dead_code,
    reason = "shared helper is used by the hosting integration targets"
)]
pub(crate) async fn served(f: &Fixture) -> String {
    serve(swarmy_node_api::router(state(f))).await
}
#[allow(
    dead_code,
    reason = "shared helper is used by the hosting integration targets"
)]
pub(crate) fn client(
    base: &str,
    token: &swarmy_core::NodeToken,
) -> swarmy_node_protocol::NodeClient {
    swarmy_node_protocol::NodeClient::new(base, token).unwrap()
}

#[allow(
    dead_code,
    reason = "shared tool fixture is used by tool integration targets"
)]
pub(crate) fn claim_request(
    job: &swarmy_core::ToolJob,
    placement: &swarmy_core::PlacementRecord,
) -> swarmy_node_protocol::ToolClaimRequest {
    swarmy_node_protocol::ToolClaimRequest {
        job: job.clone(),
        placement: placement.clone(),
        owner: swarmy_core::LeaseOwnerId::from_ulid(Ulid::generate()),
    }
}
#[allow(
    dead_code,
    reason = "shared tool fixture is used by tool integration targets"
)]
pub(crate) fn bash_result() -> swarmy_core::ToolResult {
    swarmy_core::ToolResult::Completed {
        title: "bash".into(),
        output: "{\"exit_code\":0}".into(),
        metadata: Default::default(),
    }
}
