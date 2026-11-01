#![deny(clippy::disallowed_methods)]
use axum::Router;
use jiff::Timestamp;
use std::sync::{Arc, Mutex};
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
    swarmy_node_api::test_support::TestApi::start(router)
        .await
        .detach()
}
#[allow(
    dead_code,
    reason = "only retry integration targets use the response-drop helper"
)]
#[allow(
    unused_imports,
    reason = "only retry integration targets use this helper"
)]
pub(crate) use swarmy_node_api::test_support::drop_first_response;
pub(crate) use swarmy_node_api::test_support::node;
pub(crate) fn advert(record: &NodeRecord) -> NodeAdvert {
    NodeAdvert {
        roles: record.roles.clone(),
        capacity: record.capacity.clone(),
        gpu: record.gpu.clone(),
    }
}

pub(crate) use swarmy_node_api::test_support::hosted_session;
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
pub(crate) use swarmy_node_api::test_support::{Dispatched, dispatched};
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
        metadata: std::collections::BTreeMap::default(),
    }
}
