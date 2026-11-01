#![deny(clippy::disallowed_methods)]
use super::*;
use std::{num::NonZeroUsize, time::Duration};
use swarmy_bus::{Config, SubjectToken};
use swarmy_core::ToolMetric;
use swarmy_node_api::{
    NodeApiPolicy, NodeApiState, router,
    test_support::{TestApi, dispatched},
};
use swarmy_node_protocol::{NodeAdvert, RegisterRequest};
use swarmy_store::{Store, blob::MemoryBlobStore};

fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(20)
}
fn bash_result() -> ToolResult {
    ToolResult::Completed {
        title: "bash".into(),
        output: "{\"exit_code\":0}".into(),
        metadata: Default::default(),
    }
}
async fn setup(
    tag: &str,
) -> Option<(
    Store,
    swarmy_testkit::StackGuard,
    swarmy_node_api::test_support::Dispatched,
    NodeClient,
    Keeper,
    TestApi,
)> {
    let stack = swarmy_testkit::Stack::load(tag)?;
    let (store, guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
    let d = dispatched(&store).await;
    let bus = Bus::connect(
        &stack.nats_url,
        Config {
            prefix: Some(SubjectToken::new(&stack.prefix).unwrap()),
            ..Config::default()
        },
    )
    .await
    .unwrap();
    let api = TestApi::start(router(NodeApiState::new(
        store.clone(),
        bus,
        NodeApiPolicy {
            placement_lease: Duration::from_secs(60),
            snapshot_retention: NonZeroUsize::new(10).unwrap(),
            resend_interval: Duration::from_secs(5),
        },
    )))
    .await;
    let client = NodeClient::new(api.url(), &d.token).unwrap();
    let advert = NodeAdvert {
        roles: d.node.roles.clone(),
        capacity: d.node.capacity.clone(),
        gpu: None,
    };
    let registered = client
        .register(&RegisterRequest {
            node: advert.clone(),
        })
        .await
        .unwrap();
    let keeper = Keeper::new(
        Arc::new(client.clone()),
        advert,
        registered.leases,
        Duration::from_secs(5),
    );
    Some((store, guard, d, client, keeper, api))
}
#[test]
fn claim_refusals_keep_todays_texts() {
    let node = NodeId::from_ulid(ulid::Ulid::generate());
    let elsewhere = claim_refusal(NodeRefusal::PlacedElsewhere { node, epoch: 4 }).to_string();
    assert!(elsewhere.contains("agent is placed on another node"));
    assert!(elsewhere.contains("at epoch 4"));
    assert_eq!(
        claim_refusal(NodeRefusal::Fenced(FenceKind::PlacedToolClaim)).to_string(),
        "tool call already claimed"
    );
    assert_eq!(
        claim_refusal(NodeRefusal::Unavailable(
            "display tool requires a display image".into()
        ))
        .to_string(),
        "display tool requires a display image"
    );
    assert_eq!(
        claim_refusal(NodeRefusal::Fenced(FenceKind::DispatchPlacementStale {
            detail: "x".into()
        }))
        .to_string(),
        "x"
    );
}
#[test]
fn node_metric_leaves_server_fields_empty() {
    let session = swarmy_core::SessionId::from_ulid(ulid::Ulid::generate());
    let agent = swarmy_core::AgentId::from_ulid(ulid::Ulid::generate());
    let now = jiff::Timestamp::now();
    let placement = PlacementRecord {
        agent_id: agent,
        node_id: NodeId::from_ulid(ulid::Ulid::generate()),
        epoch: 1,
        expires_at: now,
        last_changed_at: now,
        last_change_reason: swarmy_core::PlacementChangeReason::Initial,
        gpu: swarmy_core::GpuRequirement::None,
    };
    let job = ToolJob {
        session_id: session,
        request_id: swarmy_core::RequestId::for_step(session, 1),
        call_id: swarmy_core::ToolCallId("call".into()),
        step: 1,
        arguments: SandboxArguments::Bash(swarmy_core::BashArguments {
            command: "true".into(),
            timeout_ms: 1000,
            yield_seconds: 10,
            output_budget_bytes: 1024,
        }),
    };
    let claim = PlacedToolClaim {
        job,
        placement,
        owner: LeaseOwnerId::from_ulid(ulid::Ulid::generate()),
        expires_at: now,
    };
    let result = ToolResult::Completed {
        title: "bash".into(),
        output: "abc".into(),
        metadata: [("exit_code".into(), serde_json::json!(3))].into(),
    };
    let metrics = CallMetrics {
        turn: None,
        needs_computer_sample: false,
        delivered_ns: Some(1),
        claimed_ns: Some(2),
    };
    let timing = ToolTiming {
        exit_status: None,
        started_ns: Some(5),
        process_wall_ms: Some(1.5),
    };
    let tool = node_tool_metric(&claim, &metrics, &result, &timing);
    assert_eq!(tool.request_id, claim.job.request_id.to_string());
    assert_eq!(tool.name, "bash");
    assert_eq!(tool.delivered_ns, Some(1));
    assert_eq!(tool.claimed_ns, Some(2));
    assert_eq!(tool.started_ns, Some(5));
    assert_eq!(tool.exit_status, Some(3));
    assert_eq!(tool.output_bytes, Some(3));
    assert_eq!(tool.process_wall_ms, Some(1.5));
    assert_eq!(
        (
            tool.dispatched_ns,
            tool.completed_ns,
            tool.woken_ns,
            tool.queue_ms
        ),
        (None, None, None, None)
    );
}
#[tokio::test]
async fn claim_then_complete_once() {
    let Some((store, mut guard, d, client, keeper, _api)) = setup("node-tool-once").await else {
        return;
    };
    let c = claim(
        &client,
        &keeper,
        &d.placement,
        d.jobs[0].clone(),
        deadline(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(c.turn, Some(d.turn));
    assert_eq!(
        complete(
            &client,
            &c.claim,
            &c.hold,
            bash_result(),
            ToolMetric::default()
        )
        .await
        .unwrap(),
        Completion::Committed {
            interrupt_requested: false
        }
    );
    assert!(store.tool_completed(d.jobs[0].request_id).await.unwrap());
    assert_eq!(
        complete(
            &client,
            &c.claim,
            &c.hold,
            bash_result(),
            ToolMetric::default()
        )
        .await
        .unwrap(),
        Completion::AlreadyDone
    );
    assert!(
        claim(
            &client,
            &keeper,
            &d.placement,
            d.jobs[0].clone(),
            deadline()
        )
        .await
        .unwrap()
        .is_none()
    );
    guard.cleanup().await;
}
#[tokio::test]
async fn completion_answers_the_interrupt() {
    let Some((store, mut guard, d, client, keeper, _api)) = setup("node-tool-interrupt").await
    else {
        return;
    };
    let c = claim(
        &client,
        &keeper,
        &d.placement,
        d.jobs[0].clone(),
        deadline(),
    )
    .await
    .unwrap()
    .unwrap();
    store.interrupt_session(d.session).await.unwrap();
    assert_eq!(
        complete(
            &client,
            &c.claim,
            &c.hold,
            bash_result(),
            ToolMetric::default()
        )
        .await
        .unwrap(),
        Completion::Committed {
            interrupt_requested: true
        }
    );
    guard.cleanup().await;
}
#[tokio::test]
async fn claim_refused_for_a_stale_placement_is_an_error() {
    let Some((store, mut guard, d, client, keeper, _api)) = setup("node-tool-stale").await else {
        return;
    };
    let mut stale = d.placement.clone();
    stale.epoch += 1;
    assert!(
        claim(&client, &keeper, &stale, d.jobs[0].clone(), deadline())
            .await
            .is_err()
    );
    assert!(!store.tool_completed(d.jobs[0].request_id).await.unwrap());
    assert!(
        claim(
            &client,
            &keeper,
            &d.placement,
            d.jobs[0].clone(),
            deadline()
        )
        .await
        .unwrap()
        .is_some()
    );
    guard.cleanup().await;
}
#[tokio::test]
async fn claimed_call_survives_a_keeper_renewal() {
    let Some((_store, mut guard, d, client, keeper, _api)) = setup("node-tool-renew").await else {
        return;
    };
    let c = claim(
        &client,
        &keeper,
        &d.placement,
        d.jobs[0].clone(),
        deadline(),
    )
    .await
    .unwrap()
    .unwrap();
    keeper.sync_once().await.unwrap();
    keeper.sync_once().await.unwrap();
    assert_eq!(
        complete(
            &client,
            &c.claim,
            &c.hold,
            bash_result(),
            ToolMetric::default()
        )
        .await
        .unwrap(),
        Completion::Committed {
            interrupt_requested: false
        }
    );
    guard.cleanup().await;
}
