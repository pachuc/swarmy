#![deny(clippy::disallowed_methods)]
mod support;
use std::{sync::atomic::Ordering, time::Duration};
use swarmy_bus::{Bus, LiveFeed, WorkQueue};
use swarmy_core::{
    Nudge, SessionId, ToolMetric, ToolResult, TurnEvent, TurnStage, runnable_partition,
};
use swarmy_node_api::router;
use swarmy_node_protocol::{
    ClientError, FenceKind, HostingOpenRequest, NodeRefusal, TOOL_CLAIM_PATH, TOOL_COMPLETE_PATH,
    ToolCompleteRequest,
};
use ulid::Ulid;
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(20)
}
async fn setup(
    tag: &str,
) -> Option<(
    support::Fixture,
    support::Dispatched,
    swarmy_node_protocol::NodeClient,
    swarmy_core::PlacementRecord,
)> {
    let f = support::fixture(tag).await?;
    let a = support::dispatched(&f.store).await;
    let client = support::client(&support::served(&f).await, &a.token);
    let opened = client
        .open_hosting(
            &HostingOpenRequest {
                agent: a.agent,
                job: a.jobs[0].clone(),
                previous_epoch: None,
            },
            deadline(),
        )
        .await
        .unwrap();
    Some((f, a, client, opened.placement))
}
fn complete(claim: swarmy_core::PlacedToolClaim) -> ToolCompleteRequest {
    ToolCompleteRequest {
        claim,
        result: support::bash_result(),
        tool: ToolMetric {
            started_ns: Some(1),
            ..ToolMetric::default()
        },
    }
}
#[tokio::test]
async fn claim_and_complete_on_the_server_clock() {
    let Some((f, a, client, placement)) = setup("na5b-clock").await else {
        return;
    };
    let now = f.store.now();
    let answer = client
        .claim_tool(&support::claim_request(&a.jobs[0], &placement), deadline())
        .await
        .unwrap();
    assert_eq!(answer.server_now, now);
    assert_eq!(
        answer.claim.expires_at,
        now.checked_add(Duration::from_secs(30)).unwrap()
    );
    assert_eq!(answer.turn, Some(a.turn));
    let result = client
        .complete_tool(&complete(answer.claim), deadline())
        .await
        .unwrap();
    assert!(!result.interrupt_requested);
    assert!(f.store.tool_completed(a.jobs[0].request_id).await.unwrap());
}
#[tokio::test]
async fn claim_repeats_after_a_dropped_response() {
    let Some((f, a, _, placement)) = setup("na5b-repeat").await else {
        return;
    };
    let (wrapped, counter) =
        support::drop_first_response(router(support::state(&f)), TOOL_CLAIM_PATH);
    let client = support::client(&support::serve(wrapped).await, &a.token);
    let answer = client
        .claim_tool(&support::claim_request(&a.jobs[0], &placement), deadline())
        .await
        .unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 2);
    assert_eq!(
        f.store
            .renew_placed_tool(&answer.claim, answer.claim.expires_at)
            .await
            .unwrap(),
        answer.claim.expires_at
    );
}
#[tokio::test]
async fn claim_repeat_later_raises_the_stored_expiry() {
    let Some((f, a, client, placement)) = setup("na5b-later").await else {
        return;
    };
    let request = support::claim_request(&a.jobs[0], &placement);
    let first = client.claim_tool(&request, deadline()).await.unwrap();
    *f.clock.lock().unwrap() = first
        .server_now
        .checked_add(Duration::from_secs(5))
        .unwrap();
    let later = client.claim_tool(&request, deadline()).await.unwrap();
    assert_eq!(
        later.claim.expires_at,
        first
            .server_now
            .checked_add(Duration::from_secs(35))
            .unwrap()
    );
    assert_eq!(
        f.store
            .renew_placed_tool(&later.claim, later.claim.expires_at)
            .await
            .unwrap(),
        later.claim.expires_at
    );
}
#[tokio::test]
async fn claim_refusals() {
    let Some((_f, a, client, placement)) = setup("na5b-refusals").await else {
        return;
    };
    let request = support::claim_request(&a.jobs[0], &placement);
    let answer = client.claim_tool(&request, deadline()).await.unwrap();
    let other = support::claim_request(&a.jobs[0], &placement);
    assert!(matches!(
        client.claim_tool(&other, deadline()).await,
        Err(ClientError::Refused(NodeRefusal::Fenced(
            FenceKind::PlacedToolClaim
        )))
    ));
    client
        .complete_tool(&complete(answer.claim), deadline())
        .await
        .unwrap();
    assert!(matches!(
        client.claim_tool(&other, deadline()).await,
        Err(ClientError::Refused(NodeRefusal::AlreadyDone))
    ));
    let mut wrong = support::claim_request(&a.jobs[1], &placement);
    wrong.placement.epoch += 1;
    assert!(matches!(
        client.claim_tool(&wrong, deadline()).await,
        Err(ClientError::Refused(NodeRefusal::Fenced(_)))
    ));
}
#[tokio::test]
async fn completion_returns_the_interrupt_flag() {
    let Some((f, a, client, placement)) = setup("na5b-interrupt").await else {
        return;
    };
    let claim = client
        .claim_tool(&support::claim_request(&a.jobs[0], &placement), deadline())
        .await
        .unwrap()
        .claim;
    assert!(matches!(
        f.store.interrupt_session(a.session).await.unwrap(),
        swarmy_store::InterruptResult::Requested
    ));
    assert!(
        client
            .complete_tool(&complete(claim), deadline())
            .await
            .unwrap()
            .interrupt_requested
    );
}
#[tokio::test]
async fn completion_takes_screenshots_only_as_blob_keys() {
    let Some((_f, a, client, placement)) = setup("na5b-blob").await else {
        return;
    };
    let claim = client
        .claim_tool(&support::claim_request(&a.jobs[0], &placement), deadline())
        .await
        .unwrap()
        .claim;
    let mut request = complete(claim);
    if let ToolResult::Completed { metadata, .. } = &mut request.result {
        metadata.insert("image_base64".into(), serde_json::json!("abc"));
    }
    assert!(matches!(
        client.complete_tool(&request, deadline()).await,
        Err(ClientError::Rejected(_))
    ));
    if let ToolResult::Completed { metadata, .. } = &mut request.result {
        metadata.remove("image_base64");
        metadata.insert(
            "image_object_key".into(),
            serde_json::json!("blobs/not-a-hash"),
        );
    }
    assert!(matches!(
        client.complete_tool(&request, deadline()).await,
        Err(ClientError::Rejected(_))
    ));
    if let ToolResult::Completed { metadata, .. } = &mut request.result {
        metadata.insert(
            "image_object_key".into(),
            serde_json::json!(swarmy_core::blob_key(b"png bytes")),
        );
    }
    client.complete_tool(&request, deadline()).await.unwrap();
}

#[tokio::test]
async fn completion_publishes_and_nudges_exactly_once() {
    let Some(f) = support::fixture("na5b-once").await else {
        return;
    };
    let a = support::dispatched(&f.store).await;
    let state = swarmy_node_api::NodeApiState::new(
        f.store.clone(),
        f.bus.clone(),
        swarmy_node_api::NodeApiPolicy {
            resend_interval: Duration::ZERO,
            ..support::policy()
        },
    );
    let plain = support::client(&support::serve(router(state.clone())).await, &a.token);
    let mut timeline = f
        .bus
        .subscribe_live::<TurnEvent>(LiveFeed::TurnTimeline(a.session))
        .await
        .unwrap();
    let partition = runnable_partition(a.session);
    let queue = WorkQueue::Runnable(partition);
    f.bus.setup(std::slice::from_ref(&queue)).await.unwrap();
    let mut nudges = f.bus.consume::<Nudge>(&queue).await.unwrap();
    let placement = plain
        .open_hosting(
            &HostingOpenRequest {
                agent: a.agent,
                job: a.jobs[0].clone(),
                previous_epoch: None,
            },
            deadline(),
        )
        .await
        .unwrap()
        .placement;
    let first = plain
        .claim_tool(&support::claim_request(&a.jobs[0], &placement), deadline())
        .await
        .unwrap()
        .claim;
    let second = plain
        .claim_tool(&support::claim_request(&a.jobs[1], &placement), deadline())
        .await
        .unwrap()
        .claim;
    let (wrapped, counter) = support::drop_first_response(router(state), TOOL_COMPLETE_PATH);
    let retry = support::client(&support::serve(wrapped).await, &a.token);
    assert!(matches!(
        retry.complete_tool(&complete(first), deadline()).await,
        Err(ClientError::Refused(NodeRefusal::AlreadyDone))
    ));
    assert_eq!(counter.load(Ordering::SeqCst), 2);
    plain
        .complete_tool(&complete(second.clone()), deadline())
        .await
        .unwrap();
    assert!(matches!(
        plain.complete_tool(&complete(second), deadline()).await,
        Err(ClientError::Refused(NodeRefusal::AlreadyDone))
    ));
    let other = loop {
        let id = SessionId::from_ulid(Ulid::generate());
        if id != a.session && runnable_partition(id) == partition {
            break id;
        }
    };
    f.bus
        .record_turn(&Bus::turn_event(a.session, a.turn, TurnStage::Idle, None))
        .await;
    f.bus
        .publish_work(&queue, &Nudge { session_id: other })
        .await
        .unwrap();
    verify_completion_observations(&f, &a, &mut timeline, &mut nudges, other).await;
}

async fn verify_completion_observations(
    f: &support::Fixture,
    a: &support::Dispatched,
    timeline: &mut swarmy_bus::LiveMessages<TurnEvent>,
    nudges: &mut swarmy_bus::WorkMessages<Nudge>,
    other: SessionId,
) {
    let mut completed = [0; 2];
    let mut nudge_count = 0;
    loop {
        let event = tokio::time::timeout(Duration::from_secs(10), timeline.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if event.stage == TurnStage::Idle {
            break;
        }
        if event.stage == TurnStage::ToolCompleted {
            if event.request_id == Some(a.jobs[0].request_id) {
                completed[0] += 1;
            }
            if event.request_id == Some(a.jobs[1].request_id) {
                completed[1] += 1;
            }
        }
        if event.stage == TurnStage::Nudged {
            nudge_count += 1;
        }
    }
    assert_eq!(completed, [1, 1]);
    assert_eq!(nudge_count, 1);
    for expected in [a.session, other] {
        let delivery = tokio::time::timeout(Duration::from_secs(10), nudges.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(delivery.value.session_id, expected);
        delivery.acknowledge().await.unwrap();
    }
    f.store.flush_turn_metrics().await.unwrap();
    let turns = f
        .store
        .list_turn_metrics_paged(a.session, None, 10, None, None)
        .await
        .unwrap();
    let tools = &turns
        .iter()
        .find(|t| t.turn_id == a.turn.to_string())
        .unwrap()
        .tools;
    assert_eq!(tools.len(), 2);
    for job in &a.jobs {
        assert!(
            tools
                .iter()
                .any(|t| t.request_id == job.request_id.to_string())
        );
    }
}
