#![deny(clippy::disallowed_methods)]
mod support;
use std::{sync::atomic::Ordering, time::Duration};
use swarmy_core::{AgentId, Lease, LeaseOwnerId, MessageId, ToolMetric};
use swarmy_node_api::{NodeApiState, router};
use swarmy_node_protocol::{
    EvictReason, MetricBatch, NodeClient, NodeMetric, NodeRefusal, Renewal, SYNC_PATH,
    ScratchEntry, ScratchVerdict, WriterToken,
};
use ulid::Ulid;

async fn client(f: &support::Fixture, token: &swarmy_core::NodeToken) -> NodeClient {
    let base = support::serve(router(NodeApiState::new(
        f.store.clone(),
        Duration::from_secs(30),
    )))
    .await;
    NodeClient::new(&base, token).unwrap()
}
#[tokio::test]
async fn sync_renews_every_token_from_the_server_clock() {
    let Some(f) = support::fixture("sync-renew").await else {
        return;
    };
    let a = support::hosted(&f.store).await;
    let now = f.store.now().checked_add(Duration::from_secs(5)).unwrap();
    *f.clock.lock().unwrap() = now;
    let mut request = support::request(&a);
    request.placements[0].placement.expires_at = jiff::Timestamp::UNIX_EPOCH;
    request.claims[0].expires_at = jiff::Timestamp::UNIX_EPOCH;
    request.claims[0].placement.expires_at = jiff::Timestamp::UNIX_EPOCH;
    request.writers[0].lease.expires_at = jiff::Timestamp::UNIX_EPOCH;
    let response = client(&f, &a.token)
        .await
        .sync(
            &request,
            tokio::time::Instant::now() + Duration::from_secs(20),
        )
        .await
        .unwrap();
    assert_eq!(response.server_now, now);
    assert!(response.heartbeat_recorded);
    let Renewal::Renewed(placement) = &response.placements[0] else {
        panic!("placement not renewed: {:?}", response.placements);
    };
    assert_eq!(
        placement.placement.expires_at,
        now.checked_add(Duration::from_secs(30)).unwrap()
    );
    assert!(!placement.release_requested);
    assert_eq!(
        response.claims,
        vec![Renewal::Renewed(
            now.checked_add(Duration::from_secs(30)).unwrap()
        )]
    );
    let Renewal::Renewed(lease) = &response.writers[0] else {
        panic!("writer not renewed: {:?}", response.writers);
    };
    assert!(lease.same_grant(&a.lease));
    assert_eq!(
        lease.expires_at,
        now.checked_add(Duration::from_secs(60)).unwrap()
    );
    assert_eq!(
        f.store
            .get_node(a.node.node_id)
            .await
            .unwrap()
            .unwrap()
            .last_heartbeat,
        now
    );
    let status = f
        .store
        .agent_call_status(a.placement.agent_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.node_id, a.node.node_id);
    assert_eq!(status.epoch, a.placement.epoch);
    assert_eq!(status.holder_session_id, Some(a.session));
    assert_eq!(status.queued_calls, 2);
}
#[tokio::test]
async fn sync_reports_a_requested_release() {
    let Some(f) = support::fixture("sync-release").await else {
        return;
    };
    let a = support::hosted(&f.store).await;
    f.store
        .request_placement_release(a.placement.agent_id)
        .await
        .unwrap();
    let response = client(&f, &a.token)
        .await
        .sync(
            &support::request(&a),
            tokio::time::Instant::now() + Duration::from_secs(20),
        )
        .await
        .unwrap();
    assert!(matches!(&response.placements[0], Renewal::Renewed(p) if p.release_requested));
}
#[tokio::test]
async fn tokens_expired_by_the_server_clock_are_lost() {
    let Some(f) = support::fixture("sync-expired").await else {
        return;
    };
    let a = support::hosted(&f.store).await;
    let before = f.store.get_by_agent(a.placement.agent_id).await.unwrap();
    let volume = f.store.get_volume(a.volume).await.unwrap();
    let now = f.store.now().checked_add(Duration::from_secs(120)).unwrap();
    *f.clock.lock().unwrap() = now;
    let mut request = support::request(&a);
    let future = now.checked_add(Duration::from_secs(3600)).unwrap();
    request.placements[0].placement.expires_at = future;
    request.claims[0].expires_at = future;
    request.claims[0].placement.expires_at = future;
    request.writers[0].lease.expires_at = future;
    let response = client(&f, &a.token)
        .await
        .sync(
            &request,
            tokio::time::Instant::now() + Duration::from_secs(20),
        )
        .await
        .unwrap();
    assert!(matches!(
        response.placements[0],
        Renewal::Lost(NodeRefusal::Fenced(_))
    ));
    assert!(matches!(
        response.claims[0],
        Renewal::Lost(NodeRefusal::Fenced(_))
    ));
    assert!(matches!(
        response.writers[0],
        Renewal::Lost(NodeRefusal::Fenced(_))
    ));
    assert_eq!(
        f.store.get_by_agent(a.placement.agent_id).await.unwrap(),
        before
    );
    assert_eq!(f.store.get_volume(a.volume).await.unwrap(), volume);
}
#[tokio::test]
async fn a_node_cannot_renew_another_nodes_tokens() {
    let Some(f) = support::fixture("sync-owner").await else {
        return;
    };
    let a = support::hosted(&f.store).await;
    let b = support::hosted(&f.store).await;
    let before = f.store.get_by_agent(b.placement.agent_id).await.unwrap();
    let volume = f.store.get_volume(b.volume).await.unwrap();
    let mut request = support::request(&b);
    request.node = support::advert(&a.node);
    request.writers.push(WriterToken {
        volume: b.volume,
        lease: Lease {
            owner: LeaseOwnerId::from(a.node.node_id),
            seq: b.lease.seq,
            expires_at: b.lease.expires_at,
        },
    });
    let response = client(&f, &a.token)
        .await
        .sync(
            &request,
            tokio::time::Instant::now() + Duration::from_secs(20),
        )
        .await
        .unwrap();
    assert_eq!(response.placements[0], Renewal::Lost(NodeRefusal::NotYours));
    assert_eq!(response.claims[0], Renewal::Lost(NodeRefusal::NotYours));
    assert_eq!(response.writers[0], Renewal::Lost(NodeRefusal::NotYours));
    assert!(matches!(
        response.writers[1],
        Renewal::Lost(NodeRefusal::Fenced(_))
    ));
    assert_eq!(
        f.store.get_by_agent(b.placement.agent_id).await.unwrap(),
        before
    );
    assert_eq!(f.store.get_volume(b.volume).await.unwrap(), volume);
    let response = client(&f, &b.token)
        .await
        .sync(
            &support::request(&b),
            tokio::time::Instant::now() + Duration::from_secs(20),
        )
        .await
        .unwrap();
    assert!(matches!(response.placements[0], Renewal::Renewed(_)));
    assert!(matches!(response.claims[0], Renewal::Renewed(_)));
    assert!(matches!(response.writers[0], Renewal::Renewed(_)));
}
#[tokio::test]
async fn sync_retry_after_a_dropped_response() {
    let Some(f) = support::fixture("sync-retry").await else {
        return;
    };
    let a = support::hosted(&f.store).await;
    let (router, count) = support::drop_first_response(
        router(NodeApiState::new(f.store.clone(), Duration::from_secs(30))),
        SYNC_PATH,
    );
    let base = support::serve(router).await;
    let response = NodeClient::new(&base, &a.token)
        .unwrap()
        .sync(
            &support::request(&a),
            tokio::time::Instant::now() + Duration::from_secs(20),
        )
        .await
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert!(matches!(response.placements[0], Renewal::Renewed(_)));
    assert!(matches!(response.claims[0], Renewal::Renewed(_)));
    assert!(matches!(response.writers[0], Renewal::Renewed(_)));
}
#[tokio::test]
async fn sync_scratch_verdicts_and_evictions() {
    let Some(fixture) = support::fixture("sync-scratch").await else {
        return;
    };
    let hosted_a = support::hosted(&fixture.store).await;
    let hosted_b = support::hosted(&fixture.store).await;
    let hosted_c = support::hosted(&fixture.store).await;
    fixture
        .store
        .delete_computer(hosted_c.placement.agent_id)
        .await
        .unwrap();
    let idle_agent = AgentId::from_ulid(Ulid::generate());
    let kept_agent = AgentId::from_ulid(Ulid::generate());
    let mut request = support::request(&hosted_a);
    request.scratch = [
        (hosted_a.placement.agent_id, true, false),
        (hosted_b.placement.agent_id, false, false),
        (hosted_c.placement.agent_id, false, false),
        (idle_agent, false, true),
        (kept_agent, false, false),
    ]
    .into_iter()
    .map(|(agent, hosted, idle)| ScratchEntry {
        agent,
        bytes: 10,
        hosted,
        idle,
    })
    .collect();
    let client = client(&fixture, &hosted_a.token).await;
    let response = client
        .sync(
            &request,
            tokio::time::Instant::now() + Duration::from_secs(20),
        )
        .await
        .unwrap();
    assert_eq!(
        response.scratch,
        vec![
            ScratchVerdict::Keep,
            ScratchVerdict::Evict(EvictReason::Moved),
            ScratchVerdict::Evict(EvictReason::Deleted),
            ScratchVerdict::Evict(EvictReason::Idle),
            ScratchVerdict::Keep
        ]
    );
    assert_eq!(
        fixture
            .store
            .scratch(hosted_a.placement.agent_id)
            .await
            .unwrap()
            .unwrap()
            .bytes,
        10
    );
    assert_eq!(
        fixture
            .store
            .scratch(kept_agent)
            .await
            .unwrap()
            .unwrap()
            .node_id,
        hosted_a.node.node_id
    );
    assert!(
        fixture
            .store
            .scratch(hosted_b.placement.agent_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(fixture.store.scratch(idle_agent).await.unwrap().is_none());
    request.scratch.clear();
    request.scratch_evicted = vec![kept_agent];
    client
        .sync(
            &request,
            tokio::time::Instant::now() + Duration::from_secs(20),
        )
        .await
        .unwrap();
    assert!(fixture.store.scratch(kept_agent).await.unwrap().is_none());
}
#[tokio::test]
async fn sync_queues_metric_patches() {
    let Some(f) = support::fixture("sync-metrics").await else {
        return;
    };
    let a = support::hosted(&f.store).await;
    let turn = MessageId::from_ulid(Ulid::generate());
    let mut request = support::request(&a);
    request.metrics.push(MetricBatch {
        session: a.session,
        turn,
        patches: vec![NodeMetric::Tool(ToolMetric {
            request_id: "r1".into(),
            name: "bash".into(),
            ..ToolMetric::default()
        })],
    });
    client(&f, &a.token)
        .await
        .sync(
            &request,
            tokio::time::Instant::now() + Duration::from_secs(20),
        )
        .await
        .unwrap();
    f.store.flush_turn_metrics().await.unwrap();
    let page = f
        .store
        .list_turn_metrics_paged(a.session, None, 10, None, None)
        .await
        .unwrap();
    assert!(
        page.iter()
            .any(|m| m.turn_id == turn.to_string() && m.tools.iter().any(|t| t.request_id == "r1"))
    );
}
