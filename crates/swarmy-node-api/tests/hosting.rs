#![deny(clippy::disallowed_methods)]
mod support;
use std::{net::Ipv4Addr, sync::atomic::Ordering, time::Duration};
use swarmy_core::{ComputerMetric, LeaseOwnerId, PlacedToolClaim, ToolResult, VolumeId};
use swarmy_node_api::router;
use swarmy_node_protocol::{
    ClientError, HOSTING_OPEN_PATH, HostingCloseRequest, HostingOpenRequest, HostingReadyRequest,
    MetricBatch, NodeMetric, NodeRefusal, WriterToken,
};
use ulid::Ulid;
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(20)
}
fn open(a: &support::Dispatched) -> HostingOpenRequest {
    HostingOpenRequest {
        agent: a.agent,
        job: a.jobs[0].clone(),
        previous_epoch: None,
    }
}
#[tokio::test]
async fn route_names_the_agent_and_refuses_done_jobs() {
    let Some(f) = support::fixture("na5-route").await else {
        return;
    };
    let a = support::dispatched(&f.store).await;
    let base = support::served(&f).await;
    let client = support::client(&base, &a.token);
    let answer = client
        .route_tool(
            &swarmy_node_protocol::ToolRouteRequest {
                job: a.jobs[0].clone(),
            },
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(answer.agent, a.agent);
    assert_eq!(answer.server_now, f.store.now());
    let claim = PlacedToolClaim {
        job: a.jobs[0].clone(),
        owner: LeaseOwnerId::from_ulid(Ulid::generate()),
        placement: a.placement,
        expires_at: f.store.now().checked_add(Duration::from_secs(30)).unwrap(),
    };
    assert!(f.store.claim_placed_tool(&claim).await.unwrap());
    f.store
        .complete_placed_tool(
            &claim,
            3,
            &ToolResult::Completed {
                output: "done".into(),
                title: "done".into(),
                metadata: std::collections::BTreeMap::default(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        client
            .route_tool(
                &swarmy_node_protocol::ToolRouteRequest {
                    job: a.jobs[0].clone()
                },
                deadline()
            )
            .await,
        Err(ClientError::Refused(NodeRefusal::AlreadyDone))
    ));
}
#[tokio::test]
async fn open_returns_everything_a_node_needs() {
    let Some(f) = support::fixture("na5-open").await else {
        return;
    };
    let a = support::dispatched(&f.store).await;
    let base = support::served(&f).await;
    let answer = support::client(&base, &a.token)
        .open_hosting(&open(&a), deadline())
        .await
        .unwrap();
    assert_eq!(answer.server_now, f.store.now());
    assert!(answer.placement.same_grant(&a.placement));
    assert_eq!(answer.volume, VolumeId::from_ulid(a.agent.as_ulid()));
    assert_eq!(answer.writer.owner, LeaseOwnerId::from(a.node.node_id));
    assert_eq!(
        answer.writer.expires_at,
        f.store.now().checked_add(Duration::from_secs(60)).unwrap()
    );
    assert_eq!(
        answer.head,
        f.store
            .get_volume(answer.volume)
            .await
            .unwrap()
            .unwrap()
            .head_manifest
    );
    assert_eq!(
        answer.header,
        f.store.get_manifest(answer.head).await.unwrap().unwrap()
    );
    assert_eq!(
        answer.spec,
        f.store.sandbox_spec(a.agent, a.session).await.unwrap()
    );
    assert_eq!(answer.spec.requirements.memory_mib, 768);
    assert!(answer.spec.scratch.is_empty());
    assert_eq!(answer.boot_set, None);
}
#[tokio::test]
async fn open_repeats_after_a_dropped_response() {
    let Some(f) = support::fixture("na5-open-repeat").await else {
        return;
    };
    let a = support::dispatched(&f.store).await;
    let (wrapped, counter) =
        support::drop_first_response(router(support::state(&f)), HOSTING_OPEN_PATH);
    let base = support::serve(wrapped).await;
    let client = support::client(&base, &a.token);
    let first = client.open_hosting(&open(&a), deadline()).await.unwrap();
    let second = client.open_hosting(&open(&a), deadline()).await.unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 3);
    assert_eq!(first.placement.epoch, second.placement.epoch);
    assert_eq!(
        (first.writer.owner, first.writer.seq),
        (second.writer.owner, second.writer.seq)
    );
}
#[tokio::test]
async fn open_refuses_a_stopped_epoch_without_renewing() {
    let Some(f) = support::fixture("na5-stopped").await else {
        return;
    };
    let a = support::dispatched(&f.store).await;
    let client = support::client(&support::served(&f).await, &a.token);
    let opened = client.open_hosting(&open(&a), deadline()).await.unwrap();
    let record = f.store.get_by_agent(a.agent).await.unwrap().unwrap();
    let request = HostingOpenRequest {
        previous_epoch: Some(opened.placement.epoch),
        ..open(&a)
    };
    for _ in 0..2 {
        assert!(matches!(
            client.open_hosting(&request, deadline()).await,
            Err(ClientError::Refused(NodeRefusal::EpochStopped { .. }))
        ));
    }
    assert_eq!(
        f.store.get_by_agent(a.agent).await.unwrap().unwrap(),
        record
    );
    *f.clock.lock().unwrap() = record
        .expires_at
        .checked_add(Duration::from_secs(1))
        .unwrap();
    assert!(matches!(
        client.open_hosting(&request, deadline()).await,
        Err(ClientError::Refused(NodeRefusal::Fenced(_)))
    ));
    assert_eq!(
        f.store.get_by_agent(a.agent).await.unwrap().unwrap(),
        record
    );
}
#[tokio::test]
async fn ready_records_the_address_and_boot_metrics() {
    let Some(f) = support::fixture("na5-ready").await else {
        return;
    };
    let a = support::dispatched(&f.store).await;
    let client = support::client(&support::served(&f).await, &a.token);
    let opened = client.open_hosting(&open(&a), deadline()).await.unwrap();
    let address = Ipv4Addr::new(10, 0, 0, 2);
    let request = HostingReadyRequest {
        placement: opened.placement.clone(),
        address,
        metrics: vec![MetricBatch {
            session: a.session,
            turn: a.turn,
            patches: vec![NodeMetric::Computer(ComputerMetric {
                placement_ms: Some(12.5),
                ..Default::default()
            })],
        }],
    };
    client.hosting_ready(&request, deadline()).await.unwrap();
    assert_eq!(
        f.store.placement_address(&opened.placement).await.unwrap(),
        Some(address)
    );
    client.hosting_ready(&request, deadline()).await.unwrap();
    f.store.flush_turn_metrics().await.unwrap();
    let page = f
        .store
        .list_turn_metrics_paged(a.session, None, 10, None, None)
        .await
        .unwrap();
    assert!(page.iter().any(|m| {
        m.turn_id == a.turn.to_string()
            && m.computer
                .as_ref()
                .is_some_and(|c| c.placement_ms == Some(12.5))
    }));
}
#[tokio::test]
async fn close_releases_writer_then_placement_and_repeats() {
    let Some(f) = support::fixture("na5-close").await else {
        return;
    };
    let a = support::dispatched(&f.store).await;
    let client = support::client(&support::served(&f).await, &a.token);
    let opened = client.open_hosting(&open(&a), deadline()).await.unwrap();
    let request = HostingCloseRequest {
        placement: opened.placement,
        writer: Some(WriterToken {
            volume: opened.volume,
            lease: opened.writer,
        }),
    };
    client.close_hosting(&request, deadline()).await.unwrap();
    assert!(f.store.get_by_agent(a.agent).await.unwrap().is_none());
    assert!(
        f.store
            .get_volume(opened.volume)
            .await
            .unwrap()
            .unwrap()
            .writer_lease
            .is_none()
    );
    client.close_hosting(&request, deadline()).await.unwrap();
}
#[tokio::test]
async fn close_keeps_the_placement_when_the_writer_is_stale() {
    let Some(f) = support::fixture("na5-close-stale").await else {
        return;
    };
    let a = support::dispatched(&f.store).await;
    let client = support::client(&support::served(&f).await, &a.token);
    let opened = client.open_hosting(&open(&a), deadline()).await.unwrap();
    let mut lease = opened.writer;
    lease.seq += 1;
    let request = HostingCloseRequest {
        placement: opened.placement.clone(),
        writer: Some(WriterToken {
            volume: opened.volume,
            lease,
        }),
    };
    assert!(matches!(
        client.close_hosting(&request, deadline()).await,
        Err(ClientError::Refused(NodeRefusal::Fenced(
            swarmy_node_protocol::FenceKind::VolumeLease
        )))
    ));
    assert_eq!(
        f.store.get_by_agent(a.agent).await.unwrap(),
        Some(opened.placement)
    );
}
