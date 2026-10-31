#![deny(clippy::disallowed_methods)]
mod support;
use std::{net::Ipv4Addr, time::Duration};
use swarmy_core::LeaseOwnerId;
use swarmy_node_protocol::{
    ClientError, FenceKind, GithubTokenRequest, HostingCloseRequest, HostingOpenRequest,
    HostingReadyRequest, NodeRefusal, ToolRouteRequest, VolumePublishRequest, WriterToken,
};
use ulid::Ulid;
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(20)
}
fn foreign(result: Result<impl Sized, ClientError>) {
    assert!(matches!(
        result,
        Err(ClientError::Refused(NodeRefusal::NotYours))
    ));
}
#[tokio::test]
async fn a_node_cannot_act_on_another_nodes_tokens() {
    let Some(f) = support::fixture("na5-authorize").await else {
        return;
    };
    let a = support::dispatched(&f.store).await;
    let b = support::hosted(&f.store).await;
    let placement_before = f
        .store
        .get_by_agent(b.placement.agent_id)
        .await
        .unwrap()
        .unwrap();
    let volume_before = f.store.get_volume(b.volume).await.unwrap().unwrap();
    let base = support::served(&f).await;
    let client_a = support::client(&base, &a.token);
    let client_b = support::client(&base, &b.token);
    let job = b.claim.job.clone();
    assert!(
        matches!(client_a.route_tool(&ToolRouteRequest { job: job.clone() }, deadline()).await, Err(ClientError::Refused(NodeRefusal::PlacedElsewhere { node, .. })) if node == b.node.node_id)
    );
    for agent in [b.placement.agent_id, a.agent] {
        assert!(
            matches!(client_a.open_hosting(&HostingOpenRequest { agent, job: job.clone(), previous_epoch: None }, deadline()).await, Err(ClientError::Refused(NodeRefusal::PlacedElsewhere { node, .. })) if node == b.node.node_id)
        );
    }
    foreign(
        client_a
            .hosting_ready(
                &HostingReadyRequest {
                    placement: b.placement.clone(),
                    address: Ipv4Addr::LOCALHOST,
                    metrics: vec![],
                },
                deadline(),
            )
            .await,
    );
    foreign(
        client_a
            .close_hosting(
                &HostingCloseRequest {
                    placement: b.placement.clone(),
                    writer: None,
                },
                deadline(),
            )
            .await,
    );
    let own = client_a
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
    foreign(
        client_a
            .close_hosting(
                &HostingCloseRequest {
                    placement: own.placement,
                    writer: Some(WriterToken {
                        volume: b.volume,
                        lease: b.lease.clone(),
                    }),
                },
                deadline(),
            )
            .await,
    );
    let next = swarmy_core::ManifestId::from_ulid(Ulid::generate());
    let header = f
        .store
        .get_manifest(volume_before.head_manifest)
        .await
        .unwrap()
        .unwrap();
    let publish = VolumePublishRequest {
        writer: WriterToken {
            volume: b.volume,
            lease: b.lease.clone(),
        },
        previous: volume_before.head_manifest,
        next,
        header,
    };
    foreign(client_a.publish_volume(&publish, deadline()).await);
    let mut forged = publish.clone();
    forged.writer.lease.owner = LeaseOwnerId::from(a.node.node_id);
    let forged_result = client_a.publish_volume(&forged, deadline()).await;
    assert!(matches!(
        forged_result,
        Err(ClientError::Refused(NodeRefusal::Fenced(
            FenceKind::Placement
        )))
    ));
    foreign(
        client_a
            .github_token(
                &GithubTokenRequest {
                    agent: b.placement.agent_id,
                },
                deadline(),
            )
            .await,
    );
    assert_eq!(
        f.store
            .get_by_agent(b.placement.agent_id)
            .await
            .unwrap()
            .unwrap(),
        placement_before
    );
    assert_eq!(
        f.store.get_volume(b.volume).await.unwrap().unwrap(),
        volume_before
    );
    client_b
        .hosting_ready(
            &HostingReadyRequest {
                placement: b.placement,
                address: Ipv4Addr::LOCALHOST,
                metrics: vec![],
            },
            deadline(),
        )
        .await
        .unwrap();
    client_b.publish_volume(&publish, deadline()).await.unwrap();
}
