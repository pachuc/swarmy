#![deny(clippy::disallowed_methods)]
mod support;
use std::{net::Ipv4Addr, time::Duration};
use swarmy_core::LeaseOwnerId;
use swarmy_node_protocol::{
    ClientError, FenceKind, GithubTokenRequest, HostingBootSetRequest, HostingCloseRequest,
    HostingOpenRequest, HostingReadyRequest, NodeClient, NodeRefusal, ToolRouteRequest,
    VolumePublishRequest, WriterToken,
};
use ulid::Ulid;
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(20)
}
fn foreign(result: &Result<impl Sized, ClientError>) {
    assert!(matches!(
        result,
        Err(ClientError::Refused(NodeRefusal::NotYours))
    ));
}
async fn foreign_hosting(client: &NodeClient, a: &support::Dispatched, b: &support::Hosted) {
    let job = b.claim.job.clone();
    assert!(
        matches!(client.route_tool(&ToolRouteRequest { job: job.clone() }, deadline()).await, Err(ClientError::Refused(NodeRefusal::PlacedElsewhere { node, .. })) if node == b.node.node_id)
    );
    for agent in [b.placement.agent_id, a.agent] {
        assert!(
            matches!(client.open_hosting(&HostingOpenRequest { agent, job: job.clone(), previous_epoch: None }, deadline()).await, Err(ClientError::Refused(NodeRefusal::PlacedElsewhere { node, .. })) if node == b.node.node_id)
        );
    }
    foreign(
        &client
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
        &client
            .close_hosting(
                &HostingCloseRequest {
                    placement: b.placement.clone(),
                    writer: None,
                },
                deadline(),
            )
            .await,
    );
    let own = client
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
        &client
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
}
async fn foreign_volume(
    client: &NodeClient,
    a: &support::Dispatched,
    b: &support::Hosted,
    publish: &VolumePublishRequest,
) {
    foreign(&client.publish_volume(publish, deadline()).await);
    let mut forged = publish.clone();
    forged.writer.lease.owner = LeaseOwnerId::from(a.node.node_id);
    // The volume binding checks the placement before the writer's sequence.
    assert!(matches!(
        client.publish_volume(&forged, deadline()).await,
        Err(ClientError::Refused(NodeRefusal::Fenced(
            FenceKind::Placement
        )))
    ));
    foreign(
        &client
            .github_token(
                &GithubTokenRequest {
                    agent: b.placement.agent_id,
                },
                deadline(),
            )
            .await,
    );
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
    foreign_hosting(&client_a, &a, &b).await;
    let foreign_boot = HostingBootSetRequest {
        placement: b.placement.clone(),
        session: b.claim.job.session_id,
        head: volume_before.head_manifest,
        key: swarmy_core::boot_set_key(b"foreign"),
    };
    foreign(&client_a.report_boot_set(&foreign_boot, deadline()).await);
    assert_eq!(
        f.store.boot_set(volume_before.head_manifest).await.unwrap(),
        None
    );
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
        next: swarmy_core::ManifestId::from_ulid(Ulid::generate()),
        header,
    };
    foreign_volume(&client_a, &a, &b, &publish).await;
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

#[tokio::test]
async fn a_node_cannot_claim_or_complete_for_another_node() {
    let Some(f) = support::fixture("na5b-auth").await else {
        return;
    };
    let a = support::dispatched(&f.store).await;
    let b = support::dispatched(&f.store).await;
    let base = support::served(&f).await;
    let alice = support::client(&base, &a.token);
    let bob = support::client(&base, &b.token);
    let own = alice
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
    let foreign_placement = bob
        .open_hosting(
            &HostingOpenRequest {
                agent: b.agent,
                job: b.jobs[0].clone(),
                previous_epoch: None,
            },
            deadline(),
        )
        .await
        .unwrap()
        .placement;
    foreign(
        &alice
            .claim_tool(
                &support::claim_request(&b.jobs[0], &foreign_placement),
                deadline(),
            )
            .await,
    );
    let forged = support::claim_request(&b.jobs[0], &own);
    assert!(matches!(
        alice.claim_tool(&forged, deadline()).await,
        Err(ClientError::Refused(
            NodeRefusal::PlacedElsewhere { .. } | NodeRefusal::Fenced(_)
        ))
    ));
    let claim = bob
        .claim_tool(
            &support::claim_request(&b.jobs[0], &foreign_placement),
            deadline(),
        )
        .await
        .unwrap()
        .claim;
    let request = swarmy_node_protocol::ToolCompleteRequest {
        claim,
        result: support::bash_result(),
        tool: swarmy_core::ToolMetric::default(),
    };
    foreign(&alice.complete_tool(&request, deadline()).await);
    bob.complete_tool(&request, deadline()).await.unwrap();
}
