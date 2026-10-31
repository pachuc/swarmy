#![deny(clippy::disallowed_methods)]
mod support;
use std::{sync::atomic::Ordering, time::Duration};
use swarmy_core::{AgentId, NodeId, NodeToken};
use swarmy_node_api::{NodeApiState, router};
use swarmy_node_protocol::{
    BUILD_HEADER, ClientError, Failure, NodeClient, REGISTER_PATH, RegisterRequest,
};
use ulid::Ulid;
fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap()
}
async fn raw(
    base: &str,
    token: Option<&str>,
    build: Option<&str>,
    request: &RegisterRequest,
) -> (reqwest::StatusCode, Failure) {
    let mut call = http().post(format!("{base}{REGISTER_PATH}")).json(request);
    if let Some(token) = token {
        call = call.header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"));
    }
    if let Some(build) = build {
        call = call.header(BUILD_HEADER, build);
    }
    let response = call.send().await.unwrap();
    (response.status(), response.json().await.unwrap())
}
#[tokio::test]
async fn tokens_are_required_and_checked() {
    let Some(f) = support::fixture("node-api-auth").await else {
        return;
    };
    let record = support::node(&f.store, 2).await;
    let token = f.store.issue_node_credential(record.node_id).await.unwrap();
    let base = support::serve(router(NodeApiState::new(
        f.store.clone(),
        Duration::from_secs(30),
    )))
    .await;
    let request = RegisterRequest {
        node: support::advert(&record),
    };
    let other = NodeToken::new(NodeId::from_ulid(Ulid::generate()), [7; 32]);
    let wrong = NodeToken::new(record.node_id, [8; 32]);
    let other_text = other.reveal();
    let wrong_text = wrong.reveal();
    for text in [
        None,
        Some("garbage"),
        Some(other_text.as_str()),
        Some(wrong_text.as_str()),
    ] {
        let (status, failure) = raw(&base, text, Some(swarmy_version::IDENTITY), &request).await;
        assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
        assert_eq!(failure, Failure::Unauthorized);
    }
    assert!(
        NodeClient::new(&base, &token)
            .unwrap()
            .register(&request)
            .await
            .is_ok()
    );
}
#[tokio::test]
async fn revocation_takes_effect_after_the_cache() {
    let Some(f) = support::fixture("node-api-revoke").await else {
        return;
    };
    let record = support::node(&f.store, 2).await;
    let token = f.store.issue_node_credential(record.node_id).await.unwrap();
    let fresh = support::serve(router(
        NodeApiState::new(f.store.clone(), Duration::from_secs(30))
            .with_credential_ttl(Duration::ZERO),
    ))
    .await;
    let cached = support::serve(router(NodeApiState::new(
        f.store.clone(),
        Duration::from_secs(30),
    )))
    .await;
    let request = RegisterRequest {
        node: support::advert(&record),
    };
    let fresh = NodeClient::new(&fresh, &token).unwrap();
    let cached = NodeClient::new(&cached, &token).unwrap();
    fresh.register(&request).await.unwrap();
    cached.register(&request).await.unwrap();
    f.store
        .revoke_node_credential(record.node_id)
        .await
        .unwrap();
    assert!(matches!(
        fresh.register(&request).await,
        Err(ClientError::Unauthorized)
    ));
    assert!(cached.register(&request).await.is_ok());
}
#[tokio::test]
async fn wrong_build_is_a_conflict_naming_both() {
    let Some(f) = support::fixture("node-api-build").await else {
        return;
    };
    let record = support::node(&f.store, 2).await;
    let token = f.store.issue_node_credential(record.node_id).await.unwrap();
    let base = support::serve(router(NodeApiState::new(
        f.store.clone(),
        Duration::from_secs(30),
    )))
    .await;
    let request = RegisterRequest {
        node: support::advert(&record),
    };
    for (build, expected) in [(Some("0.0.0 (other)"), "0.0.0 (other)"), (None, "")] {
        let (status, failure) = raw(&base, Some(&token.reveal()), build, &request).await;
        assert_eq!(status, reqwest::StatusCode::CONFLICT);
        assert_eq!(
            failure,
            Failure::BuildMismatch {
                server: swarmy_version::IDENTITY.into(),
                node: expected.into()
            }
        );
    }
    assert_eq!(
        raw(&base, Some("garbage"), Some("wrong"), &request).await.1,
        Failure::Unauthorized
    );
}
#[tokio::test]
async fn register_records_the_node_on_the_server_clock() {
    let Some(f) = support::fixture("node-api-register").await else {
        return;
    };
    let record = support::node(&f.store, 70).await;
    let other = support::node(&f.store, 2).await;
    let token = f.store.issue_node_credential(record.node_id).await.unwrap();
    let t = f.store.now().checked_add(Duration::from_secs(10)).unwrap();
    *f.clock.lock().unwrap() = t;
    let mut agents = Vec::new();
    for _ in 0..65 {
        let agent = AgentId::from_ulid(Ulid::generate());
        f.store
            .place(
                agent,
                record.node_id,
                t.checked_add(Duration::from_secs(120)).unwrap(),
            )
            .await
            .unwrap();
        agents.push(agent);
    }
    f.store
        .place(
            AgentId::from_ulid(Ulid::generate()),
            other.node_id,
            t.checked_add(Duration::from_secs(120)).unwrap(),
        )
        .await
        .unwrap();
    let mut advert = support::advert(&record);
    advert.roles = vec![swarmy_core::NodeRole::Sandbox];
    advert.capacity.sandboxes = 71;
    advert.gpu = Some(swarmy_core::GpuCards {
        model: "test".into(),
        memory_mib: 4096,
        count: 1,
    });
    let base = support::serve(router(NodeApiState::new(
        f.store.clone(),
        Duration::from_secs(47),
    )))
    .await;
    let answer = NodeClient::new(&base, &token)
        .unwrap()
        .register(&RegisterRequest {
            node: advert.clone(),
        })
        .await
        .unwrap();
    let updated = f.store.get_node(record.node_id).await.unwrap().unwrap();
    assert_eq!(updated.last_heartbeat, t);
    assert_eq!(updated.roles, advert.roles);
    assert_eq!(updated.capacity, advert.capacity);
    assert_eq!(updated.gpu, advert.gpu);
    assert_eq!(answer.server_now, t);
    let mut found: Vec<_> = answer.previous.iter().map(|p| p.agent_id).collect();
    found.sort();
    agents.sort();
    assert_eq!(found, agents);
    assert_eq!(answer.leases.placement, Duration::from_secs(47));
    assert_eq!(answer.leases.tool_claim, Duration::from_secs(30));
    assert_eq!(answer.leases.writer, Duration::from_secs(60));
    assert_eq!(answer.leases.sync_interval, Duration::from_secs(5));
}
#[tokio::test]
async fn placements_lists_only_the_callers_with_occupancy() {
    let Some(f) = support::fixture("node-api-placements").await else {
        return;
    };
    let a = support::node(&f.store, 2).await;
    let b = support::node(&f.store, 2).await;
    let now = f.store.now();
    let mut placed = Vec::new();
    for node in [a.node_id, a.node_id, b.node_id] {
        placed.push(
            f.store
                .place(
                    AgentId::from_ulid(Ulid::generate()),
                    node,
                    now.checked_add(Duration::from_secs(120)).unwrap(),
                )
                .await
                .unwrap(),
        );
    }
    f.store.claim_placement(&placed[0]).await.unwrap();
    f.store
        .put_agent_call_status(&swarmy_core::AgentCallStatus {
            agent_id: placed[0].agent_id,
            node_id: a.node_id,
            epoch: placed[0].epoch,
            holder_session_id: Some(swarmy_core::SessionId::from_ulid(Ulid::generate())),
            queued_calls: 0,
            observed_at: now,
            expires_at: now.checked_add(Duration::from_secs(60)).unwrap(),
        })
        .await
        .unwrap();
    let token_a = f.store.issue_node_credential(a.node_id).await.unwrap();
    let token_b = f.store.issue_node_credential(b.node_id).await.unwrap();
    let base = support::serve(router(NodeApiState::new(
        f.store.clone(),
        Duration::from_secs(30),
    )))
    .await;
    let a_result = NodeClient::new(&base, &token_a)
        .unwrap()
        .placements()
        .await
        .unwrap();
    let b_result = NodeClient::new(&base, &token_b)
        .unwrap()
        .placements()
        .await
        .unwrap();
    assert_eq!(a_result.placements.len(), 2);
    assert!(
        a_result
            .placements
            .iter()
            .all(|p| p.placement.node_id == a.node_id)
    );
    assert!(
        a_result
            .placements
            .iter()
            .any(|p| p.placement.agent_id == placed[0].agent_id && p.occupied)
    );
    assert!(
        a_result
            .placements
            .iter()
            .any(|p| p.placement.agent_id == placed[1].agent_id && !p.occupied)
    );
    assert_eq!(b_result.placements.len(), 1);
    assert_eq!(
        b_result.placements[0].placement.agent_id,
        placed[2].agent_id
    );
}
#[tokio::test]
async fn register_retry_after_a_dropped_response() {
    let Some(f) = support::fixture("node-api-retry").await else {
        return;
    };
    let record = support::node(&f.store, 2).await;
    let token = f.store.issue_node_credential(record.node_id).await.unwrap();
    let (router, count) = support::drop_first_response(
        router(NodeApiState::new(f.store.clone(), Duration::from_secs(30))),
        REGISTER_PATH,
    );
    let base = support::serve(router).await;
    let result = NodeClient::new(&base, &token)
        .unwrap()
        .register(&RegisterRequest {
            node: support::advert(&record),
        })
        .await
        .unwrap();
    assert!(result.previous.is_empty());
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert!(f.store.get_node(record.node_id).await.unwrap().is_some());
}
