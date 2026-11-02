#![deny(clippy::disallowed_methods)]
mod support;
use std::time::Duration;
use swarmy_core::AgentId;
use swarmy_node_protocol::{ClientError, GithubTokenRequest, NodeRefusal};
use swarmy_store::CreateAgentOptions;
use ulid::Ulid;
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(20)
}
#[tokio::test]
async fn github_token_only_for_a_live_placement_on_this_node() {
    let Some(f) = support::fixture("na5-github").await else {
        return;
    };
    let image = swarmy_testkit::image(&f.store).await;
    let agent = f
        .store
        .create_agent(
            "github-agent",
            image,
            "",
            f.store.now(),
            Some(CreateAgentOptions {
                github_token: Some("ghp_test_token"),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let a = support::node(&f.store, 1).await;
    let b = support::node(&f.store, 1).await;
    f.store
        .place(
            agent.agent_id,
            a.node_id,
            f.store.now().checked_add(Duration::from_secs(30)).unwrap(),
        )
        .await
        .unwrap();
    let token_a = f.store.issue_node_credential(a.node_id).await.unwrap();
    let token_b = f.store.issue_node_credential(b.node_id).await.unwrap();
    let base = support::served(&f).await;
    let client_a = support::client(&base, &token_a);
    let client_b = support::client(&base, &token_b);
    let request = GithubTokenRequest {
        agent: agent.agent_id,
    };
    assert_eq!(
        client_a
            .github_token(&request, deadline())
            .await
            .unwrap()
            .token
            .as_deref(),
        Some("ghp_test_token")
    );
    assert!(matches!(
        client_b.github_token(&request, deadline()).await,
        Err(ClientError::Refused(NodeRefusal::NotYours))
    ));
    assert!(matches!(
        client_a
            .github_token(
                &GithubTokenRequest {
                    agent: AgentId::from_ulid(Ulid::generate())
                },
                deadline()
            )
            .await,
        Err(ClientError::Refused(NodeRefusal::NotYours))
    ));
    *f.clock.lock().unwrap() = f.store.now().checked_add(Duration::from_secs(31)).unwrap();
    assert!(matches!(
        client_a.github_token(&request, deadline()).await,
        Err(ClientError::Refused(NodeRefusal::NotYours))
    ));
}
