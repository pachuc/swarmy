#![deny(clippy::disallowed_methods)]
use axum::{Router, routing::get};
use swarmy_node_api::test_support::TestApi;
use std::time::Duration;

fn router() -> Router { Router::new().route("/ping", get(|| async { "pong" })) }
fn client() -> reqwest::Client { reqwest::Client::builder().http2_prior_knowledge().timeout(Duration::from_secs(5)).build().unwrap() }

#[tokio::test]
async fn paused_api_refuses_and_resumes_on_the_same_url() {
    let mut api = TestApi::start(router()).await;
    let client = client();
    let url = format!("{}/ping", api.url());
    assert!(client.get(&url).send().await.unwrap().status().is_success());
    api.pause();
    assert!(client.get(&url).send().await.is_err());
    api.resume().await;
    assert!(client.get(&url).send().await.unwrap().status().is_success());
}

#[tokio::test]
async fn detached_api_keeps_serving() {
    let url = TestApi::start(router()).await.detach();
    assert!(client().get(format!("{url}/ping")).send().await.unwrap().status().is_success());
}
