#![deny(clippy::disallowed_methods)]
//! SIGTERM drains the API instead of killing it: an open `/v1/events`
//! stream does not hang shutdown, and queued turn metrics are durable by
//! exit. Spawns the real `swarmy-api` binary against the dev stack.

use std::{sync::Arc, time::Duration};

use swarmy_api_types::{AppendMessage, AppendedMessage, Cursor, LogId, Subscription};
use swarmy_store::blob::MemoryBlobStore;
use ulid::Ulid;

#[tokio::test]
async fn sigterm_flushes_queued_metrics_despite_open_event_stream() {
    let Some(stack) = swarmy_testkit::Stack::load("shutdown") else {
        return;
    };
    let (store, _guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
    swarmy_testkit::image(&store).await;
    let agent = store
        .create_agent(
            "shutdown-agent",
            "fixture:test",
            "",
            jiff::Timestamp::now(),
            None,
        )
        .await
        .unwrap();
    let (session, _) = store
        .open_main_session(agent.agent_id, jiff::Timestamp::now())
        .await
        .unwrap();

    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let token = Ulid::generate().to_string();
    let child = tokio::process::Command::new(swarmy_testkit::bin("swarmy-api"))
        .env("SWARMY_FDB_CLUSTER_FILE", &stack.cluster)
        .env("SWARMY_NATS_URL", &stack.nats_url)
        .env("SWARMY_STORE_DIRECTORY", &stack.prefix)
        .env("SWARMY_BUS_PREFIX", &stack.prefix)
        .env("SWARMY_API_LISTEN", format!("127.0.0.1:{port}"))
        .env("SWARMY_API_TOKEN", &token)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut guard = swarmy_testkit::ChildGuard::new(child);
    let base = format!("http://127.0.0.1:{port}");
    // The service opens the store and bus before binding, so an open port
    // means it is ready for messages.
    swarmy_testkit::eventually("swarmy-api listens", Duration::from_secs(60), async || {
        if guard.has_exited() {
            panic!("swarmy-api exited during startup");
        }
        std::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .is_ok()
            .then_some(())
    })
    .await;

    let client = reqwest::Client::new();
    // One message through the real append path queues the submitted and
    // appended turn stages in the service's metrics queue.
    let response = client
        .post(format!("{base}/v1/sessions/{session}/messages"))
        .bearer_auth(&token)
        .json(&AppendMessage {
            queue: false,
            idempotency_key: "shutdown-turn".into(),
            expected_head: 0,
            text: "hello".into(),
        })
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success(), "{}", response.status());
    let _appended: AppendedMessage = response.json().await.unwrap();

    // A never-ending event stream stays live across shutdown: the capped
    // graceful wait must close it instead of hanging on it.
    let subscription = Subscription {
        cursors: vec![Cursor {
            log_id: LogId::Session(session.to_string()),
            sequence: 0,
        }],
        token_deltas: false,
    };
    let mut url = reqwest::Url::parse(&format!("{base}/v1/events")).unwrap();
    url.query_pairs_mut().append_pair(
        "subscription",
        &serde_json::to_string(&subscription).unwrap(),
    );
    let stream = client.get(url).bearer_auth(&token).send().await.unwrap();
    assert!(stream.status().is_success(), "{}", stream.status());
    let reader = tokio::spawn(async move {
        let mut stream = stream;
        while stream.chunk().await.unwrap_or(None).is_some() {}
    });

    assert!(
        std::process::Command::new("kill")
            .args(["-TERM", &guard.id().unwrap().to_string()])
            .status()
            .unwrap()
            .success()
    );
    swarmy_testkit::eventually(
        "api exits on SIGTERM",
        Duration::from_secs(20),
        async || guard.has_exited().then_some(()),
    )
    .await;
    let status = guard.wait().await;
    assert!(status.success(), "{status}");
    reader.abort();

    // One read, no polling: the queued stages must be durable by exit.
    let records = store
        .list_turn_metrics_paged(session, None, 10, None, None)
        .await
        .unwrap();
    assert_eq!(records.len(), 1, "{records:?}");
    assert!(
        records[0].stages.iter().any(|row| row.stage == "submitted"),
        "{records:?}"
    );
    assert!(
        records[0].stages.iter().any(|row| row.stage == "appended"),
        "{records:?}"
    );
}
