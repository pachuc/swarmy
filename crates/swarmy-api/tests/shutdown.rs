#![deny(clippy::disallowed_methods)]
//! SIGTERM drains the API instead of killing it: an open `/v1/events`
//! stream does not hang shutdown, and queued turn metrics are durable by
//! exit. Spawns the real `swarmy-api` binary against the dev stack.

use std::{sync::Arc, time::Duration};

use swarmy_api_types::{AppendMessage, AppendedMessage, Cursor, LogId, Subscription};
use swarmy_core::SessionId;
use swarmy_store::{Store, blob::MemoryBlobStore};
use ulid::Ulid;

struct ApiFixture {
    store: Store,
    session: SessionId,
    base: String,
    token: String,
    service: swarmy_testkit::ChildGuard,
    // Held for their Drops: the stack keys vanish and the service dies
    // even when the test panics.
    _stack: swarmy_testkit::StackGuard,
}

impl ApiFixture {
    async fn start() -> Option<Self> {
        let stack = swarmy_testkit::Stack::load("shutdown")?;
        let (store, stack_guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
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
        let mut service = swarmy_testkit::ChildGuard::new(child);
        // The service opens the store and bus before binding, so an open
        // port means it is ready for messages.
        swarmy_testkit::eventually("swarmy-api listens", Duration::from_secs(60), async || {
            assert!(!service.has_exited(), "swarmy-api exited during startup");
            std::net::TcpStream::connect(format!("127.0.0.1:{port}"))
                .is_ok()
                .then_some(())
        })
        .await;
        Some(Self {
            store,
            session,
            base: format!("http://127.0.0.1:{port}"),
            token,
            service,
            _stack: stack_guard,
        })
    }

    /// One message through the real append path queues the submitted and
    /// appended turn stages in the service's metrics queue.
    async fn post_message(&self) {
        let response = reqwest::Client::new()
            .post(format!(
                "{}/v1/sessions/{}/messages",
                self.base, self.session
            ))
            .bearer_auth(&self.token)
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
    }

    /// A never-ending event stream that stays live across shutdown: the
    /// capped graceful wait must close it instead of hanging on it.
    async fn open_stream(&self) -> tokio::task::JoinHandle<()> {
        let subscription = Subscription {
            cursors: vec![Cursor {
                log_id: LogId::Session(self.session.to_string()),
                sequence: 0,
            }],
            token_deltas: false,
        };
        let mut url = reqwest::Url::parse(&format!("{}/v1/events", self.base)).unwrap();
        url.query_pairs_mut().append_pair(
            "subscription",
            &serde_json::to_string(&subscription).unwrap(),
        );
        let stream = reqwest::Client::new()
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .unwrap();
        assert!(stream.status().is_success(), "{}", stream.status());
        tokio::spawn(async move {
            let mut stream = stream;
            while stream.chunk().await.unwrap_or(None).is_some() {}
        })
    }
}

#[tokio::test]
async fn sigterm_flushes_queued_metrics_despite_open_event_stream() {
    let Some(mut fixture) = ApiFixture::start().await else {
        return;
    };
    fixture.post_message().await;
    let reader = fixture.open_stream().await;
    assert!(
        std::process::Command::new("kill")
            .args(["-TERM", &fixture.service.id().unwrap().to_string()])
            .status()
            .unwrap()
            .success()
    );
    swarmy_testkit::eventually(
        "api exits on SIGTERM",
        Duration::from_secs(20),
        async || fixture.service.has_exited().then_some(()),
    )
    .await;
    let status = fixture.service.wait().await;
    assert!(status.success(), "{status}");
    reader.abort();

    // One read, no polling: the queued stages must be durable by exit.
    let records = fixture
        .store
        .list_turn_metrics_paged(fixture.session, None, 10, None, None)
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
