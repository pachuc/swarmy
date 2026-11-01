//! In-process node API and response faults for tests.
use axum::{Router, middleware, response::IntoResponse};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::{
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};

/// A router behind a relay whose existing connections can be dropped.
pub struct TestApi {
    url: String,
    relay: SocketAddr,
    backend: SocketAddr,
    server: Option<JoinHandle<()>>,
    relaying: Option<JoinHandle<()>>,
}

impl TestApi {
    /// Bind the router and relay to separate loopback ports.
    /// # Panics
    /// If a listener cannot bind.
    pub async fn start(router: Router) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("backend bind");
        let backend = listener.local_addr().expect("backend address");
        // The test fixture owns this server until drop or detach.
        // ast-grep-ignore: no-spawn-in-libraries
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve node API");
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("relay bind");
        let relay = listener.local_addr().expect("relay address");
        let relaying = Self::relay(listener, backend);
        Self {
            url: format!("http://{relay}"),
            relay,
            backend,
            server: Some(server),
            relaying: Some(relaying),
        }
    }

    fn relay(listener: TcpListener, backend: SocketAddr) -> JoinHandle<()> {
        // Aborting this task drops its JoinSet and every accepted connection.
        // ast-grep-ignore: no-spawn-in-libraries
        tokio::spawn(async move {
            let mut connections = JoinSet::new();
            while let Ok((mut incoming, _)) = listener.accept().await {
                // Connections belong to the relay task, not the server.
                // ast-grep-ignore: no-spawn-in-libraries
                connections.spawn(async move {
                    if let Ok(mut outgoing) = TcpStream::connect(backend).await {
                        let _copy =
                            tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await;
                    }
                });
            }
        })
    }

    /// The relay URL, stable across pause and resume.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Refuse new connections and drop all existing relay connections.
    pub fn pause(&mut self) {
        if let Some(handle) = self.relaying.take() {
            handle.abort();
        }
    }

    /// Rebind the original relay address.
    /// # Panics
    /// If the relay address cannot be rebound.
    pub async fn resume(&mut self) {
        if self.relaying.is_none() {
            let listener = TcpListener::bind(self.relay).await.expect("relay rebind");
            self.relaying = Some(Self::relay(listener, self.backend));
        }
    }

    /// Leave the server and relay running for the test process lifetime.
    #[must_use]
    pub fn detach(mut self) -> String {
        self.server.take();
        self.relaying.take();
        std::mem::take(&mut self.url)
    }
}

impl Drop for TestApi {
    fn drop(&mut self) {
        if let Some(handle) = self.relaying.take() {
            handle.abort();
        }
        if let Some(handle) = self.server.take() {
            handle.abort();
        }
    }
}

/// Replace the first successful response on `path` with a 503.
pub fn drop_first_response(router: Router, path: &'static str) -> (Router, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    let copy = count.clone();
    let wrapped = router.layer(middleware::from_fn(
        move |request: axum::extract::Request, next: middleware::Next| {
            let count = copy.clone();
            async move {
                let selected = request.uri().path() == path;
                let answer = next.run(request).await;
                if selected
                    && count.fetch_add(1, Ordering::SeqCst) == 0
                    && answer.status().is_success()
                {
                    axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response()
                } else {
                    answer
                }
            }
        },
    ));
    (wrapped, count)
}

use jiff::Timestamp;
use swarmy_core::{NodeCapacity, NodeId, NodeRecord, NodeRole};
use swarmy_store::Store;
use ulid::Ulid;
/// One node with two dispatched jobs and a claimed placement, before volume binding.
/// # Panics
/// Panics if the test fixture cannot be created.
#[allow(dead_code, reason = "the fixture is shared across integration targets")]
pub struct Dispatched {
    pub node: NodeRecord,
    pub token: swarmy_core::NodeToken,
    pub session: swarmy_core::SessionId,
    pub agent: swarmy_core::AgentId,
    pub turn: swarmy_core::MessageId,
    pub jobs: Vec<swarmy_core::ToolJob>,
    pub placement: swarmy_core::PlacementRecord,
}
#[allow(dead_code, reason = "the fixture is shared across integration targets")]
/// Create dispatched tool calls for node API tests.
/// # Panics
/// Panics if the test store rejects the fixture.
pub async fn dispatched(store: &Store) -> Dispatched {
    use std::time::Duration;
    use swarmy_core::{
        BashArguments, Event, LeaseOwnerId, Message, MessageId, MessageRole, Part, RequestId,
        SandboxArguments, ToolCallId, ToolCallRecord, ToolJob,
    };
    let (session, agent) = hosted_session(store).await;
    let node = node(store, 1).await;
    let now = store.now();
    let placement = store
        .place(
            agent,
            node.node_id,
            now.checked_add(Duration::from_secs(30))
                .expect("test fixture setup succeeds"),
        )
        .await
        .expect("test fixture setup succeeds");
    let turn = MessageId::from_ulid(Ulid::generate());
    store
        .append_user_message(
            session,
            0,
            &Message {
                id: turn,
                role: MessageRole::User,
                parts: vec![Part::Text { text: "run".into() }],
            },
        )
        .await
        .expect("test fixture setup succeeds");
    let session_lease = store
        .claim_lease(
            session,
            LeaseOwnerId::from_ulid(Ulid::generate()),
            now.checked_add(Duration::from_secs(30))
                .expect("test fixture setup succeeds"),
        )
        .await
        .expect("test fixture setup succeeds");
    let jobs: Vec<ToolJob> = (2..=3)
        .map(|step| ToolJob {
            session_id: session,
            step,
            request_id: RequestId::for_step(session, step),
            call_id: ToolCallId(format!("bash-{step}")),
            arguments: SandboxArguments::Bash(BashArguments {
                command: "echo test".into(),
                timeout_ms: 1000,
                yield_seconds: 10,
                output_budget_bytes: 32768,
            }),
        })
        .collect();
    let events: Vec<Event> = jobs
        .iter()
        .map(|job| Event::ToolCallRequested {
            seq: 0,
            request_id: job.request_id,
            call: ToolCallRecord {
                call_id: job.call_id.clone(),
                tool: "bash".into(),
                arguments: job.arguments.parameters(),
                result: None,
            },
        })
        .collect();
    store
        .append_events_leased(session, 1, &events, &session_lease, now)
        .await
        .expect("test fixture setup succeeds");
    store
        .dispatch_placed_tool_jobs(session, &session_lease, &jobs, &placement)
        .await
        .expect("test fixture setup succeeds");
    store
        .claim_placement(&placement)
        .await
        .expect("test fixture setup succeeds");
    let token = store
        .issue_node_credential(node.node_id)
        .await
        .expect("test fixture setup succeeds");
    Dispatched {
        node,
        token,
        session,
        agent,
        turn,
        jobs,
        placement,
    }
}

/// Create a registered fixture node.
/// # Panics
/// Panics if the test store rejects the node.
pub async fn node(store: &Store, sandboxes: u32) -> NodeRecord {
    let record = NodeRecord {
        node_id: NodeId::from_ulid(Ulid::generate()),
        roles: vec![NodeRole::Sandbox, NodeRole::Volume],
        capacity: NodeCapacity {
            cpu_millis: 64_000,
            memory_bytes: 1 << 40,
            disk_bytes: 1 << 40,
            sandboxes,
        },
        last_heartbeat: store.now(),
        cached_images: Vec::new(),
        gpu: None,
    };
    store
        .put_node(&record)
        .await
        .expect("test fixture setup succeeds");
    record
}
#[allow(
    dead_code,
    reason = "the shared fixture is used by the sync integration target only"
)]
/// Create a session whose agent has a test image.
/// # Panics
/// Panics if the test store rejects the session or image.
pub async fn hosted_session(store: &Store) -> (swarmy_core::SessionId, swarmy_core::AgentId) {
    use swarmy_core::{
        AgentId, CHUNK_SIZE, ContentHash, ImageTag, ManifestHeader, ManifestId, SessionKind,
        SessionRecord, SessionState,
    };
    let session = swarmy_core::SessionId::from_ulid(Ulid::generate());
    let agent = AgentId::from_ulid(Ulid::generate());
    let image = ManifestId::from_ulid(Ulid::generate());
    store
        .put_manifest(
            image,
            &ManifestHeader {
                size: u64::from(CHUNK_SIZE),
                chunk_size: CHUNK_SIZE,
                root_hash: ContentHash([1; 32]),
            },
        )
        .await
        .expect("test fixture setup succeeds");
    store
        .put_image("base", &ImageTag("test".into()), image, None)
        .await
        .expect("test fixture setup succeeds");
    store
        .create_session(
            &SessionRecord {
                interrupt_requested: false,
                session_id: session,
                agent_id: agent,
                state: SessionState::Idle,
                head_seq: 0,
                snapshot_ref: None,
                inference: swarmy_core::InferenceSelection::default(),
                route: None,
                route_step: 0,
                kind: SessionKind::Ephemeral,
                computer_deleted: false,
                plan: Vec::new(),
            },
            Timestamp::now(),
            "base:test",
        )
        .await
        .expect("test fixture setup succeeds");
    (session, agent)
}
