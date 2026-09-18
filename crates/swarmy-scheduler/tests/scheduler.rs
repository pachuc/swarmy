#[path = "../../swarmy-store/tests/support/mod.rs"]
mod image_fixture;

use std::{
    collections::HashSet,
    future::Future,
    panic::AssertUnwindSafe,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use foundationdb::{
    Database,
    directory::{Directory, DirectoryLayer},
};
use futures_util::{FutureExt, StreamExt};
use jiff::Timestamp;
use swarmy_bus::{Bus, Config, SubjectToken};
use swarmy_core::{
    AgentId, LeaseOwnerId, Nudge, RunnableEntry, SessionId, SessionRecord, SessionState, WakeReply,
    decode,
};
use swarmy_store::{Store, blob::MemoryBlobStore, runnable_partition};
use tokio::time::{Instant, sleep, timeout};
use ulid::Ulid;

const SCAN: Duration = Duration::from_millis(200);
const RESEND: Duration = Duration::from_millis(800);
const WAIT: Duration = Duration::from_secs(15);

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[derive(Clone)]
struct Fixture {
    store: Store,
    bus: Bus,
    admin: async_nats::Client,
    cluster: String,
    url: String,
    directory: String,
    prefix: String,
    prefixes: Arc<Mutex<Vec<String>>>,
    processes: Arc<Mutex<Vec<Process>>>,
}

impl Fixture {
    async fn start(&self, partitions: &str, prefix: &str) -> usize {
        self.start_with_retention(partitions, prefix, 86400).await
    }

    async fn start_with_retention(&self, partitions: &str, prefix: &str, retention: u64) -> usize {
        let child = Command::new(env!("CARGO_BIN_EXE_swarmy-scheduler"))
            .env("SWARMY_FDB_CLUSTER_FILE", &self.cluster)
            .env("SWARMY_NATS_URL", &self.url)
            .env("SWARMY_STORE_DIRECTORY", &self.directory)
            .env("SWARMY_BUS_PREFIX", prefix)
            .env("SWARMY_SCHEDULER_PARTITIONS", partitions)
            .env(
                "SWARMY_SCHEDULER_SCAN_INTERVAL_MS",
                SCAN.as_millis().to_string(),
            )
            .env(
                "SWARMY_SCHEDULER_RESEND_INTERVAL_MS",
                RESEND.as_millis().to_string(),
            )
            .env("SWARMY_EPHEMERAL_RETENTION_SECONDS", retention.to_string())
            .env("RUST_LOG", "warn")
            .env("TOKIO_WORKER_THREADS", "2")
            .stdin(Stdio::null())
            .spawn()
            .unwrap();
        let index = {
            let mut processes = self.processes.lock().unwrap();
            let index = processes.len();
            processes.push(Process(child));
            index
        };
        let bus = self.bus_for(prefix).await;
        timeout(WAIT, async {
            loop {
                assert!(
                    self.processes.lock().unwrap()[index]
                        .0
                        .try_wait()
                        .unwrap()
                        .is_none(),
                    "scheduler exited during startup"
                );
                if matches!(bus.request_wake(id(), SCAN).await, Ok(WakeReply::NotFound)) {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("scheduler did not start");
        index
    }

    fn kill(&self, index: usize) {
        let mut processes = self.processes.lock().unwrap();
        processes[index].0.kill().unwrap();
        let status = processes[index].0.wait().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(9));
        }
    }

    async fn bus_for(&self, prefix: &str) -> Bus {
        self.prefixes.lock().unwrap().push(prefix.to_owned());
        Bus::connect(
            &self.url,
            Config {
                prefix: Some(SubjectToken::new(prefix).unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    }

    async fn observe(&self, prefix: &str) -> async_nats::Subscriber {
        let observer = self
            .admin
            .subscribe(format!("{prefix}.sched.runnable.*"))
            .await
            .unwrap();
        // A request to our own inbox confirms the server processed the subscription.
        let inbox = self.admin.new_inbox();
        self.admin
            .send_request(inbox.clone(), async_nats::Request::new().inbox(inbox))
            .await
            .unwrap();
        observer
    }

    async fn create(&self, partition: u16, state: SessionState, wake_at: Timestamp) -> SessionId {
        let session_id = std::iter::repeat_with(id)
            .find(|&id| runnable_partition(id) == partition)
            .unwrap();
        self.store
            .create_session(
                &SessionRecord {
                    session_id,
                    agent_id: AgentId::from_ulid(Ulid::generate()),
                    state,
                    head_seq: 0,
                    snapshot_ref: None,
                    kind: swarmy_core::SessionKind::Ephemeral,
                    computer_deleted: false,
                },
                wake_at,
                image_fixture::image(&self.store).await,
            )
            .await
            .unwrap();
        session_id
    }

    async fn state(&self, session_id: SessionId) -> SessionState {
        self.store
            .fetch_session(session_id)
            .await
            .unwrap()
            .unwrap()
            .state
    }

    async fn cleanup(&self) {
        self.processes.lock().unwrap().clear();
        let db = Database::new(Some(&self.cluster)).unwrap();
        let path = vec![self.directory.clone()];
        db.run(|trx, _| {
            let path = &path;
            async move {
                DirectoryLayer::default()
                    .remove_if_exists(&trx, path)
                    .await?;
                Ok(())
            }
        })
        .await
        .unwrap();
        let context = async_nats::jetstream::new(self.admin.clone());
        let prefixes: HashSet<_> = self.prefixes.lock().unwrap().drain(..).collect();
        for prefix in prefixes {
            for stream in ["INFER_REQ", "SCHED_RUNNABLE", "TOOL_REMOTE", "TOOL_NODE"] {
                if let Err(error) = context.delete_stream(format!("{prefix}_{stream}")).await {
                    assert!(
                        matches!(error.kind(), async_nats::jetstream::context::DeleteStreamErrorKind::JetStream(ref e) if e.code() == 404),
                        "cleanup failed: {error}"
                    );
                }
            }
        }
    }
}

fn id() -> SessionId {
    SessionId::from_ulid(Ulid::generate())
}

async fn run<F: Future<Output = ()>>(test: impl FnOnce(Fixture) -> F) {
    static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();
    let (Ok(cluster), Ok(url)) = (
        std::env::var("SWARMY_FDB_CLUSTER_FILE"),
        std::env::var("SWARMY_NATS_URL"),
    ) else {
        eprintln!(
            "skipping scheduler integration test: SWARMY_FDB_CLUSTER_FILE or SWARMY_NATS_URL is unset"
        );
        return;
    };
    NETWORK.get_or_init(swarmy_store::boot);
    let prefix = Ulid::generate().to_string();
    let directory = format!("scheduler-test-{prefix}");
    let fixture = Fixture {
        store: Store::open(
            Some(&cluster),
            Some(std::slice::from_ref(&directory)),
            Arc::new(MemoryBlobStore::default()),
        )
        .await
        .unwrap(),
        bus: Bus::connect(
            &url,
            Config {
                prefix: Some(SubjectToken::new(&prefix).unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
        admin: async_nats::connect(&url).await.unwrap(),
        cluster,
        url,
        directory,
        prefixes: Arc::new(Mutex::new(vec![prefix.clone()])),
        prefix,
        processes: Arc::default(),
    };
    let result = AssertUnwindSafe(timeout(Duration::from_secs(60), test(fixture.clone())))
        .catch_unwind()
        .await;
    fixture.cleanup().await;
    match result {
        Ok(result) => result.expect("scheduler test timed out"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

async fn next(observer: &mut async_nats::Subscriber) -> SessionId {
    let message = timeout(WAIT, observer.next()).await.unwrap().unwrap();
    let nudge: Nudge = decode(&message.payload).unwrap();
    assert!(message.subject.ends_with(&format!(
        ".sched.runnable.{}",
        runnable_partition(nudge.session_id)
    )));
    nudge.session_id
}

#[tokio::test]
async fn runnable_is_nudged_within_one_scan_and_resent_after_the_interval() {
    run(|f| async move {
        let mut observer = f.observe(&f.prefix).await;
        f.start("7", &f.prefix).await;
        let session = f.create(7, SessionState::Runnable, Timestamp::now()).await;
        assert_eq!(
            timeout(SCAN, next(&mut observer))
                .await
                .expect("missed scan interval"),
            session
        );
        let first = Instant::now();
        assert!(
            timeout(RESEND / 2, observer.next()).await.is_err(),
            "resent too soon"
        );
        assert_eq!(timeout(RESEND, next(&mut observer)).await.unwrap(), session);
        assert!(first.elapsed() >= RESEND.checked_sub(Duration::from_millis(50)).unwrap());
    })
    .await;
}

#[tokio::test]
async fn expired_leases_are_reaped_across_pages_and_live_leases_survive() {
    run(|f| async move {
        let mut expected = HashSet::new();
        for _ in 0..65 {
            let session = f.create(7, SessionState::Runnable, Timestamp::now()).await;
            f.store
                .claim_lease(
                    session,
                    LeaseOwnerId::from_ulid(Ulid::generate()),
                    Timestamp::UNIX_EPOCH,
                )
                .await
                .unwrap();
            expected.insert(session);
        }
        let live = f.create(7, SessionState::Runnable, Timestamp::now()).await;
        f.store
            .claim_lease(
                live,
                LeaseOwnerId::from_ulid(Ulid::generate()),
                Timestamp::now()
                    .checked_add(Duration::from_secs(60))
                    .unwrap(),
            )
            .await
            .unwrap();
        let mut observer = f.observe(&f.prefix).await;
        f.start("7", &f.prefix).await;
        while !expected.is_empty() {
            let session = next(&mut observer).await;
            assert_ne!(session, live);
            expected.remove(&session);
            assert_eq!(f.state(session).await, SessionState::Runnable);
        }
        assert_eq!(f.state(live).await, SessionState::Leased);
    })
    .await;
}

#[tokio::test]
async fn disjoint_instances_only_nudge_and_reap_their_owned_partitions() {
    run(|f| async move {
        // Separate bus prefixes let the observer identify the publishing process.
        // Both schedulers scan the same FoundationDB directory.
        let other_prefix = Ulid::generate().to_string();
        let mut first = f.observe(&f.prefix).await;
        let mut second = f.observe(&other_prefix).await;
        f.start("0-127", &f.prefix).await;
        f.start("128-255", &other_prefix).await;
        let a = f.create(0, SessionState::Runnable, Timestamp::now()).await;
        let b = f
            .create(255, SessionState::Runnable, Timestamp::now())
            .await;
        assert_eq!(next(&mut first).await, a);
        assert_eq!(next(&mut second).await, b);
        for session in [a, b] {
            f.store
                .claim_lease(
                    session,
                    LeaseOwnerId::from_ulid(Ulid::generate()),
                    Timestamp::UNIX_EPOCH,
                )
                .await
                .unwrap();
        }
        let observation = async {
            loop {
                tokio::select! {
                    session = next(&mut first) => assert_eq!(session, a),
                    session = next(&mut second) => assert_eq!(session, b),
                }
            }
        };
        assert!(timeout(RESEND * 2, observation).await.is_err());
        assert_eq!(f.state(a).await, SessionState::Runnable);
        assert_eq!(f.state(b).await, SessionState::Runnable);
    })
    .await;
}

#[tokio::test]
async fn restart_rediscovers_every_runnable_session_across_pages() {
    run(|f| async move {
        let process = f.start("7", &f.prefix).await;
        let mut expected = HashSet::new();
        for _ in 0..65 {
            expected.insert(f.create(7, SessionState::Runnable, Timestamp::now()).await);
        }
        f.kill(process);
        // A fresh core subscription cannot see persisted messages from before death.
        let mut observer = f.observe(&f.prefix).await;
        f.start("7", &f.prefix).await;
        while !expected.is_empty() {
            expected.remove(&next(&mut observer).await);
        }
    })
    .await;
}

#[tokio::test]
async fn wake_request_makes_idle_runnable_and_preserves_active_sessions() {
    run(|f| async move {
        let mut observer = f.observe(&f.prefix).await;
        f.start("7", &f.prefix).await;
        let session = f.create(7, SessionState::Idle, Timestamp::now()).await;
        assert_eq!(
            f.bus.request_wake(session, WAIT).await.unwrap(),
            WakeReply::Runnable
        );
        assert_eq!(f.state(session).await, SessionState::Runnable);
        assert_eq!(next(&mut observer).await, session);
        assert_eq!(
            f.bus.request_wake(session, WAIT).await.unwrap(),
            WakeReply::Runnable
        );
        let lease = f
            .store
            .claim_lease(
                session,
                LeaseOwnerId::from_ulid(Ulid::generate()),
                Timestamp::now()
                    .checked_add(Duration::from_secs(60))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            f.bus.request_wake(session, WAIT).await.unwrap(),
            WakeReply::Unchanged(SessionState::Leased)
        );
        f.store
            .set_state(
                session,
                SessionState::WaitingInference,
                Some(&lease),
                Timestamp::now(),
            )
            .await
            .unwrap();
        assert_eq!(
            f.bus.request_wake(session, WAIT).await.unwrap(),
            WakeReply::Unchanged(SessionState::WaitingInference)
        );
        assert_eq!(f.state(session).await, SessionState::WaitingInference);
        assert_eq!(
            f.bus.request_wake(id(), WAIT).await.unwrap(),
            WakeReply::NotFound
        );
    })
    .await;
}

#[tokio::test]
async fn future_wake_times_do_not_hide_due_entries_with_lower_priority() {
    run(|f| async move {
        let mut observer = f.observe(&f.prefix).await;
        f.start("7", &f.prefix).await;
        let wake_at = Timestamp::now()
            .checked_add(Duration::from_secs(2))
            .unwrap();
        let future = f.create(7, SessionState::Runnable, wake_at).await;
        f.store
            .insert_runnable(&RunnableEntry {
                session_id: future,
                priority: -1,
                wake_at,
            })
            .await
            .unwrap();
        let due = f.create(7, SessionState::Runnable, Timestamp::now()).await;
        assert_eq!(
            f.bus.request_wake(future, WAIT).await.unwrap(),
            WakeReply::Runnable
        );
        assert_eq!(next(&mut observer).await, due);
        loop {
            if next(&mut observer).await == future {
                assert!(Timestamp::now() >= wake_at, "nudged before wake_at");
                break;
            }
        }
    })
    .await;
}

#[tokio::test]
async fn schedulers_share_one_deployment_and_serve_concurrent_wakes() {
    run(|f| async move {
        let mut observer = f.observe(&f.prefix).await;
        tokio::join!(
            Box::pin(f.start("7", &f.prefix)),
            Box::pin(f.start("8", &f.prefix))
        );
        let a = f.create(7, SessionState::Idle, Timestamp::now()).await;
        let b = f.create(8, SessionState::Idle, Timestamp::now()).await;
        let (first, duplicate, second) = tokio::join!(
            f.bus.request_wake(a, WAIT),
            f.bus.request_wake(a, WAIT),
            f.bus.request_wake(b, WAIT),
        );
        for reply in [first, duplicate, second] {
            assert_eq!(reply.unwrap(), WakeReply::Runnable);
        }
        let mut expected = HashSet::from([a, b]);
        while !expected.is_empty() {
            let session = next(&mut observer).await;
            assert!([a, b].contains(&session));
            expected.remove(&session);
        }
        assert_eq!(f.state(a).await, SessionState::Runnable);
        assert_eq!(f.state(b).await, SessionState::Runnable);
    })
    .await;
}

#[tokio::test]
async fn wake_received_by_another_partition_owner_is_found_by_the_owner_scan() {
    run(|f| async move {
        let other_prefix = Ulid::generate().to_string();
        let mut first = f.observe(&f.prefix).await;
        let mut second = f.observe(&other_prefix).await;
        f.start("7", &f.prefix).await;
        f.start("8", &other_prefix).await;
        let session = f.create(8, SessionState::Idle, Timestamp::now()).await;
        assert_eq!(
            f.bus.request_wake(session, WAIT).await.unwrap(),
            WakeReply::Runnable
        );
        assert_eq!(next(&mut second).await, session);
        assert!(timeout(RESEND, first.next()).await.is_err());
    })
    .await;
}

#[tokio::test]
async fn scan_recovers_an_atomic_user_append_whose_nudge_was_lost() {
    run(|f| async move {
        f.start("7", &f.prefix).await;
        let session = f.create(7, SessionState::Idle, Timestamp::now()).await;
        let mut observer = f.observe(&f.prefix).await;
        let message = swarmy_core::Message {
            id: swarmy_core::MessageId::from_ulid(Ulid::generate()),
            role: swarmy_core::MessageRole::User,
            parts: vec![swarmy_core::Part::Text {
                text: "lost publication".into(),
            }],
        };
        // Simulate a client dying after commit and before its NATS publication.
        f.store
            .append_user_message(session, 0, &message)
            .await
            .unwrap();
        assert_eq!(
            timeout(SCAN * 3, next(&mut observer)).await.unwrap(),
            session
        );
        assert_eq!(f.state(session).await, SessionState::Runnable);
    })
    .await;
}

#[tokio::test]
async fn timer_closes_only_idle_ephemeral_sessions() {
    run(|f| async move {
        let old = Timestamp::now()
            .checked_sub(Duration::from_secs(10))
            .unwrap();
        let idle = f.create(7, SessionState::Idle, old).await;
        let active = f.create(7, SessionState::Runnable, old).await;
        let agent = f
            .store
            .create_agent("named", image_fixture::image(&f.store).await, "", old)
            .await
            .unwrap();
        let named = f
            .store
            .create_session_for_agent(id(), Some(agent.agent_id), None, old)
            .await
            .unwrap();
        f.start_with_retention("7", &f.prefix, 1).await;
        timeout(WAIT, async {
            while f.state(idle).await != SessionState::Completed {
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("ephemeral timer did not close old idle session");
        assert_eq!(f.state(active).await, SessionState::Runnable);
        assert_eq!(f.state(named.session_id).await, SessionState::Idle);
        assert!(
            f.store
                .fetch_session(idle)
                .await
                .unwrap()
                .unwrap()
                .computer_deleted
        );
        assert!(
            !f.store
                .fetch_session(active)
                .await
                .unwrap()
                .unwrap()
                .computer_deleted
        );
        assert!(
            !f.store
                .fetch_session(named.session_id)
                .await
                .unwrap()
                .unwrap()
                .computer_deleted
        );
    })
    .await;
}
