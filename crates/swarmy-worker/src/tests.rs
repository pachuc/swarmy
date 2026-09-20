#[path = "../../swarmy-store/tests/support/mod.rs"]
mod image_fixture;

use std::{
    collections::BTreeSet,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use foundationdb::{
    Database,
    directory::{Directory, DirectoryLayer},
};
use futures::future::BoxFuture;
use jiff::Timestamp;
use serde_json::{Value, json};
use swarmy_bus::{Bus, Config as BusConfig, SubjectToken, WorkQueue};
use swarmy_core::{
    AgentId, Event, Message, MessageId, MessageRole, Nudge, Part, RequestId, SessionId,
    SessionRecord, SessionState, ToolCallId, ToolCallRecord,
};
use swarmy_harness::{Harness, Tool, ToolRegistry, execution_result};
use swarmy_llm::GenerationSettings;
use swarmy_store::{Store, blob::MemoryBlobStore};
use tokio::time::{sleep, timeout};
use ulid::Ulid;

use crate::{config::Config, worker::Worker};

static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();

struct SlowTool(Arc<AtomicUsize>);
impl Tool for SlowTool {
    fn name(&self) -> &'static str {
        "slow"
    }
    fn description(&self) -> &'static str {
        "Exercise renewal while a local tool is running."
    }
    fn parameters(&self) -> Value {
        json!({"type": "object"})
    }
    fn execute(&self, _: Value) -> BoxFuture<'_, Result<String, String>> {
        Box::pin(async {
            self.0.fetch_add(1, Ordering::SeqCst);
            sleep(Duration::from_millis(1600)).await;
            Ok("done".into())
        })
    }
}

fn config(cluster: String, url: String, prefix: &str, calls: Arc<AtomicUsize>) -> Config {
    let mut tools = ToolRegistry::default();
    tools.register(Box::new(SlowTool(calls)));
    Config {
        cluster,
        directory: vec![prefix.into()],
        nats: url,
        bus: BusConfig {
            prefix: Some(SubjectToken::new(prefix).unwrap()),
            ..Default::default()
        },
        partitions: BTreeSet::from([7]),
        provider: "fake".into(),
        lease_duration: Duration::from_millis(600),
        placement_lease: Duration::from_secs(30),
        recovery_interval: Duration::from_secs(5),
        harness: Harness {
            system_prompt_template: String::new(),
            settings: GenerationSettings::default(),
            tools,
        },
        summarize_at_tokens: Some(300_000),
        model_context_window_tokens: None,
        catalog: swarmy_llm::catalog::Catalog::get().clone(),
        memory_dir: "/home/agent/memory".into(),
        memory_max_bytes: 32768,
        kill_point: None,
    }
}

fn partial_batch(id: SessionId) -> Vec<Event> {
    let first = ToolCallRecord {
        call_id: ToolCallId("first".into()),
        tool: "slow".into(),
        arguments: json!({}),
        result: None,
    };
    let second = ToolCallRecord {
        call_id: ToolCallId("second".into()),
        ..first.clone()
    };
    let parts = [&first, &second]
        .into_iter()
        .map(|call| Part::ToolCall {
            call_id: call.call_id.clone(),
            tool: call.tool.clone(),
            input: call.arguments.clone(),
        })
        .collect();
    vec![
        Event::InferenceCompleted {
            provider: String::new(),
            model: String::new(),
            effort_used: None,
            usage: swarmy_core::TokenUsage::default(),
            cost_micros: 0,
            effort_requested: None,
            effort_clamped: false,
            seq: 0,
            request_id: RequestId::for_step(id, 1),
            message: Message {
                id: MessageId::from_ulid(Ulid::generate()),
                role: MessageRole::Assistant,
                parts,
            },
        },
        Event::ToolCallRequested {
            seq: 0,
            request_id: RequestId::for_step(id, 2),
            call: first.clone(),
        },
        Event::ToolCallRequested {
            seq: 0,
            request_id: RequestId::for_step(id, 3),
            call: second,
        },
        Event::ToolCallCompleted {
            seq: 0,
            request_id: RequestId::for_step(id, 2),
            call_id: first.call_id,
            result: execution_result("slow", Ok("already done".into())),
        },
    ]
}

#[tokio::test]
async fn partial_tool_batch_resumes_with_lease_renewal() {
    let (Ok(cluster), Ok(url)) = (
        std::env::var("SWARMY_FDB_CLUSTER_FILE"),
        std::env::var("SWARMY_NATS_URL"),
    ) else {
        eprintln!("skipping slow-tool test: SWARMY_FDB_CLUSTER_FILE or SWARMY_NATS_URL is unset");
        return;
    };
    NETWORK.get_or_init(swarmy_store::boot);
    let prefix = format!("worker_slow_{}", Ulid::generate());
    let calls = Arc::new(AtomicUsize::new(0));
    let config = config(cluster.clone(), url.clone(), &prefix, calls.clone());
    let blobs = Arc::new(MemoryBlobStore::default());
    let store = Store::open(Some(&cluster), Some(&config.directory), blobs.clone())
        .await
        .unwrap();
    let bus = Bus::connect(&url, config.bus.clone()).await.unwrap();
    let queue = WorkQueue::Runnable(7);
    bus.setup(std::slice::from_ref(&queue)).await.unwrap();
    let mut messages = bus.consume::<Nudge>(&queue).await.unwrap();
    let id = SessionId::from_ulid(Ulid::generate());
    store
        .create_session(
            &SessionRecord {
                session_id: id,
                agent_id: AgentId::from_ulid(Ulid::generate()),
                state: SessionState::Runnable,
                head_seq: 0,
                snapshot_ref: None,
                inference: swarmy_core::InferenceSelection::default(),
                kind: swarmy_core::SessionKind::Ephemeral,
                computer_deleted: false,
                plan: Vec::new(),
            },
            Timestamp::now(),
            image_fixture::image(&store).await,
        )
        .await
        .unwrap();
    store
        .append_events(id, 0, &partial_batch(id))
        .await
        .unwrap();
    bus.publish_work(&queue, &Nudge { session_id: id })
        .await
        .unwrap();
    let delivery = timeout(Duration::from_secs(10), messages.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let worker = Worker::new(store.clone(), bus, blobs, config);
    let work = worker.handle(&delivery);
    tokio::pin!(work);
    let mut expiries = BTreeSet::new();
    timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                result = &mut work => { result.unwrap(); break; }
                () = sleep(Duration::from_millis(50)) => {
                    let now = Timestamp::now();
                    assert!(store.scan_expired_leases(now, None, 64).await.unwrap().is_empty(), "slow tool lost its lease");
                    for (_, lease) in store.scan_expired_leases(now.checked_add(Duration::from_secs(10)).unwrap(), None, 64).await.unwrap() {
                        assert_eq!(lease.owner, worker.owner);
                        expiries.insert(lease.expires_at);
                    }
                }
            }
        }
    }).await.unwrap();
    assert!(expiries.len() >= 3, "lease was not renewed repeatedly");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "completed tool was executed again"
    );
    let events = store.read_events(id, 0, 64).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Event::ToolCallRequested { .. }))
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Event::ToolCallCompleted { .. }))
            .count(),
        2
    );
    assert_eq!(
        store.fetch_session(id).await.unwrap().unwrap().state,
        SessionState::WaitingInference
    );
    cleanup(&cluster, &url, &prefix).await;
}

async fn cleanup(cluster: &str, url: &str, prefix: &str) {
    let db = Database::new(Some(cluster)).unwrap();
    let path = vec![prefix.to_owned()];
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
    let context = async_nats::jetstream::new(async_nats::connect(url).await.unwrap());
    for stream in ["INFER_REQ", "SCHED_RUNNABLE", "TOOL_REMOTE", "TOOL_NODE"] {
        context
            .delete_stream(format!("{prefix}_{stream}"))
            .await
            .unwrap();
    }
}

mod routing;

#[tokio::test]
async fn deleted_computer_refuses_remote_tools_with_durable_message() {
    let (Ok(cluster), Ok(url)) = (
        std::env::var("SWARMY_FDB_CLUSTER_FILE"),
        std::env::var("SWARMY_NATS_URL"),
    ) else {
        eprintln!("skipping slow-tool test: SWARMY_FDB_CLUSTER_FILE or SWARMY_NATS_URL is unset");
        return;
    };
    NETWORK.get_or_init(swarmy_store::boot);
    let prefix = format!("worker_slow_{}", Ulid::generate());
    let calls = Arc::new(AtomicUsize::new(0));
    let config = config(cluster.clone(), url.clone(), &prefix, calls.clone());
    let blobs = Arc::new(MemoryBlobStore::default());
    let store = Store::open(Some(&cluster), Some(&config.directory), blobs.clone())
        .await
        .unwrap();
    let bus = Bus::connect(&url, config.bus.clone()).await.unwrap();
    let queue = WorkQueue::Runnable(7);
    bus.setup(std::slice::from_ref(&queue)).await.unwrap();
    let mut messages = bus.consume::<Nudge>(&queue).await.unwrap();
    let id = SessionId::from_ulid(Ulid::generate());
    store
        .create_session(
            &SessionRecord {
                session_id: id,
                agent_id: AgentId::from_ulid(Ulid::generate()),
                state: SessionState::Runnable,
                head_seq: 0,
                snapshot_ref: None,
                inference: swarmy_core::InferenceSelection::default(),
                kind: swarmy_core::SessionKind::Ephemeral,
                computer_deleted: false,
                plan: Vec::new(),
            },
            Timestamp::now(),
            image_fixture::image(&store).await,
        )
        .await
        .unwrap();
    store
        .append_events(id, 0, &partial_batch(id))
        .await
        .unwrap();
    bus.publish_work(&queue, &Nudge { session_id: id })
        .await
        .unwrap();
    let delivery = timeout(Duration::from_secs(10), messages.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let worker = Worker::new(store.clone(), bus, blobs, config);
    let agent = store.fetch_session(id).await.unwrap().unwrap().agent_id;
    store.delete_computer(agent).await.unwrap();
    worker.handle(&delivery).await.unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "deleted computer ran a tool"
    );
    let events = store.read_events(id, 0, 64).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Event::ToolCallRequested { .. }))
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Event::ToolCallCompleted { .. }))
            .count(),
        2
    );
    assert_eq!(
        store.fetch_session(id).await.unwrap().unwrap().state,
        SessionState::WaitingInference
    );
    assert!(events.iter().any(|event| matches!(event,
        Event::ToolCallCompleted { result: swarmy_core::ToolResult::Error { error }, .. }
        if error == "This session's computer has been deleted. Create a new session to run tools.")));
    cleanup(&cluster, &url, &prefix).await;
}

mod agent_settings;

#[test]
fn summarization_threshold_uses_catalog_model_and_explicit_overrides() {
    let mut config = config(
        String::new(),
        String::new(),
        "context_fixture",
        Arc::default(),
    );
    config.catalog = swarmy_config::Settings {
        models: vec![swarmy_config::CustomModel {
            provider: "fake".into(),
            id: "small-context".into(),
            context_window: Some(1000),
            ..Default::default()
        }],
        ..Default::default()
    }
    .catalog()
    .unwrap();
    config.summarize_at_tokens = None;
    assert_eq!(
        config.summarization_threshold("fake", "small-context"),
        Some(750)
    );
    assert_eq!(config.summarization_threshold("fake", "unknown"), None);
    config.model_context_window_tokens = Some(2000);
    assert_eq!(
        config.summarization_threshold("fake", "small-context"),
        Some(1500)
    );
    config.summarize_at_tokens = Some(100);
    assert_eq!(
        config.summarization_threshold("fake", "small-context"),
        Some(100)
    );
}
