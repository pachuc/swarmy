#[path = "../../swarmy-store/tests/support/mod.rs"]
mod image_fixture;

use std::{
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, OnceLock},
    time::Duration,
};

use foundationdb::{
    Database,
    directory::{Directory, DirectoryLayer},
};
use futures::FutureExt;
use jiff::Timestamp;
use swarmy_bus::{Bus, Config, LiveFeed, SubjectToken, WorkQueue};
use swarmy_core::{
    AgentId, Event, IdempotencyState, InflightRecord, LeaseOwnerId, Part, RequestId, SessionId,
    SessionRecord, SessionState, encode,
};
use swarmy_llm::{
    Delta, GenerationSettings, InferenceJob, Request, Response, StopReason, TokenUsage,
};
use swarmy_store::{
    CredentialKey, Store,
    blob::{BlobStore, ObjectBlobStore},
};
use tempfile::TempDir;
use tokio::{
    process::{Child, Command},
    time::{sleep, timeout},
};
use ulid::Ulid;

const ACK_WAIT: Duration = Duration::from_millis(600);
const WAIT: Duration = Duration::from_secs(20);

struct Fixture {
    store: Store,
    bus: Bus,
    queue: WorkQueue,
    prefix: String,
    files: TempDir,
    children: Vec<Child>,
}

impl Fixture {
    async fn new() -> Option<Self> {
        static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();
        for variable in [
            "SWARMY_FDB_CLUSTER_FILE",
            "SWARMY_NATS_URL",
            "SWARMY_S3_ENDPOINT",
        ] {
            if std::env::var(variable).is_err() {
                eprintln!("skipping gateway integration test: {variable} is unset");
                return None;
            }
        }
        NETWORK.get_or_init(swarmy_store::boot);
        let prefix = format!("gateway_{}", Ulid::generate());
        let store = Store::open(
            Some(&std::env::var("SWARMY_FDB_CLUSTER_FILE").unwrap()),
            Some(std::slice::from_ref(&prefix)),
            Arc::new(ObjectBlobStore::from_env().unwrap()),
        )
        .await
        .unwrap();
        let bus = Bus::connect(
            &std::env::var("SWARMY_NATS_URL").unwrap(),
            Config {
                prefix: Some(SubjectToken::new(&prefix).unwrap()),
                ack_wait: ACK_WAIT,
                max_deliver: 3,
            },
        )
        .await
        .unwrap();
        let queue = WorkQueue::Inference(SubjectToken::new("fake").unwrap());
        bus.setup(std::slice::from_ref(&queue)).await.unwrap();
        Some(Self {
            store,
            bus,
            queue,
            prefix,
            files: TempDir::new().unwrap(),
            children: Vec::new(),
        })
    }

    fn script(&self, latency_ms: u64, fail: bool, text: &str) -> Response {
        let response = Response {
            parts: vec![Part::Text { text: text.into() }],
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage {
                output_tokens: 42,
                ..TokenUsage::default()
            },
        };
        let responses: std::collections::BTreeMap<_, _> =
            (0..10).map(|turn| (turn, &response)).collect();
        std::fs::write(
            self.files.path().join("script.json"),
            serde_json::to_vec(&serde_json::json!({
                "latency_ms": latency_ms, "fail": fail, "responses": responses,
            }))
            .unwrap(),
        )
        .unwrap();
        response
    }

    fn failure_script(&self, status: u16, retry_after_seconds: Option<u64>) {
        std::fs::write(
            self.files.path().join("script.json"),
            serde_json::to_vec(&serde_json::json!({
                "failures": {"0": {
                    "status": status,
                    "message": if status == 429 { "quota reached" } else { "invalid credentials" },
                    "retry_after_seconds": retry_after_seconds,
                }}
            }))
            .unwrap(),
        )
        .unwrap();
    }

    fn start(&mut self, concurrency: usize) {
        self.start_with(concurrency, "fake", &std::collections::BTreeMap::new(), &[]);
    }

    fn start_with(
        &mut self,
        concurrency: usize,
        providers: &str,
        custom_providers: &std::collections::BTreeMap<String, swarmy_config::CustomProvider>,
        models: &[swarmy_config::CustomModel],
    ) {
        self.children.push(
            Command::new(env!("CARGO_BIN_EXE_swarmy-gateway"))
                .env("SWARMY_PROVIDER", "fake")
                .env("SWARMY_PROVIDERS", providers)
                .env(
                    "SWARMY_CUSTOM_PROVIDERS",
                    serde_json::to_string(custom_providers).unwrap(),
                )
                .env("SWARMY_MODELS", serde_json::to_string(models).unwrap())
                .env("SWARMY_STORE_DIRECTORY", &self.prefix)
                .env("SWARMY_BUS_PREFIX", &self.prefix)
                .env("SWARMY_BUS_ACK_WAIT_MS", ACK_WAIT.as_millis().to_string())
                .env("SWARMY_BUS_MAX_DELIVER", "3")
                .env("SWARMY_GATEWAY_CONCURRENCY", concurrency.to_string())
                .env("SWARMY_FAKE_SCRIPT", self.files.path().join("script.json"))
                .env("SWARMY_FAKE_CALL_LOG", self.files.path().join("calls"))
                .kill_on_drop(true)
                .spawn()
                .unwrap(),
        );
    }

    async fn kill(&mut self) {
        for child in &mut self.children {
            child.kill().await.unwrap();
        }
        self.children.clear();
    }

    fn calls(&self) -> usize {
        std::fs::read_to_string(self.files.path().join("calls"))
            .unwrap_or_default()
            .lines()
            .count()
    }

    async fn job(&self) -> InferenceJob {
        self.job_for_agent(AgentId::from_ulid(Ulid::generate()))
            .await
    }

    async fn job_for_agent(&self, agent_id: AgentId) -> InferenceJob {
        let session_id = SessionId::from_ulid(Ulid::generate());
        let now = Timestamp::now();
        let settings = swarmy_config::Settings {
            default_image: Some(image_fixture::image(&self.store).await.into()),
            ..Default::default()
        };
        self.store
            .create_session(
                &SessionRecord {
                    interrupt_requested: false,
                    session_id,
                    agent_id,
                    state: SessionState::Runnable,
                    head_seq: 0,
                    snapshot_ref: None,
                    inference: swarmy_core::InferenceSelection::default(),
                    kind: swarmy_core::SessionKind::Ephemeral,
                    computer_deleted: false,
                    plan: Vec::new(),
                },
                now,
                settings.session_image(None).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            self.store.session_image(session_id).await.unwrap(),
            self.store
                .get_image("fixture", &swarmy_core::ImageTag("test".into()))
                .await
                .unwrap()
        );
        let lease = self
            .store
            .claim_lease(
                session_id,
                LeaseOwnerId::from_ulid(Ulid::generate()),
                now.checked_add(Duration::from_secs(60)).unwrap(),
            )
            .await
            .unwrap();
        let request_id = RequestId::for_step(session_id, lease.seq);
        self.store
            .append_events(
                session_id,
                0,
                &[Event::InferenceRequested {
                    seq: 0,
                    request_id,
                    step: lease.seq,
                }],
            )
            .await
            .unwrap();
        self.store
            .put_inflight(
                request_id,
                &InflightRecord {
                    session_id,
                    seq: lease.seq,
                    provider: "fake".into(),
                    key_id: "fake".into(),
                },
            )
            .await
            .unwrap();
        self.store
            .set_state(
                session_id,
                SessionState::WaitingInference,
                Some(&lease),
                now,
            )
            .await
            .unwrap();
        InferenceJob {
            provider: "fake".into(),
            session_id,
            step: lease.seq,
            request_id,
            request: Request {
                system_prompt: String::new(),
                messages: Vec::new(),
                tools: Vec::new(),
                settings: GenerationSettings::default(),
            },
        }
    }

    async fn publish(&self, job: &InferenceJob) {
        self.bus.publish_work(&self.queue, job).await.unwrap();
    }

    async fn terminal(&self, job: &InferenceJob) -> Event {
        timeout(WAIT, async {
            loop {
                let events = self.store.read_events(job.session_id, 1, 64).await.unwrap();
                if let Some(event) = events.first() {
                    let idle = matches!(event, Event::InferenceCompleted { message, .. }
                        if !message.parts.iter().any(|part| matches!(part, Part::ToolCall { .. })));
                    assert_eq!(events.len(), if idle { 2 } else { 1 });
                    let session = self
                        .store
                        .fetch_session(job.session_id)
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(
                        session.state,
                        if idle {
                            SessionState::Idle
                        } else {
                            SessionState::Runnable
                        }
                    );
                    if idle {
                        assert!(matches!(
                            events.last(),
                            Some(Event::StateChanged {
                                to: SessionState::Idle,
                                ..
                            })
                        ));
                        let reference = session.snapshot_ref.unwrap();
                        assert_eq!(reference.seq, session.head_seq);
                        let bytes = ObjectBlobStore::from_env()
                            .unwrap()
                            .get(&reference.object_key)
                            .await
                            .unwrap();
                        let snapshot: swarmy_harness::Snapshot =
                            swarmy_core::decode(&bytes).unwrap();
                        let Event::InferenceCompleted { message, .. } = event else {
                            unreachable!()
                        };
                        assert_eq!(snapshot.messages().last(), Some(message));
                        assert_eq!(
                            &snapshot.messages()[..snapshot.messages().len() - 1],
                            job.request.messages
                        );
                    }
                    assert!(
                        self.store
                            .get_inflight(job.request_id)
                            .await
                            .unwrap()
                            .is_none()
                    );
                    assert_eq!(
                        self.store
                            .get_idempotency(job.request_id)
                            .await
                            .unwrap()
                            .unwrap()
                            .state,
                        IdempotencyState::Completed
                    );
                    return event.clone();
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap()
    }

    async fn drained(&self) {
        let context = async_nats::jetstream::new(
            async_nats::connect(std::env::var("SWARMY_NATS_URL").unwrap())
                .await
                .unwrap(),
        );
        timeout(WAIT, async {
            loop {
                if context
                    .get_stream(format!("{}_INFER_REQ", self.prefix))
                    .await
                    .unwrap()
                    .cached_info()
                    .state
                    .messages
                    == 0
                {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
    }

    async fn cleanup(mut self) {
        self.kill().await;
        let db = Database::new(Some(&std::env::var("SWARMY_FDB_CLUSTER_FILE").unwrap())).unwrap();
        let path = vec![self.prefix.clone()];
        db.run(|trx, _| {
            let path = &path;
            async move {
                DirectoryLayer::default().remove(&trx, path).await?;
                Ok(())
            }
        })
        .await
        .unwrap();
        let context = async_nats::jetstream::new(
            async_nats::connect(std::env::var("SWARMY_NATS_URL").unwrap())
                .await
                .unwrap(),
        );
        for name in ["INFER_REQ", "SCHED_RUNNABLE", "TOOL_REMOTE", "TOOL_NODE"] {
            context
                .delete_stream(format!("{}_{name}", self.prefix))
                .await
                .unwrap();
        }
    }
}

async fn run<F>(test: impl FnOnce(Fixture) -> F)
where
    F: Future<Output = ()>,
{
    if let Some(fixture) = Fixture::new().await {
        test(fixture).await;
    }
}

#[tokio::test]
async fn one_request_completes_and_duplicates_across_gateways_call_once() {
    run(|mut f| async move {
        let expected = f.script(100, false, "hello");
        f.start(4);
        f.start(4);
        let mut job = f.job().await;
        job.request.messages.push(swarmy_core::Message {
            id: swarmy_core::MessageId::from_ulid(Ulid::generate()),
            role: swarmy_core::MessageRole::User,
            parts: vec![Part::Text {
                text: "preserve this history".into(),
            }],
        });
        let result = AssertUnwindSafe(async {
            f.publish(&job).await;
            f.publish(&job).await;
            assert!(matches!(
                f.terminal(&job).await,
                Event::InferenceCompleted { .. }
            ));
            f.drained().await;
            f.publish(&job).await;
            f.drained().await;
            assert_eq!(f.calls(), 1);
            let totals = f.store.session_usage(job.session_id).await.unwrap();
            assert_eq!(totals.usage.output_tokens, 42);
            let session = f
                .store
                .fetch_session(job.session_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(f.store.agent_usage(session.agent_id).await.unwrap(), totals);
            assert_eq!(
                f.store
                    .get_inference_result::<Result<Response, String>>(job.request_id)
                    .await
                    .unwrap(),
                Some(Ok(expected))
            );
        })
        .catch_unwind()
        .await;
        f.cleanup().await;
        result.unwrap();
    })
    .await;
}

#[tokio::test]
async fn kill_mid_stream_redelivers_and_live_deltas_precede_completion() {
    run(|mut f| async move {
        f.script(2_000, false, "partial output");
        f.start(4);
        let job = f.job().await;
        let result = AssertUnwindSafe(async {
            let mut live = f
                .bus
                .subscribe_live::<Delta>(LiveFeed::ModelDeltas(job.session_id))
                .await
                .unwrap();
            f.publish(&job).await;
            let first = timeout(WAIT, live.next()).await.unwrap().unwrap().unwrap();
            assert!(matches!(first, Delta::PartDone { .. }));
            assert!(
                f.store
                    .read_events(job.session_id, 1, 64)
                    .await
                    .unwrap()
                    .is_empty()
            );
            f.kill().await; // Child::kill sends SIGKILL on Unix and waits for exit.
            f.start(4);
            assert!(matches!(
                f.terminal(&job).await,
                Event::InferenceCompleted { .. }
            ));
            f.drained().await;
            assert_eq!(f.calls(), 2);
        })
        .catch_unwind()
        .await;
        f.cleanup().await;
        result.unwrap();
    })
    .await;
}

#[tokio::test]
async fn exhausted_retries_append_failure_and_stop_delivery() {
    run(|mut f| async move {
        f.script(0, true, "unused");
        f.start(4);
        let job = f.job().await;
        let result = AssertUnwindSafe(async {
            f.publish(&job).await;
            assert!(matches!(
                f.terminal(&job).await,
                Event::InferenceFailed { .. }
            ));
            f.drained().await;
            sleep(ACK_WAIT * 3).await;
            assert_eq!(f.calls(), 3);
            let mut work = f.bus.consume::<InferenceJob>(&f.queue).await.unwrap();
            assert!(timeout(ACK_WAIT * 2, work.next()).await.is_err());
        })
        .catch_unwind()
        .await;
        f.cleanup().await;
        result.unwrap();
    })
    .await;
}

#[tokio::test]
async fn rate_limit_opens_durable_breaker_and_keeps_provider_text() {
    run(|mut f| async move {
        f.failure_script(429, Some(2));
        f.start(4);
        let job = f.job().await;
        let result = AssertUnwindSafe(async {
            let started = Timestamp::now();
            f.publish(&job).await;
            let event = f.terminal(&job).await;
            assert!(matches!(event, Event::InferenceFailed { retryable: true, retry_at: Some(at), error, .. }
                if at >= started.checked_add(Duration::from_secs(2)).unwrap() && error.contains("quota reached")));
            let key = CredentialKey("fake".into());
            assert!(f.store.provider_open_until(&key, Timestamp::now()).await.unwrap().is_some());
            assert_eq!(f.calls(), 1);
            f.kill().await;
            f.start(4);
            assert!(f.store.provider_open_until(&key, Timestamp::now()).await.unwrap().is_some());
        }).catch_unwind().await;
        f.cleanup().await;
        result.unwrap();
    }).await;
}

#[tokio::test]
async fn authentication_failure_does_not_open_breaker() {
    run(|mut f| async move {
        f.failure_script(401, None);
        f.start(4);
        let job = f.job().await;
        let result = AssertUnwindSafe(async {
            f.publish(&job).await;
            assert!(matches!(f.terminal(&job).await, Event::InferenceFailed { retryable: false, error, .. } if error.contains("invalid credentials")));
            assert!(f.store.provider_open_until(&CredentialKey("fake".into()), Timestamp::now()).await.unwrap().is_none());
            assert_eq!(f.calls(), 1);
        }).catch_unwind().await;
        f.cleanup().await;
        result.unwrap();
    }).await;
}

#[tokio::test]
async fn concurrency_limit_and_large_responses_preserve_full_results() {
    run(|mut f| async move {
        let text = format!("{}{}", f.prefix, "x".repeat(100 * 1024));
        let expected = f.script(200, false, &text);
        f.start(1);
        let first = f.job().await;
        let second = f.job().await;
        let result = AssertUnwindSafe(async {
            let mut live = f
                .bus
                .subscribe_live::<Delta>(LiveFeed::ModelDeltas(first.session_id))
                .await
                .unwrap();
            f.publish(&first).await;
            f.publish(&second).await;
            timeout(WAIT, live.next()).await.unwrap().unwrap().unwrap();
            assert_eq!(f.calls(), 1);
            for job in [&first, &second] {
                let event = f.terminal(job).await;
                let bytes = encode(&event).unwrap();
                assert!(bytes.len() > swarmy_store::INLINE_LIMIT);
                let blobs = ObjectBlobStore::from_env().unwrap();
                blobs
                    .delete(&format!("blobs/{}", blake3::hash(&bytes).to_hex()))
                    .await
                    .unwrap();
                assert_eq!(
                    f.store
                        .get_inference_result::<Result<Response, String>>(job.request_id)
                        .await
                        .unwrap(),
                    Some(Ok(expected.clone()))
                );
            }
            f.drained().await;
            assert_eq!(f.calls(), 2);
            let bytes = encode(&Ok::<_, String>(expected)).unwrap();
            ObjectBlobStore::from_env()
                .unwrap()
                .delete(&format!("blobs/{}", blake3::hash(&bytes).to_hex()))
                .await
                .unwrap();
        })
        .catch_unwind()
        .await;
        f.cleanup().await;
        result.unwrap();
    })
    .await;
}

#[tokio::test]
async fn invalid_request_id_is_acknowledged_without_provider_call() {
    run(|mut f| async move {
        f.script(0, false, "unused");
        f.start(4);
        let mut job = f.job().await;
        job.request_id = RequestId::for_step(job.session_id, job.step + 1);
        let result = AssertUnwindSafe(async {
            f.publish(&job).await;
            f.drained().await;
            assert_eq!(f.calls(), 0);
            assert!(
                f.store
                    .read_events(job.session_id, 1, 64)
                    .await
                    .unwrap()
                    .is_empty()
            );
        })
        .catch_unwind()
        .await;
        f.cleanup().await;
        result.unwrap();
    })
    .await;
}

#[tokio::test]
async fn terminal_response_leaves_intervening_events_for_worker_replay() {
    run(|mut f| async move {
        f.script(100, false, "answer");
        f.start(4);
        let job = f.job().await;
        let result = AssertUnwindSafe(async {
            let mut live = f
                .bus
                .subscribe_live::<Delta>(LiveFeed::ModelDeltas(job.session_id))
                .await
                .unwrap();
            f.publish(&job).await;
            timeout(WAIT, live.next()).await.unwrap().unwrap().unwrap();
            let notice = Event::MessageAppended {
                seq: 0,
                message: swarmy_core::Message {
                    id: swarmy_core::MessageId::from_ulid(Ulid::generate()),
                    role: swarmy_core::MessageRole::System,
                    parts: vec![Part::Text {
                        text: "concurrent notice".into(),
                    }],
                },
            };
            f.store
                .append_events(job.session_id, 1, &[notice])
                .await
                .unwrap();
            timeout(WAIT, async {
                loop {
                    let session = f
                        .store
                        .fetch_session(job.session_id)
                        .await
                        .unwrap()
                        .unwrap();
                    if session.state == SessionState::Runnable {
                        assert_eq!(session.head_seq, 3);
                        assert!(session.snapshot_ref.is_none());
                        break;
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let events = f.store.read_events(job.session_id, 1, 64).await.unwrap();
            assert!(matches!(
                events.as_slice(),
                [
                    Event::MessageAppended { seq: 2, .. },
                    Event::InferenceCompleted { seq: 3, .. }
                ]
            ));
            f.drained().await;
            assert_eq!(f.calls(), 1);
        })
        .catch_unwind()
        .await;
        f.cleanup().await;
        result.unwrap();
    })
    .await;
}

#[tokio::test]
async fn two_providers_share_one_gateway_and_record_selection_and_cost() {
    run(|mut f| async move {
        f.script(50, false, "routed");
        let model = swarmy_config::CustomModel {
            id: "scripted-model".into(),
            api: Some(swarmy_llm::catalog::Api::Fake),
            cost: Some(swarmy_llm::catalog::Cost { output: 2.0, ..Default::default() }),
            reasoning: Some(vec![swarmy_core::ReasoningEffort::Low]),
            ..Default::default()
        };
        let models: Vec<_> = ["fake", "scripted"].map(|provider| swarmy_config::CustomModel { provider: provider.into(), ..model.clone() }).into();
        let custom_providers = std::collections::BTreeMap::from([(
            "scripted".to_owned(),
            swarmy_config::CustomProvider { api: Some(swarmy_llm::catalog::Api::Fake), base_url: Some("fake://scripted".into()) },
        )]);
        f.start_with(1, "fake,scripted", &custom_providers, &models);
        let result = AssertUnwindSafe(async {
            let queue = WorkQueue::Inference(SubjectToken::new("scripted").unwrap());
            f.bus.setup(std::slice::from_ref(&queue)).await.unwrap();
            let agent_id = AgentId::from_ulid(Ulid::generate());
            let mut first = f.job_for_agent(agent_id).await;
            first.provider = "fake".into();
            first.request.settings.model = model.id.clone();
            first.request.settings.reasoning_effort = Some(swarmy_core::ReasoningEffort::Max);
            let mut second = f.job_for_agent(agent_id).await;
            second.provider = "scripted".into();
            second.request.settings = first.request.settings.clone();
            f.publish(&first).await;
            f.bus.publish_work(&queue, &second).await.unwrap();
            for job in [&first, &second] {
                let event = f.terminal(job).await;
                assert!(matches!(event, Event::InferenceCompleted { provider, model: used_model, effort_used: Some(swarmy_core::ReasoningEffort::Low), effort_requested: Some(swarmy_core::ReasoningEffort::Max), effort_clamped: true, cost_micros: 84, usage, .. } if provider == job.provider && used_model == model.id && usage.output_tokens == 42));
                let total = f.store.session_usage(job.session_id).await.unwrap();
                assert_eq!(total.cost_micros, 84);
            }
            f.drained().await;
            assert_eq!(f.calls(), 2);
            let agent_usage = f.store.agent_usage(agent_id).await.unwrap();
            assert_eq!(agent_usage.cost_micros, 168);
            assert_eq!(agent_usage.usage.output_tokens, 84);
            f.publish(&first).await;
            f.drained().await;
            assert_eq!(f.store.agent_usage(agent_id).await.unwrap(), agent_usage);
            assert!(f.store.gateway_serves("scripted").await.unwrap());
            assert_eq!(f.store.gateway_provider("scripted").await.unwrap().unwrap().reason, "credentials resolved");
        }).catch_unwind().await;
        f.cleanup().await;
        result.unwrap();
    }).await;
}

#[tokio::test]
async fn unknown_model_is_a_permanent_failure_without_provider_calls() {
    run(|mut f| async move {
        f.script(0, false, "unused");
        f.start(1);
        let mut job = f.job().await;
        job.provider = "fake".into();
        job.request.settings.model = "unknown-model".into();
        let result = AssertUnwindSafe(async {
            f.publish(&job).await;
            assert!(matches!(f.terminal(&job).await, Event::InferenceFailed { error, .. } if error.contains("unknown catalog model: fake/unknown-model")));
            f.drained().await;
            let context = async_nats::jetstream::new(async_nats::connect(std::env::var("SWARMY_NATS_URL").unwrap()).await.unwrap());
            let stream = context.get_stream(format!("{}_INFER_REQ", f.prefix)).await.unwrap();
            let consumer: async_nats::jetstream::consumer::PullConsumer = stream.get_consumer("infer_fake").await.unwrap();
            assert_eq!(consumer.cached_info().delivered.consumer_sequence, 1);
            assert_eq!(f.calls(), 0);
            assert_eq!(f.store.session_usage(job.session_id).await.unwrap(), swarmy_core::UsageTotals::default());
        }).catch_unwind().await;
        f.cleanup().await;
        result.unwrap();
    }).await;
}
