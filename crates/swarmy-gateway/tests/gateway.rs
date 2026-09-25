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
    AgentId, CredentialKind, CredentialRecord, CredentialScope, Event, IdempotencyState,
    InflightRecord, LeaseOwnerId, Part, RequestId, SessionId, SessionRecord, SessionState, encode,
};
use swarmy_llm::{
    Delta, GenerationSettings, InferenceJob, InferenceJobRef, Request, Response, StopReason,
    TokenUsage,
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
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

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
            quota_remaining: std::collections::BTreeMap::new(),
            quota_resets: std::collections::BTreeMap::new(),
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
                .env("SWARMY_KEYRING", self.files.path().join("keyring"))
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

    async fn job_with_settings(&self, settings: GenerationSettings) -> InferenceJob {
        self.job_for_agent_with_request(
            AgentId::from_ulid(Ulid::generate()),
            Request {
                system_prompt: String::new(),
                messages: Vec::new(),
                tools: Vec::new(),
                settings,
            },
        )
        .await
    }

    async fn job_for_agent(&self, agent_id: AgentId) -> InferenceJob {
        self.job_for_agent_with_request(
            agent_id,
            Request {
                system_prompt: String::new(),
                messages: Vec::new(),
                tools: Vec::new(),
                settings: GenerationSettings::default(),
            },
        )
        .await
    }

    async fn job_for_agent_with_request(
        &self,
        agent_id: AgentId,
        request: Request,
    ) -> InferenceJob {
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
        let job = InferenceJob {
            provider: "fake".into(),
            session_id,
            step: lease.seq,
            request_id,
            request,
        };
        self.store
            .submit_inference_after_with_request(
                0,
                &lease,
                &InflightRecord {
                    session_id,
                    seq: lease.seq,
                    provider: "fake".into(),
                    key_id: "fake".into(),
                },
                &job,
                &job.request,
                &[],
            )
            .await
            .unwrap();
        job
    }

    async fn publish(&self, job: &InferenceJob) {
        self.bus
            .publish_work(&self.queue, &InferenceJobRef::from(job))
            .await
            .unwrap();
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

#[tokio::test]
async fn credential_changes_update_a_running_gateway() {
    run(|mut f| async move {
        let keyring = swarmy_config::Keyring::generate_at(&f.files.path().join("keyring")).unwrap();
        let credentials = f.store.credentials(keyring);
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ready\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
                "text/event-stream",
            ))
            .mount(&server)
            .await;
        let providers = std::collections::BTreeMap::from([(
            "openrouter".into(),
            swarmy_config::CustomProvider {
                api: None,
                base_url: Some(format!("{}/api/v1/", server.uri())),
            },
        )]);
        f.start_with(1, "openrouter", &providers, &[]);
        let result = AssertUnwindSafe(async {
            timeout(WAIT, async {
                while f.store.gateway_provider("openrouter").await.unwrap().is_none() {
                    sleep(Duration::from_millis(20)).await;
                }
            }).await.unwrap();
            assert!(!f.store.gateway_serves("openrouter").await.unwrap());
            credentials.put_entry(CredentialScope::Cluster, "openrouter", "primary", &CredentialRecord {
                kind: CredentialKind::ApiKey { key: "fixture-key".into(), extra: std::collections::BTreeMap::default() },
                updated_at: Timestamp::now(),
            }).await.unwrap();
            credentials.put_entry(CredentialScope::Cluster, "openrouter", "backup", &CredentialRecord {
                kind: CredentialKind::ApiKey { key: "fixture-backup".into(), extra: std::collections::BTreeMap::default() },
                updated_at: Timestamp::now(),
            }).await.unwrap();
            timeout(Duration::from_secs(65), async {
                while !f.store.gateway_serves("openrouter").await.unwrap() {
                    sleep(Duration::from_millis(100)).await;
                }
            }).await.unwrap();
            timeout(Duration::from_secs(65), async {
                while f.store.gateway_entry("openrouter", "primary").await.unwrap().is_none()
                    || f.store.gateway_entry("openrouter", "backup").await.unwrap().is_none()
                {
                    sleep(Duration::from_millis(100)).await;
                }
            }).await.unwrap();
            let queue = WorkQueue::Inference(SubjectToken::new("openrouter").unwrap());
            let mut job = f.job_with_settings(GenerationSettings {
                model: "openai/gpt-5.5".into(), ..Default::default()
            }).await;
            job.provider = "openrouter".into();
            f.bus.publish_work(&queue, &InferenceJobRef::from(&job)).await.unwrap();
            assert!(matches!(f.terminal(&job).await, Event::InferenceCompleted { .. }));
            let usage = f.store.inference_usage_record(job.request_id).await.unwrap().unwrap();
            assert_eq!(usage.entry.as_deref(), Some("primary"));
            let entries = credentials.list_entries(CredentialScope::Cluster).await.unwrap();
            assert!(entries.iter().find(|entry| entry.label == "primary").unwrap().last_used_at.is_some());
            assert!(entries.iter().find(|entry| entry.label == "backup").unwrap().last_used_at.is_none());
            credentials.delete_entry(CredentialScope::Cluster, "openrouter", "primary").await.unwrap();
            credentials.delete_entry(CredentialScope::Cluster, "openrouter", "backup").await.unwrap();
            timeout(Duration::from_secs(65), async {
                while f.store.gateway_serves("openrouter").await.unwrap() {
                    sleep(Duration::from_millis(100)).await;
                }
            }).await.unwrap();
            let mut rejected = f.job_with_settings(GenerationSettings {
                model: "openai/gpt-5.5".into(), ..Default::default()
            }).await;
            rejected.provider = "openrouter".into();
            f.bus.publish_work(&queue, &InferenceJobRef::from(&rejected)).await.unwrap();
            assert!(matches!(f.terminal(&rejected).await, Event::InferenceFailed { error, .. } if error.contains("provider is not served by this gateway")));
        }).catch_unwind().await;
        f.cleanup().await;
        result.unwrap();
    }).await;
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
        let message = swarmy_core::Message {
            id: swarmy_core::MessageId::from_ulid(Ulid::generate()),
            role: swarmy_core::MessageRole::User,
            parts: vec![Part::Text {
                text: "preserve this history".into(),
            }],
        };
        let job = f
            .job_for_agent_with_request(
                AgentId::from_ulid(Ulid::generate()),
                Request {
                    system_prompt: String::new(),
                    messages: vec![message],
                    tools: Vec::new(),
                    settings: GenerationSettings::default(),
                },
            )
            .await;
        let result = AssertUnwindSafe(async {
            f.publish(&job).await;
            f.publish(&job).await;
            assert!(matches!(
                f.terminal(&job).await,
                Event::InferenceCompleted { .. }
            ));
            assert!(
                f.store
                    .get_inference_request::<Request>(job.request_id)
                    .await
                    .unwrap()
                    .is_none()
            );
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
            let mut work = f.bus.consume::<InferenceJobRef>(&f.queue).await.unwrap();
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
            assert!(f.store.get_inference_request::<Request>(job.request_id).await.unwrap().is_none());
            let key = CredentialKey::provider("fake");
            assert!(f.store.entry_open_until(&key, Timestamp::now()).await.unwrap().is_some());
            assert_eq!(f.calls(), 1);
            f.kill().await;
            f.start(4);
            assert!(f.store.entry_open_until(&key, Timestamp::now()).await.unwrap().is_some());
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
            assert!(f.store.get_inference_request::<Request>(job.request_id).await.unwrap().is_none());
            assert!(f.store.entry_open_until(&CredentialKey::provider("fake"), Timestamp::now()).await.unwrap().is_none());
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
            let settings = GenerationSettings {
                model: model.id.clone(),
                reasoning_effort: Some(swarmy_core::ReasoningEffort::Max),
                ..Default::default()
            };
            let mut first = f.job_for_agent_with_request(agent_id, Request {
                system_prompt: String::new(), messages: Vec::new(), tools: Vec::new(),
                settings: settings.clone(),
            }).await;
            first.provider = "fake".into();
            let mut second = f.job_for_agent_with_request(agent_id, Request {
                system_prompt: String::new(), messages: Vec::new(), tools: Vec::new(), settings,
            }).await;
            second.provider = "scripted".into();
            f.publish(&first).await;
            f.bus.publish_work(&queue, &InferenceJobRef::from(&second)).await.unwrap();
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

fn switch_script() -> (Response, Response) {
    let call = Part::ToolCall {
        call_id: swarmy_core::ToolCallId("clock-0".into()),
        tool: "get_time".into(),
        input: serde_json::json!({}),
    };
    let first = Response {
        parts: vec![
            Part::Text {
                text: "Checking.".into(),
            },
            call,
        ],
        stop_reason: StopReason::ToolCalls,
        usage: TokenUsage::default(),
        quota_remaining: std::collections::BTreeMap::new(),
        quota_resets: std::collections::BTreeMap::new(),
    };
    let second = Response {
        parts: vec![Part::Text {
            text: "done".into(),
        }],
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage::default(),
        quota_remaining: std::collections::BTreeMap::new(),
        quota_resets: std::collections::BTreeMap::new(),
    };
    (first, second)
}

fn switch_models() -> (
    swarmy_config::CustomModel,
    Vec<swarmy_config::CustomModel>,
    std::collections::BTreeMap<String, swarmy_config::CustomProvider>,
) {
    let model = swarmy_config::CustomModel {
        id: "scripted-model".into(),
        api: Some(swarmy_llm::catalog::Api::Fake),
        ..Default::default()
    };
    let models: Vec<_> = ["fake", "scripted"]
        .map(|provider| swarmy_config::CustomModel {
            provider: provider.into(),
            ..model.clone()
        })
        .into();
    let providers = std::collections::BTreeMap::from([(
        "scripted".to_owned(),
        swarmy_config::CustomProvider {
            api: Some(swarmy_llm::catalog::Api::Fake),
            base_url: Some("fake://scripted".into()),
        },
    )]);
    (model, models, providers)
}

fn switch_user() -> swarmy_core::Message {
    swarmy_core::Message {
        id: swarmy_core::MessageId::from_ulid(Ulid::generate()),
        role: swarmy_core::MessageRole::User,
        parts: vec![Part::Text {
            text: "What time is it?".into(),
        }],
    }
}

fn switch_tool_result() -> swarmy_core::Message {
    swarmy_core::Message {
        id: swarmy_core::MessageId::from_ulid(Ulid::generate()),
        role: swarmy_core::MessageRole::Tool,
        parts: vec![swarmy_core::Part::ToolResult {
            call_id: swarmy_core::ToolCallId("clock-0".into()),
            result: swarmy_core::ToolResult::Completed {
                output: "12:00".into(),
                title: "get_time".into(),
                metadata: std::collections::BTreeMap::new(),
            },
        }],
    }
}

fn switch_notice() -> swarmy_core::Message {
    swarmy_core::Message {
        id: swarmy_core::MessageId::from_ulid(Ulid::generate()),
        role: swarmy_core::MessageRole::System,
        parts: vec![Part::Text {
            text: "Your computer was evicted while idle. Recovery began; check external side effects before retrying.".into(),
        }],
    }
}

fn logged_histories(f: &Fixture) -> Vec<Vec<swarmy_core::Message>> {
    std::fs::read_to_string(f.files.path().join("calls"))
        .unwrap()
        .lines()
        .map(|line| {
            let entry: serde_json::Value = serde_json::from_str(line).unwrap();
            serde_json::from_value(entry["messages"].clone()).unwrap()
        })
        .collect()
}

fn assert_logged_history_order(messages: &[swarmy_core::Message]) {
    // The fake provider bypasses both wire adapters, so the gateway must
    // forward the interleaved history intact: the call stays before its
    // result, the notice stays present, and neither is duplicated. Wire
    // pairing for real providers is covered by the protocol and completions
    // suites in swarmy-llm.
    let call = messages
        .iter()
        .position(|m| {
            m.parts.iter().any(|p| {
                matches!(p, swarmy_core::Part::ToolCall { call_id, .. } if call_id.0 == "clock-0")
            })
        })
        .expect("call in logged history");
    let result = messages
        .iter()
        .position(|m| {
            m.parts.iter().any(|p| {
                matches!(p, swarmy_core::Part::ToolResult { call_id, .. } if call_id.0 == "clock-0")
            })
        })
        .expect("result in logged history");
    assert!(call < result, "logged call must precede its result");
    assert!(
        messages.iter().any(|m| m.parts.iter().any(|p| matches!(
            p,
            Part::Text { text } if text.contains("Your computer was evicted")
        ))),
        "logged history must keep the notice"
    );
    for (kind, count) in [
        (
            "call",
            messages
                .iter()
                .filter(|m| {
                    m.parts.iter().any(|p| {
                matches!(p, swarmy_core::Part::ToolCall { call_id, .. } if call_id.0 == "clock-0")
            })
                })
                .count(),
        ),
        (
            "result",
            messages
                .iter()
                .filter(|m| {
                    m.parts.iter().any(|p| {
                matches!(p, swarmy_core::Part::ToolResult { call_id, .. } if call_id.0 == "clock-0")
            })
                })
                .count(),
        ),
    ] {
        assert_eq!(count, 1, "logged history must not duplicate the {kind}");
    }
}

async fn switch_turns(f: &mut Fixture, model: &swarmy_config::CustomModel) {
    let scripted = WorkQueue::Inference(SubjectToken::new("scripted").unwrap());
    f.bus.setup(std::slice::from_ref(&scripted)).await.unwrap();
    let agent = AgentId::from_ulid(Ulid::generate());
    let settings = GenerationSettings {
        model: model.id.clone(),
        ..Default::default()
    };
    let mut first = f
        .job_for_agent_with_request(
            agent,
            Request {
                system_prompt: String::new(),
                messages: vec![switch_user()],
                tools: Vec::new(),
                settings: settings.clone(),
            },
        )
        .await;
    first.provider = "fake".into();
    f.publish(&first).await;
    let Event::InferenceCompleted { message, .. } = f.terminal(&first).await else {
        panic!("first turn did not complete");
    };
    assert!(
        message
            .parts
            .iter()
            .any(|p| matches!(p, swarmy_core::Part::ToolCall { .. }))
    );
    let history = vec![
        switch_user(),
        message.clone(),
        switch_notice(),
        switch_tool_result(),
    ];
    let mut second = f
        .job_for_agent_with_request(
            agent,
            Request {
                system_prompt: String::new(),
                messages: history.clone(),
                tools: Vec::new(),
                settings,
            },
        )
        .await;
    second.provider = "scripted".into();
    f.bus
        .publish_work(&scripted, &InferenceJobRef::from(&second))
        .await
        .unwrap();
    assert!(matches!(
        f.terminal(&second).await,
        Event::InferenceCompleted { .. }
    ));
    let histories = logged_histories(f);
    assert_eq!(histories.len(), 2, "one log entry per turn so far");
    assert_logged_history_order(&histories[1]);
    let mut back = f
        .job_for_agent_with_request(
            agent,
            Request {
                system_prompt: String::new(),
                messages: history,
                tools: Vec::new(),
                settings: GenerationSettings {
                    model: model.id.clone(),
                    ..Default::default()
                },
            },
        )
        .await;
    back.provider = "fake".into();
    f.publish(&back).await;
    assert!(matches!(
        f.terminal(&back).await,
        Event::InferenceCompleted { .. }
    ));
    let histories = logged_histories(f);
    assert_eq!(histories.len(), 3, "one log entry per turn");
    assert_logged_history_order(&histories[2]);
}

#[tokio::test]
async fn provider_switch_preserves_tool_history() {
    run(|mut f| async move {
        let (first, second) = switch_script();
        std::fs::write(
            f.files.path().join("script.json"),
            serde_json::to_vec(&serde_json::json!({
                "responses": {"0": first, "1": second, "2": second, "3": second},
            }))
            .unwrap(),
        )
        .unwrap();
        let (model, models, providers) = switch_models();
        f.start_with(1, "fake,scripted", &providers, &models);
        let result = AssertUnwindSafe(switch_turns(&mut f, &model))
            .catch_unwind()
            .await;
        f.cleanup().await;
        result.unwrap();
    })
    .await;
}

#[tokio::test]
async fn unknown_model_is_a_permanent_failure_without_provider_calls() {
    run(|mut f| async move {
        f.script(0, false, "unused");
        f.start(1);
        let mut job = f.job_with_settings(GenerationSettings {
            model: "unknown-model".into(), ..Default::default()
        }).await;
        job.provider = "fake".into();
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
