use super::*;
use futures::FutureExt;
use swarmy_core::{AgentSettings, ReasoningEffort};
use swarmy_llm::InferenceJob;

struct Fixture {
    store: Store,
    bus: Bus,
    worker: Worker,
    cluster: String,
    url: String,
    prefix: String,
}

impl Fixture {
    async fn new() -> Option<Self> {
        let (Ok(cluster), Ok(url)) = (
            std::env::var("SWARMY_FDB_CLUSTER_FILE"),
            std::env::var("SWARMY_NATS_URL"),
        ) else {
            eprintln!("skipping agent inference test: FoundationDB or NATS environment is unset");
            return None;
        };
        NETWORK.get_or_init(swarmy_store::boot);
        let prefix = format!("agent_inference_{}", Ulid::generate());
        let mut config = config(cluster.clone(), url.clone(), &prefix, Arc::default());
        config.lease_duration = Duration::from_secs(5);
        config.harness.system_prompt_template = "Default system prompt.\n".into();
        config.harness.settings.model = "default-model".into();
        config.harness.settings.reasoning_effort = Some(ReasoningEffort::Medium);
        let blobs = Arc::new(MemoryBlobStore::default());
        let store = Store::open(Some(&cluster), Some(&config.directory), blobs.clone())
            .await
            .unwrap();
        image_fixture::image(&store).await;
        let bus = Bus::connect(&url, config.bus.clone()).await.unwrap();
        bus.setup(&[]).await.unwrap();
        let worker = Worker::new(store.clone(), bus.clone(), blobs, config);
        Some(Self {
            store,
            bus,
            worker,
            cluster,
            url,
            prefix,
        })
    }

    async fn session(&self, agent: Option<AgentId>) -> SessionId {
        let id = SessionId::from_ulid(Ulid::generate());
        self.store
            .create_session_for_agent(
                id,
                agent,
                agent.is_none().then_some("fixture:test"),
                Timestamp::now(),
            )
            .await
            .unwrap();
        id
    }

    async fn infer(&self, id: SessionId) -> InferenceJob {
        self.store
            .append_events(
                id,
                0,
                &[Event::MessageAppended {
                    seq: 0,
                    message: Message {
                        id: MessageId::from_ulid(Ulid::generate()),
                        role: MessageRole::User,
                        parts: vec![swarmy_core::Part::Text {
                            text: "Hello".into(),
                        }],
                    },
                }],
            )
            .await
            .unwrap();
        self.store.wake_session(id, Timestamp::now()).await.unwrap();
        let queue = WorkQueue::Runnable(7);
        let mut messages = self.bus.consume::<Nudge>(&queue).await.unwrap();
        self.bus
            .publish_work(&queue, &Nudge { session_id: id })
            .await
            .unwrap();
        let delivery = timeout(Duration::from_secs(10), messages.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        self.worker.handle(&delivery).await.unwrap();
        let events = self.store.read_events(id, 0, 64).await.unwrap();
        let request_id = events
            .iter()
            .find_map(|event| match event {
                Event::InferenceRequested { request_id, .. } => Some(*request_id),
                _ => None,
            })
            .expect("worker did not request inference");
        self.store
            .get_inference_input(request_id)
            .await
            .unwrap()
            .unwrap()
    }
}

fn assert_request(job: &InferenceJob, prompt: &str, model: &str, effort: ReasoningEffort) {
    // A named agent's prompt is followed by its memory block; the configured prompt
    // must still lead, with the memory directory placeholder substituted.
    assert!(
        job.request.system_prompt.starts_with(prompt),
        "prompt {:?} does not start with {prompt:?}",
        job.request.system_prompt
    );
    assert_eq!(job.request.settings.model, model);
    assert_eq!(job.request.settings.reasoning_effort, Some(effort));
    assert_eq!(job.request.messages.len(), 1);
    assert_eq!(job.request.tools.len(), 1);
}

#[tokio::test]
async fn named_agent_overrides_and_ephemeral_defaults_reach_durable_inference() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let result = std::panic::AssertUnwindSafe(async {
        let agent = f
            .store
            .create_agent_with_settings(
                "custom",
                "fixture:test",
                "",
                &AgentSettings {
                    system_prompt: Some("  Agent prompt.\n".into()),
                    model: Some("agent-model".into()),
                    reasoning_effort: Some(ReasoningEffort::High),
                },
                Timestamp::now(),
            )
            .await
            .unwrap();
        let first = f.session(Some(agent.agent_id)).await;
        let existing = f.session(Some(agent.agent_id)).await;
        let job = f.infer(first).await;
        assert_request(
            &job,
            "  Agent prompt.\n",
            "agent-model",
            ReasoningEffort::High,
        );
        f.store
            .set_agent(
                agent.agent_id,
                &AgentSettings {
                    system_prompt: Some(String::new()),
                    model: Some("updated-model".into()),
                    reasoning_effort: Some(ReasoningEffort::None),
                },
            )
            .await
            .unwrap();
        for id in [existing, f.session(Some(agent.agent_id)).await] {
            assert_request(
                &f.infer(id).await,
                "",
                "updated-model",
                ReasoningEffort::None,
            );
        }
        // A settings update must not change an already durable request on retry.
        assert_eq!(
            f.store
                .get_inference_input::<InferenceJob>(job.request_id)
                .await
                .unwrap(),
            Some(job)
        );
        assert_default_settings(&f).await;
    })
    .catch_unwind()
    .await;
    cleanup(&f.cluster, &f.url, &f.prefix).await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

async fn assert_default_settings(f: &Fixture) {
    let partial = f
        .store
        .create_agent_with_settings(
            "partial",
            "fixture:test",
            "",
            &AgentSettings {
                model: Some("partial-model".into()),
                ..Default::default()
            },
            Timestamp::now(),
        )
        .await
        .unwrap();
    assert_request(
        &f.infer(f.session(Some(partial.agent_id)).await).await,
        "Default system prompt.\n",
        "partial-model",
        ReasoningEffort::Medium,
    );
    let defaults = f
        .store
        .create_agent("defaults", "fixture:test", "", Timestamp::now())
        .await
        .unwrap();
    for agent in [None, Some(defaults.agent_id)] {
        assert_request(
            &f.infer(f.session(agent).await).await,
            "Default system prompt.\n",
            "default-model",
            ReasoningEffort::Medium,
        );
    }
}
