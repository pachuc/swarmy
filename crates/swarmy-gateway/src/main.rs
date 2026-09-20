mod config;
mod credentials;

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use jiff::Timestamp;
use swarmy_bus::{Bus, LiveFeed, WorkMessage, WorkQueue};
use swarmy_core::{
    Event, IdempotencyState, LeaseOwnerId, Message, MessageId, MessageRole, RequestId,
};
use swarmy_llm::{Delta, InferenceJob, Provider, Response};
use swarmy_store::{
    InferenceClaim, InferenceCompletion, Store,
    blob::{BlobStore, ObjectBlobStore},
};
use tokio::{
    sync::Semaphore,
    task::JoinSet,
    time::{Instant, interval_at, sleep},
};
use tracing::{error, info, warn};
use ulid::Ulid;

struct Gateway {
    store: Store,
    blobs: Arc<dyn BlobStore>,
    bus: Bus,
    provider: Arc<dyn Provider>,
    ack_wait: Duration,
    max_deliver: i64,
    resend_interval: Duration,
}

// Boot before the runtime so the network guard outlives all database tasks.
fn main() -> Result<()> {
    swarmy_version::parse::<swarmy_version::ServiceArgs>("swarmy-gateway")?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let config = config::Config::from_env()?;
    let _network = swarmy_store::boot();
    tokio::runtime::Runtime::new()?.block_on(async {
        tokio::select! {
            result = run(config) => result,
            result = tokio::signal::ctrl_c() => Ok(result?),
        }
    })
}

async fn run(config: config::Config) -> Result<()> {
    let blobs = Arc::new(ObjectBlobStore::from_env()?);
    let store = Store::open(
        Some(&config.cluster),
        Some(&config.directory),
        blobs.clone(),
    )
    .await?;
    let provider = match config.provider {
        config::ConfiguredProvider::ChatGpt {
            credential_file,
            model,
        } => {
            let (provider, model) = config::chatgpt_catalog(&model)?;
            let credentials =
                credentials::ClusterCredentials::new(store.clone(), &credential_file).await?;
            swarmy_llm::client_for(
                provider,
                model,
                swarmy_llm::ClientAuth::ChatGpt(Arc::new(credentials)),
            )
            .context("cannot build the ChatGPT inference client")?
        }
        config::ConfiguredProvider::Fake(provider) => provider,
    };
    let bus = Bus::connect(&config.nats, config.bus.clone()).await?;
    let queue = WorkQueue::Inference(config.class);
    bus.setup(std::slice::from_ref(&queue)).await?;
    let mut messages = bus.consume::<InferenceJob>(&queue).await?;
    let gateway = Arc::new(Gateway {
        store,
        blobs,
        bus,
        provider,
        ack_wait: config.bus.ack_wait,
        max_deliver: config.bus.max_deliver,
        resend_interval: config.resend_interval,
    });
    let semaphore = Arc::new(Semaphore::new(config.concurrency));
    let mut tasks = JoinSet::new();
    info!(concurrency = config.concurrency, "gateway ready");
    loop {
        while let Some(result) = tasks.try_join_next() {
            result?;
        }
        let permit = semaphore.clone().acquire_owned().await?;
        let Some(delivery) = messages.next().await else {
            bail!("work stream ended");
        };
        let message = match delivery {
            Ok(message) => message,
            Err(error) => {
                warn!(%error, "cannot decode work delivery");
                continue;
            }
        };
        let gateway = gateway.clone();
        tasks.spawn(async move {
            let _permit = permit;
            if let Err(error) = gateway.handle(&message).await {
                error!(%error, "delivery left unacknowledged");
            }
        });
    }
}

impl Gateway {
    async fn completed(&self, request: RequestId) -> Result<bool> {
        Ok(self
            .store
            .get_idempotency(request)
            .await?
            .is_some_and(|record| record.state == IdempotencyState::Completed))
    }

    async fn handle(&self, message: &WorkMessage<InferenceJob>) -> Result<()> {
        let job = &message.value;
        if job.request_id != RequestId::for_step(job.session_id, job.step) {
            // Invalid jobs cannot identify legitimate work to fail in the session log.
            warn!(request_id = %job.request_id, "rejecting mismatched request id");
            return Ok(message.acknowledge().await?);
        }
        let mut claim = InferenceClaim {
            session_id: job.session_id,
            request_id: job.request_id,
            owner: LeaseOwnerId::from_ulid(Ulid::generate()),
            expires_at: Timestamp::now(),
        };
        loop {
            let now = Timestamp::now();
            claim.expires_at = now.checked_add(self.ack_wait)?;
            if self.store.start_inference(&claim, now).await? {
                break;
            }
            if self.completed(job.request_id).await? {
                return Ok(message.acknowledge().await?);
            }
            message.extend_deadline().await?;
            sleep(self.ack_wait / 3).await;
        }
        let work = self.process(message, &claim);
        tokio::pin!(work);
        let period = self.ack_wait / 3;
        let mut heartbeat = interval_at(Instant::now() + period, period);
        loop {
            tokio::select! {
                result = &mut work => return result,
                _ = heartbeat.tick() => {
                    message.extend_deadline().await?;
                    let now = Timestamp::now();
                    let renewal = InferenceClaim { expires_at: now.checked_add(self.ack_wait)?, ..claim.clone() };
                    if !self.store.start_inference(&renewal, now).await? {
                        if self.completed(job.request_id).await? { return Ok(message.acknowledge().await?); }
                        bail!("inference claim was replaced");
                    }
                }
            }
        }
    }

    async fn infer(&self, job: &InferenceJob) -> Result<Response, swarmy_llm::Error> {
        let mut stream = self
            .provider
            .request_for_session(job.request.clone(), job.session_id);
        let mut response = None;
        while let Some(delta) = stream.next().await {
            let delta = delta?;
            if response.is_some() {
                return Err(swarmy_llm::Error::Protocol("delta after completion".into()));
            }
            // Live feeds are ephemeral. An unavailable observer path must not lose
            // a provider result that can still be committed to the durable log.
            if let Err(error) = self
                .bus
                .publish_live(LiveFeed::ModelDeltas(job.session_id), &delta)
                .await
            {
                warn!(%error, "live delta publication failed");
            }
            if let Delta::Completed(completed) = delta {
                response = Some(completed);
            }
        }
        response
            .ok_or_else(|| swarmy_llm::Error::Protocol("stream ended without completion".into()))
    }

    async fn process(
        &self,
        message: &WorkMessage<InferenceJob>,
        claim: &InferenceClaim,
    ) -> Result<()> {
        let job = &message.value;
        let turn = job
            .request
            .messages
            .iter()
            .rev()
            .find(|message| message.role == MessageRole::User)
            .map(|message| message.id);
        if let Some(turn) = turn {
            self.bus
                .record_turn(&Bus::turn_event(
                    job.session_id,
                    turn,
                    swarmy_core::TurnStage::InferenceStarted,
                    Some(job.request_id),
                ))
                .await;
        }
        let result = self.infer(job).await;
        if let Some(turn) = turn {
            self.bus
                .record_turn(&Bus::turn_event(
                    job.session_id,
                    turn,
                    swarmy_core::TurnStage::InferenceFinished,
                    Some(job.request_id),
                ))
                .await;
        }
        let result = match result {
            Ok(response) => Ok(response),
            Err(error) if message.delivery_count()? < self.max_deliver => {
                warn!(%error, request_id = %job.request_id, "provider failed; retrying");
                self.store.release_inference(claim).await?;
                let exponent = u32::try_from(message.delivery_count()?.saturating_sub(1).min(5))?;
                message
                    .negative_acknowledge(Some(Duration::from_millis(100) * 2_u32.pow(exponent)))
                    .await?;
                return Ok(());
            }
            Err(error) => Err(error.to_string()),
        };
        let event = match &result {
            Ok(response) => Event::InferenceCompleted {
                seq: 0,
                request_id: job.request_id,
                message: Message {
                    id: MessageId::from_ulid(Ulid::generate()),
                    role: MessageRole::Assistant,
                    parts: response.parts.clone(),
                },
            },
            Err(error) => Event::InferenceFailed {
                seq: 0,
                request_id: job.request_id,
                error: error.clone(),
            },
        };
        self.persist_response(job, claim, event, &result, turn)
            .await?;
        message.acknowledge().await?;
        Ok(())
    }

    async fn persist_response(
        &self,
        job: &InferenceJob,
        claim: &InferenceClaim,
        mut event: Event,
        result: &std::result::Result<Response, String>,
        turn: Option<MessageId>,
    ) -> Result<()> {
        // Keep the finished response and renew the deadline during store outages.
        // Retrying only this transaction avoids spending another provider call.
        let mut expected_head = job.step;
        loop {
            let completion = InferenceCompletion {
                claim: claim.clone(),
                expected_head,
                event: event.clone(),
                now: Timestamp::now(),
            };
            let snapshot = match self.terminal_snapshot(job, &completion).await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    warn!(%error, "retrying terminal snapshot upload");
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let committed = if let Some(snapshot) = &snapshot {
                self.store
                    .complete_inference_and_idle(&completion, result, snapshot)
                    .await
            } else {
                self.store.complete_inference(&completion, result).await
            };
            match committed {
                Ok(false) => break,
                Ok(true) => {
                    event.set_seq(expected_head + 1);
                    self.notify_completion(job.session_id, &event, turn, snapshot.as_ref())
                        .await;
                    break;
                }
                Err(swarmy_store::StoreError::StaleSequence { actual, .. }) => {
                    expected_head = actual;
                }
                Err(error) => {
                    warn!(%error, "retrying terminal store update");
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
        Ok(())
    }

    async fn terminal_snapshot(
        &self,
        job: &InferenceJob,
        completion: &InferenceCompletion,
    ) -> Result<Option<swarmy_core::SnapshotRef>> {
        // Main sessions need a worker turn-end step to decide whether to summarize.
        if let Some(session) = self.store.fetch_session(job.session_id).await?
            && matches!(session.kind, swarmy_core::SessionKind::Named { .. })
            && let Some(agent) = self.store.get_agent(session.agent_id).await?
            && agent.main_session == Some(job.session_id)
        {
            return Ok(None);
        }
        // A concurrent log append is not in the immutable request. Let the
        // worker replay it instead of advancing a snapshot over unseen events.
        if completion.expected_head != job.step {
            return Ok(None);
        }
        let Event::InferenceCompleted { message, .. } = &completion.event else {
            return Ok(None);
        };
        if message
            .parts
            .iter()
            .any(|part| matches!(part, swarmy_core::Part::ToolCall { .. }))
        {
            return Ok(None);
        }
        // The worker's immutable request contains the complete replayed message
        // history. Replay the response with the same harness that resumes it.
        let mut events: Vec<_> = job
            .request
            .messages
            .iter()
            .cloned()
            .map(|message| Event::MessageAppended { seq: 0, message })
            .collect();
        events.push(completion.event.clone());
        let bytes = swarmy_core::encode(&swarmy_harness::Snapshot::default().replay(&events))?;
        let snapshot = swarmy_core::SnapshotRef {
            seq: completion
                .expected_head
                .checked_add(2)
                .context("sequence overflow")?,
            object_key: format!("blobs/{}", blake3::hash(&bytes).to_hex()),
        };
        self.blobs.put(&snapshot.object_key, bytes.into()).await?;
        Ok(Some(snapshot))
    }

    async fn notify_completion(
        &self,
        id: swarmy_core::SessionId,
        event: &Event,
        turn: Option<MessageId>,
        snapshot: Option<&swarmy_core::SnapshotRef>,
    ) {
        if let Err(error) = self
            .bus
            .publish_live(LiveFeed::SessionEvents(id), event)
            .await
        {
            warn!(%error, "completion event publication failed; client will catch up");
        }
        if let Some(snapshot) = snapshot {
            if let Some(turn) = turn {
                self.bus
                    .record_turn(&Bus::turn_event(
                        id,
                        turn,
                        swarmy_core::TurnStage::Idle,
                        None,
                    ))
                    .await;
            }
            if let Err(error) = self
                .bus
                .publish_live(
                    LiveFeed::SessionEvents(id),
                    &Event::StateChanged {
                        seq: snapshot.seq,
                        from: swarmy_core::SessionState::WaitingInference,
                        to: swarmy_core::SessionState::Idle,
                    },
                )
                .await
            {
                warn!(%error, "idle event publication failed; client will catch up");
            }
            return;
        }
        if let Err(error) = self
            .bus
            .nudge(id, event.seq(), turn, self.resend_interval, false)
            .await
        {
            warn!(%error, "completion nudge failed; scheduler will recover");
        }
    }
}
