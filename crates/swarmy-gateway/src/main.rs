mod config;

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use futures::StreamExt;
use jiff::Timestamp;
use swarmy_bus::{Bus, LiveFeed, WorkMessage, WorkQueue};
use swarmy_core::{
    Event, IdempotencyState, LeaseOwnerId, Message, MessageId, MessageRole, RequestId,
};
use swarmy_llm::{Delta, InferenceJob, Provider, Response};
use swarmy_store::{InferenceClaim, InferenceCompletion, Store, blob::ObjectBlobStore};
use tokio::{
    sync::Semaphore,
    task::JoinSet,
    time::{interval, sleep},
};
use tracing::{error, info, warn};
use ulid::Ulid;

struct Gateway {
    store: Store,
    bus: Bus,
    provider: Arc<dyn Provider>,
    ack_wait: Duration,
    max_deliver: i64,
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
    let store = Store::open(
        Some(&config.cluster),
        Some(&config.directory),
        Arc::new(ObjectBlobStore::from_env()?),
    )
    .await?;
    let bus = Bus::connect(&config.nats, config.bus.clone()).await?;
    let queue = WorkQueue::Inference(config.class);
    bus.setup(std::slice::from_ref(&queue)).await?;
    let mut messages = bus.consume::<InferenceJob>(&queue).await?;
    let gateway = Arc::new(Gateway {
        store,
        bus,
        provider: config.provider,
        ack_wait: config.bus.ack_wait,
        max_deliver: config.bus.max_deliver,
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
            if self.completed(job.request_id).await? {
                return Ok(message.acknowledge().await?);
            }
            let now = Timestamp::now();
            claim.expires_at = now.checked_add(self.ack_wait)?;
            if self.store.start_inference(&claim, now).await? {
                break;
            }
            message.extend_deadline().await?;
            sleep(self.ack_wait / 3).await;
        }
        let work = self.process(message, &claim);
        tokio::pin!(work);
        let mut heartbeat = interval(self.ack_wait / 3);
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
        let mut stream = self.provider.request(job.request.clone());
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
        // Keep the finished response and renew the deadline during store outages.
        // Retrying only this transaction avoids spending another provider call.
        loop {
            if self.completed(job.request_id).await? {
                break;
            }
            let session = self
                .store
                .fetch_session(job.session_id)
                .await?
                .context("session missing")?;
            ensure!(
                session.state == swarmy_core::SessionState::WaitingInference,
                "session is not waiting for inference"
            );
            let completion = InferenceCompletion {
                claim: claim.clone(),
                expected_head: session.head_seq,
                event: event.clone(),
                now: Timestamp::now(),
            };
            match self.store.complete_inference(&completion, &result).await {
                Ok(()) => break,
                Err(error) => {
                    warn!(%error, "retrying terminal store update");
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
        message.acknowledge().await?;
        Ok(())
    }
}
