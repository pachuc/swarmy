//! Typed work queues and live observer feeds. Consumers must re-check the store
//! before acting: delivery is at least once, and acknowledging is not a store lease.
//!
//! Call [`Bus::setup`] with the routes this deployment consumes before publishing.
//! Multiple workers consuming the same route share a durable consumer. Dropping a
//! work message leaves it unacknowledged. After `max_deliver` attempts it remains
//! in the stream for operator inspection; this layer does not delete poisoned work.
//!
//! ```no_run
//! use swarmy_bus::{Bus, Config, WorkQueue};
//!
//! # async fn example() -> Result<(), swarmy_bus::Error> {
//! let bus = Bus::connect("nats://127.0.0.1:4222", Config::default()).await?;
//! let queue = WorkQueue::Runnable(0);
//! bus.setup(std::slice::from_ref(&queue)).await?;
//! bus.publish_work(&queue, &42_u64).await?;
//! let mut work = bus.consume::<u64>(&queue).await?;
//! while let Some(delivery) = work.next().await {
//!     let message = delivery?;
//!     // Re-check FoundationDB and commit the work's result before acknowledging.
//!     message.acknowledge().await?;
//! }
//! # Ok(())
//! # }
//! ```

pub mod subjects;

use std::{marker::PhantomData, time::Duration};

use async_nats::jetstream::{
    self,
    consumer::{self, IntoConsumerConfig, PullConsumer, pull},
    stream,
};
use futures_util::StreamExt;
use serde::{Serialize, de::DeserializeOwned};
use swarmy_core::{EncodingError, NodeId, SessionId, decode, encode};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid bus configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("existing NATS configuration differs for {0}")]
    ConfigMismatch(String),
    #[error(transparent)]
    Encoding(#[from] EncodingError),
    #[error("NATS operation failed: {0}")]
    Nats(#[source] Box<dyn std::error::Error + Send + Sync>),
}

fn nats(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Error {
    Error::Nats(error.into())
}

/// A single routing component, restricted to letters, digits, underscores and hyphens.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubjectToken(String);

impl SubjectToken {
    /// # Errors
    /// Rejects empty components and characters that could change NATS routing.
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        if value.is_empty()
            || !value
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
        {
            return Err(Error::InvalidConfig(
                "routing tokens must contain only letters, digits, _ or -",
            ));
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkQueue {
    Inference(SubjectToken),
    Runnable(u16),
    RemoteTools,
    NodeTools(NodeId),
}

impl WorkQueue {
    fn stream_index(&self) -> usize {
        match self {
            Self::Inference(_) => 0,
            Self::Runnable(_) => 1,
            Self::RemoteTools => 2,
            Self::NodeTools(_) => 3,
        }
    }

    fn subject(&self) -> String {
        match self {
            Self::Inference(provider) => {
                subjects::INFER_REQ.replace("{provider_class}", &provider.0)
            }
            Self::Runnable(partition) => {
                subjects::SCHED_RUNNABLE.replace("{partition}", &partition.to_string())
            }
            Self::RemoteTools => subjects::TOOL_REMOTE.to_owned(),
            Self::NodeTools(node) => subjects::TOOL_NODE.replace("{node_id}", &node.to_string()),
        }
    }

    fn durable_name(&self) -> String {
        match self {
            Self::Inference(provider) => format!("infer_{}", provider.0),
            Self::Runnable(partition) => format!("runnable_{partition}"),
            Self::RemoteTools => "remote_tools".to_owned(),
            Self::NodeTools(node) => format!("node_{node}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LiveFeed {
    ModelDeltas(SessionId),
    SessionEvents(SessionId),
}

impl LiveFeed {
    fn subject(self) -> String {
        match self {
            Self::ModelDeltas(session) => {
                subjects::INFER_LIVE.replace("{session_id}", &session.to_string())
            }
            Self::SessionEvents(session) => {
                subjects::SESSION_EVENTS.replace("{session_id}", &session.to_string())
            }
        }
    }
}

/// An absent prefix uses the design's subjects. Work stream names are `INFER_REQ`,
/// `SCHED_RUNNABLE`, `TOOL_REMOTE`, and `TOOL_NODE`. A prefix adds `prefix.` to
/// subjects and `prefix_` to stream names, isolating a deployment or test.
#[derive(Clone, Debug)]
pub struct Config {
    pub prefix: Option<SubjectToken>,
    pub ack_wait: Duration,
    pub max_deliver: i64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            prefix: None,
            ack_wait: Duration::from_secs(30),
            max_deliver: 5,
        }
    }
}

impl Config {
    fn subject(&self, subject: &str) -> String {
        self.prefix.as_ref().map_or_else(
            || subject.to_owned(),
            |prefix| format!("{}.{subject}", prefix.0),
        )
    }

    fn streams(&self) -> [stream::Config; 4] {
        [
            (
                "INFER_REQ",
                subjects::INFER_REQ.replace("{provider_class}", "*"),
            ),
            (
                "SCHED_RUNNABLE",
                subjects::SCHED_RUNNABLE.replace("{partition}", "*"),
            ),
            ("TOOL_REMOTE", subjects::TOOL_REMOTE.to_owned()),
            ("TOOL_NODE", subjects::TOOL_NODE.replace("{node_id}", "*")),
        ]
        .map(|(name, subject)| stream::Config {
            name: self
                .prefix
                .as_ref()
                .map_or_else(|| name.to_owned(), |prefix| format!("{}_{name}", prefix.0)),
            subjects: vec![self.subject(&subject)],
            retention: stream::RetentionPolicy::WorkQueue,
            storage: stream::StorageType::File,
            discard: stream::DiscardPolicy::New,
            max_consumers: -1,
            max_messages: -1,
            max_messages_per_subject: -1,
            max_bytes: -1,
            max_message_size: -1,
            num_replicas: 1,
            duplicate_window: Duration::from_secs(120),
            compression: Some(stream::Compression::None),
            ..Default::default()
        })
    }

    fn consumer(&self, queue: &WorkQueue) -> pull::Config {
        pull::Config {
            durable_name: Some(queue.durable_name()),
            name: Some(queue.durable_name()),
            filter_subject: self.subject(&queue.subject()),
            ack_policy: consumer::AckPolicy::Explicit,
            ack_wait: self.ack_wait,
            max_deliver: self.max_deliver,
            max_waiting: 512,
            max_ack_pending: 1000,
            num_replicas: 1,
            ..Default::default()
        }
    }
}

#[derive(Clone)]
pub struct Bus {
    client: async_nats::Client,
    jetstream: jetstream::Context,
    config: Config,
}

impl Bus {
    /// # Errors
    /// Rejects zero deadlines, nonpositive delivery limits, and connection failures.
    pub async fn connect(url: &str, config: Config) -> Result<Self, Error> {
        if config.ack_wait.is_zero() || config.max_deliver <= 0 {
            return Err(Error::InvalidConfig(
                "ack_wait and max_deliver must be positive",
            ));
        }
        let client = async_nats::connect(url).await.map_err(nats)?;
        Ok(Self {
            jetstream: jetstream::new(client.clone()),
            client,
            config,
        })
    }

    /// Create the four work streams and durable consumers for the supplied routes.
    /// Safe to repeat; existing resources are checked but never updated.
    ///
    /// # Errors
    /// Returns server errors or a configuration mismatch instead of changing an
    /// existing stream or consumer. A failed setup may have created some resources.
    pub async fn setup(&self, queues: &[WorkQueue]) -> Result<(), Error> {
        for requested in self.config.streams() {
            let existing = self
                .jetstream
                .get_or_create_stream(&requested)
                .await
                .map_err(nats)?;
            let mut actual = existing.cached_info().config.clone();
            remove_server_metadata(&mut actual.metadata);
            if actual != requested {
                return Err(Error::ConfigMismatch(requested.name));
            }
        }
        for queue in queues {
            self.ensure_consumer(queue).await?;
        }
        Ok(())
    }

    async fn ensure_consumer(&self, queue: &WorkQueue) -> Result<PullConsumer, Error> {
        let stream = self
            .jetstream
            .get_stream(&self.config.streams()[queue.stream_index()].name)
            .await
            .map_err(nats)?;
        let requested = self.config.consumer(queue);
        let consumer = stream
            .get_or_create_consumer(&queue.durable_name(), requested.clone())
            .await
            .map_err(nats)?;
        let mut actual = consumer.cached_info().config.clone();
        remove_server_metadata(&mut actual.metadata);
        if actual != requested.into_consumer_config() {
            return Err(Error::ConfigMismatch(queue.durable_name()));
        }
        Ok(consumer)
    }

    /// Publish versioned work and wait for the server's persistence acknowledgement.
    ///
    /// # Errors
    /// Returns encoding, publication, or persistence acknowledgement failures.
    pub async fn publish_work<T: Serialize + ?Sized>(
        &self,
        queue: &WorkQueue,
        value: &T,
    ) -> Result<(), Error> {
        self.jetstream
            .publish(self.config.subject(&queue.subject()), encode(value)?.into())
            .await
            .map_err(nats)?
            .await
            .map_err(nats)?;
        Ok(())
    }

    /// Open a pull loop sharing this route's durable consumer with other workers.
    /// Limit each pull batch to one message to keep local prefetch small.
    ///
    /// # Errors
    /// Returns setup, configuration, or pull subscription errors.
    pub async fn consume<T: DeserializeOwned>(
        &self,
        queue: &WorkQueue,
    ) -> Result<WorkMessages<T>, Error> {
        let consumer = self.ensure_consumer(queue).await?;
        let messages = consumer
            .stream()
            .max_messages_per_batch(1)
            .messages()
            .await
            .map_err(nats)?;
        Ok(WorkMessages {
            messages,
            payload: PhantomData,
        })
    }

    /// Publish an ephemeral versioned delta or event for current observers.
    ///
    /// # Errors
    /// Returns encoding or core NATS publication failures.
    pub async fn publish_live<T: Serialize + ?Sized>(
        &self,
        feed: LiveFeed,
        value: &T,
    ) -> Result<(), Error> {
        self.client
            .publish(self.config.subject(&feed.subject()), encode(value)?.into())
            .await
            .map_err(nats)?;
        Ok(())
    }

    /// Subscribe and confirm the registration before returning, so a subsequent
    /// publication from another connection cannot race the subscription.
    ///
    /// # Errors
    /// Returns core NATS subscription or registration round-trip failures.
    pub async fn subscribe_live<T: DeserializeOwned>(
        &self,
        feed: LiveFeed,
    ) -> Result<LiveMessages<T>, Error> {
        let messages = self
            .client
            .subscribe(self.config.subject(&feed.subject()))
            .await
            .map_err(nats)?;
        // Flush only drains the client buffer. Receiving our own message on a
        // private inbox confirms the server processed the preceding subscription.
        let inbox = self.client.new_inbox();
        self.client
            .send_request(inbox.clone(), async_nats::Request::new().inbox(inbox))
            .await
            .map_err(nats)?;
        Ok(LiveMessages {
            messages,
            payload: PhantomData,
        })
    }
}

pub struct WorkMessages<T> {
    messages: pull::Stream,
    payload: PhantomData<T>,
}

impl<T: DeserializeOwned> WorkMessages<T> {
    /// Await the next delivery. Decode failures leave the message unacknowledged,
    /// allowing retry up to the configured delivery limit.
    ///
    /// # Errors
    /// Surfaces transport and payload decoding failures without ending the loop.
    pub async fn next(&mut self) -> Option<Result<WorkMessage<T>, Error>> {
        let message = match self.messages.next().await? {
            Ok(message) => message,
            Err(error) => return Some(Err(nats(error))),
        };
        Some(
            decode(&message.payload)
                .map(|value| WorkMessage { value, message })
                .map_err(Error::from),
        )
    }
}

pub struct WorkMessage<T> {
    pub value: T,
    message: jetstream::Message,
}

impl<T> WorkMessage<T> {
    /// Finish this delivery and wait for the server to confirm the acknowledgement.
    ///
    /// # Errors
    /// Returns acknowledgement or confirmation failures; callers must tolerate retries.
    pub async fn acknowledge(&self) -> Result<(), Error> {
        self.message.double_ack().await.map_err(nats)
    }

    /// Release work for immediate retry or after an optional delay.
    ///
    /// # Errors
    /// Returns acknowledgement publication failures.
    pub async fn negative_acknowledge(&self, delay: Option<Duration>) -> Result<(), Error> {
        self.message
            .ack_with(jetstream::AckKind::Nak(delay))
            .await
            .map_err(nats)
    }

    /// Reset the acknowledgement deadline using an in-progress acknowledgement.
    ///
    /// # Errors
    /// Returns acknowledgement publication failures.
    pub async fn extend_deadline(&self) -> Result<(), Error> {
        self.message
            .ack_with(jetstream::AckKind::Progress)
            .await
            .map_err(nats)
    }
}

pub struct LiveMessages<T> {
    messages: async_nats::Subscriber,
    payload: PhantomData<T>,
}

impl<T: DeserializeOwned> LiveMessages<T> {
    /// # Errors
    /// Returns a decode error for malformed live payloads. Later messages remain readable.
    pub async fn next(&mut self) -> Option<Result<T, Error>> {
        let message = self.messages.next().await?;
        Some(decode(&message.payload).map_err(Error::from))
    }
}

// NATS adds its version and feature levels to responses. These are server facts,
// not requested configuration; retain all other metadata when comparing configs.
fn remove_server_metadata(metadata: &mut std::collections::HashMap<String, String>) {
    for key in ["_nats.ver", "_nats.level", "_nats.req.level"] {
        metadata.remove(key);
    }
}
