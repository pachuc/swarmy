//! `FoundationDB` key layout and atomic session operations.
//!
//! Call `boot` once at process startup and retain its guard until every store and
//! runtime using `FoundationDB` has stopped. The default directory is `swarmy`.
//! Event, snapshot, and request payloads above 80 KiB are uploaded before transactions start;
//! failed transactions can leave unreferenced, content-addressed blobs for later GC.
//! Session headers retain only the snapshot sequence so lease transactions never
//! fetch blobs. Scans are bounded and callers paginate by their last result. A commit with an unknown outcome is reported without replaying it.

mod agents;
pub use agents::AgentCreationReplay;
mod api_idempotency;
pub mod blob;
mod computers;
pub mod credentials;
mod inference;
mod inference_wait;
mod interrupt;
mod metrics;
pub use interrupt::InterruptResult;
pub use metrics::{MetricPatch, WaitKind, completion_patches, dispatch_patches};
mod selection;
mod services;
pub use selection::GatewayProvider;
pub use services::{
    SERVICE_EXPIRE_SECONDS, SERVICE_STALE_SECONDS, ServiceDetail, ServiceHealth, ServiceHeartbeat,
    ServiceRole,
};
mod keys;
pub use inference::{InferenceClaim, InferenceCompletion};
pub mod metering;
pub mod quota;
pub use metering::{MeteringDimension, UsageGroup, UsageGroupBy};
pub use quota::{EntryQuota, ObservedQuota, QuotaConfig, QuotaSource};
mod gc;
mod leases;
mod routes;
pub use routes::{ExpandedChain, RouteSnapshot, RouteStepStatus};
mod nodes;
mod placed_tools;
mod placements;
pub use placements::ScratchRecord;
mod plans;
mod session_images;
mod timers;
mod tool_routing;
mod tools;
mod turns;
pub use turns::SubmitRouteStep;
mod volumes;

pub use inference_wait::{BreakerCandidate, CredentialKey, InferenceFailureWait, InferenceWait};
pub use keys::{RUNNABLE_PARTITIONS, runnable_partition};

use std::{future::Future, sync::Arc};

use foundationdb::{
    Database, FdbBindingError, RangeOption, RetryableTransaction, Transaction,
    directory::{Directory, DirectoryLayer},
    options::TransactionOption,
    tuple::Subspace,
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use swarmy_core::{
    AgentRecord, EncodingError, Event, IdempotencyRecord, InflightRecord, RequestId, SessionId,
    SessionRecord, SessionState, SnapshotRef, decode, encode,
};

use blob::{BlobError, BlobStore};

pub const INLINE_LIMIT: usize = 80 * 1024;
/// Keep reads and mutations below `FoundationDB`'s transaction byte limit.
const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_SCAN_LIMIT: usize = 64;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("keyring cannot decrypt credential; check SWARMY_KEYRING and the cluster key")]
    Keyring,
    #[error("credential does not exist")]
    CredentialMissing,
    #[error("route does not exist")]
    RouteMissing,
    #[error("invalid route: {0}")]
    InvalidRoute(String),
    #[error("credential refresh failed; login required")]
    CredentialRefresh,
    #[error("GitHub token must contain 1-4096 printable ASCII characters without whitespace")]
    InvalidGithubToken,

    #[error(transparent)]
    FoundationDb(#[from] foundationdb::FdbError),
    #[error(transparent)]
    Binding(#[from] FdbBindingError),
    #[error(transparent)]
    Encoding(#[from] EncodingError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("agent does not exist")]
    AgentMissing,
    #[error("agent id or name already exists")]
    AgentExists,
    #[error("agent name must be nonempty and contain no control characters")]
    InvalidAgentName,
    #[error("--image cannot be used with a named agent; its pinned image is used")]
    NamedAgentImage,
    #[error("an ephemeral session requires an image")]
    SessionImageRequired,
    #[error("This session's computer has been deleted. Create a new session to run tools.")]
    ComputerDeleted,
    #[error("cannot close an agent main session; use swarmy agent delete to delete the agent")]
    MainSessionClose,
    #[error("main session must be an open session belonging to the agent")]
    InvalidMainSession,
    #[error("node does not exist")]
    NodeMissing,
    #[error("node has no computer capacity available: {detail}")]
    NodeAtCapacity { detail: String },
    #[error("sandbox requirements can only change after the current placement is evicted")]
    ActiveSandboxRequirements,
    #[error("placement already exists")]
    PlacementExists,
    #[error("volume does not exist")]
    VolumeMissing,
    #[error("volume already exists")]
    VolumeExists,
    #[error("image {image:?} is not registered; registered images: {registered}")]
    ImageMissing { image: String, registered: String },
    #[error("expected image NAME:TAG")]
    InvalidImage,
    #[error("manifest does not exist")]
    ManifestMissing,
    #[error("manifest id already refers to a different header")]
    ManifestExists,
    #[error("invalid manifest dimensions")]
    InvalidManifest,
    #[error("volume head changed since this writer opened it")]
    VolumeHeadMismatch,
    #[error("session does not exist")]
    SessionMissing,
    #[error("session already exists")]
    SessionExists,
    #[error("session is idle or completed; there is nothing to interrupt")]
    NothingToInterrupt,
    #[error("session interruption was requested before the turn ended")]
    InterruptPending,
    #[error("expected head {expected}, found {actual}")]
    StaleSequence { expected: u64, actual: u64 },
    #[error("invalid state transition or initial session record")]
    InvalidState,
    #[error("lease is absent, expired, or no longer matches")]
    LeaseMismatch,
    #[error("sequence number overflow")]
    SequenceOverflow,
    #[error("metadata or batch exceeds the storage budget")]
    TooLarge,
    #[error("scan limit must be between 1 and 64")]
    InvalidLimit,
    #[error("stored key or blob is corrupt")]
    Corrupt,
    #[error("commit outcome is unknown; read durable state before retrying")]
    CommitUnknown,
}

pub type Result<T> = std::result::Result<T, StoreError>;

/// Start the `FoundationDB` network once per process.
///
/// Keep the returned guard alive until all database handles have been dropped.
/// Tests should share one guard through `std::sync::OnceLock`.
/// # Panics
/// Panics if the `FoundationDB` client cannot initialize or was already booted.
#[allow(unsafe_code)]
#[must_use]
pub fn boot() -> foundationdb::api::NetworkAutoStop {
    // The FoundationDB client requires unsafe boot; callers retain the network
    // guard so its thread outlives all client operations.
    unsafe { foundationdb::boot() }
}

#[derive(Serialize, Deserialize)]
enum StoredValue {
    Inline(Vec<u8>),
    Blob(String),
}

// Keep the snapshot sequence in the header so state and head updates never
// need object storage, even when the snapshot metadata is large.
#[derive(Serialize, Deserialize)]
struct StoredSession {
    session_id: SessionId,
    agent_id: swarmy_core::AgentId,
    state: SessionState,
    head_seq: u64,
    snapshot_seq: Option<u64>,
    // Side rows are read with the header but never change its legacy encoding.
    #[serde(skip)]
    kind: swarmy_core::SessionKind,
    #[serde(skip)]
    computer_deleted: bool,
    #[serde(skip)]
    plan: Vec<swarmy_core::PlanStep>,
    #[serde(skip)]
    inference: swarmy_core::InferenceSelection,
    #[serde(skip)]
    interrupt_requested: bool,
    // The route override and attempt position live in side rows so the
    // header keeps its legacy encoding.
    #[serde(skip)]
    route: Option<String>,
    #[serde(skip)]
    route_step: u32,
}

#[derive(Clone)]
pub struct Store {
    db: Arc<Database>,
    root: Subspace,
    blobs: Arc<dyn BlobStore>,
    images: session_images::ImageCache,
}

impl Store {
    /// Open the `swarmy` directory, or a separate directory path for isolation.
    /// # Errors
    /// Returns client, directory, or transaction errors.
    pub async fn open(
        cluster_file: Option<&str>,
        directory: Option<&[String]>,
        blobs: Arc<dyn BlobStore>,
    ) -> Result<Self> {
        let db = Arc::new(Database::new(cluster_file)?);
        let path = directory.map_or_else(|| vec!["swarmy".into()], <[String]>::to_vec);
        let prefix = db
            .run(|trx, _| {
                let path = &path;
                async move {
                    trx.set_option(TransactionOption::Timeout(4_500))?;
                    let output = DirectoryLayer::default()
                        .create_or_open(&trx, path, None, None)
                        .await?;
                    Ok(output.bytes()?.to_vec())
                }
            })
            .await?;
        Ok(Self {
            db,
            root: Subspace::from_bytes(prefix),
            images: Arc::default(),
            blobs,
        })
    }

    /// Use an explicitly allocated root prefix, primarily for isolated tests.
    #[must_use]
    pub fn with_subspace(db: Arc<Database>, root: Subspace, blobs: Arc<dyn BlobStore>) -> Self {
        Self {
            db,
            root,
            blobs,
            images: Arc::default(),
        }
    }

    async fn transaction<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: Fn(RetryableTransaction) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let result = self
            .db
            .run(|trx, maybe_committed| {
                let operation = &operation;
                async move {
                    if bool::from(maybe_committed) {
                        return Err(FdbBindingError::new_custom_error(Box::new(
                            StoreError::CommitUnknown,
                        )));
                    }
                    trx.set_option(TransactionOption::Timeout(4_500))?;
                    trx.set_option(TransactionOption::RetryLimit(20))?;
                    operation(trx).await.map_err(|error| match error {
                        StoreError::FoundationDb(error) => error.into(),
                        other => FdbBindingError::new_custom_error(Box::new(other)),
                    })
                }
            })
            .await;
        result.map_err(|error| match error {
            FdbBindingError::CustomError(error) => match error.downcast::<StoreError>() {
                Ok(error) => *error,
                Err(error) => StoreError::Binding(FdbBindingError::CustomError(error)),
            },
            other => StoreError::Binding(other),
        })
    }

    /// Persist binary tool content without adding it to the session event log.
    ///
    /// # Errors
    /// Returns an error if object storage rejects the upload.
    pub async fn put_tool_blob(&self, bytes: Vec<u8>) -> Result<String> {
        let key = format!("blobs/{}", blake3::hash(&bytes).to_hex());
        self.blobs.put(&key, bytes.into()).await?;
        Ok(key)
    }

    async fn prepare<T: Serialize>(&self, value: &T) -> Result<Vec<u8>> {
        let bytes = encode(value)?;
        let stored = if bytes.len() > INLINE_LIMIT {
            let key = format!("blobs/{}", blake3::hash(&bytes).to_hex());
            self.blobs.put(&key, bytes.into()).await?;
            StoredValue::Blob(key)
        } else {
            StoredValue::Inline(bytes)
        };
        Ok(encode(&stored)?)
    }

    async fn hydrate<T: DeserializeOwned>(&self, value: &[u8]) -> Result<T> {
        match decode(value)? {
            StoredValue::Inline(bytes) => Ok(decode(&bytes)?),
            StoredValue::Blob(key) => {
                let bytes = self.blobs.get(&key).await?;
                if key != format!("blobs/{}", blake3::hash(&bytes).to_hex()) {
                    return Err(StoreError::Corrupt);
                }
                Ok(decode(&bytes)?)
            }
        }
    }

    pub(crate) async fn session(&self, trx: &Transaction, id: SessionId) -> Result<StoredSession> {
        read(trx, &self.session_key(id))
            .await?
            .ok_or(StoreError::SessionMissing)
    }

    /// Create an empty session and pin its registered image in the same transaction.
    /// The separate image row preserves the binary layout of legacy session headers.
    /// Runnable creation also indexes the session.
    /// # Errors
    /// Rejects unknown images, duplicate ids, nonempty logs, and invalid initial state.
    pub async fn create_session(
        &self,
        session: &SessionRecord,
        wake_at: jiff::Timestamp,
        image: &str,
    ) -> Result<()> {
        self.create_session_record(session, wake_at, Some(image))
            .await
    }

    /// # Errors
    /// Returns storage or decoding errors.
    pub async fn fetch_session(&self, id: SessionId) -> Result<Option<SessionRecord>> {
        Ok(self
            .fetch_session_with_agent(id)
            .await?
            .map(|(session, _)| session))
    }

    /// Fetch a session with its agent record in one transaction, so the
    /// scheduler resolves the agent's route assignment without a second
    /// transaction per session. The agent is `None` for ephemeral sessions
    /// and deleted agents.
    /// # Errors
    /// Returns storage or decoding errors.
    pub async fn fetch_session_with_agent(
        &self,
        id: SessionId,
    ) -> Result<Option<(SessionRecord, Option<AgentRecord>)>> {
        let stored = self
            .transaction(|trx| async move {
                let Some(session) = read::<StoredSession>(&trx, &self.session_key(id)).await?
                else {
                    return Ok(None);
                };
                let snapshot = if let Some(seq) = session.snapshot_seq {
                    Some(
                        trx.get(&self.snapshot_key(id, seq), false)
                            .await?
                            .ok_or(StoreError::Corrupt)?
                            .to_vec(),
                    )
                } else {
                    None
                };
                let agent_id = session.agent_id;
                let session = self.session_metadata(&trx, session).await?;
                let agent = self.read_agent(&trx, agent_id).await?;
                Ok(Some((session, snapshot, agent)))
            })
            .await?;
        let Some((session, snapshot, agent)) = stored else {
            return Ok(None);
        };
        Ok(Some((
            self.hydrate_session(session, snapshot).await?,
            agent,
        )))
    }

    async fn session_metadata(
        &self,
        trx: &Transaction,
        mut session: StoredSession,
    ) -> Result<StoredSession> {
        (
            session.kind,
            session.computer_deleted,
            session.plan,
            session.inference,
            session.interrupt_requested,
            session.route,
            session.route_step,
        ) = futures::try_join!(
            self.session_kind(trx, session.session_id),
            self.computer_deleted(trx, session.agent_id),
            async {
                Ok::<_, StoreError>(
                    read(trx, &self.session_plan_key(session.session_id))
                        .await?
                        .unwrap_or_default(),
                )
            },
            async {
                Ok::<_, StoreError>(
                    read(trx, &self.session_inference_key(session.session_id))
                        .await?
                        .unwrap_or_default(),
                )
            },
            async {
                Ok::<_, StoreError>(
                    read(trx, &self.interrupt_key(session.session_id))
                        .await?
                        .unwrap_or(false),
                )
            },
            async {
                Ok::<_, StoreError>(
                    read::<Option<String>>(trx, &self.session_route_key(session.session_id))
                        .await?
                        .flatten(),
                )
            },
            async {
                Ok::<_, StoreError>(
                    read(trx, &self.session_route_step_key(session.session_id))
                        .await?
                        .unwrap_or(0),
                )
            },
        )?;
        Ok(session)
    }

    async fn hydrate_session(
        &self,
        session: StoredSession,
        snapshot: Option<Vec<u8>>,
    ) -> Result<SessionRecord> {
        let snapshot_ref = match snapshot {
            Some(value) => Some(self.hydrate(&value).await?),
            None => None,
        };
        Ok(SessionRecord {
            interrupt_requested: session.interrupt_requested,
            kind: session.kind,
            computer_deleted: session.computer_deleted,
            plan: session.plan,
            session_id: session.session_id,
            agent_id: session.agent_id,
            state: session.state,
            head_seq: session.head_seq,
            snapshot_ref,
            inference: session.inference,
            route: session.route,
            route_step: session.route_step,
        })
    }

    /// List sessions in ascending id order, strictly after `after`.
    /// Each page reads headers and snapshot pointers in one transaction. Pages
    /// are independent views; sessions created behind the cursor are not included.
    /// # Errors
    /// Rejects invalid limits and returns storage, blob, or decoding errors.
    pub async fn list_sessions(
        &self,
        after: Option<SessionId>,
        limit: usize,
    ) -> Result<Vec<SessionRecord>> {
        check_limit(limit)?;
        let stored = self
            .transaction(|trx| async move {
                let (mut begin, end) = self.root.subspace(&("session",)).range();
                if let Some(id) = after {
                    begin = self.session_key(id);
                    begin.push(0);
                }
                let mut sessions = Vec::new();
                for (_, value) in scan(&trx, (begin, end), limit).await? {
                    let session: StoredSession = decode(&value)?;
                    let snapshot = if let Some(seq) = session.snapshot_seq {
                        Some(
                            trx.get(&self.snapshot_key(session.session_id, seq), false)
                                .await?
                                .ok_or(StoreError::Corrupt)?
                                .to_vec(),
                        )
                    } else {
                        None
                    };
                    let session = self.session_metadata(&trx, session).await?;
                    sessions.push((session, snapshot));
                }
                Ok(sessions)
            })
            .await?;
        let mut sessions = Vec::with_capacity(stored.len());
        for (session, snapshot) in stored {
            sessions.push(self.hydrate_session(session, snapshot).await?);
        }
        Ok(sessions)
    }

    /// Assign sequences after `expected_head`, ignoring input event sequences.
    /// Blob uploads precede the transaction; events and head commit atomically.
    /// # Errors
    /// Rejects a stale head, missing session, sequence overflow, or oversized batch.
    pub async fn append_events(
        &self,
        id: SessionId,
        expected_head: u64,
        events: &[Event],
    ) -> Result<u64> {
        self.append_events_inner(id, expected_head, events, None, false)
            .await
    }

    /// Append under a live lease, fencing workers that were reaped or replaced.
    /// # Errors
    /// Returns append errors or `LeaseMismatch` for a stale or expired token.
    pub async fn append_events_leased(
        &self,
        id: SessionId,
        expected_head: u64,
        events: &[Event],
        lease: &swarmy_core::Lease,
        now: jiff::Timestamp,
    ) -> Result<u64> {
        self.append_events_inner(id, expected_head, events, Some((lease, now)), false)
            .await
    }

    /// Append one user message and index Runnable in the same transaction.
    /// # Errors
    /// Rejects non-user messages, non-idle sessions, stale heads and append errors.
    pub async fn append_user_message(
        &self,
        id: SessionId,
        expected_head: u64,
        message: &swarmy_core::Message,
    ) -> Result<u64> {
        if message.role != swarmy_core::MessageRole::User {
            return Err(StoreError::InvalidState);
        }
        self.append_events_inner(
            id,
            expected_head,
            &[Event::MessageAppended {
                seq: 0,
                message: message.clone(),
            }],
            None,
            true,
        )
        .await
    }

    /// Atomically deduplicate an API append with the user message and Runnable transition.
    /// # Errors
    /// Rejects non-user messages, stale heads, non-idle sessions, or storage failures.
    pub async fn append_user_message_idempotent(
        &self,
        id: SessionId,
        expected_head: u64,
        message: &swarmy_core::Message,
        key: &str,
    ) -> Result<(u64, bool)> {
        if message.role != swarmy_core::MessageRole::User {
            return Err(StoreError::InvalidState);
        }
        let head = expected_head
            .checked_add(1)
            .ok_or(StoreError::SequenceOverflow)?;
        let event = Event::MessageAppended {
            seq: head,
            message: message.clone(),
        };
        let value = self.prepare(&event).await?;
        if value.len() > MAX_BATCH_BYTES {
            return Err(StoreError::TooLarge);
        }
        let replay_key = self.root.pack(&("api_append", key));
        self.transaction(|trx| {
            let replay_key = &replay_key;
            let value = &value;
            async move {
                if let Some(previous) = read::<u64>(&trx, replay_key).await? {
                    return Ok((previous, false));
                }
                let mut session = self.session(&trx, id).await?;
                if session.head_seq != expected_head {
                    return Err(StoreError::StaleSequence {
                        expected: expected_head,
                        actual: session.head_seq,
                    });
                }
                if session.state != SessionState::Idle {
                    return Err(StoreError::InvalidState);
                }
                trx.set(&self.event_space(id).pack(&(head,)), value);
                write(&trx, &self.turn_key(id), &message.id)?;
                write(&trx, replay_key, &head)?;
                session.head_seq = head;
                self.transition(
                    &trx,
                    session,
                    SessionState::Runnable,
                    jiff::Timestamp::now(),
                )
                .await?;
                Ok((head, true))
            }
        })
        .await
    }

    async fn append_events_inner(
        &self,
        id: SessionId,
        expected_head: u64,
        events: &[Event],
        fence: Option<(&swarmy_core::Lease, jiff::Timestamp)>,
        wake: bool,
    ) -> Result<u64> {
        let head = expected_head
            .checked_add(u64::try_from(events.len()).map_err(|_| StoreError::TooLarge)?)
            .ok_or(StoreError::SequenceOverflow)?;
        let mut prepared = Vec::with_capacity(events.len());
        let mut size = 0;
        for (event, seq) in events.iter().zip((expected_head..head).map(|n| n + 1)) {
            let mut event = event.clone();
            event.set_seq(seq);
            let key = self.event_space(id).pack(&(seq,));
            let value = self.prepare(&event).await?;
            size += key.len() + value.len();
            if size > MAX_BATCH_BYTES {
                return Err(StoreError::TooLarge);
            }
            prepared.push((key, value));
        }
        self.transaction(|trx| {
            let prepared = &prepared;
            async move {
                if let Some((lease, now)) = fence {
                    self.check_worker_lease(&trx, id, lease, now).await?;
                }
                let mut session = self.session(&trx, id).await?;
                if session.head_seq != expected_head {
                    return Err(StoreError::StaleSequence {
                        expected: expected_head,
                        actual: session.head_seq,
                    });
                }
                if wake && session.state != SessionState::Idle {
                    return Err(StoreError::InvalidState);
                }
                for (key, value) in prepared {
                    trx.set(key, value);
                }
                for event in events {
                    if let Event::MessageAppended { message, .. } = event
                        && message.role == swarmy_core::MessageRole::User
                    {
                        write(&trx, &self.turn_key(id), &message.id)?;
                    }
                    if let Event::InferenceRequested { request_id, .. }
                    | Event::ToolCallRequested { request_id, .. } = event
                        && let Some(turn) =
                            read::<swarmy_core::MessageId>(&trx, &self.turn_key(id)).await?
                    {
                        write(&trx, &self.request_turn_key(*request_id), &turn)?;
                    }
                }
                session.head_seq = head;
                if wake {
                    self.transition(
                        &trx,
                        session,
                        SessionState::Runnable,
                        jiff::Timestamp::now(),
                    )
                    .await
                } else {
                    write(&trx, &self.session_key(id), &session)
                }
            }
        })
        .await?;
        Ok(head)
    }

    /// Read a bounded tail in ascending order, strictly after `after`.
    /// # Errors
    /// Returns an error for invalid limits, missing blobs, or storage failures.
    pub async fn read_events(&self, id: SessionId, after: u64, limit: usize) -> Result<Vec<Event>> {
        check_limit(limit)?;
        let values = self
            .transaction(|trx| async move {
                let space = self.event_space(id);
                let mut begin = space.pack(&(after,));
                begin.push(0);
                scan(&trx, (begin, space.range().1), limit).await
            })
            .await?;
        let mut events = Vec::with_capacity(values.len());
        for (_, value) in values {
            events.push(self.hydrate(&value).await?);
        }
        Ok(events)
    }

    /// Store a snapshot pointer in both the snapshot index and session record.
    /// # Errors
    /// Rejects snapshots ahead of the log or older than the current snapshot.
    pub async fn write_snapshot(&self, id: SessionId, snapshot: &SnapshotRef) -> Result<()> {
        let value = self.prepare(snapshot).await?;
        self.transaction(|trx| {
            let value = &value;
            async move {
                let mut session = self.session(&trx, id).await?;
                if snapshot.seq > session.head_seq
                    || session.snapshot_seq.is_some_and(|old| old > snapshot.seq)
                {
                    return Err(StoreError::StaleSequence {
                        expected: snapshot.seq,
                        actual: session.head_seq,
                    });
                }
                trx.set(&self.snapshot_key(id, snapshot.seq), value);
                session.snapshot_seq = Some(snapshot.seq);
                write(&trx, &self.session_key(id), &session)
            }
        })
        .await
    }

    fn snapshot_key(&self, id: SessionId, seq: u64) -> Vec<u8> {
        self.root
            .pack(&("snapshot", id.as_ulid().to_bytes().as_slice(), seq))
    }

    /// # Errors
    /// Returns storage, blob, or decoding errors.
    pub async fn get_idempotency(&self, id: RequestId) -> Result<Option<IdempotencyRecord>> {
        self.get_payload(self.root.pack(&("idem", id.as_bytes().as_slice())))
            .await
    }

    /// # Errors
    /// Returns storage or blob upload errors.
    pub async fn put_idempotency(&self, id: RequestId, record: &IdempotencyRecord) -> Result<()> {
        self.put_payload(self.root.pack(&("idem", id.as_bytes().as_slice())), record)
            .await
    }

    /// # Errors
    /// Returns storage or blob upload errors.
    pub async fn put_inflight(&self, id: RequestId, record: &InflightRecord) -> Result<()> {
        self.put_payload(
            self.root.pack(&("inflight", id.as_bytes().as_slice())),
            record,
        )
        .await
    }

    /// # Errors
    /// Returns storage, blob, or decoding errors.
    pub async fn get_inflight(&self, id: RequestId) -> Result<Option<InflightRecord>> {
        self.get_payload(self.root.pack(&("inflight", id.as_bytes().as_slice())))
            .await
    }

    /// # Errors
    /// Returns transaction errors.
    pub async fn clear_inflight(&self, id: RequestId) -> Result<()> {
        self.transaction(|trx| async move {
            trx.clear(&self.root.pack(&("inflight", id.as_bytes().as_slice())));
            Ok(())
        })
        .await
    }

    async fn put_payload<T: Serialize>(&self, key: Vec<u8>, value: &T) -> Result<()> {
        let value = self.prepare(value).await?;
        self.transaction(|trx| {
            let key = &key;
            let value = &value;
            async move {
                trx.set(key, value);
                Ok(())
            }
        })
        .await
    }

    async fn get_payload<T: DeserializeOwned>(&self, key: Vec<u8>) -> Result<Option<T>> {
        let bytes = self
            .transaction(|trx| {
                let key = &key;
                async move { Ok(trx.get(key, false).await?.map(|v| v.to_vec())) }
            })
            .await?;
        match bytes {
            Some(value) => Ok(Some(self.hydrate(&value).await?)),
            None => Ok(None),
        }
    }
}

async fn read<T: DeserializeOwned>(trx: &Transaction, key: &[u8]) -> Result<Option<T>> {
    trx.get(key, false)
        .await?
        .map(|value| decode(&value).map_err(Into::into))
        .transpose()
}

fn write<T: Serialize>(trx: &Transaction, key: &[u8], value: &T) -> Result<()> {
    let bytes = encode(value)?;
    if bytes.len() > INLINE_LIMIT {
        return Err(StoreError::TooLarge);
    }
    trx.set(key, &bytes);
    Ok(())
}

fn check_limit(limit: usize) -> Result<()> {
    if (1..=MAX_SCAN_LIMIT).contains(&limit) {
        Ok(())
    } else {
        Err(StoreError::InvalidLimit)
    }
}

async fn scan(
    trx: &Transaction,
    range: (Vec<u8>, Vec<u8>),
    limit: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    check_limit(limit)?;
    let options = RangeOption {
        limit: Some(limit),
        ..range.into()
    };
    Ok(trx
        .get_ranges_keyvalues(options, false)
        .map_ok(|kv| (kv.key().to_vec(), kv.value().to_vec()))
        .try_collect()
        .await?)
}

#[cfg(test)]
mod compatibility_tests {
    use super::*;

    #[test]
    fn legacy_session_header_is_still_readable_and_writes_the_same_bytes() {
        // A tuple encodes the original five postcard fields without adding metadata.
        let id = SessionId::from_ulid(ulid::Ulid::from_parts(1, 2));
        let agent = swarmy_core::AgentId::from_ulid(ulid::Ulid::from_parts(1, 3));
        let original = encode(&(id, agent, SessionState::Idle, 42_u64, Some(20_u64))).unwrap();
        let mut header: StoredSession = decode(&original).unwrap();
        assert_eq!(header.session_id, id);
        assert_eq!(header.agent_id, agent);
        assert_eq!(header.state, SessionState::Idle);
        assert_eq!(header.head_seq, 42);
        assert_eq!(header.snapshot_seq, Some(20));
        assert_eq!(header.kind, swarmy_core::SessionKind::Ephemeral);
        assert!(!header.computer_deleted);
        assert_eq!(header.inference, swarmy_core::InferenceSelection::default());
        header.kind = swarmy_core::SessionKind::Named { agent_id: agent };
        header.computer_deleted = true;
        assert_eq!(encode(&header).unwrap(), original);
    }
}

mod usage;
pub use usage::{UsageAttribution, UsageRecord};
