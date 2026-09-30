//! `FoundationDB` key layout and atomic session operations.
#![deny(unreachable_pub)]
//!
//! Call `boot` once at process startup and retain its guard until every store and
//! runtime using `FoundationDB` has stopped. The default directory is `swarmy`.
//! Event, snapshot, and request payloads above 80 KiB are uploaded before transactions start;
//! failed transactions can leave unreferenced, content-addressed blobs for later GC.
//! Session records use a per-record version. Scans are bounded and callers paginate by their last result. A commit with an unknown outcome is reported without replaying it.

mod agents;
pub use agents::{AgentSessionOptions, CreateAgentOptions};
mod api_idempotency;
mod errors;
pub use errors::{DomainError, FenceError, Result, StorageError, StoreError};
mod session;
pub(crate) use session::{
    SESSION_CHUNK_MARKER, SESSION_MAX_BYTES, SESSION_RECORD_VERSION, StoredSession,
    StoredSessionCurrent,
};
pub mod blob;
mod computers;
pub mod credentials;
mod inference;
mod inference_wait;
mod interrupt;
mod metrics;
mod metrics_codec;
mod metrics_model;
#[cfg(test)]
mod metrics_tests;
pub use interrupt::InterruptResult;
pub use metrics::{
    AgentMetrics, ComputerMetric, InferenceMetric, LatencyPercentiles, MetricPatch, StageTiming,
    ToolMetric, TurnMetrics, WaitKind, completion_patches, dispatch_patches,
};
mod selection;
mod services;
pub use selection::GatewayProvider;
pub use services::{
    SERVICE_EXPIRE_SECONDS, SERVICE_STALE_SECONDS, ServiceDetail, ServiceHealth, ServiceHeartbeat,
    ServiceRole,
};
mod keys;
#[cfg(test)]
mod keys_tests;
mod runnable;
pub use inference::{InferenceClaim, InferenceCompletion};
pub mod metering;
mod queued;
pub mod quota;
pub use metering::{DimensionTotal, MeteringDimension, UsageGroup, UsageGroupBy};
pub use quota::{EntryQuota, ObservedQuota, QuotaConfig, QuotaSource};
mod gc;
mod leases;
mod routes;
pub use routes::{
    ExpandedChain, FailoverAction, FailoverOutcome, PoolEntry, RouteCache, RouteFailure,
    RouteSelection, RouteSnapshot, RouteStepStatus,
};
mod nodes;
pub mod objects;
mod placed_tools;
mod placements;
pub use placements::ScratchRecord;
mod plans;
mod session_images;
mod timers;
mod tool_routing;
mod tools;
mod turns;
pub use turns::{SubmitInferenceOptions, SubmitRouteStep};
mod volumes;
pub use volumes::PutImageOptions;

pub use inference_wait::{BreakerCandidate, CredentialKey, InferenceFailureWait, InferenceWait};
pub use runnable::{RUNNABLE_PARTITIONS, runnable_partition};

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use foundationdb::{
    Database, FdbBindingError, RangeOption, RetryableTransaction, Transaction,
    directory::{Directory, DirectoryLayer},
    options::TransactionOption,
    tuple::Subspace,
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use swarmy_core::{
    AgentRecord, EncodingError, Event, IdempotencyRecord, RequestId, SessionId, SessionRecord,
    SessionState, decode, encode,
};

#[cfg(any(test, feature = "test-support"))]
use swarmy_core::{InflightRecord, SnapshotRef};

use blob::BlobStore;

pub const INLINE_LIMIT: usize = 80 * 1024;
/// Keep reads and mutations below `FoundationDB`'s transaction byte limit.
const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_SCAN_LIMIT: usize = 64;

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

/// Open a `FoundationDB` database from a cluster file path without converting
/// at the call site. Rejects non-UTF-8 paths instead of silently mangling them.
/// # Errors
/// Returns client errors or rejects a non-UTF-8 cluster path.
pub fn database(cluster_file: &std::path::Path) -> Result<Database> {
    let path = cluster_file
        .to_str()
        .ok_or(StorageError::NonUtf8ClusterFile)?;
    Ok(Database::new(Some(path))?)
}

#[derive(Serialize, Deserialize)]
enum StoredValue {
    Inline(Vec<u8>),
    Blob(String),
}

/// Receiver for the metrics queue, held until the drain task starts.
type MetricsReceiver = tokio::sync::mpsc::Receiver<crate::metrics::MetricMsg>;

#[derive(Clone)]
pub struct Store {
    db: Arc<Database>,
    root: Subspace,
    clock: Arc<dyn Fn() -> jiff::Timestamp + Send + Sync>,
    blobs: Arc<dyn BlobStore>,
    images: session_images::ImageCache,
    /// Logical store transactions started through [`Store::transaction`].
    /// Binding-level retries inside one call count once; the counter exists
    /// so tests can compare per-operation transaction costs.
    transactions: Arc<AtomicU64>,
    session_record_reads: Arc<AtomicU64>,
    metrics_tx: tokio::sync::mpsc::Sender<crate::metrics::MetricMsg>,
    /// Receiver held until the drain task starts. `open` starts the drain
    /// immediately; `with_subspace` may run without a runtime, in which case
    /// the first observation or flush starts it lazily.
    metrics_rx: Arc<std::sync::Mutex<Option<MetricsReceiver>>>,
    /// Drain task handle, retained so every `Store` does not leak a task:
    /// the drain owns only a [`crate::metrics::MetricsWriter`], never a
    /// `Store`, so dropping all stores closes the channel and ends the task.
    metrics_drain: Arc<std::sync::OnceLock<tokio::task::JoinHandle<()>>>,
}

fn metrics_channel() -> (
    tokio::sync::mpsc::Sender<crate::metrics::MetricMsg>,
    tokio::sync::mpsc::Receiver<crate::metrics::MetricMsg>,
) {
    tokio::sync::mpsc::channel(crate::metrics::METRICS_CHANNEL_BOUND)
}

/// Store and blob handles opened together from one settings object.
pub struct OpenedStore {
    pub store: Store,
    pub blobs: Arc<crate::blob::ObjectBlobStore>,
}

impl Store {
    /// Open the `swarmy` directory, or a separate directory path for isolation.
    /// # Errors
    /// Returns client, directory, or transaction errors.
    pub async fn open(
        cluster_file: Option<&std::path::Path>,
        directory: Option<&[String]>,
        blobs: Arc<dyn BlobStore>,
    ) -> Result<Self> {
        let db = match cluster_file {
            Some(path) => Arc::new(crate::database(path)?),
            None => Arc::new(Database::new(None)?),
        };
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
        let (metrics_tx, metrics_rx) = metrics_channel();
        let store = Self {
            db,
            root: Subspace::from_bytes(prefix),
            clock: Arc::new(jiff::Timestamp::now),
            images: Arc::default(),
            blobs,
            transactions: Arc::default(),
            session_record_reads: Arc::default(),
            metrics_tx,
            metrics_rx: Arc::new(std::sync::Mutex::new(Some(metrics_rx))),
            metrics_drain: Arc::new(std::sync::OnceLock::new()),
        };
        store.ensure_metrics_drain();
        Ok(store)
    }

    /// Open the store described by `settings`: its cluster file, directory
    /// namespace, and object namespace. Every service starts here instead of
    /// splitting the directory and building a blob client by hand.
    /// # Errors
    /// Returns configuration, client, directory, or transaction errors.
    pub async fn open_store(settings: &swarmy_config::Settings) -> Result<OpenedStore> {
        let directory = settings
            .store_directory_path()
            .map_err(crate::blob::BlobError::from)?;
        let blobs = Arc::new(crate::blob::ObjectBlobStore::from_settings(settings)?);
        let store = Self::open(
            Some(settings.store.cluster_file.as_path()),
            Some(&directory),
            blobs.clone(),
        )
        .await?;
        Ok(OpenedStore { store, blobs })
    }

    /// Use an explicitly allocated root prefix, primarily for isolated tests.
    /// The metrics drain starts lazily on the first observation or flush
    /// when no async runtime exists yet at construction time.
    #[must_use]
    pub fn with_subspace(db: Arc<Database>, root: Subspace, blobs: Arc<dyn BlobStore>) -> Self {
        let (metrics_tx, metrics_rx) = metrics_channel();
        let store = Self {
            db,
            root,
            clock: Arc::new(jiff::Timestamp::now),
            blobs,
            images: Arc::default(),
            transactions: Arc::default(),
            session_record_reads: Arc::default(),
            metrics_tx,
            metrics_rx: Arc::new(std::sync::Mutex::new(Some(metrics_rx))),
            metrics_drain: Arc::new(std::sync::OnceLock::new()),
        };
        store.ensure_metrics_drain();
        store
    }

    pub(crate) fn metrics_writer(&self) -> crate::metrics::MetricsWriter {
        crate::metrics::MetricsWriter::new(
            self.db.clone(),
            self.root.clone(),
            self.transactions.clone(),
        )
    }

    /// Start the metrics drain task once a runtime exists. Synchronous so
    /// both constructors and the non-blocking observation path can call it;
    /// a second call is a no-op once the receiver has been taken.
    pub(crate) fn ensure_metrics_drain(&self) {
        if self.metrics_drain.get().is_some() {
            return;
        }
        let rx = match self.metrics_rx.lock() {
            Ok(mut guard) => guard.take(),
            Err(error) => {
                tracing::warn!(%error, "metrics queue lock poisoned; skipping drain start");
                return;
            }
        };
        let Some(rx) = rx else {
            return;
        };
        if tokio::runtime::Handle::try_current().is_err() {
            if let Ok(mut guard) = self.metrics_rx.lock() {
                *guard = Some(rx);
            }
            return;
        }
        let handle = crate::metrics::spawn_metrics_drain(self.metrics_writer(), rx);
        if self.metrics_drain.set(handle).is_err() {
            tracing::debug!("metrics drain already started");
        }
    }

    /// Use a deterministic clock for lease and expiry tests.
    #[must_use]
    #[cfg(any(test, feature = "test-support"))]
    pub fn with_clock(
        mut self,
        clock: impl Fn() -> jiff::Timestamp + Send + Sync + 'static,
    ) -> Self {
        self.clock = Arc::new(clock);
        self
    }

    pub(crate) fn keys(&self) -> crate::keys::Keys<'_> {
        crate::keys::Keys::new(&self.root)
    }

    /// Test-only entry point, also available with the `test-support` feature.
    #[must_use]
    #[cfg(any(test, feature = "test-support"))]
    pub fn now(&self) -> jiff::Timestamp {
        (self.clock)()
    }

    #[cfg(not(any(test, feature = "test-support")))]
    pub(crate) fn now(&self) -> jiff::Timestamp {
        (self.clock)()
    }

    /// Logical store transactions started so far. Tests use it to compare
    /// per-operation costs; production code never branches on it.
    /// Test-only entry point, also available with the `test-support` feature.
    #[must_use]
    #[cfg(any(test, feature = "test-support"))]
    pub fn transaction_count(&self) -> u64 {
        self.transactions.load(Ordering::Relaxed)
    }

    /// Number of session-record key reads made by this store instance.
    /// Test-only entry point, also available with the `test-support` feature.
    #[must_use]
    #[cfg(any(test, feature = "test-support"))]
    pub fn session_record_read_count(&self) -> u64 {
        self.session_record_reads.load(Ordering::Relaxed)
    }

    async fn transaction<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: Fn(RetryableTransaction) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        run_transaction(&self.db, &self.transactions, operation).await
    }

    /// Read every row in `range` across one transaction per page, paging by
    /// the last key. Callers that must cross transaction boundaries use this
    /// instead of copying the per-page `transaction` then `push(0)` loop.
    pub(crate) async fn scan_all_pages(
        &self,
        begin: Vec<u8>,
        end: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut cursor: Option<Vec<u8>> = None;
        let mut out = Vec::new();
        loop {
            let page = self
                .transaction(|trx| {
                    let (begin, end) = (begin.clone(), end.clone());
                    let cursor = cursor.clone();
                    async move {
                        let start = cursor.unwrap_or(begin);
                        scan(&trx, (start, end), MAX_SCAN_LIMIT).await
                    }
                })
                .await?;
            let full = page.len() == MAX_SCAN_LIMIT;
            let next = page.last().map(|(key, _)| next_cursor(key));
            out.extend(page);
            if !full {
                break;
            }
            cursor = next;
        }
        Ok(out)
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
                    return Err(StoreError::Storage(crate::StorageError::Corrupt));
                }
                Ok(decode(&bytes)?)
            }
        }
    }

    async fn decode_session_in(&self, trx: &Transaction, bytes: &[u8]) -> Result<StoredSession> {
        if bytes.first() != Some(&SESSION_RECORD_VERSION) {
            return Err(StoreError::Storage(crate::StorageError::Corrupt));
        }
        let payload = if bytes.get(1) == Some(&SESSION_CHUNK_MARKER) {
            if bytes.len() != 20 {
                return Err(StoreError::Storage(crate::StorageError::Corrupt));
            }
            let id = keys::session_id(bytes[2..18].to_vec())?;
            let count = u16::from_be_bytes([bytes[18], bytes[19]]);
            if count == 0 || usize::from(count) > SESSION_MAX_BYTES.div_ceil(INLINE_LIMIT) {
                return Err(StoreError::Storage(crate::StorageError::Corrupt));
            }
            let mut payload = Vec::new();
            for index in 0..count {
                let chunk = trx
                    .get(&self.keys().session_chunk(id, index), false)
                    .await?
                    .ok_or(StoreError::Storage(crate::StorageError::Corrupt))?;
                payload.extend_from_slice(&chunk);
            }
            payload
        } else {
            bytes[1..].to_vec()
        };
        let v: StoredSessionCurrent =
            postcard::from_bytes(&payload).map_err(EncodingError::Payload)?;
        let mut session: StoredSession = v.into();
        // The agent tombstone is authoritative for every named side session.
        // Deleting a computer cannot atomically rewrite an unbounded set
        // of conversations, so keep this one shared fence until queried.
        session.computer_deleted |= self.computer_deleted(trx, session.agent_id).await?;
        Ok(session)
    }

    pub(crate) async fn fetch_session_in(
        &self,
        trx: &Transaction,
        id: SessionId,
    ) -> Result<Option<(StoredSession, Option<Vec<u8>>)>> {
        self.session_record_reads.fetch_add(1, Ordering::Relaxed);
        let Some(bytes) = trx.get(&self.keys().session(id), false).await? else {
            return Ok(None);
        };
        let session = self.decode_session_in(trx, &bytes).await?;
        let snapshot = self.snapshot_for_session_in(trx, &session).await?;
        Ok(Some((session, snapshot)))
    }

    pub(crate) async fn snapshot_for_session_in(
        &self,
        trx: &Transaction,
        session: &StoredSession,
    ) -> Result<Option<Vec<u8>>> {
        match session.snapshot_seq {
            Some(seq) => Ok(Some(
                trx.get(&self.keys().snapshot(session.session_id, seq), false)
                    .await?
                    .ok_or(StoreError::Storage(crate::StorageError::Corrupt))?
                    .to_vec(),
            )),
            None => Ok(None),
        }
    }

    pub(crate) async fn session(&self, trx: &Transaction, id: SessionId) -> Result<StoredSession> {
        self.session_record_reads.fetch_add(1, Ordering::Relaxed);
        let bytes = trx
            .get(&self.keys().session(id), false)
            .await?
            .ok_or(StoreError::Domain(crate::DomainError::SessionMissing))?;
        self.decode_session_in(trx, &bytes).await
    }

    pub(crate) fn write_session(&self, trx: &Transaction, session: &StoredSession) -> Result<()> {
        let mut bytes = vec![SESSION_RECORD_VERSION];
        bytes.extend(
            postcard::to_allocvec(&StoredSessionCurrent::from(session))
                .map_err(EncodingError::Payload)?,
        );
        if bytes.len() > SESSION_MAX_BYTES {
            return Err(StoreError::Storage(crate::StorageError::TooLarge));
        }
        let (begin, end) = self.keys().session_chunk_space(session.session_id).range();
        trx.clear_range(&begin, &end);
        if bytes.len() > INLINE_LIMIT {
            let payload = &bytes[1..];
            let count = u16::try_from(payload.len().div_ceil(INLINE_LIMIT))
                .map_err(|_| StoreError::Storage(crate::StorageError::TooLarge))?;
            for (index, chunk) in payload.chunks(INLINE_LIMIT).enumerate() {
                trx.set(
                    &self.keys().session_chunk(
                        session.session_id,
                        u16::try_from(index)
                            .map_err(|_| StoreError::Storage(crate::StorageError::TooLarge))?,
                    ),
                    chunk,
                );
            }
            bytes.truncate(1);
            bytes.push(SESSION_CHUNK_MARKER);
            bytes.extend(session.session_id.as_ulid().to_bytes());
            bytes.extend(count.to_be_bytes());
        }
        trx.set(&self.keys().session(session.session_id), &bytes);
        Ok(())
    }

    /// Create an empty session and pin its registered image in the same transaction.
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

    /// Fetch a session without its agent record. Callers that also need the
    /// agent's route assignment use [`Store::fetch_session_with_agent`].
    /// # Errors
    /// Returns storage or decoding errors.
    pub async fn fetch_session(&self, id: SessionId) -> Result<Option<SessionRecord>> {
        let stored = self
            .transaction(|trx| async move {
                let Some((session, snapshot)) = self.fetch_session_in(&trx, id).await? else {
                    return Ok(None);
                };
                Ok(Some((session, snapshot)))
            })
            .await?;
        let Some((session, snapshot)) = stored else {
            return Ok(None);
        };
        Ok(Some(self.hydrate_session(session, snapshot).await?))
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
                let Some((session, snapshot)) = self.fetch_session_in(&trx, id).await? else {
                    return Ok(None);
                };
                let agent = self.read_agent(&trx, session.agent_id).await?;
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
                let (mut begin, end) = self.keys().session_space().range();
                if let Some(id) = after {
                    begin = crate::next_cursor(&self.keys().session(id));
                }
                let mut sessions = Vec::new();
                for (_, value) in scan(&trx, (begin, end), limit).await? {
                    let session = self.decode_session_in(&trx, &value).await?;
                    let snapshot = self.snapshot_for_session_in(&trx, &session).await?;
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
            return Err(StoreError::Domain(crate::DomainError::InvalidMessageRole));
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
            return Err(StoreError::Domain(crate::DomainError::InvalidMessageRole));
        }
        let head = expected_head
            .checked_add(1)
            .ok_or(StoreError::Storage(crate::StorageError::SequenceOverflow))?;
        let event = Event::MessageAppended {
            seq: head,
            message: message.clone(),
        };
        let value = self.prepare(&event).await?;
        if value.len() > MAX_BATCH_BYTES {
            return Err(StoreError::Storage(crate::StorageError::TooLarge));
        }
        let replay_key = self.keys().api_append(key);
        self.transaction(|trx| {
            let replay_key = &replay_key;
            let value = &value;
            async move {
                if let Some(previous) = read::<u64>(&trx, replay_key).await? {
                    return Ok((previous, false));
                }
                let mut session = self.session(&trx, id).await?;
                crate::check_head(session.head_seq, expected_head)?;
                if session.state != SessionState::Idle {
                    return Err(StoreError::Domain(crate::DomainError::SessionNotIdle));
                }
                trx.set(&self.keys().event(id, head), value);
                write(&trx, &self.keys().turn(id), &message.id)?;
                write(&trx, replay_key, &head)?;
                session.head_seq = head;
                self.transition(&trx, session, SessionState::Runnable, self.now())
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
            .checked_add(
                u64::try_from(events.len())
                    .map_err(|_| StoreError::Storage(crate::StorageError::TooLarge))?,
            )
            .ok_or(StoreError::Storage(crate::StorageError::SequenceOverflow))?;
        let mut prepared = Vec::with_capacity(events.len());
        let mut size = 0;
        for (event, seq) in events.iter().zip((expected_head..head).map(|n| n + 1)) {
            let mut event = event.clone();
            event.set_seq(seq);
            let key = self.keys().event(id, seq);
            let value = self.prepare(&event).await?;
            size += key.len() + value.len();
            if size > MAX_BATCH_BYTES {
                return Err(StoreError::Storage(crate::StorageError::TooLarge));
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
                crate::check_head(session.head_seq, expected_head)?;
                if wake && session.state != SessionState::Idle {
                    return Err(StoreError::Domain(crate::DomainError::SessionNotIdle));
                }
                for (key, value) in prepared {
                    trx.set(key, value);
                }
                for event in events {
                    if let Event::MessageAppended { message, .. } = event
                        && message.role == swarmy_core::MessageRole::User
                    {
                        write(&trx, &self.keys().turn(id), &message.id)?;
                    }
                    if let Event::InferenceRequested { request_id, .. }
                    | Event::ToolCallRequested { request_id, .. } = event
                        && let Some(turn) =
                            read::<swarmy_core::MessageId>(&trx, &self.keys().turn(id)).await?
                    {
                        write(&trx, &self.keys().request_turn(*request_id), &turn)?;
                    }
                }
                session.head_seq = head;
                if wake {
                    self.transition(&trx, session, SessionState::Runnable, self.now())
                        .await
                } else {
                    self.write_session(&trx, &session)
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
                let space = self.keys().event_space(id);
                let begin = crate::next_cursor(&self.keys().event(id, after));
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
    #[cfg(any(test, feature = "test-support"))]
    pub async fn write_snapshot(&self, id: SessionId, snapshot: &SnapshotRef) -> Result<()> {
        let value = self.prepare(snapshot).await?;
        self.transaction(|trx| {
            let value = &value;
            async move {
                let mut session = self.session(&trx, id).await?;
                if snapshot.seq > session.head_seq
                    || session.snapshot_seq.is_some_and(|old| old > snapshot.seq)
                {
                    return Err(StoreError::Fence(crate::FenceError::StaleSequence {
                        expected: snapshot.seq,
                        actual: session.head_seq,
                    }));
                }
                trx.set(&self.keys().snapshot(id, snapshot.seq), value);
                session.snapshot_seq = Some(snapshot.seq);
                self.write_session(&trx, &session)
            }
        })
        .await
    }

    /// # Errors
    /// Returns storage, blob, or decoding errors.
    pub async fn get_idempotency(&self, id: RequestId) -> Result<Option<IdempotencyRecord>> {
        self.get_payload(self.keys().idem(id)).await
    }

    /// # Errors
    /// Returns storage or blob upload errors.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn put_inflight(&self, id: RequestId, record: &InflightRecord) -> Result<()> {
        self.put_payload(self.keys().inflight(id), record).await
    }

    /// # Errors
    /// Returns storage, blob, or decoding errors.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn get_inflight(&self, id: RequestId) -> Result<Option<InflightRecord>> {
        self.get_payload(self.keys().inflight(id)).await
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

/// One shared transaction runner for `Store` and the metrics drain writer.
/// Binding-level retries inside one call count once against `counter`.
pub(crate) async fn run_transaction<T, F, Fut>(
    db: &Database,
    counter: &AtomicU64,
    operation: F,
) -> Result<T>
where
    F: Fn(RetryableTransaction) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    counter.fetch_add(1, Ordering::Relaxed);
    let result = db
        .run(|trx, maybe_committed| {
            let operation = &operation;
            async move {
                if bool::from(maybe_committed) {
                    return Err(FdbBindingError::new_custom_error(Box::new(
                        StoreError::Storage(crate::StorageError::CommitUnknown),
                    )));
                }
                trx.set_option(TransactionOption::Timeout(4_500))?;
                trx.set_option(TransactionOption::RetryLimit(20))?;
                operation(trx).await.map_err(|error| match error {
                    StoreError::Storage(crate::StorageError::FoundationDb(error)) => error.into(),
                    other => FdbBindingError::new_custom_error(Box::new(other)),
                })
            }
        })
        .await;
    result.map_err(|error| match error {
        FdbBindingError::CustomError(error) => match error.downcast::<StoreError>() {
            Ok(error) => *error,
            Err(error) => StoreError::Storage(crate::StorageError::Binding(
                FdbBindingError::CustomError(error),
            )),
        },
        other => StoreError::Storage(crate::StorageError::Binding(other)),
    })
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
        return Err(StoreError::Storage(crate::StorageError::TooLarge));
    }
    trx.set(key, &bytes);
    Ok(())
}

fn check_limit(limit: usize) -> Result<()> {
    if (1..=MAX_SCAN_LIMIT).contains(&limit) {
        Ok(())
    } else {
        Err(StoreError::Domain(crate::DomainError::InvalidLimit))
    }
}

pub(crate) fn check_head(actual: u64, expected: u64) -> Result<()> {
    if actual != expected {
        return Err(StoreError::Fence(crate::FenceError::StaleSequence {
            expected,
            actual,
        }));
    }
    Ok(())
}

pub(crate) fn next_cursor(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    next.push(0);
    next
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

/// Read every row in `range` inside one transaction, paging by the last key.
/// Callers that fit their scan in one transaction use this instead of copying
/// the `scan` then `push(0)` loop.
pub(crate) async fn scan_all(
    trx: &Transaction,
    range: (Vec<u8>, Vec<u8>),
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let (mut begin, end) = range;
    let mut out = Vec::new();
    loop {
        let page = scan(trx, (begin.clone(), end.clone()), MAX_SCAN_LIMIT).await?;
        let full = page.len() == MAX_SCAN_LIMIT;
        let next = page.last().map(|(key, _)| next_cursor(key));
        out.extend(page);
        if !full {
            break;
        }
        begin = next.unwrap_or_else(|| end.clone());
    }
    Ok(out)
}

mod usage;
pub use usage::{UsageAttribution, UsageRecord};
