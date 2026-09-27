//! `FoundationDB` key layout and atomic session operations.
//!
//! Call `boot` once at process startup and retain its guard until every store and
//! runtime using `FoundationDB` has stopped. The default directory is `swarmy`.
//! Event, snapshot, and request payloads above 80 KiB are uploaded before transactions start;
//! failed transactions can leave unreferenced, content-addressed blobs for later GC.
//! Session records use a per-record version; legacy side rows are migrated. Scans are bounded and callers paginate by their last result. A commit with an unknown outcome is reported without replaying it.

mod agents;
pub use agents::{AgentSessionOptions, CreateAgentOptions};
mod api_idempotency;
pub mod blob;
mod computers;
pub mod credentials;
mod inference;
mod inference_wait;
mod interrupt;
mod metrics;
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
pub use inference::{InferenceClaim, InferenceCompletion};
pub mod metering;
pub mod quota;
pub use metering::{DimensionTotal, MeteringDimension, UsageGroup, UsageGroupBy};
pub use quota::{EntryQuota, ObservedQuota, QuotaConfig, QuotaSource};
mod gc;
mod leases;
mod routes;
pub use routes::{
    ExpandedChain, FailoverAction, FailoverOutcome, PoolEntry, RouteCache, RouteSnapshot,
    RouteStepStatus,
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
pub use keys::{RUNNABLE_PARTITIONS, runnable_partition};

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

const SESSION_RECORD_VERSION: u8 = 2;
// Postcard encodes a session id with a 26-byte prefix, so this marker cannot
// collide with an inline V2 record. Oversized V1 side rows need bounded chunks.
const SESSION_CHUNK_MARKER: u8 = 0xff;
const SESSION_MAX_BYTES: usize = 10 * INLINE_LIMIT;

/// Counts from one bounded-page boot migration.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SessionMigration {
    pub migrated: usize,
    pub skipped: usize,
}

// Frozen version-one header. Do not add fields here.
#[derive(Serialize, Deserialize)]
struct StoredSessionV1 {
    session_id: SessionId,
    agent_id: swarmy_core::AgentId,
    state: SessionState,
    head_seq: u64,
    snapshot_seq: Option<u64>,
}

/// Version two owns all session-local metadata. Add future fields only with
/// `swarmy_core::trailing`; never change the shape of existing fields.
#[derive(Serialize, Deserialize)]
struct StoredSessionV2 {
    session_id: SessionId,
    agent_id: swarmy_core::AgentId,
    state: SessionState,
    head_seq: u64,
    snapshot_seq: Option<u64>,
    kind: swarmy_core::SessionKind,
    computer_deleted: bool,
    plan: Vec<swarmy_core::PlanStep>,
    inference: swarmy_core::InferenceSelection,
    interrupt_requested: bool,
    route: Option<String>,
    route_step: u32,
    image: Option<swarmy_core::ImageRecord>,
    idle_since: Option<jiff::Timestamp>,
    state_since: Option<jiff::Timestamp>,
}

// Working copy shared by the state machine; V1 and V2 have different wire layouts.
struct StoredSession {
    session_id: SessionId,
    agent_id: swarmy_core::AgentId,
    state: SessionState,
    head_seq: u64,
    snapshot_seq: Option<u64>,
    kind: swarmy_core::SessionKind,
    computer_deleted: bool,
    plan: Vec<swarmy_core::PlanStep>,
    inference: swarmy_core::InferenceSelection,
    interrupt_requested: bool,
    route: Option<String>,
    route_step: u32,
    image: Option<swarmy_core::ImageRecord>,
    idle_since: Option<jiff::Timestamp>,
    state_since: Option<jiff::Timestamp>,
}

impl From<StoredSessionV2> for StoredSession {
    fn from(v: StoredSessionV2) -> Self {
        Self {
            session_id: v.session_id,
            agent_id: v.agent_id,
            state: v.state,
            head_seq: v.head_seq,
            snapshot_seq: v.snapshot_seq,
            kind: v.kind,
            computer_deleted: v.computer_deleted,
            plan: v.plan,
            inference: v.inference,
            interrupt_requested: v.interrupt_requested,
            route: v.route,
            route_step: v.route_step,
            image: v.image,
            idle_since: v.idle_since,
            state_since: v.state_since,
        }
    }
}
impl From<&StoredSession> for StoredSessionV2 {
    fn from(v: &StoredSession) -> Self {
        Self {
            session_id: v.session_id,
            agent_id: v.agent_id,
            state: v.state,
            head_seq: v.head_seq,
            snapshot_seq: v.snapshot_seq,
            kind: v.kind,
            computer_deleted: v.computer_deleted,
            plan: v.plan.clone(),
            inference: v.inference.clone(),
            interrupt_requested: v.interrupt_requested,
            route: v.route.clone(),
            route_step: v.route_step,
            image: v.image.clone(),
            idle_since: v.idle_since,
            state_since: v.state_since,
        }
    }
}

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
            clock: Arc::new(jiff::Timestamp::now),
            images: Arc::default(),
            blobs,
            transactions: Arc::default(),
            session_record_reads: Arc::default(),
        })
    }

    /// Use an explicitly allocated root prefix, primarily for isolated tests.
    #[must_use]
    pub fn with_subspace(db: Arc<Database>, root: Subspace, blobs: Arc<dyn BlobStore>) -> Self {
        Self {
            db,
            root,
            clock: Arc::new(jiff::Timestamp::now),
            blobs,
            images: Arc::default(),
            transactions: Arc::default(),
            session_record_reads: Arc::default(),
        }
    }

    /// Use a deterministic clock for lease and expiry tests.
    #[must_use]
    pub fn with_clock(
        mut self,
        clock: impl Fn() -> jiff::Timestamp + Send + Sync + 'static,
    ) -> Self {
        self.clock = Arc::new(clock);
        self
    }

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
        self.transactions.fetch_add(1, Ordering::Relaxed);
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

    async fn hydrate_legacy_session(
        &self,
        trx: &Transaction,
        header: StoredSessionV1,
    ) -> Result<StoredSession> {
        let id = header.session_id;
        let (
            kind,
            computer_deleted,
            plan,
            inference,
            interrupt_requested,
            route,
            route_step,
            image,
            idle_since,
            state_since,
        ) = futures::try_join!(
            async {
                Ok::<_, StoreError>(
                    read(trx, &self.session_kind_key(id))
                        .await?
                        .unwrap_or_default(),
                )
            },
            self.computer_deleted(trx, header.agent_id),
            async {
                Ok::<_, StoreError>(
                    read(trx, &self.session_plan_key(id))
                        .await?
                        .unwrap_or_default(),
                )
            },
            async {
                Ok::<_, StoreError>(
                    read(trx, &self.session_inference_key(id))
                        .await?
                        .unwrap_or_default(),
                )
            },
            async {
                Ok::<_, StoreError>(read(trx, &self.interrupt_key(id)).await?.unwrap_or(false))
            },
            async {
                Ok::<_, StoreError>(
                    read::<Option<String>>(trx, &self.session_route_key(id))
                        .await?
                        .flatten(),
                )
            },
            async {
                Ok::<_, StoreError>(
                    read(trx, &self.session_route_step_key(id))
                        .await?
                        .unwrap_or(0),
                )
            },
            async { read(trx, &self.session_image_key(id)).await },
            async { read(trx, &self.session_idle_key(id)).await },
            async { read(trx, &self.session_state_since_key(id)).await },
        )?;
        Ok(StoredSession {
            session_id: id,
            agent_id: header.agent_id,
            state: header.state,
            head_seq: header.head_seq,
            snapshot_seq: header.snapshot_seq,
            kind,
            computer_deleted,
            plan,
            inference,
            interrupt_requested,
            route,
            route_step,
            image,
            idle_since,
            state_since,
        })
    }

    fn session_chunk_key(&self, id: SessionId, index: u16) -> Vec<u8> {
        crate::keys::Keys::new(&self.root)
            .session_chunk(&(id.as_ulid().to_bytes().as_slice(), index))
    }

    async fn decode_session_in(&self, trx: &Transaction, bytes: &[u8]) -> Result<StoredSession> {
        if bytes.first() == Some(&SESSION_RECORD_VERSION) {
            let payload = if bytes.get(1) == Some(&SESSION_CHUNK_MARKER) {
                if bytes.len() != 20 {
                    return Err(StoreError::Corrupt);
                }
                let id = keys::session_id(bytes[2..18].to_vec())?;
                let count = u16::from_be_bytes([bytes[18], bytes[19]]);
                if count == 0 || usize::from(count) > SESSION_MAX_BYTES.div_ceil(INLINE_LIMIT) {
                    return Err(StoreError::Corrupt);
                }
                let mut payload = Vec::new();
                for index in 0..count {
                    let chunk = trx
                        .get(&self.session_chunk_key(id, index), false)
                        .await?
                        .ok_or(StoreError::Corrupt)?;
                    payload.extend_from_slice(&chunk);
                }
                payload
            } else {
                bytes[1..].to_vec()
            };
            let v: StoredSessionV2 =
                postcard::from_bytes(&payload).map_err(EncodingError::Payload)?;
            let mut session: StoredSession = v.into();
            // The agent tombstone is authoritative for every named side session.
            // Deleting a computer cannot atomically rewrite an unbounded set
            // of conversations, so keep this one shared fence until queried.
            session.computer_deleted |= self.computer_deleted(trx, session.agent_id).await?;
            Ok(session)
        } else {
            self.hydrate_legacy_session(trx, decode(bytes)?).await
        }
    }

    pub(crate) async fn fetch_session_in(
        &self,
        trx: &Transaction,
        id: SessionId,
    ) -> Result<Option<(StoredSession, Option<Vec<u8>>)>> {
        self.session_record_reads.fetch_add(1, Ordering::Relaxed);
        let Some(bytes) = trx.get(&self.session_key(id), false).await? else {
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
                trx.get(&self.snapshot_key(session.session_id, seq), false)
                    .await?
                    .ok_or(StoreError::Corrupt)?
                    .to_vec(),
            )),
            None => Ok(None),
        }
    }

    pub(crate) async fn session(&self, trx: &Transaction, id: SessionId) -> Result<StoredSession> {
        self.session_record_reads.fetch_add(1, Ordering::Relaxed);
        let bytes = trx
            .get(&self.session_key(id), false)
            .await?
            .ok_or(StoreError::SessionMissing)?;
        self.decode_session_in(trx, &bytes).await
    }

    // Clear legacy rows with the V2 write, never leaving side data that can
    // override a newer version if an old process or maintenance job retries.
    pub(crate) fn write_session(&self, trx: &Transaction, session: &StoredSession) -> Result<()> {
        let mut bytes = vec![SESSION_RECORD_VERSION];
        bytes.extend(
            postcard::to_allocvec(&StoredSessionV2::from(session))
                .map_err(EncodingError::Payload)?,
        );
        if bytes.len() > SESSION_MAX_BYTES {
            return Err(StoreError::TooLarge);
        }
        let (begin, end) = crate::keys::Keys::new(&self.root)
            .session_chunk_space(&(session.session_id.as_ulid().to_bytes().as_slice(),))
            .range();
        trx.clear_range(&begin, &end);
        if bytes.len() > INLINE_LIMIT {
            let payload = &bytes[1..];
            let count = u16::try_from(payload.len().div_ceil(INLINE_LIMIT))
                .map_err(|_| StoreError::TooLarge)?;
            for (index, chunk) in payload.chunks(INLINE_LIMIT).enumerate() {
                trx.set(
                    &self.session_chunk_key(
                        session.session_id,
                        u16::try_from(index).map_err(|_| StoreError::TooLarge)?,
                    ),
                    chunk,
                );
            }
            bytes.truncate(1);
            bytes.push(SESSION_CHUNK_MARKER);
            bytes.extend(session.session_id.as_ulid().to_bytes());
            bytes.extend(count.to_be_bytes());
        }
        trx.set(&self.session_key(session.session_id), &bytes);
        let id = session.session_id;
        for key in [
            self.session_kind_key(id),
            self.session_plan_key(id),
            self.session_inference_key(id),
            self.interrupt_key(id),
            self.session_route_key(id),
            self.session_route_step_key(id),
            self.session_image_key(id),
            self.session_idle_key(id),
            self.session_state_since_key(id),
        ] {
            trx.clear(&key);
        }
        Ok(())
    }

    /// Rewrite remaining V1 sessions in bounded scan pages. Concurrent writers
    /// are safe: each rewrite reads the header in its committing transaction.
    /// # Errors
    /// Returns scan and transaction errors; malformed individual rows are skipped.
    pub async fn migrate_legacy_sessions(&self) -> Result<SessionMigration> {
        let mut after = None;
        let mut outcome = SessionMigration::default();
        loop {
            let page: Vec<(SessionId, bool)> = self
                .transaction(|trx| async move {
                    let (mut begin, end) = crate::keys::Keys::new(&self.root)
                        .session_space(&())
                        .range();
                    if let Some(id) = after {
                        begin = self.session_key(id);
                        begin.push(0);
                    }
                    let mut ids = Vec::new();
                    for (key, value) in scan(&trx, (begin, end), MAX_SCAN_LIMIT).await? {
                        let (_, bytes): (String, Vec<u8>) =
                            self.root.unpack(&key).map_err(|_| StoreError::Corrupt)?;
                        ids.push((
                            keys::session_id(bytes)?,
                            value.first() != Some(&SESSION_RECORD_VERSION),
                        ));
                    }
                    Ok(ids)
                })
                .await?;
            if page.is_empty() {
                break;
            }
            after = page.last().map(|(id, _)| *id);
            for (id, legacy) in page {
                if !legacy {
                    continue;
                }
                let result = self
                    .transaction(|trx| async move {
                        let Some(value) = trx.get(&self.session_key(id), false).await? else {
                            return Ok(false);
                        };
                        if value.first() == Some(&SESSION_RECORD_VERSION) {
                            return Ok(false);
                        }
                        let session = self.hydrate_legacy_session(&trx, decode(&value)?).await?;
                        self.write_session(&trx, &session)?;
                        Ok(true)
                    })
                    .await;
                match result {
                    Ok(true) => outcome.migrated += 1,
                    Ok(false) => {}
                    Err(
                        error @ (StoreError::Encoding(_)
                        | StoreError::TooLarge
                        | StoreError::Corrupt),
                    ) => {
                        tracing::warn!(%id, %error, "skipping invalid legacy session");
                        outcome.skipped += 1;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(outcome)
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
                let (mut begin, end) = crate::keys::Keys::new(&self.root)
                    .session_space(&())
                    .range();
                if let Some(id) = after {
                    begin = self.session_key(id);
                    begin.push(0);
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
        let replay_key = crate::keys::Keys::new(&self.root).api_append(&(key));
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
                self.write_session(&trx, &session)
            }
        })
        .await
    }

    fn snapshot_key(&self, id: SessionId, seq: u64) -> Vec<u8> {
        crate::keys::Keys::new(&self.root).snapshot(&(id.as_ulid().to_bytes().as_slice(), seq))
    }

    /// # Errors
    /// Returns storage, blob, or decoding errors.
    pub async fn get_idempotency(&self, id: RequestId) -> Result<Option<IdempotencyRecord>> {
        self.get_payload(crate::keys::Keys::new(&self.root).idem(&(id.as_bytes().as_slice())))
            .await
    }

    /// # Errors
    /// Returns storage or blob upload errors.
    /// Test-only entry point, also available with the `test-support` feature.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn put_idempotency(&self, id: RequestId, record: &IdempotencyRecord) -> Result<()> {
        self.put_payload(
            crate::keys::Keys::new(&self.root).idem(&(id.as_bytes().as_slice())),
            record,
        )
        .await
    }

    /// # Errors
    /// Returns storage or blob upload errors.
    pub async fn put_inflight(&self, id: RequestId, record: &InflightRecord) -> Result<()> {
        self.put_payload(
            crate::keys::Keys::new(&self.root).inflight(&(id.as_bytes().as_slice())),
            record,
        )
        .await
    }

    /// # Errors
    /// Returns storage, blob, or decoding errors.
    pub async fn get_inflight(&self, id: RequestId) -> Result<Option<InflightRecord>> {
        self.get_payload(crate::keys::Keys::new(&self.root).inflight(&(id.as_bytes().as_slice())))
            .await
    }

    /// # Errors
    /// Returns transaction errors.
    pub async fn clear_inflight(&self, id: RequestId) -> Result<()> {
        self.transaction(|trx| async move {
            trx.clear(&crate::keys::Keys::new(&self.root).inflight(&(id.as_bytes().as_slice())));
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
    fn fixed_versioned_session_bytes() {
        let id = SessionId::from_ulid(ulid::Ulid::from(0_u128));
        let agent = swarmy_core::AgentId::from_ulid(ulid::Ulid::from(0_u128));
        let v1 = StoredSessionV1 {
            session_id: id,
            agent_id: agent,
            state: SessionState::Idle,
            head_seq: 0,
            snapshot_seq: None,
        };
        let mut v1_bytes = vec![1, 26];
        v1_bytes.extend([b'0'; 26]);
        v1_bytes.push(26);
        v1_bytes.extend([b'0'; 26]);
        v1_bytes.extend([0, 0, 0]); // Idle, empty head, no snapshot.
        assert_eq!(encode(&v1).unwrap(), v1_bytes);
        let v2 = StoredSessionV2 {
            session_id: id,
            agent_id: agent,
            state: SessionState::Idle,
            head_seq: 0,
            snapshot_seq: None,
            kind: swarmy_core::SessionKind::Ephemeral,
            computer_deleted: false,
            plan: Vec::new(),
            inference: swarmy_core::InferenceSelection::default(),
            interrupt_requested: false,
            route: None,
            route_step: 0,
            image: None,
            idle_since: None,
            state_since: None,
        };
        let mut bytes = vec![SESSION_RECORD_VERSION];
        bytes.extend(postcard::to_allocvec(&v2).unwrap());
        let mut v2_bytes = v1_bytes;
        v2_bytes[0] = 2;
        v2_bytes.extend([0; 12]); // Kind through state-since are empty defaults.
        assert_eq!(bytes, v2_bytes);
        let decoded: StoredSessionV2 = postcard::from_bytes(&bytes[1..]).unwrap();
        assert_eq!(decoded.session_id, id);
        assert_eq!(decoded.route_step, 0);
    }

    #[test]
    fn fixed_nondefault_v2_session_bytes() {
        let id = SessionId::from_ulid(ulid::Ulid::from(0_u128));
        let v2 = StoredSessionV2 {
            session_id: id,
            agent_id: swarmy_core::AgentId::from_ulid(ulid::Ulid::from(0_u128)),
            state: SessionState::Runnable,
            head_seq: 0,
            snapshot_seq: None,
            kind: swarmy_core::SessionKind::Ephemeral,
            computer_deleted: false,
            plan: Vec::new(),
            inference: swarmy_core::InferenceSelection::default(),
            interrupt_requested: true,
            route: None,
            route_step: 3,
            image: None,
            idle_since: None,
            state_since: None,
        };
        let mut expected = vec![SESSION_RECORD_VERSION, 26];
        expected.extend([b'0'; 26]);
        expected.push(26);
        expected.extend([b'0'; 26]);
        expected.extend([1, 0, 0]); // Runnable, empty log and snapshot.
        expected.extend([0, 0, 0, 0, 0, 0, 1, 0, 3, 0, 0, 0]);
        let mut actual = vec![SESSION_RECORD_VERSION];
        actual.extend(postcard::to_allocvec(&v2).unwrap());
        assert_eq!(actual, expected);
        let decoded: StoredSessionV2 = postcard::from_bytes(&expected[1..]).unwrap();
        assert!(decoded.interrupt_requested);
        assert_eq!(decoded.route_step, 3);
    }

    #[test]
    fn legacy_session_header_is_still_readable_and_writes_the_same_bytes() {
        // A tuple encodes the original five postcard fields without adding metadata.
        let id = SessionId::from_ulid(ulid::Ulid::from_parts(1, 2));
        let agent = swarmy_core::AgentId::from_ulid(ulid::Ulid::from_parts(1, 3));
        let original = encode(&(id, agent, SessionState::Idle, 42_u64, Some(20_u64))).unwrap();
        let header: StoredSessionV1 = decode(&original).unwrap();
        assert_eq!(header.session_id, id);
        assert_eq!(header.agent_id, agent);
        assert_eq!(header.state, SessionState::Idle);
        assert_eq!(header.head_seq, 42);
        assert_eq!(header.snapshot_seq, Some(20));
        assert_eq!(encode(&header).unwrap(), original);
    }
}

mod usage;
pub use usage::{UsageAttribution, UsageRecord};
