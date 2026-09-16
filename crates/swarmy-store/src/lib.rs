//! `FoundationDB` key layout and atomic session operations.
//!
//! Call `boot` once at process startup and retain its guard until every store and
//! runtime using `FoundationDB` has stopped. The default directory is `swarmy`.
//! Event, snapshot, and request payloads above 80 KiB are uploaded before transactions start;
//! failed transactions can leave unreferenced, content-addressed blobs for later GC.
//! Session headers retain only the snapshot sequence so lease transactions never
//! fetch blobs. Scans are bounded and callers paginate by their last result. A commit with an unknown outcome is reported without replaying it.

pub mod blob;
mod inference;
mod keys;
pub use inference::{InferenceClaim, InferenceCompletion};
mod gc;
mod leases;
mod nodes;
mod placed_tools;
mod placements;
mod tool_routing;
mod tools;
mod volumes;

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
    EncodingError, Event, IdempotencyRecord, InflightRecord, RequestId, SessionId, SessionRecord,
    SessionState, SnapshotRef, decode, encode,
};

use blob::{BlobError, BlobStore};

pub const INLINE_LIMIT: usize = 80 * 1024;
/// Keep reads and mutations below `FoundationDB`'s transaction byte limit.
const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_SCAN_LIMIT: usize = 64;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error(transparent)]
    FoundationDb(#[from] foundationdb::FdbError),
    #[error(transparent)]
    Binding(#[from] FdbBindingError),
    #[error(transparent)]
    Encoding(#[from] EncodingError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("node does not exist")]
    NodeMissing,
    #[error("node has no computer capacity available")]
    NodeAtCapacity,
    #[error("placement already exists")]
    PlacementExists,
    #[error("volume does not exist")]
    VolumeMissing,
    #[error("volume already exists")]
    VolumeExists,
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
}

#[derive(Clone)]
pub struct Store {
    db: Arc<Database>,
    root: Subspace,
    blobs: Arc<dyn BlobStore>,
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
            blobs,
        })
    }

    /// Use an explicitly allocated root prefix, primarily for isolated tests.
    #[must_use]
    pub fn with_subspace(db: Arc<Database>, root: Subspace, blobs: Arc<dyn BlobStore>) -> Self {
        Self { db, root, blobs }
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

    async fn session(&self, trx: &Transaction, id: SessionId) -> Result<StoredSession> {
        read(trx, &self.session_key(id))
            .await?
            .ok_or(StoreError::SessionMissing)
    }

    /// Create an empty Idle or Runnable session. Runnable creation also indexes it.
    /// # Errors
    /// Rejects duplicate ids, nonempty logs, and invalid initial state.
    pub async fn create_session(
        &self,
        session: &SessionRecord,
        wake_at: jiff::Timestamp,
    ) -> Result<()> {
        if session.head_seq != 0
            || session.snapshot_ref.is_some()
            || !matches!(session.state, SessionState::Idle | SessionState::Runnable)
        {
            return Err(StoreError::InvalidState);
        }
        self.transaction(|trx| async move {
            let key = self.session_key(session.session_id);
            if trx.get(&key, false).await?.is_some() {
                return Err(StoreError::SessionExists);
            }
            write(
                &trx,
                &key,
                &StoredSession {
                    session_id: session.session_id,
                    agent_id: session.agent_id,
                    state: session.state,
                    head_seq: 0,
                    snapshot_seq: None,
                },
            )?;
            if session.state == SessionState::Runnable {
                self.index_runnable(
                    &trx,
                    &swarmy_core::RunnableEntry {
                        session_id: session.session_id,
                        priority: 0,
                        wake_at,
                    },
                )
                .await?;
            }
            Ok(())
        })
        .await
    }

    /// # Errors
    /// Returns storage or decoding errors.
    pub async fn fetch_session(&self, id: SessionId) -> Result<Option<SessionRecord>> {
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
                Ok(Some((session, snapshot)))
            })
            .await?;
        let Some((session, snapshot)) = stored else {
            return Ok(None);
        };
        Ok(Some(self.hydrate_session(session, snapshot).await?))
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
            session_id: session.session_id,
            agent_id: session.agent_id,
            state: session.state,
            head_seq: session.head_seq,
            snapshot_ref,
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
        self.append_events_inner(id, expected_head, events, None)
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
        self.append_events_inner(id, expected_head, events, Some((lease, now)))
            .await
    }

    async fn append_events_inner(
        &self,
        id: SessionId,
        expected_head: u64,
        events: &[Event],
        fence: Option<(&swarmy_core::Lease, jiff::Timestamp)>,
    ) -> Result<u64> {
        let head = expected_head
            .checked_add(u64::try_from(events.len()).map_err(|_| StoreError::TooLarge)?)
            .ok_or(StoreError::SequenceOverflow)?;
        let mut prepared = Vec::with_capacity(events.len());
        let mut size = 0;
        for (event, seq) in events.iter().zip((expected_head..head).map(|n| n + 1)) {
            let mut event = event.clone();
            match &mut event {
                Event::MessageAppended { seq: n, .. }
                | Event::InferenceRequested { seq: n, .. }
                | Event::InferenceCompleted { seq: n, .. }
                | Event::ToolCallRequested { seq: n, .. }
                | Event::ToolCallCompleted { seq: n, .. }
                | Event::StateChanged { seq: n, .. }
                | Event::SnapshotWritten { seq: n, .. }
                | Event::InferenceFailed { seq: n, .. } => *n = seq,
            }
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
                for (key, value) in prepared {
                    trx.set(key, value);
                }
                session.head_seq = head;
                write(&trx, &self.session_key(id), &session)
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
