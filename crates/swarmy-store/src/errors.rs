//! Store error taxonomy: storage, fence, and domain failures.
use foundationdb::{FdbBindingError, FdbError};
use swarmy_core::EncodingError;

use crate::blob::BlobError;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("keyring cannot decrypt credential")]
    Keyring,
    /// The operating system refused to supply randomness for credential
    /// encryption. The source carries the OS failure; unlike decryption
    /// failures below, there is nothing secret to hide here.
    #[error("keyring randomness unavailable")]
    Randomness(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    FoundationDb(#[from] FdbError),
    #[error(transparent)]
    Binding(#[from] FdbBindingError),
    #[error(transparent)]
    Encoding(#[from] EncodingError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("memory capacity overflow")]
    MemoryCapacityOverflow,
    #[error("sequence number overflow")]
    SequenceOverflow,
    #[error("metadata or batch exceeds the storage budget")]
    TooLarge,
    #[error("stored key or blob is corrupt")]
    Corrupt,
    /// A stored key or blob failed to decode. The source names the codec
    /// failure (tuple layout, JSON shape, byte length, or id text) so a
    /// corruption report says what actually broke instead of only where the
    /// read happened. Use this where a real decode error is in hand; keep
    /// [`StorageError::Corrupt`] for invariant violations with no cause.
    #[error("stored key or blob is corrupt")]
    Decode(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("commit outcome is unknown; read durable state before retrying")]
    CommitUnknown,
    #[error("cluster file path is not UTF-8")]
    NonUtf8ClusterFile,
}

#[derive(Debug, thiserror::Error)]
pub enum FenceError {
    #[error("volume head changed since this writer opened it")]
    VolumeHeadMismatch,
    #[error("expected head {expected}, found {actual}")]
    StaleSequence { expected: u64, actual: u64 },
    #[error("inflight mismatch")]
    InflightMismatch,
    #[error("placement agent mismatch")]
    PlacementAgentMismatch,
    #[error("session agent mismatch")]
    SessionAgentMismatch,
    #[error("tool claim mismatch")]
    ToolClaimMismatch,
    #[error("tool job mismatch")]
    ToolJobMismatch,
    #[error("lease is absent, expired, or no longer matches")]
    LeaseMismatch,
    #[error("placement lease or epoch no longer matches")]
    PlacementMismatch,
    #[error("placed tool claim no longer matches")]
    PlacedToolClaimMismatch,
    #[error("gc run lease no longer matches")]
    GcLeaseMismatch,
    #[error("credential refresh claim no longer matches")]
    CredentialRefreshMismatch,
    #[error("volume writer lease no longer matches")]
    VolumeLeaseMismatch,
}

#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    #[error("credential does not exist")]
    CredentialMissing,
    #[error("route does not exist")]
    RouteMissing,
    #[error("invalid route: {0}")]
    InvalidRoute(String),
    #[error("credential refresh failed")]
    CredentialRefresh,
    #[error("GitHub token must contain 1-4096 printable ASCII characters without whitespace")]
    InvalidGithubToken,
    #[error("agent does not exist")]
    AgentMissing,
    #[error("agent id or name already exists")]
    AgentExists,
    #[error("agent name must be nonempty and contain no control characters")]
    InvalidAgentName,
    #[error("named agent has a pinned image")]
    NamedAgentImage,
    #[error("an ephemeral session requires an image")]
    SessionImageRequired,
    #[error("this session's computer has been deleted; create a new session to run tools")]
    ComputerDeleted,
    #[error("cannot close an agent main session")]
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
    #[error("session does not exist")]
    SessionMissing,
    #[error("session already exists")]
    SessionExists,
    #[error("session is idle or completed; there is nothing to interrupt")]
    NothingToInterrupt,
    #[error("session interruption was requested before the turn ended")]
    InterruptPending,
    #[error("empty tool jobs")]
    EmptyToolJobs,
    #[error("invalid inference completion")]
    InvalidInferenceCompletion,
    #[error("invalid inference request")]
    InvalidInferenceRequest,
    #[error("invalid memory requirement")]
    InvalidMemoryRequirement,
    #[error("invalid message role")]
    InvalidMessageRole,
    #[error("invalid partition")]
    InvalidPartition,
    #[error("invalid retention")]
    InvalidRetention,
    #[error("invalid session record: {0}")]
    InvalidSessionRecord(String),
    #[error("invalid snapshot")]
    InvalidSnapshot,
    #[error("invalid tool call: {0}")]
    InvalidToolCall(String),
    #[error("invalid transition")]
    InvalidTransition,
    #[error("missing inference wait")]
    MissingInferenceWait,
    #[error("missing inflight")]
    MissingInflight,
    #[error("missing tool request")]
    MissingToolRequest,
    #[error("node not sandbox")]
    NodeNotSandbox,
    #[error("session computer exists")]
    SessionComputerExists,
    #[error("session not idle")]
    SessionNotIdle,
    #[error("queued input arrived before the turn could finish")]
    QueuedInputPending,
    #[error("unexpected session state")]
    UnexpectedSessionState,
    #[error("lease TTL must be greater than zero")]
    InvalidLeaseTtl,
    #[error("scan limit must be between 1 and 64")]
    InvalidLimit,
}
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Fence(#[from] FenceError),
    #[error(transparent)]
    Domain(#[from] DomainError),
}

impl From<FdbError> for StoreError {
    fn from(error: FdbError) -> Self {
        StorageError::from(error).into()
    }
}
impl From<FdbBindingError> for StoreError {
    fn from(error: FdbBindingError) -> Self {
        StorageError::from(error).into()
    }
}
impl From<EncodingError> for StoreError {
    fn from(error: EncodingError) -> Self {
        StorageError::from(error).into()
    }
}
impl From<BlobError> for StoreError {
    fn from(error: BlobError) -> Self {
        StorageError::from(error).into()
    }
}

impl From<foundationdb::tuple::PackError> for StoreError {
    fn from(error: foundationdb::tuple::PackError) -> Self {
        StorageError::from(error).into()
    }
}

impl From<serde_json::Error> for StoreError {
    fn from(error: serde_json::Error) -> Self {
        StorageError::from(error).into()
    }
}

impl From<std::array::TryFromSliceError> for StoreError {
    fn from(error: std::array::TryFromSliceError) -> Self {
        StorageError::from(error).into()
    }
}

impl From<ulid::DecodeError> for StoreError {
    fn from(error: ulid::DecodeError) -> Self {
        StorageError::from(error).into()
    }
}

impl From<foundationdb::tuple::PackError> for StorageError {
    fn from(error: foundationdb::tuple::PackError) -> Self {
        Self::Decode(Box::new(error))
    }
}

impl From<serde_json::Error> for StorageError {
    fn from(error: serde_json::Error) -> Self {
        Self::Decode(Box::new(error))
    }
}

impl From<std::array::TryFromSliceError> for StorageError {
    fn from(error: std::array::TryFromSliceError) -> Self {
        Self::Decode(Box::new(error))
    }
}

impl From<ulid::DecodeError> for StorageError {
    fn from(error: ulid::DecodeError) -> Self {
        Self::Decode(Box::new(error))
    }
}

pub type Result<T> = std::result::Result<T, StoreError>;
