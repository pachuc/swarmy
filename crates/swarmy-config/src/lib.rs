#![deny(unreachable_pub)]
//! Shared configuration for services and command-line programs.
pub mod keyring;
pub use keyring::Keyring;
mod exports;
mod models;
mod object;
mod partitions;
pub use models::{CustomModel, CustomProvider};
pub use partitions::{Partitions, PartitionsError};
mod remote;
pub use exports::parse_exports;
pub use object::ObjectPrefix;
pub use remote::{
    AwsSettings, BucketCredentials, BucketSpec, Provider, RemoteNode, RemotePorts, RemoteProfile,
    RemoteServices, RemoteSettings, default_sandboxes, ownership_marker_key, remote_path,
    validate_remote_name, validate_service_user,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid model catalog configuration: {0}")]
    Catalog(String),
    #[error("keyring: {0}")]
    Keyring(&'static str),
    #[error("a new session requires --image NAME:TAG or default_image (SWARMY_DEFAULT_IMAGE)")]
    MissingImage,
    #[error("remote configuration: {0}")]
    Remote(&'static str),
    #[error("invalid remote JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid S3 namespace: {0}")]
    S3Namespace(&'static str),
    #[error("invalid store directory: {0}")]
    StoreDirectory(&'static str),
    #[error("configuration I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid configuration: {0}")]
    Decode(#[from] toml::de::Error),
    #[error("cannot encode configuration: {0}")]
    Encode(#[from] toml::ser::Error),
    #[error("invalid environment variable {0}")]
    Environment(String),
    #[error("invalid dev stack export on line {0}")]
    Export(usize),
}

/// One duration rule for TOML and environment: every duration is positive.
/// TOML keys carry their unit (`*_secs` or `*_ms`); environment values use
/// the same unit as the variable suffix. Both paths share one [`Unit`]
/// implementation through the thin `secs` and `ms` serde modules below.
mod duration {
    use std::time::Duration;
    #[derive(Clone, Copy)]
    pub(crate) enum Unit {
        Secs,
        Millis,
    }
    fn checked(raw: u64, unit: Unit) -> Result<Duration, String> {
        if raw == 0 {
            return Err("duration must be positive".into());
        }
        Ok(match unit {
            Unit::Secs => Duration::from_secs(raw),
            Unit::Millis => Duration::from_millis(raw),
        })
    }
    fn as_raw(value: Duration, unit: Unit) -> Result<u64, String> {
        match unit {
            Unit::Secs => Ok(value.as_secs()),
            Unit::Millis => {
                u64::try_from(value.as_millis()).map_err(|_| "duration too large".to_owned())
            }
        }
    }
    pub(crate) mod secs {
        use serde::{Deserialize, Deserializer, Serialize, Serializer};
        use std::time::Duration;
        pub(crate) fn serialize<S: Serializer>(
            value: &Duration,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            super::as_raw(*value, super::Unit::Secs)
                .map_err(serde::ser::Error::custom)?
                .serialize(serializer)
        }
        pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Duration, D::Error> {
            let raw = u64::deserialize(deserializer)?;
            super::checked(raw, super::Unit::Secs).map_err(serde::de::Error::custom)
        }
    }
    pub(crate) mod ms {
        use serde::{Deserialize, Deserializer, Serialize, Serializer};
        use std::time::Duration;
        pub(crate) fn serialize<S: Serializer>(
            value: &Duration,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            super::as_raw(*value, super::Unit::Millis)
                .map_err(serde::ser::Error::custom)?
                .serialize(serializer)
        }
        pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Duration, D::Error> {
            let raw = u64::deserialize(deserializer)?;
            super::checked(raw, super::Unit::Millis).map_err(serde::de::Error::custom)
        }
    }
    pub(crate) fn parse(value: &str, unit: Unit) -> Result<Duration, ()> {
        value
            .parse()
            .map_err(|_| ())
            .and_then(|raw| checked(raw, unit).map_err(|_| ()))
    }
    pub(crate) fn format(value: Duration, unit: Unit) -> String {
        match unit {
            Unit::Secs => value.as_secs().to_string(),
            Unit::Millis => value.as_millis().to_string(),
        }
    }
}

const DEFAULT_VOLUME_SNAPSHOT_PERIOD: Duration = Duration::from_secs(600);
const DEFAULT_VOLUME_SNAPSHOT_RETENTION: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(10).expect("snapshot retention count is nonzero");
const DEFAULT_INFERENCE_MAX_WAIT: Duration = Duration::from_secs(3600);
const DEFAULT_INFERENCE_MAX_BACKOFF: Duration = Duration::from_secs(300);
const DEFAULT_INFERENCE_GATEWAY_WAIT: Duration = Duration::from_secs(30);
const DEFAULT_METERING_RAW_RETENTION_DAYS: std::num::NonZeroU64 =
    std::num::NonZeroU64::new(90).expect("retention days count is nonzero");
const DEFAULT_GC_GRACE: Duration = Duration::from_hours(6);
const DEFAULT_GC_INTERVAL: Duration = Duration::from_hours(1);
const DEFAULT_GC_FILTER_BYTES: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(64 * 1024 * 1024).expect("filter byte size is nonzero");
const DEFAULT_GC_BATCH_SIZE: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(256).expect("batch size is nonzero");
const DEFAULT_GC_DELETE_CONCURRENCY: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(32).expect("delete concurrency is nonzero");
const DEFAULT_EPHEMERAL_RETENTION: Duration = Duration::from_hours(24);
const DEFAULT_SANDBOX_IDLE: Duration = Duration::from_mins(30);
const DEFAULT_PLACEMENT_LEASE: Duration = Duration::from_secs(30);
const DEFAULT_NODE_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(5000);
const DEFAULT_SCHEDULER_SCAN_INTERVAL: Duration = Duration::from_millis(5000);
const DEFAULT_SCHEDULER_RESEND_INTERVAL: Duration = Duration::from_millis(5000);
const DEFAULT_WORKER_LEASE: Duration = Duration::from_secs(30);
const DEFAULT_WORKER_RECOVERY_INTERVAL: Duration = Duration::from_millis(5000);
const DEFAULT_BUS_ACK_WAIT: Duration = Duration::from_secs(30);
const DEFAULT_MEMORY_MAX_BYTES: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(32 * 1024).expect("memory byte limit is nonzero");
const DEFAULT_NODE_CAPACITY: swarmy_core::NodeCapacity = swarmy_core::NodeCapacity {
    cpu_millis: 1000,
    memory_bytes: 1_073_741_824,
    disk_bytes: 34_359_738_368,
    sandboxes: 1,
};

/// Publication policy shared by every volume attachment.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct VolumeSnapshots {
    #[serde(with = "duration::secs")]
    pub period_secs: Duration,
    pub retention: std::num::NonZeroUsize,
}
impl Default for VolumeSnapshots {
    fn default() -> Self {
        Self {
            period_secs: DEFAULT_VOLUME_SNAPSHOT_PERIOD,
            retention: DEFAULT_VOLUME_SNAPSHOT_RETENTION,
        }
    }
}

/// Chunk collection policy. All durations are positive.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GarbageCollection {
    #[serde(with = "duration::secs")]
    pub grace_secs: Duration,
    #[serde(with = "duration::secs")]
    pub interval_secs: Duration,
    pub filter_bytes: std::num::NonZeroUsize,
    pub batch_size: std::num::NonZeroUsize,
    pub delete_concurrency: std::num::NonZeroUsize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Inference {
    #[serde(with = "duration::secs")]
    pub max_wait_secs: Duration,
    #[serde(with = "duration::secs")]
    pub max_backoff_secs: Duration,
    #[serde(with = "duration::secs")]
    pub gateway_wait_secs: Duration,
    pub default_route: Option<String>,
}

impl Default for Inference {
    fn default() -> Self {
        Self {
            max_wait_secs: DEFAULT_INFERENCE_MAX_WAIT,
            max_backoff_secs: DEFAULT_INFERENCE_MAX_BACKOFF,
            gateway_wait_secs: DEFAULT_INFERENCE_GATEWAY_WAIT,
            default_route: None,
        }
    }
}

/// Metering retention policy. Raw completion records are kept for query
/// debugging while hourly rollups remain the durable query path.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Metering {
    pub raw_retention_days: std::num::NonZeroU64,
}

impl Default for Metering {
    fn default() -> Self {
        Self {
            raw_retention_days: DEFAULT_METERING_RAW_RETENTION_DAYS,
        }
    }
}
impl Default for GarbageCollection {
    fn default() -> Self {
        Self {
            grace_secs: DEFAULT_GC_GRACE,
            interval_secs: DEFAULT_GC_INTERVAL,
            filter_bytes: DEFAULT_GC_FILTER_BYTES,
            batch_size: DEFAULT_GC_BATCH_SIZE,
            delete_concurrency: DEFAULT_GC_DELETE_CONCURRENCY,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ApiSettings {
    pub listen: String,
    pub url: Option<String>,
    pub token: String,
}
impl Default for ApiSettings {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:8742".into(),
            url: None,
            token: String::new(),
        }
    }
}

/// Object storage namespace shared by every service on one metadata namespace.
#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct S3Settings {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub bucket: String,
    pub prefix: ObjectPrefix,
    pub region: String,
    /// Write chunks and manifests with a create-only PUT (`If-None-Match: *`).
    /// Providers that reject the header need `false`, which makes the call a
    /// plain PUT. The objects are content-addressed, so overwriting identical
    /// bytes is safe.
    pub conditional_create: bool,
}

impl std::fmt::Debug for S3Settings {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3Settings")
            .field("endpoint", &self.endpoint)
            .field("access_key", &"..redacted..")
            .field("secret_key", &"..redacted..")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("region", &self.region)
            .field("conditional_create", &self.conditional_create)
            .finish()
    }
}
impl Default for S3Settings {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:8333".into(),
            access_key: "swarmy-dev".into(),
            secret_key: "swarmy-dev-secret".into(),
            bucket: "swarmy".into(),
            prefix: ObjectPrefix::default(),
            region: "us-east-1".into(),
            conditional_create: true,
        }
    }
}

/// NATS transport: connection URL, subject namespace, and delivery policy.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BusSettings {
    pub nats_url: String,
    pub prefix: String,
    #[serde(with = "duration::ms")]
    pub ack_wait_ms: Duration,
    pub max_deliver: u64,
}
impl Default for BusSettings {
    fn default() -> Self {
        Self {
            nats_url: "nats://127.0.0.1:4222".into(),
            prefix: String::new(),
            ack_wait_ms: DEFAULT_BUS_ACK_WAIT,
            max_deliver: 5,
        }
    }
}
impl BusSettings {
    /// Convert to the transport config, mapping an empty prefix to the shared
    /// namespace. Every service starts here instead of repeating the mapping.
    /// # Errors
    /// Rejects an invalid subject prefix.
    pub fn bus_config(&self) -> Result<swarmy_bus::Config, swarmy_bus::Error> {
        Ok(swarmy_bus::Config {
            prefix: if self.prefix.is_empty() {
                None
            } else {
                Some(swarmy_bus::SubjectToken::new(self.prefix.clone())?)
            },
            ack_wait: self.ack_wait_ms,
            max_deliver: i64::try_from(self.max_deliver).unwrap_or(i64::MAX),
        })
    }
}

/// Step worker progress: owned partitions, lease timing, and failure injection.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkerSettings {
    pub partitions: Partitions,
    #[serde(with = "duration::ms")]
    pub lease_ms: Duration,
    #[serde(with = "duration::ms")]
    pub recovery_interval_ms: Duration,
    pub kill_point: Option<String>,
}
impl Default for WorkerSettings {
    fn default() -> Self {
        Self {
            partitions: Partitions::default(),
            lease_ms: DEFAULT_WORKER_LEASE,
            recovery_interval_ms: DEFAULT_WORKER_RECOVERY_INTERVAL,
            kill_point: None,
        }
    }
}

/// Scheduler progress: owned partitions and scan timing.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulerSettings {
    pub partitions: Partitions,
    #[serde(with = "duration::ms")]
    pub scan_interval_ms: Duration,
    #[serde(with = "duration::ms")]
    pub resend_interval_ms: Duration,
    #[serde(with = "duration::secs")]
    pub ephemeral_retention_secs: Duration,
    #[serde(with = "duration::secs")]
    pub placement_lease_secs: Duration,
}
impl Default for SchedulerSettings {
    fn default() -> Self {
        Self {
            partitions: Partitions::default(),
            scan_interval_ms: DEFAULT_SCHEDULER_SCAN_INTERVAL,
            resend_interval_ms: DEFAULT_SCHEDULER_RESEND_INTERVAL,
            ephemeral_retention_secs: DEFAULT_EPHEMERAL_RETENTION,
            placement_lease_secs: DEFAULT_PLACEMENT_LEASE,
        }
    }
}

/// Gateway admission: concurrent inference deliveries.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GatewaySettings {
    pub concurrency: usize,
}
impl Default for GatewaySettings {
    fn default() -> Self {
        Self { concurrency: 4 }
    }
}

/// Node identity, offered roles and capacity, and heartbeat timing.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeSettings {
    pub id: Option<swarmy_core::NodeId>,
    pub roles: Vec<swarmy_core::NodeRole>,
    pub capacity: swarmy_core::NodeCapacity,
    /// When set, advertise RAM minus this reserve as sandbox memory.
    pub memory_reserve_mib: Option<u64>,
    #[serde(with = "duration::ms")]
    pub heartbeat_interval_ms: Duration,
}
impl Default for NodeSettings {
    fn default() -> Self {
        Self {
            id: None,
            roles: vec![
                swarmy_core::NodeRole::Sandbox,
                swarmy_core::NodeRole::Volume,
            ],
            capacity: DEFAULT_NODE_CAPACITY,
            memory_reserve_mib: None,
            heartbeat_interval_ms: DEFAULT_NODE_HEARTBEAT_INTERVAL,
        }
    }
}

/// Inference selection shared by every session without an agent override.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SelectionSettings {
    pub provider: String,
    pub providers: Option<Vec<String>>,
    pub custom_providers: BTreeMap<String, CustomProvider>,
    pub models: Vec<CustomModel>,
    pub model: String,
    pub effort: swarmy_core::ReasoningEffort,
    pub default_image: Option<String>,
    pub credential_file: PathBuf,
}
impl Default for SelectionSettings {
    fn default() -> Self {
        Self {
            provider: "fake".into(),
            providers: None,
            custom_providers: BTreeMap::new(),
            models: Vec::new(),
            model: "gpt-5".into(),
            effort: swarmy_core::ReasoningEffort::Medium,
            default_image: None,
            credential_file: PathBuf::new(),
        }
    }
}

/// Conversation context: compaction thresholds and the system prompt.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ContextSettings {
    /// Override the default `context_window` minus 16,384.
    pub summarize_at: Option<std::num::NonZeroU64>,
    pub context_window: Option<std::num::NonZeroU64>,
    pub system_prompt: String,
}
impl Default for ContextSettings {
    fn default() -> Self {
        Self {
            summarize_at: None,
            context_window: None,
            system_prompt: include_str!("system_prompt.txt").into(),
        }
    }
}

/// Agent memory files: location and excerpt budget.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MemorySettings {
    pub dir: PathBuf,
    pub max_bytes: std::num::NonZeroUsize,
}
impl Default for MemorySettings {
    fn default() -> Self {
        Self {
            dir: "/home/agent/memory".into(),
            max_bytes: DEFAULT_MEMORY_MAX_BYTES,
        }
    }
}

/// Image uploads: largest streamed upload the API accepts, in bytes.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ImageSettings {
    pub upload_max_bytes: u64,
}
impl Default for ImageSettings {
    fn default() -> Self {
        Self {
            upload_max_bytes: 16 * 1024 * 1024 * 1024,
        }
    }
}

/// `FoundationDB` connection and directory namespace.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StoreSettings {
    pub cluster_file: PathBuf,
    pub directory: String,
}
impl Default for StoreSettings {
    fn default() -> Self {
        Self {
            cluster_file: ".dev/fdb.cluster".into(),
            directory: "swarmy".into(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    pub api: ApiSettings,
    pub state_dir: PathBuf,
    pub remote: RemoteSettings,
    pub volume_snapshots: VolumeSnapshots,
    pub sandbox: SandboxSettings,
    pub gc: GarbageCollection,
    pub inference: Inference,
    pub metering: Metering,
    pub node: NodeSettings,
    pub store: StoreSettings,
    pub s3: S3Settings,
    pub bus: BusSettings,
    pub selection: SelectionSettings,
    pub scheduler: SchedulerSettings,
    pub worker: WorkerSettings,
    pub gateway: GatewaySettings,
    pub context: ContextSettings,
    pub memory: MemorySettings,
    pub fake: Fake,
    pub image: ImageSettings,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SandboxSettings {
    #[serde(with = "duration::secs")]
    pub idle_secs: Duration,
    pub scratch_idle_days: u64,
    pub scratch_high_water: u8,
    pub scratch_low_water: u8,
}

impl Default for SandboxSettings {
    fn default() -> Self {
        Self {
            idle_secs: DEFAULT_SANDBOX_IDLE,
            scratch_idle_days: 7,
            scratch_high_water: 80,
            scratch_low_water: 70,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Fake {
    pub script: PathBuf,
    pub call_log: PathBuf,
}
impl Default for Fake {
    fn default() -> Self {
        Self {
            script: ".swarmy/dev/fake.json".into(),
            call_log: ".swarmy/dev/calls.log".into(),
        }
    }
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            state_dir: ".swarmy".into(),
            api: ApiSettings::default(),
            remote: RemoteSettings::default(),
            volume_snapshots: VolumeSnapshots::default(),
            sandbox: SandboxSettings::default(),
            gc: GarbageCollection::default(),
            inference: Inference::default(),
            metering: Metering::default(),
            node: NodeSettings::default(),
            store: StoreSettings::default(),
            s3: S3Settings::default(),
            bus: BusSettings::default(),
            selection: SelectionSettings::default(),
            scheduler: SchedulerSettings::default(),
            worker: WorkerSettings::default(),
            gateway: GatewaySettings::default(),
            context: ContextSettings::default(),
            memory: MemorySettings::default(),
            fake: Fake::default(),
            image: ImageSettings::default(),
        }
    }
}

/// A discovered file and its effective settings. Relative paths are anchored to
/// the project containing `.swarmy`, or to the user configuration directory.
pub struct Loaded {
    pub path: Option<PathBuf>,
    pub root: PathBuf,
    pub settings: Settings,
}

impl Loaded {
    /// Read the configured node id, or persist a generated id under `.swarmy`.
    /// A file lock serializes concurrent CLI starts on this host.
    /// # Errors
    /// Returns filesystem errors or rejects an invalid persisted id.
    pub fn node_id(&self) -> Result<swarmy_core::NodeId, Error> {
        use std::io::{Read, Seek, Write};
        if let Some(id) = self.settings.node.id {
            return Ok(id);
        }
        let directory = self.root.join(".swarmy");
        std::fs::create_dir_all(&directory)?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join("node-id"))?;
        fs2::FileExt::lock_exclusive(&file)?;
        let mut value = String::new();
        file.read_to_string(&mut value)?;
        let id = if value.is_empty() {
            let id = swarmy_core::NodeId::from_ulid(ulid::Ulid::generate());
            file.rewind()?;
            file.write_all(id.to_string().as_bytes())?;
            file.sync_all()?;
            std::fs::File::open(directory)?.sync_all()?;
            id
        } else {
            swarmy_core::NodeId::from_ulid(
                value
                    .trim()
                    .parse()
                    .map_err(|_| Error::Environment("persisted node-id".into()))?,
            )
        };
        Ok(id)
    }
}

/// Initialise process-wide tracing once from `RUST_LOG`, falling back to info.
/// Services log plain text to stderr, keeping `field=value` pairs greppable.
pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
}

/// Wait for a process shutdown signal: SIGINT (Ctrl-C) or SIGTERM.
/// Systemd and the node launchers stop services with SIGTERM, so waiting
/// only for Ctrl-C would skip the metric flush on every real shutdown.
/// Callers await this instead of `tokio::signal::ctrl_c` directly.
/// This lives beside the service bootstrap because every service binary
/// needs it, not because the store owns process signals.
/// # Panics
/// Panics if the SIGTERM handler cannot be installed.
pub async fn shutdown_signal() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler must install");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = terminate.recv() => {},
    }
}

/// How often every service reports health while running. One shared tick so
/// the worker, API, scheduler, and gateway advertisement stay in step; the
/// store-side heartbeat loop below drives the actual reports.
pub const SERVICE_HEALTH_INTERVAL: Duration = Duration::from_secs(30);

/// Host label for health records. Every service reports the same way instead
/// of repeating the environment lookup.
#[must_use]
pub fn service_hostname() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into())
}

impl Settings {
    /// Select the image for a new session, giving an explicit flag precedence.
    /// # Errors
    /// Requires a configured default or an explicit image.
    pub fn session_image<'a>(&'a self, explicit: Option<&'a str>) -> Result<&'a str, Error> {
        explicit
            .or(self.selection.default_image.as_deref())
            .filter(|image| !image.is_empty())
            .ok_or(Error::MissingImage)
    }

    /// Split the `FoundationDB` directory namespace into path components.
    /// # Errors
    /// Rejects empty components such as a leading, trailing, or doubled slash.
    pub fn store_directory_path(&self) -> Result<Vec<String>, Error> {
        let path: Vec<String> = self.store.directory.split('/').map(str::to_owned).collect();
        if path.iter().any(String::is_empty) {
            return Err(Error::StoreDirectory("empty store directory component"));
        }
        Ok(path)
    }

    /// Discover configuration and apply the current process's environment.
    /// # Errors
    /// Fails for unreadable files, invalid TOML, or invalid overrides.
    pub fn load() -> Result<Loaded, Error> {
        let mut loaded = Self::load_base()?;
        loaded.settings.apply_remote()?;
        Ok(loaded)
    }

    /// Load configuration without opening the selected tunnel profile.
    /// # Errors
    /// Returns invalid configuration or filesystem errors.
    pub fn load_base() -> Result<Loaded, Error> {
        let cwd = std::env::current_dir()?;
        let environment = std::env::vars_os()
            .map(|(key, value)| {
                let key = key.to_string_lossy().into_owned();
                let value = value
                    .into_string()
                    .map_err(|_| Error::Environment(key.clone()));
                (key, value)
            })
            .filter(|(key, _)| {
                key.starts_with("SWARMY_") || key == "HOME" || key == "XDG_CONFIG_HOME"
            })
            .map(|(key, value)| value.map(|value| (key, value)))
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        Self::load_base_from(&cwd, &environment)
    }

    /// Apply the selected profile after environment overrides.
    /// # Errors
    /// Returns errors for missing or invalid profiles.
    pub fn apply_remote(&mut self) -> Result<(), Error> {
        if let Some(name) = &self.remote.profile {
            let profile = RemoteProfile::read(&self.state_dir, name)?;
            profile.validate_fdb_port()?;
            profile.apply(self);
        }
        Ok(())
    }

    fn load_base_from(cwd: &Path, environment: &BTreeMap<String, String>) -> Result<Loaded, Error> {
        let path = Self::discover_from(cwd, environment);
        let root = path.as_ref().and_then(|path| path.parent()).map_or_else(
            || cwd.to_owned(),
            |dir| {
                if dir.file_name().is_some_and(|name| name == ".swarmy") {
                    dir.parent().unwrap_or(dir).to_owned()
                } else {
                    dir.to_owned()
                }
            },
        );
        let mut settings = if let Some(path) = &path {
            Self::read(path)?
        } else {
            Self::default()
        };
        if settings.selection.credential_file.as_os_str().is_empty() {
            settings.selection.credential_file = environment
                .get("HOME")
                .map_or_else(|| root.clone(), PathBuf::from)
                .join(".swarmy/auth.json");
        }
        settings.resolve_paths(&root);
        settings.apply_environment(environment)?;
        // Relative environment paths retain their historical meaning: the invoking directory.
        settings.resolve_paths(cwd);
        Ok(Loaded {
            path,
            root,
            settings,
        })
    }

    /// Discover the configuration path without parsing its contents.
    /// This also lets diagnostics identify a file that cannot be loaded.
    #[must_use]
    pub fn discover_from(cwd: &Path, environment: &BTreeMap<String, String>) -> Option<PathBuf> {
        let local = cwd
            .ancestors()
            .map(|dir| dir.join(".swarmy/config.toml"))
            .find(|path| path.is_file());
        let user = environment
            .get("XDG_CONFIG_HOME")
            .filter(|value| Path::new(value).is_absolute())
            .map(PathBuf::from)
            .or_else(|| {
                environment
                    .get("HOME")
                    .map(|home| PathBuf::from(home).join(".config"))
            })
            .map(|dir| dir.join("swarmy/config.toml"))
            .filter(|path| path.is_file());
        local.or(user)
    }

    /// Read a file without environment overrides or path resolution.
    /// # Errors
    /// Fails if the file cannot be read or decoded.
    pub fn read(path: &Path) -> Result<Self, Error> {
        let mut settings: Self = toml::from_str(&std::fs::read_to_string(path)?)?;
        // An absent service user predates the setting; configuration files
        // take the new default while saved node records keep the empty value
        // so they fall back to the SSH login they were provisioned with.
        settings.remote.normalize_service_user();
        settings.validate()?;
        settings.catalog()?;
        Ok(settings)
    }

    /// Encode settings for a configuration file.
    /// # Errors
    /// Fails if TOML serialization fails.
    pub fn to_toml(&self) -> Result<String, Error> {
        Ok(toml::to_string_pretty(self)?)
    }

    /// Apply existing `SWARMY_*` names over file values.
    /// # Errors
    /// Fails if an override cannot be parsed or the settings are invalid.
    pub fn apply_environment(
        &mut self,
        environment: &BTreeMap<String, String>,
    ) -> Result<(), Error> {
        for entry in ENV_TABLE {
            if let Some(value) = environment.get(entry.name) {
                (entry.apply)(self, value, environment)
                    .map_err(|()| Error::Environment(entry.name.into()))?;
            }
        }
        self.validate()?;
        Ok(())
    }

    /// Check directory, object namespace, and worker timing floors once at load.
    /// # Errors
    /// Rejects invalid namespaces and worker durations below 30 ms.
    fn validate(&self) -> Result<(), Error> {
        self.store_directory_path()?;
        self.s3_namespace()?;
        for (name, value) in [
            ("SWARMY_WORKER_LEASE_MS", self.worker.lease_ms),
            (
                "SWARMY_WORKER_RECOVERY_INTERVAL_MS",
                self.worker.recovery_interval_ms,
            ),
        ] {
            if value < Duration::from_millis(30) {
                return Err(Error::Environment(name.into()));
            }
        }
        Ok(())
    }

    /// Pass the same effective settings to child processes without mutating globals.
    #[must_use]
    pub fn environment(&self) -> BTreeMap<String, String> {
        let mut environment = BTreeMap::new();
        for entry in ENV_TABLE {
            if let Some(value) = (entry.format)(self) {
                environment.insert(entry.name.to_owned(), value);
            }
        }
        environment
    }

    /// Anchor filesystem paths so invocation from subdirectories is consistent.
    pub fn resolve_paths(&mut self, root: &Path) {
        for value in [
            &mut self.state_dir,
            &mut self.store.cluster_file,
            &mut self.selection.credential_file,
            &mut self.fake.script,
            &mut self.fake.call_log,
        ] {
            if value.is_relative() {
                *value = root.join(&*value);
            }
        }
    }
}

struct EnvEntry {
    name: &'static str,
    apply: EnvApply,
    format: EnvFormat,
}

type EnvApply = fn(&mut Settings, &str, &BTreeMap<String, String>) -> Result<(), ()>;
type EnvFormat = fn(&Settings) -> Option<String>;

macro_rules! e {
    ($name:literal, $apply:expr, $format:expr) => {
        EnvEntry {
            name: $name,
            apply: $apply,
            format: $format,
        }
    };
}

/// One table drives both `apply_environment` and `environment`: each variable
/// is one entry with its setter and formatter. No variable was removed;
/// every entry below is read by at least one service or test.
#[rustfmt::skip]
static ENV_TABLE: &[EnvEntry] = &[
    e!("SWARMY_STATE_DIR", |s, v, _| assign(&mut s.state_dir, v), |s| Some(s.state_dir.to_string_lossy().into_owned())),
    e!("SWARMY_REMOTE", |s, v, _| { s.remote.profile = Some(v.into()); Ok(()) }, |s| s.remote.profile.clone()),
    e!("SWARMY_FDB_CLUSTER_FILE", |s, v, _| assign(&mut s.store.cluster_file, v), |s| Some(s.store.cluster_file.to_string_lossy().into_owned())),
    e!("SWARMY_NATS_URL", |s, v, _| assign(&mut s.bus.nats_url, v), |s| Some(s.bus.nats_url.clone())),
    e!("SWARMY_S3_ENDPOINT", |s, v, _| assign(&mut s.s3.endpoint, v), |s| Some(s.s3.endpoint.clone())),
    e!("SWARMY_S3_ACCESS_KEY", |s, v, _| assign(&mut s.s3.access_key, v), |s| Some(s.s3.access_key.clone())),
    e!("SWARMY_S3_SECRET_KEY", |s, v, _| assign(&mut s.s3.secret_key, v), |s| Some(s.s3.secret_key.clone())),
    e!("SWARMY_S3_BUCKET", |s, v, _| assign(&mut s.s3.bucket, v), |s| Some(s.s3.bucket.clone())),
    e!("SWARMY_S3_PREFIX", |s, v, _| assign(&mut s.s3.prefix, v), |s| Some(s.s3.prefix.as_str().into())),
    e!("SWARMY_S3_REGION", |s, v, _| assign(&mut s.s3.region, v), |s| Some(s.s3.region.clone())),
    e!("SWARMY_S3_CONDITIONAL_CREATE", |s, v, _| assign(&mut s.s3.conditional_create, v), |s| Some(s.s3.conditional_create.to_string())),
    e!("SWARMY_STORE_DIRECTORY", |s, v, _| assign(&mut s.store.directory, v), |s| Some(s.store.directory.clone())),
    e!("SWARMY_BUS_PREFIX", |s, v, _| assign(&mut s.bus.prefix, v), |s| Some(s.bus.prefix.clone())),
    e!("SWARMY_API_URL", |s, v, _| { s.api.url = Some(v.into()); Ok(()) }, |s| s.api.url.clone()),
    e!("SWARMY_API_TOKEN", |s, v, _| assign(&mut s.api.token, v), |s| Some(s.api.token.clone())),
    e!("SWARMY_API_LISTEN", |s, v, _| assign(&mut s.api.listen, v), |s| Some(s.api.listen.clone())),
    e!("SWARMY_PROVIDER", |s, v, env| { set_provider(s, v, env); Ok(()) }, |s| Some(s.selection.provider.clone())),
    e!("SWARMY_PROVIDERS", |s, v, _| { set_providers(s, v); Ok(()) }, |s| Some(format_providers(s))),
    e!("SWARMY_CUSTOM_PROVIDERS", set_custom_providers, |s| Some(format_custom_providers(s))),
    e!("SWARMY_MODELS", set_models, |s| Some(format_models(s))),
    e!("SWARMY_MODEL", |s, v, _| assign(&mut s.selection.model, v), |s| Some(s.selection.model.clone())),
    e!("SWARMY_DEFAULT_IMAGE", |s, v, _| { assign_opt_string(&mut s.selection.default_image, v); Ok(()) }, |s| Some(s.selection.default_image.clone().unwrap_or_default())),
    e!("SWARMY_REASONING_EFFORT", |s, v, _| assign(&mut s.selection.effort, v), |s| Some(s.selection.effort.to_string())),
    e!("SWARMY_CHATGPT_AUTH", |s, v, _| assign(&mut s.selection.credential_file, v), |s| Some(s.selection.credential_file.to_string_lossy().into_owned())),
    e!("SWARMY_SYSTEM_PROMPT", |s, v, _| assign(&mut s.context.system_prompt, v), |s| Some(s.context.system_prompt.clone())),
    e!("SWARMY_SUMMARIZE_AT_TOKENS", |s, v, _| assign_opt_nonzero(&mut s.context.summarize_at, v), |s| Some(format_summarize_at(s))),
    e!("SWARMY_MODEL_CONTEXT_WINDOW_TOKENS", |s, v, _| assign_opt_nonzero(&mut s.context.context_window, v), |s| Some(format_context_window(s))),
    e!("SWARMY_MEMORY_MAX_BYTES", |s, v, _| assign(&mut s.memory.max_bytes, v), |s| Some(s.memory.max_bytes.to_string())),
    e!("SWARMY_MEMORY_DIR", |s, v, _| assign(&mut s.memory.dir, v), |s| Some(s.memory.dir.to_string_lossy().into_owned())),
    e!("SWARMY_WORKER_PARTITIONS", |s, v, _| assign(&mut s.worker.partitions, v), |s| Some(s.worker.partitions.to_string())),
    e!("SWARMY_SCHEDULER_PARTITIONS", |s, v, _| assign(&mut s.scheduler.partitions, v), |s| Some(s.scheduler.partitions.to_string())),
    e!("SWARMY_SCHEDULER_SCAN_INTERVAL_MS", |s, v, _| assign_duration(&mut s.scheduler.scan_interval_ms, v, duration::Unit::Millis), |s| Some(duration::format(s.scheduler.scan_interval_ms, duration::Unit::Millis))),
    e!("SWARMY_SCHEDULER_RESEND_INTERVAL_MS", |s, v, _| assign_duration(&mut s.scheduler.resend_interval_ms, v, duration::Unit::Millis), |s| Some(duration::format(s.scheduler.resend_interval_ms, duration::Unit::Millis))),
    e!("SWARMY_WORKER_LEASE_MS", |s, v, _| assign_duration(&mut s.worker.lease_ms, v, duration::Unit::Millis), |s| Some(duration::format(s.worker.lease_ms, duration::Unit::Millis))),
    e!("SWARMY_WORKER_RECOVERY_INTERVAL_MS", |s, v, _| assign_duration(&mut s.worker.recovery_interval_ms, v, duration::Unit::Millis), |s| Some(duration::format(s.worker.recovery_interval_ms, duration::Unit::Millis))),
    e!("SWARMY_BUS_ACK_WAIT_MS", |s, v, _| assign_duration(&mut s.bus.ack_wait_ms, v, duration::Unit::Millis), |s| Some(duration::format(s.bus.ack_wait_ms, duration::Unit::Millis))),
    e!("SWARMY_BUS_MAX_DELIVER", |s, v, _| assign(&mut s.bus.max_deliver, v), |s| Some(s.bus.max_deliver.to_string())),
    e!("SWARMY_GATEWAY_CONCURRENCY", |s, v, _| assign(&mut s.gateway.concurrency, v), |s| Some(s.gateway.concurrency.to_string())),
    e!("SWARMY_WORKER_KILL_POINT", |s, v, _| { s.worker.kill_point = Some(v.into()); Ok(()) }, |s| s.worker.kill_point.clone()),
    e!("SWARMY_FAKE_SCRIPT", |s, v, _| assign(&mut s.fake.script, v), |s| Some(s.fake.script.to_string_lossy().into_owned())),
    e!("SWARMY_FAKE_CALL_LOG", |s, v, _| assign(&mut s.fake.call_log, v), |s| Some(s.fake.call_log.to_string_lossy().into_owned())),
    e!("SWARMY_IMAGE_UPLOAD_MAX_BYTES", |s, v, _| assign(&mut s.image.upload_max_bytes, v), |s| Some(s.image.upload_max_bytes.to_string())),
    e!("SWARMY_EPHEMERAL_RETENTION_SECONDS", |s, v, _| assign_duration(&mut s.scheduler.ephemeral_retention_secs, v, duration::Unit::Secs), |s| Some(duration::format(s.scheduler.ephemeral_retention_secs, duration::Unit::Secs))),
    e!("SWARMY_SANDBOX_IDLE_SECONDS", |s, v, _| assign_duration(&mut s.sandbox.idle_secs, v, duration::Unit::Secs), |s| Some(duration::format(s.sandbox.idle_secs, duration::Unit::Secs))),
    e!("SWARMY_PLACEMENT_LEASE_SECONDS", |s, v, _| assign_duration(&mut s.scheduler.placement_lease_secs, v, duration::Unit::Secs), |s| Some(duration::format(s.scheduler.placement_lease_secs, duration::Unit::Secs))),
    e!("SWARMY_INFERENCE_MAX_WAIT_SECONDS", |s, v, _| assign_duration(&mut s.inference.max_wait_secs, v, duration::Unit::Secs), |s| Some(duration::format(s.inference.max_wait_secs, duration::Unit::Secs))),
    e!("SWARMY_INFERENCE_MAX_BACKOFF_SECONDS", |s, v, _| assign_duration(&mut s.inference.max_backoff_secs, v, duration::Unit::Secs), |s| Some(duration::format(s.inference.max_backoff_secs, duration::Unit::Secs))),
    e!("SWARMY_INFERENCE_GATEWAY_WAIT_SECONDS", |s, v, _| assign_duration(&mut s.inference.gateway_wait_secs, v, duration::Unit::Secs), |s| Some(duration::format(s.inference.gateway_wait_secs, duration::Unit::Secs))),
    e!("SWARMY_INFERENCE_DEFAULT_ROUTE", |s, v, _| { assign_opt_string(&mut s.inference.default_route, v); Ok(()) }, |s| Some(s.inference.default_route.clone().unwrap_or_default())),
    e!("SWARMY_VOLUME_SNAPSHOT_PERIOD_SECONDS", |s, v, _| assign_duration(&mut s.volume_snapshots.period_secs, v, duration::Unit::Secs), |s| Some(duration::format(s.volume_snapshots.period_secs, duration::Unit::Secs))),
    e!("SWARMY_VOLUME_SNAPSHOT_RETENTION", |s, v, _| assign(&mut s.volume_snapshots.retention, v), |s| Some(s.volume_snapshots.retention.to_string())),
    e!("SWARMY_GC_GRACE_SECONDS", |s, v, _| assign_duration(&mut s.gc.grace_secs, v, duration::Unit::Secs), |s| Some(duration::format(s.gc.grace_secs, duration::Unit::Secs))),
    e!("SWARMY_GC_INTERVAL_SECONDS", |s, v, _| assign_duration(&mut s.gc.interval_secs, v, duration::Unit::Secs), |s| Some(duration::format(s.gc.interval_secs, duration::Unit::Secs))),
    e!("SWARMY_GC_FILTER_BYTES", |s, v, _| assign(&mut s.gc.filter_bytes, v), |s| Some(s.gc.filter_bytes.to_string())),
    e!("SWARMY_GC_BATCH_SIZE", |s, v, _| assign(&mut s.gc.batch_size, v), |s| Some(s.gc.batch_size.to_string())),
    e!("SWARMY_GC_DELETE_CONCURRENCY", |s, v, _| assign(&mut s.gc.delete_concurrency, v), |s| Some(s.gc.delete_concurrency.to_string())),
    e!("SWARMY_METERING_RAW_RETENTION_DAYS", |s, v, _| assign(&mut s.metering.raw_retention_days, v), |s| Some(s.metering.raw_retention_days.to_string())),
    e!("SWARMY_NODE_ROLES", set_node_roles, |s| Some(format_node_roles(s))),
    e!("SWARMY_NODE_HEARTBEAT_INTERVAL_MS", |s, v, _| assign_duration(&mut s.node.heartbeat_interval_ms, v, duration::Unit::Millis), |s| Some(duration::format(s.node.heartbeat_interval_ms, duration::Unit::Millis))),
    e!("SWARMY_NODE_CPU_MILLIS", |s, v, _| assign(&mut s.node.capacity.cpu_millis, v), |s| Some(s.node.capacity.cpu_millis.to_string())),
    e!("SWARMY_NODE_MEMORY_RESERVE_MIB", set_node_memory_reserve, |s| s.node.memory_reserve_mib.map(|v| v.to_string())),
    e!("SWARMY_NODE_MEMORY_BYTES", |s, v, _| assign(&mut s.node.capacity.memory_bytes, v), |s| Some(s.node.capacity.memory_bytes.to_string())),
    e!("SWARMY_NODE_DISK_BYTES", |s, v, _| assign(&mut s.node.capacity.disk_bytes, v), |s| Some(s.node.capacity.disk_bytes.to_string())),
    e!("SWARMY_NODE_SANDBOXES", |s, v, _| assign(&mut s.node.capacity.sandboxes, v), |s| Some(s.node.capacity.sandboxes.to_string())),
    e!("SWARMY_NODE_ID", set_node_id, |s| s.node.id.map(|v| v.to_string())),
];

fn assign<T: std::str::FromStr>(field: &mut T, value: &str) -> Result<(), ()> {
    *field = value.parse().map_err(|_| ())?;
    Ok(())
}

fn assign_duration(field: &mut Duration, value: &str, unit: duration::Unit) -> Result<(), ()> {
    *field = duration::parse(value, unit)?;
    Ok(())
}

fn assign_opt_nonzero(field: &mut Option<std::num::NonZeroU64>, value: &str) -> Result<(), ()> {
    *field = if value.is_empty() {
        None
    } else {
        Some(value.parse().map_err(|_| ())?)
    };
    Ok(())
}

fn assign_opt_string(field: &mut Option<String>, value: &str) {
    *field = (!value.is_empty()).then(|| value.to_owned());
}

fn format_providers(settings: &Settings) -> String {
    settings
        .selection
        .providers
        .as_ref()
        .map_or_else(String::new, |ids| ids.join(","))
}

fn format_custom_providers(settings: &Settings) -> String {
    serde_json::to_string(&settings.selection.custom_providers).expect("custom providers serialize")
}

fn format_models(settings: &Settings) -> String {
    serde_json::to_string(&settings.selection.models).expect("catalog models serialize")
}

fn format_summarize_at(settings: &Settings) -> String {
    settings
        .context
        .summarize_at
        .map_or_else(String::new, |n| n.to_string())
}

fn format_context_window(settings: &Settings) -> String {
    settings
        .context
        .context_window
        .map_or_else(String::new, |n| n.to_string())
}

fn format_node_roles(settings: &Settings) -> String {
    settings
        .node
        .roles
        .iter()
        .map(|role| match role {
            swarmy_core::NodeRole::Sandbox => "sandbox",
            swarmy_core::NodeRole::Volume => "volume",
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn set_provider(settings: &mut Settings, value: &str, environment: &BTreeMap<String, String>) {
    settings.selection.provider = value.into();
    if !environment.contains_key("SWARMY_PROVIDERS") {
        settings.selection.providers = Some(vec![value.to_owned()]);
    }
}

fn set_providers(settings: &mut Settings, value: &str) {
    settings.selection.providers = if value.is_empty() {
        None
    } else {
        Some(value.split(',').map(|id| id.trim().to_owned()).collect())
    };
}

fn set_custom_providers(
    settings: &mut Settings,
    value: &str,
    _environment: &BTreeMap<String, String>,
) -> Result<(), ()> {
    settings.selection.custom_providers = serde_json::from_str(value).map_err(|_| ())?;
    Ok(())
}

fn set_models(
    settings: &mut Settings,
    value: &str,
    _environment: &BTreeMap<String, String>,
) -> Result<(), ()> {
    settings.selection.models = serde_json::from_str(value).map_err(|_| ())?;
    Ok(())
}

fn set_node_roles(
    settings: &mut Settings,
    value: &str,
    _environment: &BTreeMap<String, String>,
) -> Result<(), ()> {
    settings.node.roles = value
        .split(',')
        .map(|role| match role.trim() {
            "sandbox" => Ok(swarmy_core::NodeRole::Sandbox),
            "volume" => Ok(swarmy_core::NodeRole::Volume),
            _ => Err(()),
        })
        .collect::<Result<_, _>>()?;
    Ok(())
}

fn set_node_memory_reserve(
    settings: &mut Settings,
    value: &str,
    _environment: &BTreeMap<String, String>,
) -> Result<(), ()> {
    settings.node.memory_reserve_mib = Some(value.parse().map_err(|_| ())?);
    Ok(())
}

fn set_node_id(
    settings: &mut Settings,
    value: &str,
    _environment: &BTreeMap<String, String>,
) -> Result<(), ()> {
    settings.node.id = Some(swarmy_core::NodeId::from_ulid(
        value.parse().map_err(|_| ())?,
    ));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn load_with_remote(
        cwd: &Path,
        environment: &BTreeMap<String, String>,
    ) -> Result<Loaded, Error> {
        let mut loaded = Settings::load_base_from(cwd, environment)?;
        loaded.settings.apply_remote()?;
        Ok(loaded)
    }

    #[test]
    fn hosting_policy_defaults_and_overrides() {
        let mut settings = Settings::default();
        let environment = BTreeMap::from([
            ("SWARMY_SANDBOX_IDLE_SECONDS".into(), "2".into()),
            ("SWARMY_PLACEMENT_LEASE_SECONDS".into(), "3".into()),
        ]);
        settings.apply_environment(&environment).unwrap();
        for (key, value) in environment {
            assert_eq!(settings.environment()[&key], value);
        }
        for value in [
            "[sandbox]\nidle_secs = 0",
            "[scheduler]\nephemeral_retention_secs = 0",
            "[scheduler]\nplacement_lease_secs = 0",
        ] {
            assert!(toml::from_str::<Settings>(value).is_err());
        }
        for name in [
            "SWARMY_SANDBOX_IDLE_SECONDS",
            "SWARMY_EPHEMERAL_RETENTION_SECONDS",
            "SWARMY_PLACEMENT_LEASE_SECONDS",
        ] {
            assert!(
                settings
                    .apply_environment(&BTreeMap::from([(name.into(), "0".into())]))
                    .is_err()
            );
        }
    }

    #[test]
    fn gc_defaults_overrides_and_positive_values() {
        let mut settings = Settings::default();
        for (name, field) in [
            ("SWARMY_GC_GRACE_SECONDS", "grace_secs"),
            ("SWARMY_GC_INTERVAL_SECONDS", "interval_secs"),
            ("SWARMY_GC_FILTER_BYTES", "filter_bytes"),
            ("SWARMY_GC_BATCH_SIZE", "batch_size"),
            ("SWARMY_GC_DELETE_CONCURRENCY", "delete_concurrency"),
        ] {
            settings
                .apply_environment(&BTreeMap::from([(name.into(), "123".into())]))
                .unwrap();
            assert_eq!(settings.environment()[name], "123");
            assert!(
                settings
                    .apply_environment(&BTreeMap::from([(name.into(), "0".into())]))
                    .is_err()
            );
            assert!(toml::from_str::<Settings>(&format!("[gc]\n{field} = 0")).is_err());
        }
    }

    #[test]
    fn snapshot_policy_defaults_overrides_and_positive_values() {
        let mut settings = Settings::default();
        let environment = BTreeMap::from([
            ("SWARMY_VOLUME_SNAPSHOT_PERIOD_SECONDS".into(), "2".into()),
            ("SWARMY_VOLUME_SNAPSHOT_RETENTION".into(), "3".into()),
        ]);
        settings.apply_environment(&environment).unwrap();
        for (key, value) in environment {
            assert_eq!(settings.environment()[&key], value);
        }
        assert!(toml::from_str::<Settings>("[volume_snapshots]\nperiod_secs = 0").is_err());
        assert!(toml::from_str::<Settings>("[volume_snapshots]\nretention = 0").is_err());
        for name in [
            "SWARMY_VOLUME_SNAPSHOT_PERIOD_SECONDS",
            "SWARMY_VOLUME_SNAPSHOT_RETENTION",
        ] {
            assert!(
                settings
                    .apply_environment(&BTreeMap::from([(name.into(), "0".into())]))
                    .is_err()
            );
        }
    }

    #[test]
    fn node_settings_apply_and_reject_invalid_roles() {
        let mut settings = Settings::default();
        let environment = BTreeMap::from([
            ("SWARMY_NODE_ROLES".into(), "volume".into()),
            ("SWARMY_NODE_CPU_MILLIS".into(), "4000".into()),
            ("SWARMY_NODE_MEMORY_BYTES".into(), "16106127360".into()),
            ("SWARMY_NODE_DISK_BYTES".into(), "96636764160".into()),
            ("SWARMY_NODE_SANDBOXES".into(), "12".into()),
            ("SWARMY_NODE_HEARTBEAT_INTERVAL_MS".into(), "250".into()),
        ]);
        settings.apply_environment(&environment).unwrap();
        assert_eq!(settings.node.roles, [swarmy_core::NodeRole::Volume]);
        assert_eq!(settings.node.capacity.cpu_millis, 4000);
        assert_eq!(settings.node.capacity.sandboxes, 12);
        for (key, value) in environment {
            assert_eq!(settings.environment()[&key], value);
        }
        assert!(
            settings
                .apply_environment(&BTreeMap::from([(
                    "SWARMY_NODE_ROLES".into(),
                    "unknown".into()
                )]))
                .is_err()
        );
        assert!(
            settings
                .apply_environment(&BTreeMap::from([(
                    "SWARMY_NODE_CPU_MILLIS".into(),
                    "-1".into()
                )]))
                .is_err()
        );
    }

    #[test]
    fn s3_settings_debug_redacts_both_keys() {
        let settings = S3Settings {
            access_key: "test-access".into(),
            secret_key: "test-secret".into(),
            ..S3Settings::default()
        };
        let debug = format!("{settings:?}");
        assert!(!debug.contains("test-access"), "{debug}");
        assert!(!debug.contains("test-secret"), "{debug}");
        assert!(debug.contains("..redacted.."), "{debug}");
    }

    #[test]
    fn node_identity_persists_and_environment_can_select_another_node() {
        let dir = tempfile::tempdir().unwrap();
        let loaded = load_with_remote(dir.path(), &BTreeMap::new()).unwrap();
        let first = loaded.node_id().unwrap();
        assert_eq!(loaded.node_id().unwrap(), first);
        let second = swarmy_core::NodeId::from_ulid(ulid::Ulid::generate());
        let environment = BTreeMap::from([("SWARMY_NODE_ID".into(), second.to_string())]);
        let loaded = load_with_remote(dir.path(), &environment).unwrap();
        assert_eq!(loaded.node_id().unwrap(), second);
        assert_eq!(
            loaded.settings.environment()["SWARMY_NODE_ID"],
            second.to_string()
        );
    }

    #[test]
    fn discovery_overrides_and_paths_are_consistent() {
        let temp = tempfile::tempdir().unwrap();
        let project = temp.path().join("project");
        let nested = project.join("src/nested");
        let user = temp.path().join("xdg/swarmy");
        std::fs::create_dir_all(project.join(".swarmy")).unwrap();
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::create_dir_all(&user).unwrap();
        std::fs::write(user.join("config.toml"), "[store]\ndirectory = 'user'\n").unwrap();
        let config = project.join(".swarmy/config.toml");
        std::fs::write(&config, "[store]\ndirectory = 'project'\n[selection]\nmodel = 'custom'\ncredential_file = 'credentials/auth.json'\n[fake]\nscript = 'fixtures/reply.json'\n").unwrap();
        let mut environment = BTreeMap::from([(
            "XDG_CONFIG_HOME".into(),
            temp.path().join("xdg").to_str().unwrap().into(),
        )]);
        let loaded = load_with_remote(&nested, &environment).unwrap();
        assert_eq!(loaded.path, Some(config.clone()));
        assert_eq!(loaded.settings.store.directory, "project");
        assert_eq!(loaded.settings.selection.model, "custom");
        assert_eq!(
            loaded.settings.fake.script,
            project.join("fixtures/reply.json")
        );
        assert_eq!(
            loaded.settings.selection.credential_file,
            project.join("credentials/auth.json")
        );
        environment.insert("SWARMY_STORE_DIRECTORY".into(), "override".into());
        environment.insert("SWARMY_GATEWAY_CONCURRENCY".into(), "7".into());
        let loaded = load_with_remote(&nested, &environment).unwrap();
        assert_eq!(loaded.settings.store.directory, "override");
        assert_eq!(loaded.settings.gateway.concurrency, 7);
        environment.remove("SWARMY_STORE_DIRECTORY");
        std::fs::remove_file(config).unwrap();
        assert_eq!(
            load_with_remote(&nested, &environment)
                .unwrap()
                .settings
                .store
                .directory,
            "user"
        );
        environment.insert("SWARMY_GATEWAY_CONCURRENCY".into(), "bad".into());
        assert!(load_with_remote(&nested, &environment).is_err());
    }

    #[test]
    fn home_fallback_and_relative_environment_paths() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path().join("work");
        let home = temp.path().join("home");
        let user = home.join(".config/swarmy");
        std::fs::create_dir_all(&cwd).unwrap();
        std::fs::create_dir_all(&user).unwrap();
        std::fs::write(
            user.join("config.toml"),
            "[selection]\nmodel = 'user-model'\n",
        )
        .unwrap();
        let environment = BTreeMap::from([
            ("HOME".into(), home.to_str().unwrap().into()),
            ("XDG_CONFIG_HOME".into(), String::new()),
            ("SWARMY_FDB_CLUSTER_FILE".into(), "custom.cluster".into()),
        ]);
        let loaded = load_with_remote(&cwd, &environment).unwrap();
        assert_eq!(loaded.settings.selection.model, "user-model");
        assert_eq!(
            loaded.settings.selection.credential_file,
            home.join(".swarmy/auth.json")
        );
        assert_eq!(
            loaded.settings.store.cluster_file,
            cwd.join("custom.cluster")
        );
        std::fs::write(
            user.join("config.toml"),
            "[selection]\ncredential_file = '.swarmy/auth.json'",
        )
        .unwrap();
        let loaded = load_with_remote(&cwd, &environment).unwrap();
        assert_eq!(
            loaded.settings.selection.credential_file,
            user.join(".swarmy/auth.json")
        );
        std::fs::write(user.join("config.toml"), "bad toml").unwrap();
        assert!(load_with_remote(&cwd, &environment).is_err());
    }

    #[test]
    fn session_images_resolve_config_environment_and_explicit_precedence() {
        let mut settings = Settings::default();
        assert!(settings.selection.default_image.is_none());
        let error = settings.session_image(None).unwrap_err().to_string();
        assert!(error.contains("default_image") && error.contains("SWARMY_DEFAULT_IMAGE"));
        assert_eq!(
            settings.session_image(Some("explicit:tag")).unwrap(),
            "explicit:tag"
        );
        settings = toml::from_str("[selection]\ndefault_image = 'configured:tag'").unwrap();
        assert_eq!(settings.session_image(None).unwrap(), "configured:tag");
        settings
            .apply_environment(&BTreeMap::from([(
                "SWARMY_DEFAULT_IMAGE".into(),
                "environment:tag".into(),
            )]))
            .unwrap();
        assert_eq!(settings.session_image(None).unwrap(), "environment:tag");
        assert_eq!(
            settings.session_image(Some("explicit:tag")).unwrap(),
            "explicit:tag"
        );
        assert_eq!(
            settings.environment()["SWARMY_DEFAULT_IMAGE"],
            "environment:tag"
        );
        settings
            .apply_environment(&Settings::default().environment())
            .unwrap();
        assert!(settings.session_image(None).is_err());
    }

    #[test]
    fn every_environment_entry_round_trips() {
        let node_id = swarmy_core::NodeId::from_ulid(ulid::Ulid::generate()).to_string();
        // Every entry gets a distinct non-default value, so a setter writing the
        // wrong field fails the round trip instead of passing on defaults.
        let distinct: &[(&str, &str)] = &[
            ("SWARMY_STATE_DIR", "/tmp/test-state"),
            ("SWARMY_REMOTE", "profile-test"),
            ("SWARMY_FDB_CLUSTER_FILE", "/tmp/test.cluster"),
            ("SWARMY_NATS_URL", "nats://127.0.0.1:4333"),
            ("SWARMY_S3_ENDPOINT", "http://127.0.0.1:8334"),
            ("SWARMY_S3_ACCESS_KEY", "test-access"),
            ("SWARMY_S3_SECRET_KEY", "test-secret"),
            ("SWARMY_S3_BUCKET", "test-bucket"),
            ("SWARMY_S3_PREFIX", "test/prefix"),
            ("SWARMY_S3_REGION", "eu-west-1"),
            ("SWARMY_S3_CONDITIONAL_CREATE", "false"),
            ("SWARMY_STORE_DIRECTORY", "testdir"),
            ("SWARMY_BUS_PREFIX", "testprefix"),
            ("SWARMY_API_URL", "http://127.0.0.1:9999"),
            ("SWARMY_API_TOKEN", "test-token-123"),
            ("SWARMY_API_LISTEN", "127.0.0.1:9998"),
            ("SWARMY_PROVIDER", "openai"),
            ("SWARMY_PROVIDERS", "openai,anthropic"),
            (
                "SWARMY_CUSTOM_PROVIDERS",
                r#"{"test":{"base_url":"https://example.com","api":null,"summarize_at":null}}"#,
            ),
            (
                "SWARMY_MODELS",
                r#"[{"provider":"test","id":"test-model","name":null,"api":null,"base_url":null,"context_window":null,"max_output_tokens":null,"reasoning":null,"cost":null,"compat":null,"summarize_at":null}]"#,
            ),
            ("SWARMY_MODEL", "gpt-4o-test"),
            ("SWARMY_DEFAULT_IMAGE", "test:tag"),
            ("SWARMY_REASONING_EFFORT", "high"),
            ("SWARMY_CHATGPT_AUTH", "/tmp/test-auth.json"),
            ("SWARMY_SYSTEM_PROMPT", "test system prompt"),
            ("SWARMY_SUMMARIZE_AT_TOKENS", "4097"),
            ("SWARMY_MODEL_CONTEXT_WINDOW_TOKENS", "8193"),
            ("SWARMY_MEMORY_MAX_BYTES", "9999"),
            ("SWARMY_MEMORY_DIR", "/tmp/test-memory"),
            ("SWARMY_WORKER_PARTITIONS", "0-1"),
            ("SWARMY_SCHEDULER_PARTITIONS", "2-3"),
            ("SWARMY_SCHEDULER_SCAN_INTERVAL_MS", "1234"),
            ("SWARMY_SCHEDULER_RESEND_INTERVAL_MS", "1235"),
            ("SWARMY_WORKER_LEASE_MS", "1236"),
            ("SWARMY_WORKER_RECOVERY_INTERVAL_MS", "1237"),
            ("SWARMY_BUS_ACK_WAIT_MS", "1238"),
            ("SWARMY_BUS_MAX_DELIVER", "9"),
            ("SWARMY_GATEWAY_CONCURRENCY", "9"),
            ("SWARMY_WORKER_KILL_POINT", "test-kill"),
            ("SWARMY_FAKE_SCRIPT", "/tmp/test-fake.json"),
            ("SWARMY_FAKE_CALL_LOG", "/tmp/test-calls.log"),
            ("SWARMY_IMAGE_UPLOAD_MAX_BYTES", "12345"),
            ("SWARMY_EPHEMERAL_RETENTION_SECONDS", "123"),
            ("SWARMY_SANDBOX_IDLE_SECONDS", "124"),
            ("SWARMY_PLACEMENT_LEASE_SECONDS", "125"),
            ("SWARMY_INFERENCE_MAX_WAIT_SECONDS", "126"),
            ("SWARMY_INFERENCE_MAX_BACKOFF_SECONDS", "127"),
            ("SWARMY_INFERENCE_GATEWAY_WAIT_SECONDS", "128"),
            ("SWARMY_INFERENCE_DEFAULT_ROUTE", "test-route"),
            ("SWARMY_VOLUME_SNAPSHOT_PERIOD_SECONDS", "129"),
            ("SWARMY_VOLUME_SNAPSHOT_RETENTION", "11"),
            ("SWARMY_GC_GRACE_SECONDS", "130"),
            ("SWARMY_GC_INTERVAL_SECONDS", "131"),
            ("SWARMY_GC_FILTER_BYTES", "123456"),
            ("SWARMY_GC_BATCH_SIZE", "123"),
            ("SWARMY_GC_DELETE_CONCURRENCY", "33"),
            ("SWARMY_METERING_RAW_RETENTION_DAYS", "7"),
            ("SWARMY_NODE_ROLES", "volume"),
            ("SWARMY_NODE_HEARTBEAT_INTERVAL_MS", "7777"),
            ("SWARMY_NODE_CPU_MILLIS", "4001"),
            ("SWARMY_NODE_MEMORY_RESERVE_MIB", "513"),
            ("SWARMY_NODE_MEMORY_BYTES", "2147483648"),
            ("SWARMY_NODE_DISK_BYTES", "21474836480"),
            ("SWARMY_NODE_SANDBOXES", "7"),
        ];
        let mut exported = BTreeMap::new();
        for (name, value) in distinct {
            exported.insert((*name).to_owned(), (*value).to_owned());
        }
        exported.insert("SWARMY_NODE_ID".to_owned(), node_id);
        for entry in ENV_TABLE {
            assert!(exported.contains_key(entry.name), "missing {}", entry.name);
        }
        assert_eq!(exported.len(), ENV_TABLE.len());
        let mut reloaded = Settings::default();
        reloaded.apply_environment(&exported).unwrap();
        assert_eq!(reloaded.environment(), exported);
    }

    #[test]
    fn store_directory_path_rejects_empty_components() {
        let settings = Settings::default();
        assert_eq!(settings.store_directory_path().unwrap(), ["swarmy"]);
        for directory in ["", "/swarmy", "swarmy/", "a//b"] {
            let settings = Settings {
                store: StoreSettings {
                    directory: directory.into(),
                    ..StoreSettings::default()
                },
                ..Settings::default()
            };
            assert!(settings.store_directory_path().is_err(), "{directory}");
            assert!(
                Settings::default()
                    .apply_environment(&BTreeMap::from([(
                        "SWARMY_STORE_DIRECTORY".into(),
                        directory.into()
                    )]))
                    .is_err(),
                "{directory}"
            );
        }
    }

    #[test]
    fn metering_retention_defaults_overrides_and_positive_values() {
        let mut settings = Settings::default();
        settings
            .apply_environment(&BTreeMap::from([(
                "SWARMY_METERING_RAW_RETENTION_DAYS".into(),
                "7".into(),
            )]))
            .unwrap();
        assert_eq!(settings.metering.raw_retention_days.get(), 7);
        assert_eq!(
            settings.environment()["SWARMY_METERING_RAW_RETENTION_DAYS"],
            "7"
        );
        assert!(
            settings
                .apply_environment(&BTreeMap::from([(
                    "SWARMY_METERING_RAW_RETENTION_DAYS".into(),
                    "0".into()
                )]))
                .is_err()
        );
        assert!(toml::from_str::<Settings>("[metering]\nraw_retention_days = 0").is_err());
    }

    #[test]
    fn exports_are_data_and_support_shell_escaped_paths() {
        let values = parse_exports("export SWARMY_FDB_CLUSTER_FILE=/tmp/a\\ b/fdb.cluster\nexport SWARMY_NATS_URL='nats://localhost:4222'\n").unwrap();
        assert_eq!(values["SWARMY_FDB_CLUSTER_FILE"], "/tmp/a b/fdb.cluster");
        assert_eq!(
            parse_exports("export SWARMY_MODEL='$(touch /tmp/do-not-execute)'\n").unwrap()["SWARMY_MODEL"],
            "$(touch /tmp/do-not-execute)"
        );
        assert!(parse_exports("export SWARMY_MODEL=x; touch /tmp/no").is_err());
        assert!(parse_exports("export HOME=/tmp").is_err());
    }
}

#[cfg(test)]
mod provider_settings_tests {
    use super::*;

    #[test]
    fn provider_subset_and_context_overrides_round_trip() {
        let mut settings = Settings::default();
        assert!(settings.selection.providers.is_none());
        assert!(settings.context.context_window.is_none());
        settings
            .apply_environment(&BTreeMap::from([(
                "SWARMY_PROVIDER".into(),
                "chatgpt".into(),
            )]))
            .unwrap();
        assert_eq!(settings.selection.providers, Some(vec!["chatgpt".into()]));
        settings
            .apply_environment(&BTreeMap::from([
                ("SWARMY_PROVIDER".into(), "chatgpt".into()),
                ("SWARMY_PROVIDERS".into(), "openai, anthropic".into()),
                ("SWARMY_MODEL_CONTEXT_WINDOW_TOKENS".into(), "4096".into()),
            ]))
            .unwrap();
        assert_eq!(
            settings.selection.providers,
            Some(vec!["openai".into(), "anthropic".into()])
        );
        assert_eq!(settings.context.context_window.unwrap().get(), 4096);
        let mut reloaded = Settings::default();
        reloaded.apply_environment(&settings.environment()).unwrap();
        assert_eq!(reloaded.selection.providers, settings.selection.providers);
        assert_eq!(
            reloaded.context.context_window,
            settings.context.context_window
        );
        assert!(
            settings
                .apply_environment(&BTreeMap::from([(
                    "SWARMY_MODEL_CONTEXT_WINDOW_TOKENS".into(),
                    "0".into()
                )]))
                .is_err()
        );
    }
}
