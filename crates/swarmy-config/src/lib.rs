#![deny(unreachable_pub)]
//! Shared configuration for services and command-line programs.
pub mod keyring;
pub use keyring::Keyring;
mod exports;
mod models;
mod object;
mod partitions;
pub use models::{CustomModel, CustomProvider};
pub use partitions::{Partitions, parse_partitions};
mod remote;
pub use exports::parse_exports;
pub use object::ObjectPrefix;
pub use remote::{
    AwsSettings, RemoteNode, RemotePorts, RemoteProfile, RemoteServices, RemoteSettings,
    default_sandboxes, remote_path, validate_remote_name,
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

mod duration_ms {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(crate) fn serialize<S: Serializer>(
        value: &Duration,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let millis =
            u64::try_from(value.as_millis()).map_err(serde::ser::Error::custom)?;
        millis.serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Duration, D::Error> {
        Ok(Duration::from_millis(u64::deserialize(deserializer)?))
    }
}

mod duration_secs {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(crate) fn serialize<S: Serializer>(
        value: &Duration,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value.as_secs().serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Duration, D::Error> {
        let secs = u64::deserialize(deserializer)?;
        if secs == 0 {
            return Err(serde::de::Error::custom("duration must be positive"));
        }
        Ok(Duration::from_secs(secs))
    }
}

const DEFAULT_VOLUME_SNAPSHOT_PERIOD: Duration = Duration::from_secs(600);
const DEFAULT_VOLUME_SNAPSHOT_RETENTION: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(10).unwrap();
const DEFAULT_INFERENCE_MAX_WAIT: Duration = Duration::from_secs(3600);
const DEFAULT_INFERENCE_MAX_BACKOFF: Duration = Duration::from_secs(300);
const DEFAULT_INFERENCE_GATEWAY_WAIT: Duration = Duration::from_secs(30);
const DEFAULT_METERING_RAW_RETENTION_DAYS: std::num::NonZeroU64 =
    std::num::NonZeroU64::new(90).unwrap();
const DEFAULT_GC_GRACE: Duration = Duration::from_secs(6 * 60 * 60);
const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(60 * 60);
const DEFAULT_GC_FILTER_BYTES: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(64 * 1024 * 1024).unwrap();
const DEFAULT_GC_BATCH_SIZE: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(256).unwrap();
const DEFAULT_GC_DELETE_CONCURRENCY: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(32).unwrap();
const DEFAULT_EPHEMERAL_RETENTION: Duration = Duration::from_secs(86400);
const DEFAULT_SANDBOX_IDLE: Duration = Duration::from_secs(1800);
const DEFAULT_PLACEMENT_LEASE: Duration = Duration::from_secs(30);
const DEFAULT_NODE_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(5000);
const DEFAULT_SCHEDULER_SCAN_INTERVAL: Duration = Duration::from_millis(5000);
const DEFAULT_SCHEDULER_RESEND_INTERVAL: Duration = Duration::from_millis(5000);
const DEFAULT_WORKER_LEASE: Duration = Duration::from_millis(30000);
const DEFAULT_WORKER_RECOVERY_INTERVAL: Duration = Duration::from_millis(5000);
const DEFAULT_BUS_ACK_WAIT: Duration = Duration::from_millis(30000);
const DEFAULT_MEMORY_MAX_BYTES: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(32 * 1024).unwrap();
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
    #[serde(with = "duration_secs")]
    pub period: Duration,
    pub retention: std::num::NonZeroUsize,
}
impl Default for VolumeSnapshots {
    fn default() -> Self {
        Self {
            period: DEFAULT_VOLUME_SNAPSHOT_PERIOD,
            retention: DEFAULT_VOLUME_SNAPSHOT_RETENTION,
        }
    }
}

/// Chunk collection policy. All durations are positive.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GarbageCollection {
    #[serde(with = "duration_secs")]
    pub grace: Duration,
    #[serde(with = "duration_secs")]
    pub interval: Duration,
    pub filter_bytes: std::num::NonZeroUsize,
    pub batch_size: std::num::NonZeroUsize,
    pub delete_concurrency: std::num::NonZeroUsize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Inference {
    #[serde(with = "duration_secs")]
    pub max_wait: Duration,
    #[serde(with = "duration_secs")]
    pub max_backoff: Duration,
    #[serde(with = "duration_secs")]
    pub gateway_wait: Duration,
    pub default_route: Option<String>,
}

impl Default for Inference {
    fn default() -> Self {
        Self {
            max_wait: DEFAULT_INFERENCE_MAX_WAIT,
            max_backoff: DEFAULT_INFERENCE_MAX_BACKOFF,
            gateway_wait: DEFAULT_INFERENCE_GATEWAY_WAIT,
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
            grace: DEFAULT_GC_GRACE,
            interval: DEFAULT_GC_INTERVAL,
            filter_bytes: DEFAULT_GC_FILTER_BYTES,
            batch_size: DEFAULT_GC_BATCH_SIZE,
            delete_concurrency: DEFAULT_GC_DELETE_CONCURRENCY,
        }
    }
}
impl GarbageCollection {
    fn add_to_environment(self, environment: &mut BTreeMap<String, String>) {
        for (name, value) in [
            ("SWARMY_GC_GRACE_SECONDS", self.grace.as_secs().to_string()),
            (
                "SWARMY_GC_INTERVAL_SECONDS",
                self.interval.as_secs().to_string(),
            ),
            ("SWARMY_GC_FILTER_BYTES", self.filter_bytes.to_string()),
            ("SWARMY_GC_BATCH_SIZE", self.batch_size.to_string()),
            (
                "SWARMY_GC_DELETE_CONCURRENCY",
                self.delete_concurrency.to_string(),
            ),
        ] {
            environment.insert(name.into(), value);
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
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct S3Settings {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub bucket: String,
    pub prefix: ObjectPrefix,
    pub region: String,
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
        }
    }
}

/// NATS transport: connection URL, subject namespace, and delivery policy.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BusSettings {
    pub nats_url: String,
    pub prefix: String,
    #[serde(with = "duration_ms")]
    pub ack_wait: Duration,
    pub max_deliver: u64,
}
impl Default for BusSettings {
    fn default() -> Self {
        Self {
            nats_url: "nats://127.0.0.1:4222".into(),
            prefix: String::new(),
            ack_wait: DEFAULT_BUS_ACK_WAIT,
            max_deliver: 5,
        }
    }
}

/// Step worker progress: owned partitions, lease timing, and failure injection.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkerSettings {
    pub partitions: Partitions,
    #[serde(with = "duration_ms")]
    pub lease: Duration,
    #[serde(with = "duration_ms")]
    pub recovery_interval: Duration,
    pub kill_point: Option<String>,
}
impl Default for WorkerSettings {
    fn default() -> Self {
        Self {
            partitions: Partitions::default(),
            lease: DEFAULT_WORKER_LEASE,
            recovery_interval: DEFAULT_WORKER_RECOVERY_INTERVAL,
            kill_point: None,
        }
    }
}

/// Scheduler progress: owned partitions and scan timing.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulerSettings {
    pub partitions: Partitions,
    #[serde(with = "duration_ms")]
    pub scan_interval: Duration,
    #[serde(with = "duration_ms")]
    pub resend_interval: Duration,
}
impl Default for SchedulerSettings {
    fn default() -> Self {
        Self {
            partitions: Partitions::default(),
            scan_interval: DEFAULT_SCHEDULER_SCAN_INTERVAL,
            resend_interval: DEFAULT_SCHEDULER_RESEND_INTERVAL,
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
    #[serde(with = "duration_ms")]
    pub heartbeat_interval: Duration,
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
            heartbeat_interval: DEFAULT_NODE_HEARTBEAT_INTERVAL,
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

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    pub api: ApiSettings,
    pub state_dir: PathBuf,
    pub remote: RemoteSettings,
    pub volume_snapshots: VolumeSnapshots,
    #[serde(with = "duration_secs")]
    pub ephemeral_retention: Duration,
    pub sandbox: SandboxSettings,
    #[serde(with = "duration_secs")]
    pub placement_lease: Duration,
    pub gc: GarbageCollection,
    pub inference: Inference,
    pub metering: Metering,
    pub node: NodeSettings,
    pub fdb_cluster_file: PathBuf,
    pub s3: S3Settings,
    pub store_directory: String,
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
    #[serde(with = "duration_secs")]
    pub idle: Duration,
    pub scratch_idle_days: u64,
    pub scratch_high_water: u8,
    pub scratch_low_water: u8,
}

impl Default for SandboxSettings {
    fn default() -> Self {
        Self {
            idle: DEFAULT_SANDBOX_IDLE,
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
            ephemeral_retention: DEFAULT_EPHEMERAL_RETENTION,
            sandbox: SandboxSettings::default(),
            placement_lease: DEFAULT_PLACEMENT_LEASE,
            gc: GarbageCollection::default(),
            inference: Inference::default(),
            metering: Metering::default(),
            node: NodeSettings::default(),
            fdb_cluster_file: ".dev/fdb.cluster".into(),
            s3: S3Settings::default(),
            store_directory: "swarmy".into(),
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

/// Initialise process-wide tracing once from `RUST_LOG`, falling back to
/// `default_level` when the variable is absent or invalid.
pub fn init_tracing(default_level: &'static str) {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| default_level.into()),
        )
        .init();
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

    /// Split the FoundationDB directory namespace into path components.
    /// # Errors
    /// Rejects empty components such as a leading, trailing, or doubled slash.
    pub fn store_directory_path(&self) -> Result<Vec<String>, Error> {
        let path: Vec<String> = self
            .store_directory
            .split('/')
            .map(str::to_owned)
            .collect();
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
        let settings: Self = toml::from_str(&std::fs::read_to_string(path)?)?;
        settings.s3_namespace()?;
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
    /// Fails if an override cannot be parsed or the S3 namespace is invalid.
    pub fn apply_environment(
        &mut self,
        environment: &BTreeMap<String, String>,
    ) -> Result<(), Error> {
        type Setter = fn(&mut Settings, &str, &BTreeMap<String, String>) -> Result<(), Error>;
        let table: &[(&str, Setter)] = &[
            ("SWARMY_STATE_DIR", |settings, value, _| {
                assign(&mut settings.state_dir, value, "SWARMY_STATE_DIR")
            }),
            ("SWARMY_REMOTE", |settings, value, _| {
                settings.remote.profile = Some(value.into());
                Ok(())
            }),
            ("SWARMY_FDB_CLUSTER_FILE", |settings, value, _| {
                assign(
                    &mut settings.fdb_cluster_file,
                    value,
                    "SWARMY_FDB_CLUSTER_FILE",
                )
            }),
            ("SWARMY_NATS_URL", |settings, value, _| {
                assign(&mut settings.bus.nats_url, value, "SWARMY_NATS_URL")
            }),
            ("SWARMY_S3_ENDPOINT", |settings, value, _| {
                assign(&mut settings.s3.endpoint, value, "SWARMY_S3_ENDPOINT")
            }),
            ("SWARMY_S3_ACCESS_KEY", |settings, value, _| {
                assign(&mut settings.s3.access_key, value, "SWARMY_S3_ACCESS_KEY")
            }),
            ("SWARMY_S3_SECRET_KEY", |settings, value, _| {
                assign(&mut settings.s3.secret_key, value, "SWARMY_S3_SECRET_KEY")
            }),
            ("SWARMY_S3_BUCKET", |settings, value, _| {
                assign(&mut settings.s3.bucket, value, "SWARMY_S3_BUCKET")
            }),
            ("SWARMY_S3_PREFIX", |settings, value, _| {
                assign(&mut settings.s3.prefix, value, "SWARMY_S3_PREFIX")
            }),
            ("SWARMY_S3_REGION", |settings, value, _| {
                assign(&mut settings.s3.region, value, "SWARMY_S3_REGION")
            }),
            ("SWARMY_STORE_DIRECTORY", |settings, value, _| {
                assign(
                    &mut settings.store_directory,
                    value,
                    "SWARMY_STORE_DIRECTORY",
                )
            }),
            ("SWARMY_BUS_PREFIX", |settings, value, _| {
                assign(&mut settings.bus.prefix, value, "SWARMY_BUS_PREFIX")
            }),
            ("SWARMY_PROVIDER", set_provider),
            ("SWARMY_PROVIDERS", |settings, value, _| {
                set_providers(settings, value)
            }),
            ("SWARMY_CUSTOM_PROVIDERS", |settings, value, _| {
                set_custom_providers(settings, value)
            }),
            ("SWARMY_MODELS", |settings, value, _| {
                set_models(settings, value)
            }),
            ("SWARMY_MODEL", |settings, value, _| {
                assign(&mut settings.selection.model, value, "SWARMY_MODEL")
            }),
            ("SWARMY_DEFAULT_IMAGE", |settings, value, _| {
                assign_opt_string(&mut settings.selection.default_image, value);
                Ok(())
            }),
            ("SWARMY_REASONING_EFFORT", |settings, value, _| {
                assign(
                    &mut settings.selection.effort,
                    value,
                    "SWARMY_REASONING_EFFORT",
                )
            }),
            ("SWARMY_CHATGPT_AUTH", |settings, value, _| {
                assign(
                    &mut settings.selection.credential_file,
                    value,
                    "SWARMY_CHATGPT_AUTH",
                )
            }),
            ("SWARMY_WORKER_PARTITIONS", |settings, value, _| {
                assign(
                    &mut settings.worker.partitions,
                    value,
                    "SWARMY_WORKER_PARTITIONS",
                )
            }),
            ("SWARMY_SCHEDULER_PARTITIONS", |settings, value, _| {
                assign(
                    &mut settings.scheduler.partitions,
                    value,
                    "SWARMY_SCHEDULER_PARTITIONS",
                )
            }),
            ("SWARMY_SCHEDULER_SCAN_INTERVAL_MS", |settings, value, _| {
                assign_ms(
                    &mut settings.scheduler.scan_interval,
                    value,
                    "SWARMY_SCHEDULER_SCAN_INTERVAL_MS",
                )
            }),
            (
                "SWARMY_SCHEDULER_RESEND_INTERVAL_MS",
                |settings, value, _| {
                    assign_ms(
                        &mut settings.scheduler.resend_interval,
                        value,
                        "SWARMY_SCHEDULER_RESEND_INTERVAL_MS",
                    )
                },
            ),
            ("SWARMY_WORKER_LEASE_MS", |settings, value, _| {
                assign_ms(&mut settings.worker.lease, value, "SWARMY_WORKER_LEASE_MS")
            }),
            ("SWARMY_WORKER_RECOVERY_INTERVAL_MS", |settings, value, _| {
                assign_ms(
                    &mut settings.worker.recovery_interval,
                    value,
                    "SWARMY_WORKER_RECOVERY_INTERVAL_MS",
                )
            }),
            ("SWARMY_BUS_ACK_WAIT_MS", |settings, value, _| {
                assign_ms(&mut settings.bus.ack_wait, value, "SWARMY_BUS_ACK_WAIT_MS")
            }),
            ("SWARMY_BUS_MAX_DELIVER", |settings, value, _| {
                assign(
                    &mut settings.bus.max_deliver,
                    value,
                    "SWARMY_BUS_MAX_DELIVER",
                )
            }),
            ("SWARMY_GATEWAY_CONCURRENCY", |settings, value, _| {
                assign(
                    &mut settings.gateway.concurrency,
                    value,
                    "SWARMY_GATEWAY_CONCURRENCY",
                )
            }),
            ("SWARMY_SYSTEM_PROMPT", |settings, value, _| {
                assign(
                    &mut settings.context.system_prompt,
                    value,
                    "SWARMY_SYSTEM_PROMPT",
                )
            }),
            ("SWARMY_WORKER_KILL_POINT", |settings, value, _| {
                settings.worker.kill_point = Some(value.into());
                Ok(())
            }),
            ("SWARMY_FAKE_SCRIPT", |settings, value, _| {
                assign(&mut settings.fake.script, value, "SWARMY_FAKE_SCRIPT")
            }),
            ("SWARMY_FAKE_CALL_LOG", |settings, value, _| {
                assign(&mut settings.fake.call_log, value, "SWARMY_FAKE_CALL_LOG")
            }),
            ("SWARMY_IMAGE_UPLOAD_MAX_BYTES", |settings, value, _| {
                assign(
                    &mut settings.image.upload_max_bytes,
                    value,
                    "SWARMY_IMAGE_UPLOAD_MAX_BYTES",
                )
            }),
            ("SWARMY_SUMMARIZE_AT_TOKENS", |settings, value, _| {
                assign_opt_nonzero(
                    &mut settings.context.summarize_at,
                    value,
                    "SWARMY_SUMMARIZE_AT_TOKENS",
                )
            }),
            ("SWARMY_MODEL_CONTEXT_WINDOW_TOKENS", |settings, value, _| {
                assign_opt_nonzero(
                    &mut settings.context.context_window,
                    value,
                    "SWARMY_MODEL_CONTEXT_WINDOW_TOKENS",
                )
            }),
            ("SWARMY_MEMORY_MAX_BYTES", |settings, value, _| {
                assign(
                    &mut settings.memory.max_bytes,
                    value,
                    "SWARMY_MEMORY_MAX_BYTES",
                )
            }),
            ("SWARMY_MEMORY_DIR", |settings, value, _| {
                assign(&mut settings.memory.dir, value, "SWARMY_MEMORY_DIR")
            }),
            ("SWARMY_EPHEMERAL_RETENTION_SECONDS", |settings, value, _| {
                assign_secs(
                    &mut settings.ephemeral_retention,
                    value,
                    "SWARMY_EPHEMERAL_RETENTION_SECONDS",
                )
            }),
            ("SWARMY_SANDBOX_IDLE_SECONDS", |settings, value, _| {
                assign_secs(
                    &mut settings.sandbox.idle,
                    value,
                    "SWARMY_SANDBOX_IDLE_SECONDS",
                )
            }),
            ("SWARMY_PLACEMENT_LEASE_SECONDS", |settings, value, _| {
                assign_secs(
                    &mut settings.placement_lease,
                    value,
                    "SWARMY_PLACEMENT_LEASE_SECONDS",
                )
            }),
            ("SWARMY_INFERENCE_MAX_WAIT_SECONDS", |settings, value, _| {
                assign_secs(
                    &mut settings.inference.max_wait,
                    value,
                    "SWARMY_INFERENCE_MAX_WAIT_SECONDS",
                )
            }),
            ("SWARMY_INFERENCE_MAX_BACKOFF_SECONDS", |settings, value, _| {
                assign_secs(
                    &mut settings.inference.max_backoff,
                    value,
                    "SWARMY_INFERENCE_MAX_BACKOFF_SECONDS",
                )
            }),
            ("SWARMY_INFERENCE_GATEWAY_WAIT_SECONDS", |settings, value, _| {
                assign_secs(
                    &mut settings.inference.gateway_wait,
                    value,
                    "SWARMY_INFERENCE_GATEWAY_WAIT_SECONDS",
                )
            }),
            ("SWARMY_INFERENCE_DEFAULT_ROUTE", |settings, value, _| {
                assign_opt_string(&mut settings.inference.default_route, value);
                Ok(())
            }),
            ("SWARMY_VOLUME_SNAPSHOT_PERIOD_SECONDS", |settings, value, _| {
                assign_secs(
                    &mut settings.volume_snapshots.period,
                    value,
                    "SWARMY_VOLUME_SNAPSHOT_PERIOD_SECONDS",
                )
            }),
            ("SWARMY_VOLUME_SNAPSHOT_RETENTION", |settings, value, _| {
                assign(
                    &mut settings.volume_snapshots.retention,
                    value,
                    "SWARMY_VOLUME_SNAPSHOT_RETENTION",
                )
            }),
            ("SWARMY_GC_GRACE_SECONDS", |settings, value, _| {
                assign_secs(&mut settings.gc.grace, value, "SWARMY_GC_GRACE_SECONDS")
            }),
            ("SWARMY_GC_INTERVAL_SECONDS", |settings, value, _| {
                assign_secs(
                    &mut settings.gc.interval,
                    value,
                    "SWARMY_GC_INTERVAL_SECONDS",
                )
            }),
            ("SWARMY_GC_FILTER_BYTES", |settings, value, _| {
                assign(
                    &mut settings.gc.filter_bytes,
                    value,
                    "SWARMY_GC_FILTER_BYTES",
                )
            }),
            ("SWARMY_GC_BATCH_SIZE", |settings, value, _| {
                assign(&mut settings.gc.batch_size, value, "SWARMY_GC_BATCH_SIZE")
            }),
            ("SWARMY_GC_DELETE_CONCURRENCY", |settings, value, _| {
                assign(
                    &mut settings.gc.delete_concurrency,
                    value,
                    "SWARMY_GC_DELETE_CONCURRENCY",
                )
            }),
            ("SWARMY_METERING_RAW_RETENTION_DAYS", |settings, value, _| {
                assign(
                    &mut settings.metering.raw_retention_days,
                    value,
                    "SWARMY_METERING_RAW_RETENTION_DAYS",
                )
            }),
            ("SWARMY_API_URL", |settings, value, _| {
                settings.api.url = Some(value.into());
                Ok(())
            }),
            ("SWARMY_API_TOKEN", |settings, value, _| {
                assign(&mut settings.api.token, value, "SWARMY_API_TOKEN")
            }),
            ("SWARMY_API_LISTEN", |settings, value, _| {
                assign(&mut settings.api.listen, value, "SWARMY_API_LISTEN")
            }),
            ("SWARMY_NODE_ROLES", set_node_roles),
            ("SWARMY_NODE_HEARTBEAT_INTERVAL_MS", |settings, value, _| {
                assign_ms(
                    &mut settings.node.heartbeat_interval,
                    value,
                    "SWARMY_NODE_HEARTBEAT_INTERVAL_MS",
                )
            }),
            ("SWARMY_NODE_CPU_MILLIS", |settings, value, _| {
                assign(
                    &mut settings.node.capacity.cpu_millis,
                    value,
                    "SWARMY_NODE_CPU_MILLIS",
                )
            }),
            ("SWARMY_NODE_MEMORY_RESERVE_MIB", |settings, value, _| {
                set_node_memory_reserve(settings, value)
            }),
            ("SWARMY_NODE_MEMORY_BYTES", |settings, value, _| {
                assign(
                    &mut settings.node.capacity.memory_bytes,
                    value,
                    "SWARMY_NODE_MEMORY_BYTES",
                )
            }),
            ("SWARMY_NODE_DISK_BYTES", |settings, value, _| {
                assign(
                    &mut settings.node.capacity.disk_bytes,
                    value,
                    "SWARMY_NODE_DISK_BYTES",
                )
            }),
            ("SWARMY_NODE_SANDBOXES", |settings, value, _| {
                assign(
                    &mut settings.node.capacity.sandboxes,
                    value,
                    "SWARMY_NODE_SANDBOXES",
                )
            }),
            ("SWARMY_NODE_ID", set_node_id),
        ];
        for (name, set) in table {
            if let Some(value) = environment.get(*name) {
                set(self, value, environment)?;
            }
        }
        self.store_directory_path()?;
        self.s3_namespace()?;
        Ok(())
    }

    /// Pass the same effective settings to child processes without mutating globals.
    #[must_use]
    pub fn environment(&self) -> BTreeMap<String, String> {
        let mut environment: BTreeMap<String, String> = [
            (
                "SWARMY_FDB_CLUSTER_FILE".into(),
                self.fdb_cluster_file.to_string_lossy().into_owned(),
            ),
            ("SWARMY_NATS_URL".into(), self.bus.nats_url.clone()),
            ("SWARMY_S3_ENDPOINT".into(), self.s3.endpoint.clone()),
            ("SWARMY_S3_ACCESS_KEY".into(), self.s3.access_key.clone()),
            ("SWARMY_S3_SECRET_KEY".into(), self.s3.secret_key.clone()),
            ("SWARMY_S3_BUCKET".into(), self.s3.bucket.clone()),
            ("SWARMY_S3_PREFIX".into(), self.s3.prefix.as_str().into()),
            ("SWARMY_S3_REGION".into(), self.s3.region.clone()),
            (
                "SWARMY_STORE_DIRECTORY".into(),
                self.store_directory.clone(),
            ),
            ("SWARMY_BUS_PREFIX".into(), self.bus.prefix.clone()),
            (
                "SWARMY_CHATGPT_AUTH".into(),
                self.selection.credential_file.to_string_lossy().into_owned(),
            ),
            (
                "SWARMY_WORKER_PARTITIONS".into(),
                self.worker.partitions.to_string(),
            ),
            (
                "SWARMY_SCHEDULER_PARTITIONS".into(),
                self.scheduler.partitions.to_string(),
            ),
            (
                "SWARMY_SCHEDULER_SCAN_INTERVAL_MS".into(),
                self.scheduler.scan_interval.as_millis().to_string(),
            ),
            (
                "SWARMY_SCHEDULER_RESEND_INTERVAL_MS".into(),
                self.scheduler.resend_interval.as_millis().to_string(),
            ),
            (
                "SWARMY_WORKER_LEASE_MS".into(),
                self.worker.lease.as_millis().to_string(),
            ),
            (
                "SWARMY_WORKER_RECOVERY_INTERVAL_MS".into(),
                self.worker.recovery_interval.as_millis().to_string(),
            ),
            (
                "SWARMY_BUS_ACK_WAIT_MS".into(),
                self.bus.ack_wait.as_millis().to_string(),
            ),
            (
                "SWARMY_BUS_MAX_DELIVER".into(),
                self.bus.max_deliver.to_string(),
            ),
            (
                "SWARMY_GATEWAY_CONCURRENCY".into(),
                self.gateway.concurrency.to_string(),
            ),
            (
                "SWARMY_SYSTEM_PROMPT".into(),
                self.context.system_prompt.clone(),
            ),
            (
                "SWARMY_FAKE_SCRIPT".into(),
                self.fake.script.to_string_lossy().into_owned(),
            ),
            (
                "SWARMY_FAKE_CALL_LOG".into(),
                self.fake.call_log.to_string_lossy().into_owned(),
            ),
            (
                "SWARMY_IMAGE_UPLOAD_MAX_BYTES".into(),
                self.image.upload_max_bytes.to_string(),
            ),
            (
                "SWARMY_API_TOKEN".into(),
                self.api.token.clone(),
            ),
            (
                "SWARMY_API_LISTEN".into(),
                self.api.listen.clone(),
            ),
            (
                "SWARMY_INFERENCE_MAX_WAIT_SECONDS".into(),
                self.inference.max_wait.as_secs().to_string(),
            ),
            (
                "SWARMY_INFERENCE_MAX_BACKOFF_SECONDS".into(),
                self.inference.max_backoff.as_secs().to_string(),
            ),
        ]
        .into();
        environment.insert(
            "SWARMY_STATE_DIR".into(),
            self.state_dir.to_string_lossy().into_owned(),
        );
        if let Some(name) = &self.remote.profile {
            environment.insert("SWARMY_REMOTE".into(), name.clone());
        }
        if let Some(url) = &self.api.url {
            environment.insert("SWARMY_API_URL".into(), url.clone());
        }
        self.provider_environment(&mut environment);
        self.session_environment(&mut environment);
        self.node_environment(&mut environment);
        self.gc.add_to_environment(&mut environment);
        self.retention_environment(&mut environment);
        if let Some(value) = &self.worker.kill_point {
            environment.insert("SWARMY_WORKER_KILL_POINT".into(), value.clone());
        }
        if let Some(value) = &self.node.memory_reserve_mib {
            environment.insert(
                "SWARMY_NODE_MEMORY_RESERVE_MIB".into(),
                value.to_string(),
            );
        }
        environment
    }

    /// Retention windows for ephemeral sessions, sandboxes, snapshots, and
    /// metering raw records. Split from `environment` for line-count limits.
    fn retention_environment(&self, environment: &mut BTreeMap<String, String>) {
        environment.insert(
            "SWARMY_EPHEMERAL_RETENTION_SECONDS".into(),
            self.ephemeral_retention.as_secs().to_string(),
        );
        environment.insert(
            "SWARMY_SANDBOX_IDLE_SECONDS".into(),
            self.sandbox.idle.as_secs().to_string(),
        );
        environment.insert(
            "SWARMY_PLACEMENT_LEASE_SECONDS".into(),
            self.placement_lease.as_secs().to_string(),
        );
        self.inference_environment(environment);
        environment.insert(
            "SWARMY_VOLUME_SNAPSHOT_PERIOD_SECONDS".into(),
            self.volume_snapshots.period.as_secs().to_string(),
        );
        environment.insert(
            "SWARMY_VOLUME_SNAPSHOT_RETENTION".into(),
            self.volume_snapshots.retention.to_string(),
        );
        environment.insert(
            "SWARMY_METERING_RAW_RETENTION_DAYS".into(),
            self.metering.raw_retention_days.to_string(),
        );
    }

    fn inference_environment(&self, environment: &mut BTreeMap<String, String>) {
        environment.insert(
            "SWARMY_INFERENCE_GATEWAY_WAIT_SECONDS".into(),
            self.inference.gateway_wait.as_secs().to_string(),
        );
        environment.insert(
            "SWARMY_INFERENCE_DEFAULT_ROUTE".into(),
            self.inference.default_route.clone().unwrap_or_default(),
        );
    }

    fn provider_environment(&self, environment: &mut BTreeMap<String, String>) {
        environment.insert(
            "SWARMY_PROVIDERS".into(),
            self.selection
                .providers
                .as_ref()
                .map_or_else(String::new, |ids| ids.join(",")),
        );
        environment.insert(
            "SWARMY_CUSTOM_PROVIDERS".into(),
            serde_json::to_string(&self.selection.custom_providers)
                .expect("custom providers serialize"),
        );
        environment.insert(
            "SWARMY_MODELS".into(),
            serde_json::to_string(&self.selection.models).expect("catalog models serialize"),
        );
    }

    fn session_environment(&self, environment: &mut BTreeMap<String, String>) {
        environment.extend([
            (
                "SWARMY_SUMMARIZE_AT_TOKENS".into(),
                self.context
                    .summarize_at
                    .map_or_else(String::new, |n| n.to_string()),
            ),
            (
                "SWARMY_MODEL_CONTEXT_WINDOW_TOKENS".into(),
                self.context
                    .context_window
                    .map_or_else(String::new, |n| n.to_string()),
            ),
            (
                "SWARMY_MEMORY_DIR".into(),
                self.memory.dir.to_string_lossy().into_owned(),
            ),
            (
                "SWARMY_MEMORY_MAX_BYTES".into(),
                self.memory.max_bytes.to_string(),
            ),
            ("SWARMY_PROVIDER".into(), self.selection.provider.clone()),
            ("SWARMY_MODEL".into(), self.selection.model.clone()),
            (
                "SWARMY_DEFAULT_IMAGE".into(),
                self.selection.default_image.clone().unwrap_or_default(),
            ),
            (
                "SWARMY_REASONING_EFFORT".into(),
                self.selection.effort.to_string(),
            ),
        ]);
    }

    fn node_environment(&self, environment: &mut BTreeMap<String, String>) {
        environment.insert(
            "SWARMY_NODE_ROLES".into(),
            self.node
                .roles
                .iter()
                .map(|role| match role {
                    swarmy_core::NodeRole::Sandbox => "sandbox",
                    swarmy_core::NodeRole::Volume => "volume",
                })
                .collect::<Vec<_>>()
                .join(","),
        );
        environment.insert(
            "SWARMY_NODE_HEARTBEAT_INTERVAL_MS".into(),
            self.node.heartbeat_interval.as_millis().to_string(),
        );
        environment.insert(
            "SWARMY_NODE_CPU_MILLIS".into(),
            self.node.capacity.cpu_millis.to_string(),
        );
        environment.insert(
            "SWARMY_NODE_MEMORY_BYTES".into(),
            self.node.capacity.memory_bytes.to_string(),
        );
        environment.insert(
            "SWARMY_NODE_DISK_BYTES".into(),
            self.node.capacity.disk_bytes.to_string(),
        );
        environment.insert(
            "SWARMY_NODE_SANDBOXES".into(),
            self.node.capacity.sandboxes.to_string(),
        );
        if let Some(value) = &self.node.id {
            environment.insert("SWARMY_NODE_ID".into(), value.to_string());
        }
    }

    /// Anchor filesystem paths so invocation from subdirectories is consistent.
    pub fn resolve_paths(&mut self, root: &Path) {
        for value in [
            &mut self.state_dir,
            &mut self.fdb_cluster_file,
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

fn assign<T: std::str::FromStr>(
    field: &mut T,
    value: &str,
    name: &'static str,
) -> Result<(), Error> {
    *field = value
        .parse::<T>()
        .map_err(|_| Error::Environment(name.into()))?;
    Ok(())
}

fn assign_secs(field: &mut Duration, value: &str, name: &'static str) -> Result<(), Error> {
    let secs: u64 = value
        .parse()
        .map_err(|_| Error::Environment(name.into()))?;
    if secs == 0 {
        return Err(Error::Environment(name.into()));
    }
    *field = Duration::from_secs(secs);
    Ok(())
}

fn assign_ms(field: &mut Duration, value: &str, name: &'static str) -> Result<(), Error> {
    let millis: u64 = value
        .parse()
        .map_err(|_| Error::Environment(name.into()))?;
    *field = Duration::from_millis(millis);
    Ok(())
}

fn assign_opt_nonzero(
    field: &mut Option<std::num::NonZeroU64>,
    value: &str,
    name: &'static str,
) -> Result<(), Error> {
    *field = if value.is_empty() {
        None
    } else {
        Some(
            value
                .parse()
                .map_err(|_| Error::Environment(name.into()))?,
        )
    };
    Ok(())
}

fn assign_opt_string(field: &mut Option<String>, value: &str) {
    *field = (!value.is_empty()).then(|| value.to_owned());
}

fn set_provider(
    settings: &mut Settings,
    value: &str,
    environment: &BTreeMap<String, String>,
) -> Result<(), Error> {
    settings.selection.provider = value.to_owned();
    if !environment.contains_key("SWARMY_PROVIDERS") {
        settings.selection.providers = Some(vec![value.to_owned()]);
    }
    Ok(())
}

fn set_providers(settings: &mut Settings, value: &str) -> Result<(), Error> {
    settings.selection.providers = if value.is_empty() {
        None
    } else {
        Some(value.split(',').map(|id| id.trim().to_owned()).collect())
    };
    Ok(())
}

fn set_custom_providers(settings: &mut Settings, value: &str) -> Result<(), Error> {
    settings.selection.custom_providers = serde_json::from_str(value)
        .map_err(|_| Error::Environment("SWARMY_CUSTOM_PROVIDERS".into()))?;
    Ok(())
}

fn set_models(settings: &mut Settings, value: &str) -> Result<(), Error> {
    settings.selection.models = serde_json::from_str(value)
        .map_err(|_| Error::Environment("SWARMY_MODELS".into()))?;
    Ok(())
}

fn set_node_roles(
    settings: &mut Settings,
    value: &str,
    _environment: &BTreeMap<String, String>,
) -> Result<(), Error> {
    settings.node.roles = value
        .split(',')
        .map(|role| match role.trim() {
            "sandbox" => Ok(swarmy_core::NodeRole::Sandbox),
            "volume" => Ok(swarmy_core::NodeRole::Volume),
            _ => Err(Error::Environment("SWARMY_NODE_ROLES".into())),
        })
        .collect::<Result<_, _>>()?;
    Ok(())
}

fn set_node_memory_reserve(settings: &mut Settings, value: &str) -> Result<(), Error> {
    settings.node.memory_reserve_mib = Some(
        value
            .parse()
            .map_err(|_| Error::Environment("SWARMY_NODE_MEMORY_RESERVE_MIB".into()))?,
    );
    Ok(())
}

fn set_node_id(
    settings: &mut Settings,
    value: &str,
    _environment: &BTreeMap<String, String>,
) -> Result<(), Error> {
    settings.node.id = Some(swarmy_core::NodeId::from_ulid(
        value
            .parse()
            .map_err(|_| Error::Environment("SWARMY_NODE_ID".into()))?,
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
        for field in ["sandbox", "placement_lease", "ephemeral_retention"] {
            let value = if field == "sandbox" {
                "[sandbox]\nidle = 0".into()
            } else {
                format!("{field} = 0")
            };
            assert!(toml::from_str::<Settings>(&value).is_err());
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
            ("SWARMY_GC_GRACE_SECONDS", "grace"),
            ("SWARMY_GC_INTERVAL_SECONDS", "interval"),
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
        assert!(toml::from_str::<Settings>("[volume_snapshots]\nperiod = 0").is_err());
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
    fn node_settings_round_trip_and_reject_invalid_roles() {
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
        let encoded = settings.to_toml().unwrap();
        let decoded: Settings = toml::from_str(&encoded).unwrap();
        assert_eq!(decoded.environment(), settings.environment());
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
        std::fs::write(user.join("config.toml"), "store_directory = 'user'\n").unwrap();
        let config = project.join(".swarmy/config.toml");
        std::fs::write(&config, "store_directory = 'project'\n[selection]\nmodel = 'custom'\ncredential_file = 'credentials/auth.json'\n[fake]\nscript = 'fixtures/reply.json'\n").unwrap();
        let mut environment = BTreeMap::from([(
            "XDG_CONFIG_HOME".into(),
            temp.path().join("xdg").to_str().unwrap().into(),
        )]);
        let loaded = load_with_remote(&nested, &environment).unwrap();
        assert_eq!(loaded.path, Some(config.clone()));
        assert_eq!(loaded.settings.store_directory, "project");
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
        assert_eq!(loaded.settings.store_directory, "override");
        assert_eq!(loaded.settings.gateway.concurrency, 7);
        environment.remove("SWARMY_STORE_DIRECTORY");
        std::fs::remove_file(config).unwrap();
        assert_eq!(
            load_with_remote(&nested, &environment)
                .unwrap()
                .settings
                .store_directory,
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
            loaded.settings.fdb_cluster_file,
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
    fn defaults_and_environment_round_trip() {
        let settings = Settings::default();
        let encoded = settings.to_toml().unwrap();
        let decoded: Settings = toml::from_str(&encoded).unwrap();
        assert_eq!(settings.environment(), decoded.environment());
        let mut overridden = Settings::default();
        overridden
            .apply_environment(&decoded.environment())
            .unwrap();
        assert_eq!(overridden.to_toml().unwrap(), encoded);
        assert_eq!(settings.selection.provider, "fake");
        assert_eq!(settings.bus.nats_url, "nats://127.0.0.1:4222");
        assert!(toml::from_str::<Settings>("store_directroy = 'typo'").is_err());
    }

    #[test]
    fn store_directory_path_rejects_empty_components() {
        let settings = Settings::default();
        assert_eq!(settings.store_directory_path().unwrap(), ["swarmy"]);
        for directory in ["", "/swarmy", "swarmy/", "a//b"] {
            let mut settings = Settings::default();
            settings.store_directory = directory.into();
            assert!(settings.store_directory_path().is_err(), "{directory}");
            assert!(
                settings
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
        assert_eq!(
            settings.selection.providers,
            Some(vec!["chatgpt".into()])
        );
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
        assert_eq!(reloaded.context.context_window, settings.context.context_window);
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
