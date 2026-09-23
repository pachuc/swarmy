//! Shared configuration for services and command-line programs.
pub mod keyring;
pub use keyring::Keyring;
mod exports;
mod models;
mod object;
pub use models::{CustomModel, CustomProvider};
mod remote;
pub use exports::parse_exports;
pub use object::ObjectPrefix;
pub use remote::{
    RemoteNode, RemotePorts, RemoteProfile, RemoteServices, RemoteSettings, remote_path,
    validate_remote_name,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
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
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
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

/// Publication policy shared by every volume attachment.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct VolumeSnapshots {
    pub period_seconds: std::num::NonZeroU64,
    pub retention: std::num::NonZeroUsize,
}
impl Default for VolumeSnapshots {
    fn default() -> Self {
        Self {
            period_seconds: std::num::NonZeroU64::new(600).unwrap(),
            retention: std::num::NonZeroUsize::new(10).unwrap(),
        }
    }
}

/// Chunk collection policy. All durations are positive seconds.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GarbageCollection {
    pub grace_seconds: std::num::NonZeroU64,
    pub interval_seconds: std::num::NonZeroU64,
    pub filter_bytes: std::num::NonZeroUsize,
    pub batch_size: std::num::NonZeroUsize,
    pub delete_concurrency: std::num::NonZeroUsize,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Inference {
    pub max_wait_seconds: std::num::NonZeroU64,
    pub max_backoff_seconds: std::num::NonZeroU64,
}

impl Default for Inference {
    fn default() -> Self {
        Self {
            max_wait_seconds: std::num::NonZeroU64::new(3600).unwrap(),
            max_backoff_seconds: std::num::NonZeroU64::new(300).unwrap(),
        }
    }
}
impl Default for GarbageCollection {
    fn default() -> Self {
        Self {
            grace_seconds: std::num::NonZeroU64::new(6 * 60 * 60).unwrap(),
            interval_seconds: std::num::NonZeroU64::new(60 * 60).unwrap(),
            filter_bytes: std::num::NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
            batch_size: std::num::NonZeroUsize::new(256).unwrap(),
            delete_concurrency: std::num::NonZeroUsize::new(32).unwrap(),
        }
    }
}
impl GarbageCollection {
    fn add_to_environment(self, environment: &mut BTreeMap<String, String>) {
        for (name, value) in [
            ("SWARMY_GC_GRACE_SECONDS", self.grace_seconds.to_string()),
            (
                "SWARMY_GC_INTERVAL_SECONDS",
                self.interval_seconds.to_string(),
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
pub struct Settings {
    pub state_dir: String,
    pub remote: RemoteSettings,
    pub volume_snapshots: VolumeSnapshots,
    pub ephemeral_retention_seconds: std::num::NonZeroU64,
    pub sandbox_idle_seconds: std::num::NonZeroU64,
    pub placement_lease_seconds: std::num::NonZeroU64,
    pub gc: GarbageCollection,
    pub inference: Inference,
    pub node_id: Option<swarmy_core::NodeId>,
    pub node_roles: Vec<swarmy_core::NodeRole>,
    pub node_capacity: swarmy_core::NodeCapacity,
    pub node_heartbeat_interval_ms: u64,
    pub fdb_cluster_file: String,
    pub nats_url: String,
    pub s3_endpoint: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_bucket: String,
    pub s3_prefix: ObjectPrefix,
    pub s3_region: String,
    pub store_directory: String,
    pub bus_prefix: String,
    pub provider: String,
    pub providers: Option<Vec<String>>,
    pub custom_providers: BTreeMap<String, CustomProvider>,
    pub models: Vec<CustomModel>,
    pub model: String,
    pub default_image: Option<String>,
    pub reasoning_effort: String,
    pub credential_file: String,
    pub worker_partitions: String,
    pub scheduler_partitions: String,
    pub scheduler_scan_interval_ms: u64,
    pub scheduler_resend_interval_ms: u64,
    pub worker_lease_ms: u64,
    pub worker_recovery_interval_ms: u64,
    pub bus_ack_wait_ms: u64,
    pub bus_max_deliver: i64,
    pub gateway_concurrency: usize,
    pub system_prompt: String,
    /// Override the default three quarters of `model_context_window_tokens`.
    pub summarize_at_tokens: Option<std::num::NonZeroU64>,
    pub model_context_window_tokens: Option<std::num::NonZeroU64>,
    pub memory_dir: String,
    pub memory_max_bytes: std::num::NonZeroUsize,
    pub worker_kill_point: Option<String>,
    pub fake: Fake,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Fake {
    pub script: String,
    pub call_log: String,
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
            remote: RemoteSettings::default(),
            volume_snapshots: VolumeSnapshots::default(),
            ephemeral_retention_seconds: std::num::NonZeroU64::new(86400).unwrap(),
            sandbox_idle_seconds: std::num::NonZeroU64::new(1800).unwrap(),
            placement_lease_seconds: std::num::NonZeroU64::new(30).unwrap(),
            gc: GarbageCollection::default(),
            inference: Inference::default(),
            node_id: None,
            node_roles: vec![
                swarmy_core::NodeRole::Sandbox,
                swarmy_core::NodeRole::Volume,
            ],
            node_capacity: swarmy_core::NodeCapacity {
                cpu_millis: 1000,
                memory_bytes: 1_073_741_824,
                disk_bytes: 34_359_738_368,
                sandboxes: 1,
            },
            node_heartbeat_interval_ms: 5000,
            fdb_cluster_file: ".dev/fdb.cluster".into(),
            nats_url: "nats://127.0.0.1:4222".into(),
            s3_endpoint: "http://127.0.0.1:8333".into(),
            s3_access_key: "swarmy-dev".into(),
            s3_secret_key: "swarmy-dev-secret".into(),
            s3_bucket: "swarmy".into(),
            s3_prefix: ObjectPrefix::default(),
            s3_region: "us-east-1".into(),
            store_directory: "swarmy".into(),
            bus_prefix: String::new(),
            provider: "fake".into(),
            providers: None,
            custom_providers: BTreeMap::new(),
            models: Vec::new(),
            model: "gpt-5".into(),
            default_image: None,
            reasoning_effort: "medium".into(),
            credential_file: String::new(),
            worker_partitions: "0-255".into(),
            scheduler_partitions: "0-255".into(),
            scheduler_scan_interval_ms: 5000,
            scheduler_resend_interval_ms: 5000,
            worker_lease_ms: 30000,
            worker_recovery_interval_ms: 5000,
            bus_ack_wait_ms: 30000,
            bus_max_deliver: 5,
            gateway_concurrency: 4,
            system_prompt: include_str!("system_prompt.txt").into(),
            summarize_at_tokens: None,
            model_context_window_tokens: None,
            memory_dir: "/home/agent/memory".into(),
            memory_max_bytes: std::num::NonZeroUsize::new(32 * 1024).unwrap(),
            worker_kill_point: None,
            fake: Fake::default(),
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
        if let Some(id) = self.settings.node_id {
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

impl Settings {
    /// Select the image for a new session, giving an explicit flag precedence.
    /// # Errors
    /// Requires a configured default or an explicit image.
    pub fn session_image<'a>(&'a self, explicit: Option<&'a str>) -> Result<&'a str, Error> {
        explicit
            .or(self.default_image.as_deref())
            .filter(|image| !image.is_empty())
            .ok_or(Error::MissingImage)
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

    /// Load with an explicit environment, without changing process globals.
    /// # Errors
    /// Fails for invalid settings or filesystem errors.
    pub fn load_from(cwd: &Path, environment: &BTreeMap<String, String>) -> Result<Loaded, Error> {
        let mut loaded = Self::load_base_from(cwd, environment)?;
        loaded.settings.apply_remote()?;
        Ok(loaded)
    }

    /// Apply the selected profile after environment overrides.
    /// # Errors
    /// Returns errors for missing or invalid profiles.
    pub fn apply_remote(&mut self) -> Result<(), Error> {
        if let Some(name) = &self.remote.profile {
            let profile = RemoteProfile::read(Path::new(&self.state_dir), name)?;
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
        if settings.credential_file.is_empty() {
            settings.credential_file = environment
                .get("HOME")
                .map_or_else(|| root.clone(), PathBuf::from)
                .join(".swarmy/auth.json")
                .to_string_lossy()
                .into_owned();
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

    fn apply_snapshot_environment(
        &mut self,
        environment: &BTreeMap<String, String>,
    ) -> Result<(), Error> {
        for (name, target) in [
            (
                "SWARMY_EPHEMERAL_RETENTION_SECONDS",
                &mut self.ephemeral_retention_seconds,
            ),
            (
                "SWARMY_SANDBOX_IDLE_SECONDS",
                &mut self.sandbox_idle_seconds,
            ),
            (
                "SWARMY_PLACEMENT_LEASE_SECONDS",
                &mut self.placement_lease_seconds,
            ),
            (
                "SWARMY_INFERENCE_MAX_WAIT_SECONDS",
                &mut self.inference.max_wait_seconds,
            ),
            (
                "SWARMY_INFERENCE_MAX_BACKOFF_SECONDS",
                &mut self.inference.max_backoff_seconds,
            ),
        ] {
            if let Some(value) = environment.get(name) {
                *target = value.parse().map_err(|_| Error::Environment(name.into()))?;
            }
        }
        if let Some(value) = environment.get("SWARMY_VOLUME_SNAPSHOT_PERIOD_SECONDS") {
            self.volume_snapshots.period_seconds = value
                .parse()
                .map_err(|_| Error::Environment("SWARMY_VOLUME_SNAPSHOT_PERIOD_SECONDS".into()))?;
        }
        if let Some(value) = environment.get("SWARMY_VOLUME_SNAPSHOT_RETENTION") {
            self.volume_snapshots.retention = value
                .parse()
                .map_err(|_| Error::Environment("SWARMY_VOLUME_SNAPSHOT_RETENTION".into()))?;
        }
        Ok(())
    }

    fn apply_gc_environment(
        &mut self,
        environment: &BTreeMap<String, String>,
    ) -> Result<(), Error> {
        for (name, target) in [
            ("SWARMY_GC_GRACE_SECONDS", &mut self.gc.grace_seconds),
            ("SWARMY_GC_INTERVAL_SECONDS", &mut self.gc.interval_seconds),
        ] {
            if let Some(value) = environment.get(name) {
                *target = value.parse().map_err(|_| Error::Environment(name.into()))?;
            }
        }
        if let Some(value) = environment.get("SWARMY_GC_FILTER_BYTES") {
            self.gc.filter_bytes = value
                .parse()
                .map_err(|_| Error::Environment("SWARMY_GC_FILTER_BYTES".into()))?;
        }
        for (name, target) in [
            ("SWARMY_GC_BATCH_SIZE", &mut self.gc.batch_size),
            (
                "SWARMY_GC_DELETE_CONCURRENCY",
                &mut self.gc.delete_concurrency,
            ),
        ] {
            if let Some(value) = environment.get(name) {
                *target = value.parse().map_err(|_| Error::Environment(name.into()))?;
            }
        }
        Ok(())
    }

    /// Apply existing `SWARMY_*` names over file values.
    /// # Errors
    /// Fails if an override cannot be parsed or the S3 namespace is invalid.
    pub fn apply_environment(
        &mut self,
        environment: &BTreeMap<String, String>,
    ) -> Result<(), Error> {
        self.apply_connection_environment(environment);
        self.apply_node_environment(environment)?;
        self.apply_snapshot_environment(environment)?;
        self.apply_gc_environment(environment)?;
        if let Some(value) = environment.get("SWARMY_S3_ACCESS_KEY") {
            self.s3_access_key.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_S3_SECRET_KEY") {
            self.s3_secret_key.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_S3_BUCKET") {
            self.s3_bucket.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_S3_PREFIX") {
            self.s3_prefix = value.parse()?;
        }
        self.s3_namespace()?;
        if let Some(value) = environment.get("SWARMY_S3_REGION") {
            self.s3_region.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_STORE_DIRECTORY") {
            self.store_directory.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_BUS_PREFIX") {
            self.bus_prefix.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_PROVIDER") {
            self.provider.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_DEFAULT_IMAGE") {
            self.default_image = (!value.is_empty()).then(|| value.clone());
        }
        if let Some(value) = environment.get("SWARMY_MODEL") {
            self.model.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_REASONING_EFFORT") {
            self.reasoning_effort.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_CHATGPT_AUTH") {
            self.credential_file.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_WORKER_PARTITIONS") {
            self.worker_partitions.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_SCHEDULER_PARTITIONS") {
            self.scheduler_partitions.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_SCHEDULER_SCAN_INTERVAL_MS") {
            self.scheduler_scan_interval_ms = value
                .parse()
                .map_err(|_| Error::Environment("SWARMY_SCHEDULER_SCAN_INTERVAL_MS".into()))?;
        }
        if let Some(value) = environment.get("SWARMY_SCHEDULER_RESEND_INTERVAL_MS") {
            self.scheduler_resend_interval_ms = value
                .parse()
                .map_err(|_| Error::Environment("SWARMY_SCHEDULER_RESEND_INTERVAL_MS".into()))?;
        }
        if let Some(value) = environment.get("SWARMY_WORKER_LEASE_MS") {
            self.worker_lease_ms = value
                .parse()
                .map_err(|_| Error::Environment("SWARMY_WORKER_LEASE_MS".into()))?;
        }
        if let Some(value) = environment.get("SWARMY_WORKER_RECOVERY_INTERVAL_MS") {
            self.worker_recovery_interval_ms = value
                .parse()
                .map_err(|_| Error::Environment("SWARMY_WORKER_RECOVERY_INTERVAL_MS".into()))?;
        }
        if let Some(value) = environment.get("SWARMY_BUS_ACK_WAIT_MS") {
            self.bus_ack_wait_ms = value
                .parse()
                .map_err(|_| Error::Environment("SWARMY_BUS_ACK_WAIT_MS".into()))?;
        }
        if let Some(value) = environment.get("SWARMY_BUS_MAX_DELIVER") {
            self.bus_max_deliver = value
                .parse()
                .map_err(|_| Error::Environment("SWARMY_BUS_MAX_DELIVER".into()))?;
        }
        if let Some(value) = environment.get("SWARMY_GATEWAY_CONCURRENCY") {
            self.gateway_concurrency = value
                .parse()
                .map_err(|_| Error::Environment("SWARMY_GATEWAY_CONCURRENCY".into()))?;
        }
        self.apply_context_environment(environment)?;
        if let Some(value) = environment.get("SWARMY_SYSTEM_PROMPT") {
            self.system_prompt.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_WORKER_KILL_POINT") {
            self.worker_kill_point = Some(value.clone());
        }
        self.apply_provider_environment(environment)?;
        if let Some(value) = environment.get("SWARMY_FAKE_SCRIPT") {
            self.fake.script.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_FAKE_CALL_LOG") {
            self.fake.call_log.clone_from(value);
        }
        Ok(())
    }

    fn apply_provider_environment(
        &mut self,
        environment: &BTreeMap<String, String>,
    ) -> Result<(), Error> {
        if let Some(value) = environment.get("SWARMY_PROVIDERS") {
            self.providers = if value.is_empty() {
                None
            } else {
                Some(value.split(',').map(|id| id.trim().to_owned()).collect())
            };
        } else if let Some(value) = environment.get("SWARMY_PROVIDER") {
            self.providers = Some(vec![value.clone()]);
        }
        if let Some(value) = environment.get("SWARMY_CUSTOM_PROVIDERS") {
            self.custom_providers = serde_json::from_str(value)
                .map_err(|_| Error::Environment("SWARMY_CUSTOM_PROVIDERS".into()))?;
        }
        if let Some(value) = environment.get("SWARMY_MODELS") {
            self.models = serde_json::from_str(value)
                .map_err(|_| Error::Environment("SWARMY_MODELS".into()))?;
        }
        Ok(())
    }

    fn apply_context_environment(
        &mut self,
        environment: &BTreeMap<String, String>,
    ) -> Result<(), Error> {
        if let Some(value) = environment.get("SWARMY_SUMMARIZE_AT_TOKENS") {
            self.summarize_at_tokens = if value.is_empty() {
                None
            } else {
                Some(
                    value
                        .parse()
                        .map_err(|_| Error::Environment("SWARMY_SUMMARIZE_AT_TOKENS".into()))?,
                )
            };
        }
        if let Some(value) = environment.get("SWARMY_MODEL_CONTEXT_WINDOW_TOKENS") {
            self.model_context_window_tokens =
                if value.is_empty() {
                    None
                } else {
                    Some(value.parse().map_err(|_| {
                        Error::Environment("SWARMY_MODEL_CONTEXT_WINDOW_TOKENS".into())
                    })?)
                };
        }
        if let Some(value) = environment.get("SWARMY_MEMORY_MAX_BYTES") {
            self.memory_max_bytes = value
                .parse()
                .map_err(|_| Error::Environment("SWARMY_MEMORY_MAX_BYTES".into()))?;
        }
        if let Some(value) = environment.get("SWARMY_MEMORY_DIR") {
            self.memory_dir.clone_from(value);
        }
        Ok(())
    }

    fn apply_connection_environment(&mut self, environment: &BTreeMap<String, String>) {
        if let Some(value) = environment.get("SWARMY_STATE_DIR") {
            self.state_dir.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_REMOTE") {
            self.remote.profile = Some(value.clone());
        }
        if let Some(value) = environment.get("SWARMY_FDB_CLUSTER_FILE") {
            self.fdb_cluster_file.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_NATS_URL") {
            self.nats_url.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_S3_ENDPOINT") {
            self.s3_endpoint.clone_from(value);
        }
    }

    fn apply_node_environment(
        &mut self,
        environment: &BTreeMap<String, String>,
    ) -> Result<(), Error> {
        if let Some(value) = environment.get("SWARMY_NODE_ROLES") {
            self.node_roles = value
                .split(',')
                .map(|role| match role.trim() {
                    "sandbox" => Ok(swarmy_core::NodeRole::Sandbox),
                    "volume" => Ok(swarmy_core::NodeRole::Volume),
                    _ => Err(Error::Environment("SWARMY_NODE_ROLES".into())),
                })
                .collect::<Result<_, _>>()?;
        }
        if let Some(value) = environment.get("SWARMY_NODE_HEARTBEAT_INTERVAL_MS") {
            self.node_heartbeat_interval_ms = value
                .parse()
                .map_err(|_| Error::Environment("SWARMY_NODE_HEARTBEAT_INTERVAL_MS".into()))?;
        }
        if let Some(value) = environment.get("SWARMY_NODE_CPU_MILLIS") {
            self.node_capacity.cpu_millis = value
                .parse()
                .map_err(|_| Error::Environment("SWARMY_NODE_CPU_MILLIS".into()))?;
        }
        if let Some(value) = environment.get("SWARMY_NODE_MEMORY_BYTES") {
            self.node_capacity.memory_bytes = value
                .parse()
                .map_err(|_| Error::Environment("SWARMY_NODE_MEMORY_BYTES".into()))?;
        }
        if let Some(value) = environment.get("SWARMY_NODE_DISK_BYTES") {
            self.node_capacity.disk_bytes = value
                .parse()
                .map_err(|_| Error::Environment("SWARMY_NODE_DISK_BYTES".into()))?;
        }
        if let Some(value) = environment.get("SWARMY_NODE_SANDBOXES") {
            self.node_capacity.sandboxes = value
                .parse()
                .map_err(|_| Error::Environment("SWARMY_NODE_SANDBOXES".into()))?;
        }
        if let Some(value) = environment.get("SWARMY_NODE_ID") {
            self.node_id = Some(swarmy_core::NodeId::from_ulid(
                value
                    .parse()
                    .map_err(|_| Error::Environment("SWARMY_NODE_ID".into()))?,
            ));
        }
        Ok(())
    }

    /// Pass the same effective settings to child processes without mutating globals.
    #[must_use]
    pub fn environment(&self) -> BTreeMap<String, String> {
        let mut environment: BTreeMap<String, String> = [
            (
                "SWARMY_FDB_CLUSTER_FILE".into(),
                self.fdb_cluster_file.clone(),
            ),
            ("SWARMY_NATS_URL".into(), self.nats_url.clone()),
            ("SWARMY_S3_ENDPOINT".into(), self.s3_endpoint.clone()),
            ("SWARMY_S3_ACCESS_KEY".into(), self.s3_access_key.clone()),
            ("SWARMY_S3_SECRET_KEY".into(), self.s3_secret_key.clone()),
            ("SWARMY_S3_BUCKET".into(), self.s3_bucket.clone()),
            ("SWARMY_S3_PREFIX".into(), self.s3_prefix.as_str().into()),
            ("SWARMY_S3_REGION".into(), self.s3_region.clone()),
            (
                "SWARMY_STORE_DIRECTORY".into(),
                self.store_directory.clone(),
            ),
            ("SWARMY_BUS_PREFIX".into(), self.bus_prefix.clone()),
            ("SWARMY_CHATGPT_AUTH".into(), self.credential_file.clone()),
            (
                "SWARMY_WORKER_PARTITIONS".into(),
                self.worker_partitions.clone(),
            ),
            (
                "SWARMY_SCHEDULER_PARTITIONS".into(),
                self.scheduler_partitions.clone(),
            ),
            (
                "SWARMY_SCHEDULER_SCAN_INTERVAL_MS".into(),
                self.scheduler_scan_interval_ms.to_string(),
            ),
            (
                "SWARMY_SCHEDULER_RESEND_INTERVAL_MS".into(),
                self.scheduler_resend_interval_ms.to_string(),
            ),
            (
                "SWARMY_WORKER_LEASE_MS".into(),
                self.worker_lease_ms.to_string(),
            ),
            (
                "SWARMY_WORKER_RECOVERY_INTERVAL_MS".into(),
                self.worker_recovery_interval_ms.to_string(),
            ),
            (
                "SWARMY_BUS_ACK_WAIT_MS".into(),
                self.bus_ack_wait_ms.to_string(),
            ),
            (
                "SWARMY_BUS_MAX_DELIVER".into(),
                self.bus_max_deliver.to_string(),
            ),
            (
                "SWARMY_GATEWAY_CONCURRENCY".into(),
                self.gateway_concurrency.to_string(),
            ),
            ("SWARMY_SYSTEM_PROMPT".into(), self.system_prompt.clone()),
            ("SWARMY_FAKE_SCRIPT".into(), self.fake.script.clone()),
            ("SWARMY_FAKE_CALL_LOG".into(), self.fake.call_log.clone()),
        ]
        .into();
        environment.insert("SWARMY_STATE_DIR".into(), self.state_dir.clone());
        if let Some(name) = &self.remote.profile {
            environment.insert("SWARMY_REMOTE".into(), name.clone());
        }
        self.provider_environment(&mut environment);
        self.session_environment(&mut environment);
        self.node_environment(&mut environment);
        self.gc.add_to_environment(&mut environment);
        environment.insert(
            "SWARMY_EPHEMERAL_RETENTION_SECONDS".into(),
            self.ephemeral_retention_seconds.to_string(),
        );
        environment.insert(
            "SWARMY_SANDBOX_IDLE_SECONDS".into(),
            self.sandbox_idle_seconds.to_string(),
        );
        environment.insert(
            "SWARMY_PLACEMENT_LEASE_SECONDS".into(),
            self.placement_lease_seconds.to_string(),
        );
        environment.insert(
            "SWARMY_VOLUME_SNAPSHOT_PERIOD_SECONDS".into(),
            self.volume_snapshots.period_seconds.to_string(),
        );
        environment.insert(
            "SWARMY_VOLUME_SNAPSHOT_RETENTION".into(),
            self.volume_snapshots.retention.to_string(),
        );
        if let Some(value) = &self.worker_kill_point {
            environment.insert("SWARMY_WORKER_KILL_POINT".into(), value.clone());
        }
        environment
    }

    fn provider_environment(&self, environment: &mut BTreeMap<String, String>) {
        environment.insert(
            "SWARMY_PROVIDERS".into(),
            self.providers
                .as_ref()
                .map_or_else(String::new, |ids| ids.join(",")),
        );
        environment.insert(
            "SWARMY_CUSTOM_PROVIDERS".into(),
            serde_json::to_string(&self.custom_providers).expect("custom providers serialize"),
        );
        environment.insert(
            "SWARMY_MODELS".into(),
            serde_json::to_string(&self.models).expect("catalog models serialize"),
        );
    }

    fn session_environment(&self, environment: &mut BTreeMap<String, String>) {
        environment.extend([
            (
                "SWARMY_SUMMARIZE_AT_TOKENS".into(),
                self.summarize_at_tokens
                    .map_or_else(String::new, |n| n.to_string()),
            ),
            (
                "SWARMY_MODEL_CONTEXT_WINDOW_TOKENS".into(),
                self.model_context_window_tokens
                    .map_or_else(String::new, |n| n.to_string()),
            ),
            ("SWARMY_MEMORY_DIR".into(), self.memory_dir.clone()),
            (
                "SWARMY_MEMORY_MAX_BYTES".into(),
                self.memory_max_bytes.to_string(),
            ),
            ("SWARMY_PROVIDER".into(), self.provider.clone()),
            ("SWARMY_MODEL".into(), self.model.clone()),
            (
                "SWARMY_DEFAULT_IMAGE".into(),
                self.default_image.clone().unwrap_or_default(),
            ),
            (
                "SWARMY_REASONING_EFFORT".into(),
                self.reasoning_effort.clone(),
            ),
        ]);
    }

    fn node_environment(&self, environment: &mut BTreeMap<String, String>) {
        environment.insert(
            "SWARMY_NODE_ROLES".into(),
            self.node_roles
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
            self.node_heartbeat_interval_ms.to_string(),
        );
        environment.insert(
            "SWARMY_NODE_CPU_MILLIS".into(),
            self.node_capacity.cpu_millis.to_string(),
        );
        environment.insert(
            "SWARMY_NODE_MEMORY_BYTES".into(),
            self.node_capacity.memory_bytes.to_string(),
        );
        environment.insert(
            "SWARMY_NODE_DISK_BYTES".into(),
            self.node_capacity.disk_bytes.to_string(),
        );
        environment.insert(
            "SWARMY_NODE_SANDBOXES".into(),
            self.node_capacity.sandboxes.to_string(),
        );
        if let Some(value) = &self.node_id {
            environment.insert("SWARMY_NODE_ID".into(), value.to_string());
        }
    }

    /// Anchor filesystem paths so invocation from subdirectories is consistent.
    pub fn resolve_paths(&mut self, root: &Path) {
        for value in [
            &mut self.state_dir,
            &mut self.fdb_cluster_file,
            &mut self.credential_file,
            &mut self.fake.script,
            &mut self.fake.call_log,
        ] {
            if Path::new(value).is_relative() {
                *value = root.join(&*value).to_string_lossy().into_owned();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosting_policy_defaults_and_overrides() {
        let mut settings = Settings::default();
        assert_eq!(settings.sandbox_idle_seconds.get(), 1800);
        assert_eq!(settings.ephemeral_retention_seconds.get(), 86400);
        assert_eq!(settings.placement_lease_seconds.get(), 30);
        let environment = BTreeMap::from([
            ("SWARMY_SANDBOX_IDLE_SECONDS".into(), "2".into()),
            ("SWARMY_PLACEMENT_LEASE_SECONDS".into(), "3".into()),
        ]);
        settings.apply_environment(&environment).unwrap();
        for (key, value) in environment {
            assert_eq!(settings.environment()[&key], value);
        }
        for field in [
            "sandbox_idle_seconds",
            "placement_lease_seconds",
            "ephemeral_retention_seconds",
        ] {
            assert!(toml::from_str::<Settings>(&format!("{field} = 0")).is_err());
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
        assert_eq!(settings.gc.grace_seconds.get(), 21600);
        assert_eq!(settings.gc.interval_seconds.get(), 3600);
        assert_eq!(settings.gc.filter_bytes.get(), 64 * 1024 * 1024);
        assert_eq!(settings.gc.batch_size.get(), 256);
        assert_eq!(settings.gc.delete_concurrency.get(), 32);
        for (name, field) in [
            ("SWARMY_GC_GRACE_SECONDS", "grace_seconds"),
            ("SWARMY_GC_INTERVAL_SECONDS", "interval_seconds"),
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
        assert_eq!(settings.volume_snapshots.period_seconds.get(), 600);
        assert_eq!(settings.volume_snapshots.retention.get(), 10);
        let environment = BTreeMap::from([
            ("SWARMY_VOLUME_SNAPSHOT_PERIOD_SECONDS".into(), "2".into()),
            ("SWARMY_VOLUME_SNAPSHOT_RETENTION".into(), "3".into()),
        ]);
        settings.apply_environment(&environment).unwrap();
        for (key, value) in environment {
            assert_eq!(settings.environment()[&key], value);
        }
        assert!(toml::from_str::<Settings>("[volume_snapshots]\nperiod_seconds = 0").is_err());
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
        assert_eq!(settings.node_roles, [swarmy_core::NodeRole::Volume]);
        assert_eq!(settings.node_capacity.cpu_millis, 4000);
        assert_eq!(settings.node_capacity.sandboxes, 12);
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
        let loaded = Settings::load_from(dir.path(), &BTreeMap::new()).unwrap();
        let first = loaded.node_id().unwrap();
        assert_eq!(loaded.node_id().unwrap(), first);
        let second = swarmy_core::NodeId::from_ulid(ulid::Ulid::generate());
        let environment = BTreeMap::from([("SWARMY_NODE_ID".into(), second.to_string())]);
        let loaded = Settings::load_from(dir.path(), &environment).unwrap();
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
        std::fs::write(&config, "store_directory = 'project'\nmodel = 'custom'\ncredential_file = 'credentials/auth.json'\n[fake]\nscript = 'fixtures/reply.json'\n").unwrap();
        let mut environment = BTreeMap::from([(
            "XDG_CONFIG_HOME".into(),
            temp.path().join("xdg").to_str().unwrap().into(),
        )]);
        let loaded = Settings::load_from(&nested, &environment).unwrap();
        assert_eq!(loaded.path, Some(config.clone()));
        assert_eq!(loaded.settings.store_directory, "project");
        assert_eq!(loaded.settings.model, "custom");
        assert_eq!(
            loaded.settings.fake.script,
            project.join("fixtures/reply.json").to_str().unwrap()
        );
        assert_eq!(
            loaded.settings.credential_file,
            project.join("credentials/auth.json").to_str().unwrap()
        );
        environment.insert("SWARMY_STORE_DIRECTORY".into(), "override".into());
        environment.insert("SWARMY_GATEWAY_CONCURRENCY".into(), "7".into());
        let loaded = Settings::load_from(&nested, &environment).unwrap();
        assert_eq!(loaded.settings.store_directory, "override");
        assert_eq!(loaded.settings.gateway_concurrency, 7);
        environment.remove("SWARMY_STORE_DIRECTORY");
        std::fs::remove_file(config).unwrap();
        assert_eq!(
            Settings::load_from(&nested, &environment)
                .unwrap()
                .settings
                .store_directory,
            "user"
        );
        environment.insert("SWARMY_GATEWAY_CONCURRENCY".into(), "bad".into());
        assert!(Settings::load_from(&nested, &environment).is_err());
    }

    #[test]
    fn home_fallback_and_relative_environment_paths() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path().join("work");
        let home = temp.path().join("home");
        let user = home.join(".config/swarmy");
        std::fs::create_dir_all(&cwd).unwrap();
        std::fs::create_dir_all(&user).unwrap();
        std::fs::write(user.join("config.toml"), "model = 'user-model'\n").unwrap();
        let environment = BTreeMap::from([
            ("HOME".into(), home.to_str().unwrap().into()),
            ("XDG_CONFIG_HOME".into(), String::new()),
            ("SWARMY_FDB_CLUSTER_FILE".into(), "custom.cluster".into()),
        ]);
        let loaded = Settings::load_from(&cwd, &environment).unwrap();
        assert_eq!(loaded.settings.model, "user-model");
        assert_eq!(
            loaded.settings.credential_file,
            home.join(".swarmy/auth.json").to_str().unwrap()
        );
        assert_eq!(
            loaded.settings.fdb_cluster_file,
            cwd.join("custom.cluster").to_str().unwrap()
        );
        std::fs::write(
            user.join("config.toml"),
            "credential_file = '.swarmy/auth.json'",
        )
        .unwrap();
        let loaded = Settings::load_from(&cwd, &environment).unwrap();
        assert_eq!(
            loaded.settings.credential_file,
            user.join(".swarmy/auth.json").to_str().unwrap()
        );
        std::fs::write(user.join("config.toml"), "bad toml").unwrap();
        assert!(Settings::load_from(&cwd, &environment).is_err());
    }

    #[test]
    fn session_images_resolve_config_environment_and_explicit_precedence() {
        let mut settings = Settings::default();
        assert!(settings.default_image.is_none());
        let error = settings.session_image(None).unwrap_err().to_string();
        assert!(error.contains("default_image") && error.contains("SWARMY_DEFAULT_IMAGE"));
        assert_eq!(
            settings.session_image(Some("explicit:tag")).unwrap(),
            "explicit:tag"
        );
        settings = toml::from_str("default_image = 'configured:tag'").unwrap();
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
        assert_eq!(settings.provider, "fake");
        assert_eq!(settings.nats_url, "nats://127.0.0.1:4222");
        assert!(toml::from_str::<Settings>("store_directroy = 'typo'").is_err());
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
        assert!(settings.providers.is_none());
        assert!(settings.model_context_window_tokens.is_none());
        settings
            .apply_environment(&BTreeMap::from([(
                "SWARMY_PROVIDER".into(),
                "chatgpt".into(),
            )]))
            .unwrap();
        assert_eq!(settings.providers, Some(vec!["chatgpt".into()]));
        settings
            .apply_environment(&BTreeMap::from([
                ("SWARMY_PROVIDER".into(), "chatgpt".into()),
                ("SWARMY_PROVIDERS".into(), "openai, anthropic".into()),
                ("SWARMY_MODEL_CONTEXT_WINDOW_TOKENS".into(), "4096".into()),
            ]))
            .unwrap();
        assert_eq!(
            settings.providers,
            Some(vec!["openai".into(), "anthropic".into()])
        );
        assert_eq!(settings.model_context_window_tokens.unwrap().get(), 4096);
        let mut reloaded = Settings::default();
        reloaded.apply_environment(&settings.environment()).unwrap();
        assert_eq!(reloaded.providers, settings.providers);
        assert_eq!(
            reloaded.model_context_window_tokens,
            settings.model_context_window_tokens
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
