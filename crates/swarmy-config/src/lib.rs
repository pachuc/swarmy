//! Shared configuration for services and command-line programs.
mod exports;
pub use exports::parse_exports;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
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

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    pub volume_snapshots: VolumeSnapshots,
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
    pub s3_region: String,
    pub store_directory: String,
    pub bus_prefix: String,
    pub provider: String,
    pub model: String,
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
            volume_snapshots: VolumeSnapshots::default(),
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
            s3_region: "us-east-1".into(),
            store_directory: "swarmy".into(),
            bus_prefix: String::new(),
            provider: "fake".into(),
            model: "gpt-5".into(),
            reasoning_effort: "medium".into(),
            credential_file: String::new(),
            worker_partitions: "0-255".into(),
            scheduler_partitions: "0-255".into(),
            scheduler_scan_interval_ms: 1000,
            scheduler_resend_interval_ms: 5000,
            worker_lease_ms: 30000,
            worker_recovery_interval_ms: 5000,
            bus_ack_wait_ms: 30000,
            bus_max_deliver: 5,
            gateway_concurrency: 4,
            system_prompt: "You are a helpful assistant. Use tools when needed.".into(),
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
    /// Discover configuration and apply the current process's environment.
    /// # Errors
    /// Fails for unreadable files, invalid TOML, or invalid overrides.
    pub fn load() -> Result<Loaded, Error> {
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
        Self::load_from(&cwd, &environment)
    }

    /// Load with an explicit environment, without changing process globals.
    /// # Errors
    /// Fails for invalid settings or filesystem errors.
    pub fn load_from(cwd: &Path, environment: &BTreeMap<String, String>) -> Result<Loaded, Error> {
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
        Ok(toml::from_str(&std::fs::read_to_string(path)?)?)
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

    /// Apply existing `SWARMY_*` names over file values.
    /// # Errors
    /// Fails if a numeric override cannot be parsed.
    pub fn apply_environment(
        &mut self,
        environment: &BTreeMap<String, String>,
    ) -> Result<(), Error> {
        self.apply_node_environment(environment)?;
        self.apply_snapshot_environment(environment)?;
        if let Some(value) = environment.get("SWARMY_FDB_CLUSTER_FILE") {
            self.fdb_cluster_file.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_NATS_URL") {
            self.nats_url.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_S3_ENDPOINT") {
            self.s3_endpoint.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_S3_ACCESS_KEY") {
            self.s3_access_key.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_S3_SECRET_KEY") {
            self.s3_secret_key.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_S3_BUCKET") {
            self.s3_bucket.clone_from(value);
        }
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
        if let Some(value) = environment.get("SWARMY_SYSTEM_PROMPT") {
            self.system_prompt.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_WORKER_KILL_POINT") {
            self.worker_kill_point = Some(value.clone());
        }
        if let Some(value) = environment.get("SWARMY_FAKE_SCRIPT") {
            self.fake.script.clone_from(value);
        }
        if let Some(value) = environment.get("SWARMY_FAKE_CALL_LOG") {
            self.fake.call_log.clone_from(value);
        }
        Ok(())
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
            ("SWARMY_S3_REGION".into(), self.s3_region.clone()),
            (
                "SWARMY_STORE_DIRECTORY".into(),
                self.store_directory.clone(),
            ),
            ("SWARMY_BUS_PREFIX".into(), self.bus_prefix.clone()),
            ("SWARMY_PROVIDER".into(), self.provider.clone()),
            ("SWARMY_MODEL".into(), self.model.clone()),
            (
                "SWARMY_REASONING_EFFORT".into(),
                self.reasoning_effort.clone(),
            ),
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
        self.node_environment(&mut environment);
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
