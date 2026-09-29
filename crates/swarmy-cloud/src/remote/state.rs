use std::{
    fs::{self, File, OpenOptions},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

use crate::{ErrorContext as _, Result, cloud_ensure as ensure};
use swarmy_config::{RemoteNode, validate_remote_name};

/// The `remote` directory under the state directory: node records, keys, and the lock.
pub struct State {
    pub directory: PathBuf,
}

impl State {
    pub fn open(directory: &Path) -> Result<Self> {
        fs::create_dir_all(directory).map_err(crate::Error::LocalStateIo)?;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
            .map_err(crate::Error::LocalStateIo)?;
        Ok(Self {
            directory: directory
                .canonicalize()
                .map_err(crate::Error::LocalStateIo)?,
        })
    }

    /// Serialize every remote command on this state directory.
    pub fn lock(&self) -> Result<File> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .open(self.directory.join(".lock"))
            .map_err(crate::Error::LocalStateIo)?;
        fs2::FileExt::try_lock_exclusive(&file).context("another remote command is running")?;
        Ok(file)
    }

    fn path(&self, name: &str, extension: &str) -> Result<PathBuf> {
        validate_remote_name(name)?;
        Ok(self.directory.join(format!("{name}.{extension}")))
    }

    pub fn read(&self, name: &str) -> Result<Option<RemoteNode>> {
        match fs::read(self.path(name, "json")?) {
            Ok(bytes) => {
                let node: RemoteNode = serde_json::from_slice(&bytes)?;
                ensure!(
                    node.name == name,
                    "remote state name does not match its filename"
                );
                Ok(Some(node))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(crate::Error::LocalStateIo(error)),
        }
    }

    pub fn require(&self, name: &str) -> Result<RemoteNode> {
        self.read(name)?
            .ok_or_else(|| crate::Error::NotFound(name.to_owned()))
    }

    /// Only remote records are inspected; tunnel profiles also use JSON here.
    fn shared(&self, owner: &str, matches: impl Fn(&RemoteNode) -> bool) -> Result<bool> {
        for entry in fs::read_dir(&self.directory).map_err(crate::Error::LocalStateIo)? {
            let entry = entry.map_err(crate::Error::LocalStateIo)?;
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "json")
                || path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .is_none_or(|stem| validate_remote_name(stem).is_err())
            {
                continue;
            }
            let other: RemoteNode =
                serde_json::from_slice(&fs::read(&path).map_err(crate::Error::LocalStateIo)?)
                    .with_context(|| format!("reading remote state {}", path.display()))?;
            if other.name != owner && matches(&other) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn bucket_shared(&self, owner: &str, bucket: &str) -> Result<bool> {
        self.shared(owner, |other| other.bucket() == Some(bucket))
    }

    pub fn role_shared(&self, owner: &str, role: &str) -> Result<bool> {
        self.shared(owner, |other| {
            other
                .cloud_settings()
                .instance_profile(&other.name)
                .as_deref()
                == Some(role)
        })
    }

    pub fn save(&self, node: &RemoteNode) -> Result<()> {
        let path = self.path(&node.name, "json")?;
        let mut bytes = serde_json::to_vec_pretty(node)?;
        bytes.push(b'\n');
        write(&path, &bytes)?;
        File::open(&self.directory)
            .map_err(crate::Error::LocalStateIo)?
            .sync_all()
            .map_err(crate::Error::LocalStateIo)?;
        Ok(())
    }

    pub fn remove(&self, node: &RemoteNode) -> Result<()> {
        self.remove_key(node)?;
        fs::remove_file(self.path(&node.name, "json")?).map_err(crate::Error::LocalStateIo)?;
        File::open(&self.directory)
            .map_err(crate::Error::LocalStateIo)?
            .sync_all()
            .map_err(crate::Error::LocalStateIo)?;
        Ok(())
    }

    pub fn remove_key(&self, node: &RemoteNode) -> Result<()> {
        // Only remove generated files inside our directory, even if state was edited.
        ensure!(
            node.key_path.parent() == Some(self.directory.as_path()),
            "key is outside the remote state directory"
        );
        for path in [
            node.key_path.clone(),
            node.key_path.with_extension("pub"),
            node.key_path.with_extension("known_hosts"),
        ] {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(crate::Error::LocalStateIo(error)),
            }
        }
        File::open(&self.directory)
            .map_err(crate::Error::LocalStateIo)?
            .sync_all()
            .map_err(crate::Error::LocalStateIo)?;
        Ok(())
    }
}

/// Replace a file atomically; the temporary file is private to this user.
pub fn write(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let mut file = tempfile::NamedTempFile::new_in(path.parent().context("path has no parent")?)
        .map_err(crate::Error::LocalStateIo)?;
    file.write_all(bytes).map_err(crate::Error::LocalStateIo)?;
    file.as_file()
        .sync_all()
        .map_err(crate::Error::LocalStateIo)?;
    file.persist(path)
        .map_err(|error| crate::Error::LocalStateIo(error.error))?;
    Ok(())
}
