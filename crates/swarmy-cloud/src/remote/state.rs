use std::{
    fs::{self, File, OpenOptions},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

use crate::Result;
use swarmy_config::{RemoteNode, validate_remote_name};

/// The `remote` directory under the state directory: node records, keys, and the lock.
pub(super) struct State {
    pub directory: PathBuf,
}

impl State {
    pub(super) fn open(directory: &Path) -> Result<Self> {
        fs::create_dir_all(directory)?;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
        Ok(Self {
            directory: directory.canonicalize()?,
        })
    }

    /// Serialize every remote command on this state directory.
    pub(super) fn lock(&self) -> Result<File> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .open(self.directory.join(".lock"))?;
        fs2::FileExt::try_lock_exclusive(&file)?;
        Ok(file)
    }

    fn path(&self, name: &str, extension: &str) -> Result<PathBuf> {
        validate_remote_name(name)?;
        Ok(self.directory.join(format!("{name}.{extension}")))
    }

    pub(super) fn read(&self, name: &str) -> Result<Option<RemoteNode>> {
        match fs::read(self.path(name, "json")?) {
            Ok(bytes) => {
                let node: RemoteNode = serde_json::from_slice(&bytes)?;
                crate::Error::ensure(
                    node.name == name,
                    "remote state name does not match its filename",
                )?;
                Ok(Some(node))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn require(&self, name: &str) -> Result<RemoteNode> {
        self.read(name)?
            .ok_or_else(|| crate::Error::NotFound(name.to_owned()))
    }

    /// Only remote records are inspected; tunnel profiles also use JSON here.
    fn shared(&self, owner: &str, matches: impl Fn(&RemoteNode) -> bool) -> Result<bool> {
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "json")
                || path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .is_none_or(|stem| validate_remote_name(stem).is_err())
            {
                continue;
            }
            let other: RemoteNode = serde_json::from_slice(&fs::read(&path)?)?;
            if other.name != owner && matches(&other) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn bucket_shared(&self, owner: &str, bucket: &str) -> Result<bool> {
        self.shared(owner, |other| other.bucket() == Some(bucket))
    }

    pub(super) fn role_shared(&self, owner: &str, role: &str) -> Result<bool> {
        self.shared(owner, |other| {
            other
                .cloud_settings()
                .instance_profile(&other.name)
                .as_deref()
                == Some(role)
        })
    }

    pub(super) fn save(&self, node: &RemoteNode) -> Result<()> {
        let path = self.path(&node.name, "json")?;
        let mut bytes = serde_json::to_vec_pretty(node)?;
        bytes.push(b'\n');
        write(&path, &bytes)?;
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    pub(super) fn remove(&self, node: &RemoteNode) -> Result<()> {
        self.remove_key(node)?;
        fs::remove_file(self.path(&node.name, "json")?)?;
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    pub(super) fn remove_key(&self, node: &RemoteNode) -> Result<()> {
        // Only remove generated files inside our directory, even if state was edited.
        crate::Error::ensure(
            node.key_path.parent() == Some(self.directory.as_path()),
            "key is outside the remote state directory",
        )?;
        for path in [
            node.key_path.clone(),
            node.key_path.with_extension("pub"),
            node.key_path.with_extension("known_hosts"),
        ] {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }
}

/// Replace a file atomically; the temporary file is private to this user.
pub(super) fn write(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let Some(parent) = path.parent() else {
        return Err(crate::Error::other("path has no parent"));
    };
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    file.write_all(bytes)?;
    file.as_file().sync_all()?;
    file.persist(path)?;
    Ok(())
}
