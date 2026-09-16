use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, ensure};
use swarmy_config::RemoteNode;

pub struct State {
    pub directory: PathBuf,
}

impl State {
    pub fn open(directory: &Path) -> Result<Self> {
        fs::create_dir_all(directory)?;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
        Ok(Self {
            directory: directory.canonicalize()?,
        })
    }

    pub fn lock(&self) -> Result<File> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .open(self.directory.join(".lock"))?;
        fs2::FileExt::try_lock_exclusive(&file).context("another remote command is running")?;
        Ok(file)
    }

    pub fn read(&self, name: &str) -> Result<Option<RemoteNode>> {
        validate_name(name)?;
        match fs::read(self.directory.join(format!("{name}.json"))) {
            Ok(bytes) => {
                let node: RemoteNode = serde_json::from_slice(&bytes)?;
                ensure!(
                    node.name == name,
                    "remote state name does not match its filename"
                );
                Ok(Some(node))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn save(&self, node: &RemoteNode) -> Result<()> {
        validate_name(&node.name)?;
        let mut temporary = tempfile::NamedTempFile::new_in(&self.directory)?;
        serde_json::to_writer_pretty(&mut temporary, node)?;
        temporary.write_all(b"\n")?;
        temporary.as_file().sync_all()?;
        temporary.persist(self.directory.join(format!("{}.json", node.name)))?;
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    pub fn remove(&self, node: &RemoteNode) -> Result<()> {
        // Only remove generated files inside our directory, even if state was edited.
        ensure!(
            node.key_path.parent() == Some(self.directory.as_path()),
            "key is outside the remote state directory"
        );
        for path in [
            node.key_path.clone(),
            node.key_path.with_extension("pub"),
            node.key_path.with_extension("known_hosts"),
            self.directory.join(format!("{}.json", node.name)),
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

pub fn validate_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty()
            && name.len() <= 63
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "remote name must be 1-63 ASCII letters, digits, hyphens, or underscores"
    );
    Ok(())
}
