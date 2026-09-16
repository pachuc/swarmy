mod connect;
mod disconnect;
mod logs;
pub(crate) mod ssh;

use crate::remote_command::Command;
use anyhow::{Context, Result, ensure};

use ssh::{command, control, healthy};
use std::{
    fs::{File, OpenOptions},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};
use swarmy_config::{RemoteNode, Settings, remote_path};

pub async fn run(command: Command, json: bool) -> Result<()> {
    match command {
        Command::Connect { name } => connect::run(&name, json).await,
        Command::Disconnect { name } => disconnect::run(&name).await,
        Command::Logs { name } => logs::run(&name).await,
        Command::Status => unreachable!("status runs in swarmy-session"),
    }
}

pub fn state_dir() -> Result<PathBuf> {
    Ok(Settings::load_base()?.settings.state_dir.into())
}

pub fn node(state: &Path, name: &str) -> Result<RemoteNode> {
    let path = remote_path(state, name, "json")?;
    let node: RemoteNode = serde_json::from_slice(
        &std::fs::read(&path).with_context(|| format!("read {}", path.display()))?,
    )?;
    ensure!(
        node.name == name,
        "remote state name does not match filename"
    );
    Ok(node)
}

pub fn lock(state: &Path, name: &str) -> Result<File> {
    std::fs::create_dir_all(state.join("remote"))?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(remote_path(state, name, "lock")?)?;
    fs2::FileExt::try_lock_exclusive(&file).context("another remote operation is in progress")?;
    Ok(file)
}

pub fn write(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let mut file = tempfile::NamedTempFile::new_in(path.parent().context("path has no parent")?)?;
    file.write_all(bytes)?;
    file.as_file().sync_all()?;
    file.persist(path)?;
    Ok(())
}
