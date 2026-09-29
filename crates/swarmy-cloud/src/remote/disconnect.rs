use super::{connect, ssh, state::State};
use crate::{Result, cloud_ensure as ensure};
use std::path::Path;
use swarmy_config::{RemoteProfile, remote_path};

pub async fn run(state_dir: &Path, state: &State, name: &str) -> Result<()> {
    let _lock = state.lock()?;
    let path = remote_path(state_dir, name, "profile.json")?;
    if !path.exists() {
        cloud_out!("{name}: disconnected");
        return Ok(());
    }
    let profile = RemoteProfile::read(state_dir, name)?;
    if ssh::control(&profile, "check").await?.status.success() {
        let output = ssh::control(&profile, "exit").await?;
        ensure!(
            output.status.success(),
            "cannot stop tunnel: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    connect::cleanup(&profile)?;
    std::fs::remove_file(path)?;
    cloud_out!("{name}: disconnected");
    Ok(())
}
