use super::{connect, state::State};
use crate::{Result, tunnel::Tunnels};
use std::path::Path;
use swarmy_config::{RemoteProfile, remote_path};

pub(super) async fn run(state_dir: &Path, state: &State, name: &str) -> Result<()> {
    let _lock = state.lock()?;
    let dir = remote_path(state_dir, name, "tunnel")?;
    let had_record = dir.join(crate::tunnel::RECORD_FILE).exists();
    Tunnels::ssh().disconnect(&dir).await?;
    let path = remote_path(state_dir, name, "profile.json")?;
    if path.exists() {
        let profile = RemoteProfile::read(state_dir, name)?;
        if !had_record {
            connect::stop_legacy(&profile).await?;
        }
        connect::cleanup(&profile)?;
        std::fs::remove_file(path)?;
    }
    cloud_out!("{name}: disconnected");
    Ok(())
}
