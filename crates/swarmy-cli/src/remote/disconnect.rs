use super::{self as ssh, connect};
use anyhow::{Result, ensure};
use swarmy_config::{RemoteProfile, remote_path};

pub async fn run(name: &str) -> Result<()> {
    let state = ssh::state_dir()?;
    let _lock = ssh::lock(&state, name)?;
    let path = remote_path(&state, name, "profile.json")?;
    if !path.exists() {
        println!("{name}: disconnected");
        return Ok(());
    }
    let profile = RemoteProfile::read(&state, name)?;
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
    println!("{name}: disconnected");
    Ok(())
}
