use super::{ssh, state::State};
use anyhow::{Result, ensure};

pub async fn run(state: &State, name: &str) -> Result<()> {
    let node = state.require(name)?;
    let address = ssh::reachable_address(&node).await?;
    let mut child = ssh::command(&node)?
        .arg(address)
        .arg("journalctl --unit swarmyd --follow --no-pager --lines 100")
        .spawn()?;
    tokio::select! {
        result = child.wait() => ensure!(result?.success(), "remote journalctl failed"),
        result = tokio::signal::ctrl_c() => { result?; child.kill().await?; }
    }
    Ok(())
}
