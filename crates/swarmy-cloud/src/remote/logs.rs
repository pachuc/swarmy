use super::{ssh, state::State};
use crate::Result;

pub async fn run(state: &State, name: &str) -> Result<()> {
    let node = state.require(name)?;
    let address = ssh::reachable_address(&node).await?;
    let command = "remote journalctl".to_owned();
    let mut child = ssh::command(&node)?
        .arg(address)
        .arg("journalctl --unit swarmyd --follow --no-pager --lines 100")
        .spawn()
        .map_err(|source| crate::Error::Ssh {
            command: command.clone(),
            source: Box::new(source),
        })?;
    tokio::select! {
        result = child.wait() => {
            let status = result.map_err(|source| crate::Error::Ssh {
                command: command.clone(),
                source: Box::new(source),
            })?;
            if !status.success() {
                return Err(crate::Error::SshStatus { command, status });
            }
        }
        result = tokio::signal::ctrl_c() => { result?; child.kill().await.map_err(|source| crate::Error::Ssh {
            command,
            source: Box::new(source),
        })?; }
    }
    Ok(())
}
