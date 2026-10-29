//! SSH work on elastic machines, using the client's join, drain and revoke paths.
use std::time::Duration;
use swarmy_cloud::{
    provision::Machine,
    ssh::{self, Drain, LocalPrimary, Primary, Ssh},
};

#[async_trait::async_trait]
pub trait Nodes: Send + Sync {
    /// Wait for boot and join a sandbox node to this primary.
    async fn join(&self, node: &Machine, primary: &LocalPrimary) -> anyhow::Result<()>;
    /// Stop the agent after allowing its commands to finish for `timeout`.
    async fn drain(&self, node: &Machine, timeout: Duration) -> anyhow::Result<Drain>;
    /// Remove the tunnel key from the primary.
    async fn revoke(&self, node: &Machine, primary: &LocalPrimary) -> anyhow::Result<()>;
}

pub struct SshNodes {
    ssh: Ssh,
}
impl SshNodes {
    /// # Errors
    /// The installed core or loaded FoundationDB library is missing.
    pub fn installed() -> anyhow::Result<Self> {
        Ok(Self {
            ssh: Ssh::new(ssh::Binaries::installed()?),
        })
    }
}
#[async_trait::async_trait]
impl Nodes for SshNodes {
    async fn join(&self, node: &Machine, primary: &LocalPrimary) -> anyhow::Result<()> {
        ssh::wait_boot(node).await?;
        self.ssh
            .provision(node, Some(Primary::Local(primary)))
            .await?;
        Ok(())
    }
    async fn drain(&self, node: &Machine, timeout: Duration) -> anyhow::Result<Drain> {
        Ok(ssh::drain_node(node, timeout, true).await?)
    }
    async fn revoke(&self, node: &Machine, primary: &LocalPrimary) -> anyhow::Result<()> {
        Ok(ssh::revoke_tunnel(node, Primary::Local(primary)).await?)
    }
}
