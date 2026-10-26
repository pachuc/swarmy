//! The API every client command talks to: one resolution, set once per process.
use anyhow::{Context, anyhow};
use std::{path::Path, sync::OnceLock};
use swarmy_client::api_client::{self, Api};
use swarmy_cloud::tunnel::{self, Request, SshTarget, Tunnels};
use swarmy_config::swarm::RecordedHost;

static FLAG: OnceLock<Option<String>> = OnceLock::new();

pub(crate) fn select(flag: Option<String>) -> anyhow::Result<()> {
    FLAG.set(flag)
        .map_err(|_| anyhow!("the target swarm was selected twice"))
}
pub(crate) fn flag() -> Option<&'static str> {
    FLAG.get().and_then(Option::as_deref)
}
pub(crate) fn resolve() -> Result<Api, swarmy_client::Error> {
    api_client::resolve(flag())
}
pub(crate) async fn endpoint() -> anyhow::Result<(String, String)> {
    match resolve()? {
        Api::Direct { url, token } => Ok((url, token)),
        Api::Tunnel {
            swarm,
            dir,
            host,
            token,
        } => {
            let url = open_tunnel(&Tunnels::ssh(), &swarm, &dir, &host).await?;
            Ok((url, token))
        }
    }
}
fn ssh_target(host: &RecordedHost) -> SshTarget {
    let mut addresses = vec![host.public_address.clone()];
    if let Some(private) = &host.private_address {
        addresses.push(private.clone());
    }
    SshTarget {
        addresses,
        user: host.ssh_user.clone(),
        key: host.ssh_key.clone(),
        known_hosts: host.ssh_key.with_extension("known_hosts"),
    }
}
async fn open_tunnel(
    tunnels: &Tunnels,
    swarm: &str,
    dir: &Path,
    host: &RecordedHost,
) -> anyhow::Result<String> {
    let opened = tunnels
        .open(&Request {
            dir,
            target: ssh_target(host),
            placement: tunnel::Placement::Remote,
            remote_ports: swarmy_config::RemotePorts::default(),
        })
        .await
        .with_context(|| format!("opening the SSH tunnel to swarm {swarm}"))?;
    opened
        .record
        .api_url()
        .with_context(|| format!("the tunnel to swarm {swarm} forwards no API port"))
}
pub(crate) async fn connect() -> anyhow::Result<(swarmy_client::Client, String)> {
    let (url, token) = endpoint().await?;
    Ok(api_client::connect_to(&url, token)?)
}
pub(crate) async fn connect_with(
    explicit: Option<(&str, &str)>,
) -> anyhow::Result<(swarmy_client::Client, String)> {
    match explicit {
        Some((url, token)) => Ok(api_client::connect_to(url, token.to_owned())?),
        None => connect().await,
    }
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    struct Kill(std::path::PathBuf);
    impl Drop for Kill {
        fn drop(&mut self) {
            if let Ok(Some(record)) = tunnel::Record::read(&self.0)
                && record.pid > 1
            {
                swarmy_core::ignore_best_effort(
                    std::process::Command::new("kill")
                        .arg(record.pid.to_string())
                        .status(),
                    "kill test SSH master",
                );
            }
        }
    }
    #[tokio::test]
    async fn open_tunnel_uses_recorded_api_port() {
        let tmp = tempfile::tempdir().unwrap();
        let bin = tmp.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        let ssh = bin.join("ssh");
        std::os::unix::fs::symlink(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../swarmy-cloud/tests/fixtures/fake-ssh"),
            &ssh,
        )
        .unwrap();
        let dir = tmp.path().join("tunnel");
        let _kill = Kill(dir.clone());
        let host = RecordedHost {
            name: "box".into(),
            public_address: "127.0.0.1".into(),
            private_address: None,
            ssh_user: "test".into(),
            ssh_key: tmp.path().join("key"),
        };
        let tunnels = Tunnels::with_program(ssh);
        let url = open_tunnel(&tunnels, "fixture", &dir, &host).await.unwrap();
        let port = url
            .strip_prefix("http://127.0.0.1:")
            .unwrap()
            .parse::<u16>()
            .unwrap();
        assert_ne!(port, 0);
        assert!(dir.join("tunnel.json").exists());
        tunnels.disconnect(&dir).await.unwrap();
    }
    #[test]
    fn target_address_order_and_known_hosts() {
        let host = RecordedHost {
            name: "box".into(),
            public_address: "203.0.113.10".into(),
            private_address: Some("203.0.113.11".into()),
            ssh_user: "root".into(),
            ssh_key: "/keys/box".into(),
        };
        let target = ssh_target(&host);
        assert_eq!(target.addresses, ["203.0.113.10", "203.0.113.11"]);
        assert_eq!(target.known_hosts, Path::new("/keys/box.known_hosts"));
    }
}
