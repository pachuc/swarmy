use super::{ssh, state::State};
use crate::Result;
use crate::tunnel::{self, Placement, Request, Service, SshTarget, Tunnels};
use std::{
    path::Path,
    time::{Duration, Instant},
};
use swarmy_config::{RemoteNode, RemotePorts, RemoteProfile, remote_path};
use swarmy_core::ignore_best_effort;

pub(super) async fn run(state_dir: &Path, state: &State, name: &str, json: bool) -> Result<()> {
    let started = Instant::now();
    let _lock = state.lock()?;
    let node = state.require(name)?;
    let path = remote_path(state_dir, name, "profile.json")?;
    let dir = remote_path(state_dir, name, "tunnel")?;
    if path.exists() && !dir.join(tunnel::RECORD_FILE).exists() {
        let profile = RemoteProfile::read(state_dir, name)?;
        if ssh::healthy(&profile).await {
            let output = ssh::control(&profile, "exit").await?;
            crate::Error::ensure(output.status.success(), "cannot stop legacy tunnel")?;
        }
        cleanup(&profile)?;
        std::fs::remove_file(&path)?;
    }
    let opened = Tunnels::ssh()
        .open(&Request {
            dir: &dir,
            target: SshTarget::for_node(&node),
            placement: placement(&node),
            remote_ports: node.ports,
        })
        .await?;
    if opened.reused
        && let Ok(profile) = RemoteProfile::read(state_dir, name)
        && profile.socket_path == opened.record.socket
    {
        return print(
            &profile,
            json,
            &Timing::new(started, Duration::ZERO, Duration::ZERO, true),
        );
    }
    let record = &opened.record;
    let ports = RemotePorts {
        fdb: record.local_port(Service::FoundationDb).unwrap_or(0),
        nats: record.local_port(Service::Nats).unwrap_or(0),
        s3: record.local_port(Service::ObjectStore).unwrap_or(0),
    };
    let mut profile = new_profile(
        state_dir,
        &node,
        ports,
        record.local_port(Service::Api).unwrap_or(0),
        record.socket.clone(),
    )?;
    profile.pid = record.pid;
    if let Err(error) = forward_session(&node, &mut profile, &record.address, ports, &path).await {
        ignore_best_effort(
            Tunnels::ssh().disconnect(&dir).await,
            "disconnect failed tunnel",
        );
        ignore_best_effort(
            std::fs::remove_file(&profile.fdb_cluster_file),
            "remove stale cluster file",
        );
        return Err(error);
    }
    print(
        &profile,
        json,
        &Timing::new(started, opened.probe, opened.startup, opened.reused),
    )
}

pub(super) fn placement(node: &RemoteNode) -> Placement {
    if remote_api(node) {
        Placement::Remote
    } else {
        Placement::LocalServices {
            object_store: node.bucket().is_none(),
        }
    }
}

async fn read_remote_api_token(
    node: &RemoteNode,
    profile: &RemoteProfile,
    address: &str,
) -> Result<Option<String>> {
    if !remote_api(node) {
        return Ok(None);
    }
    let user = node.service_user().to_owned();
    swarmy_config::validate_service_user(&user)?;
    // Bash resolves `~user` through the passwd entry; Rust passes only the login.
    let config = format!("~{user}/swarmy/.swarmy/config.toml");
    let command = "read remote API configuration".to_owned();
    let output = ssh::command(node)?
        .arg("-S")
        .arg(&profile.socket_path)
        .arg(address)
        .arg(format!("cat {config}"))
        .output()
        .await
        .map_err(crate::Error::ssh(&command))?;
    if !output.status.success() {
        return Err(crate::Error::SshStatus {
            command,
            status: output.status,
        });
    }
    let remote: toml::Value = toml::from_str(&String::from_utf8(output.stdout)?)?;
    let token = remote
        .get("api")
        .and_then(|api| api.get("token"))
        .and_then(toml::Value::as_str)
        .unwrap_or("");
    crate::Error::ensure(
        !token.is_empty(),
        "remote API has no token; run swarmy dev up on the node",
    )?;
    Ok(Some(token.to_owned()))
}
/// Open one session over the forwarding connection and record the cluster
/// and API token. `OpenSSH` enables `TCP_NODELAY` on its server transport when
/// a session is opened; a bare -N connection otherwise adds delayed-ACK
/// stalls to small store replies.
async fn forward_session(
    node: &RemoteNode,
    profile: &mut RemoteProfile,
    address: &str,
    ports: RemotePorts,
    path: &Path,
) -> Result<()> {
    if profile.ports.fdb != 0 {
        let command = "initialize SSH forwarding session".to_owned();
        let cluster_arg = if node.launch_settings.is_some() {
            let user = node.service_user().to_owned();
            swarmy_config::validate_service_user(&user)?;
            format!("cat ~{user}/swarmy/.dev/fdb.cluster")
        } else {
            "true".into()
        };
        let output = ssh::command(node)?
            .arg("-S")
            .arg(&profile.socket_path)
            .arg(address)
            .arg(cluster_arg)
            .output()
            .await
            .map_err(crate::Error::ssh(&command))?;
        if !output.status.success() {
            return Err(crate::Error::SshStatus {
                command,
                status: output.status,
            });
        }
        let cluster = if node.launch_settings.is_some() {
            rewrite_address(&String::from_utf8(output.stdout)?, ports.fdb)?
        } else {
            format!("dev:dev@127.0.0.1:{}\n", ports.fdb)
        };
        swarmy_config::write_private(&profile.fdb_cluster_file, cluster.as_bytes())?;
    }
    profile.api_token = read_remote_api_token(node, profile, address).await?;
    swarmy_config::write_private(path, &serde_json::to_vec_pretty(profile)?)?;
    Ok(())
}

fn remote_api(node: &RemoteNode) -> bool {
    node.launch_settings
        .as_ref()
        .is_some_and(|settings| settings.services == swarmy_config::RemoteServices::Node)
}

pub(super) fn new_profile(
    state_dir: &Path,
    node: &RemoteNode,
    ports: RemotePorts,
    api_port: u16,
    socket_path: std::path::PathBuf,
) -> Result<RemoteProfile> {
    Ok(RemoteProfile {
        name: node.name.clone(),
        socket_path,
        pid: 0,
        ports,
        remote_ports: if ports.fdb == 0 {
            RemotePorts {
                fdb: 0,
                nats: 0,
                s3: 0,
            }
        } else {
            node.ports
        },
        fdb_cluster_file: remote_path(state_dir, &node.name, "cluster")?,
        nats_url: if ports.nats == 0 {
            String::new()
        } else {
            format!("nats://127.0.0.1:{}", ports.nats)
        },
        s3_endpoint: if node.bucket().is_some() || ports.s3 == 0 {
            String::new()
        } else {
            format!("http://127.0.0.1:{}", ports.s3)
        },
        api_url: Some(if api_port == 0 {
            let api = swarmy_config::Settings::load_base()?.settings.api;
            api.url.unwrap_or_else(|| format!("http://{}", api.listen))
        } else {
            format!("http://127.0.0.1:{api_port}")
        }),
        api_token: None,
        bucket: node.bucket_spec(),
        default_image: node.default_image.clone(),
    })
}

// Keep the cluster identity from the server while connecting through the local tunnel.
fn rewrite_address(cluster: &str, port: u16) -> Result<String> {
    let Some(line) = cluster
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('#'))
    else {
        return Err(crate::Error::other("empty cluster file"));
    };
    let Some((identity, address)) = line.split_once('@') else {
        return Err(crate::Error::other("invalid cluster file"));
    };
    crate::Error::ensure(
        identity.contains(':') && address.parse::<std::net::SocketAddr>().is_ok(),
        "expected one coordinator address in remote cluster file",
    )?;
    Ok(format!("{identity}@127.0.0.1:{port}\n"))
}

#[derive(serde::Serialize)]
struct Timing {
    elapsed_seconds: f64,
    address_probe_seconds: f64,
    tunnel_startup_seconds: f64,
    reused: bool,
}

impl Timing {
    fn new(started: Instant, probing: Duration, startup: Duration, reused: bool) -> Self {
        Self {
            elapsed_seconds: started.elapsed().as_secs_f64(),
            address_probe_seconds: probing.as_secs_f64(),
            tunnel_startup_seconds: startup.as_secs_f64(),
            reused,
        }
    }
}

/// The `connect --json` document. The profile file carries static keys for
/// the laptop's services, but stdout must never carry them: the credential
/// source stays, the key values do not.
fn redacted_profile(profile: &RemoteProfile) -> Result<serde_json::Value> {
    let mut output = serde_json::to_value(profile)?;
    if let Some(credentials) = output
        .get_mut("bucket")
        .and_then(|bucket| bucket.get_mut("credentials"))
        && credentials.get("source").and_then(|source| source.as_str()) == Some("static_keys")
    {
        *credentials = serde_json::json!({"source": "static_keys"});
    }
    Ok(output)
}

fn print(profile: &RemoteProfile, json: bool, timing: &Timing) -> Result<()> {
    if json {
        let mut output = redacted_profile(profile)?;
        output["timing"] = serde_json::to_value(timing)?;
        cloud_out!("{output}");
    } else {
        // The bucket description prints without secrets; keys stay in the
        // 0600 profile file and never reach terminal output or logs.
        let s3 = match &profile.bucket {
            Some(spec) => spec.describe(),
            None => profile.s3_endpoint.clone(),
        };
        if profile.ports.fdb == 0 {
            cloud_out!(
                "export SWARMY_REMOTE={}\n# API: {}",
                profile.name,
                profile.api_url.as_deref().unwrap_or("")
            );
        } else {
            cloud_out!(
                "export SWARMY_REMOTE={}\n# FoundationDB: {}\n# NATS: {}\n# S3: {}",
                profile.name,
                profile.fdb_cluster_file.display(),
                profile.nats_url,
                s3
            );
        }
        if let Some(image) = &profile.default_image {
            cloud_out!("# Default image: {image}");
        }
        cloud_out!("# swarmy dev up --remote {}", profile.name);
        cloud_out!(
            "# Connected in {:.3}s (address probing: {:.3}s; tunnel startup: {:.3}s; reused: {})",
            timing.elapsed_seconds,
            timing.address_probe_seconds,
            timing.tunnel_startup_seconds,
            timing.reused
        );
    }
    if profile.validate_fdb_port().is_err() {
        cloud_err!(
            "FoundationDB coordinator forwarded on {}, but the server advertises {}. FoundationDB rejects port remapping. Free the advertised port and reconnect before starting services; doctor will report this profile as unusable.",
            profile.ports.fdb,
            profile.remote_ports.fdb
        );
    }
    Ok(())
}

pub(super) fn cleanup(profile: &RemoteProfile) -> Result<()> {
    for path in [&profile.socket_path, &profile.fdb_cluster_file] {
        if path.exists() {
            std::fs::remove_file(path)?;
        }
    }
    if let Some(parent) = profile.socket_path.parent().filter(|p| *p != Path::new("")) {
        // Never recursively remove a directory supplied by a profile.
        ignore_best_effort(std::fs::remove_dir(parent), "remove parent directory");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;

    #[test]
    fn json_output_carries_no_static_keys() {
        let profile = RemoteProfile {
            name: "test".into(),
            socket_path: "socket".into(),
            pid: 1,
            ports: RemotePorts::default(),
            remote_ports: RemotePorts::default(),
            fdb_cluster_file: "cluster".into(),
            nats_url: "nats://127.0.0.1:14222".into(),
            s3_endpoint: String::new(),
            api_url: None,
            api_token: None,
            bucket: Some(swarmy_config::BucketSpec {
                endpoint: "https://objects.example.invalid".into(),
                region: "eu-west-1".into(),
                bucket: "test-bucket".into(),
                credentials: swarmy_config::BucketCredentials::StaticKeys {
                    access_key: "test-access".into(),
                    secret_key: "test-secret".into(),
                },
                ..Default::default()
            }),
            default_image: None,
        };
        let rendered = redacted_profile(&profile).unwrap().to_string();
        assert!(!rendered.contains("test-access"), "{rendered}");
        assert!(!rendered.contains("test-secret"), "{rendered}");
        // The source stays so output readers know which coordinates apply.
        assert!(rendered.contains("static_keys"), "{rendered}");
        assert!(rendered.contains("test-bucket"), "{rendered}");
    }

    #[test]
    fn advertised_address_rewrite_preserves_cluster_identity() {
        assert_eq!(
            rewrite_address("# comment\nstack:secret@10.2.3.4:4500\n", 4500).unwrap(),
            "stack:secret@127.0.0.1:4500\n"
        );
        assert_eq!(
            rewrite_address("dev:dev@127.0.0.1:4500", 4500).unwrap(),
            "dev:dev@127.0.0.1:4500\n"
        );
        for bad in [
            "",
            "dev",
            "dev:dev@invalid",
            "dev:dev@10.0.0.1:4500,10.0.0.2:4500",
        ] {
            assert!(rewrite_address(bad, 4500).is_err());
        }
    }
    #[test]
    fn placement_follows_the_services_setting() {
        let mut node: RemoteNode = serde_json::from_value(serde_json::json!({
            "name": "test", "region": "test", "instance_id": "i-test",
            "public_ip": "127.0.0.1", "private_ip": "127.0.0.1",
            "key_path": "key", "launch_attempted": true, "created_at": "now"
        }))
        .unwrap();
        assert_eq!(
            placement(&node),
            Placement::LocalServices { object_store: true }
        );
        node.launch_settings = Some(swarmy_config::RemoteSettings {
            services: swarmy_config::RemoteServices::Node,
            ..Default::default()
        });
        assert_eq!(placement(&node), Placement::Remote);
        node.launch_settings = Some(swarmy_config::RemoteSettings {
            bucket: Some(swarmy_config::BucketSpec::aws("example")),
            ..Default::default()
        });
        assert_eq!(
            placement(&node),
            Placement::LocalServices {
                object_store: false
            }
        );
    }

    #[test]
    fn api_only_profile_passes_the_fdb_port_check() {
        let node: RemoteNode = serde_json::from_value(serde_json::json!({
            "name": "test", "region": "test", "instance_id": "i-test",
            "public_ip": "127.0.0.1", "private_ip": "127.0.0.1",
            "key_path": "key", "launch_attempted": true, "created_at": "now"
        }))
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let zero = RemotePorts {
            fdb: 0,
            nats: 0,
            s3: 0,
        };
        let profile =
            new_profile(dir.path(), &node, zero, 40000, dir.path().join("socket")).unwrap();
        assert!(profile.validate_fdb_port().is_ok());
        assert_eq!(profile.nats_url, "");
        assert_eq!(profile.s3_endpoint, "");
        assert_eq!(profile.api_url.as_deref(), Some("http://127.0.0.1:40000"));
    }
}
