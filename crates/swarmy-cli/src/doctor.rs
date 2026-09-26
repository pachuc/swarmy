#[cfg(feature = "remote")]
use crate::remote::ssh as remote_ssh;
#[cfg(not(feature = "remote"))]
use crate::remote_ssh;
use std::{path::Path, process::Stdio, time::Duration};

use serde::Serialize;
use swarmy_config::{Loaded, Settings};
use tokio::{process::Command, time::timeout};

#[derive(Serialize)]
struct Check {
    name: String,
    ok: bool,
    status: &'static str,
    detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    fix: Option<String>,
}

impl Check {
    fn warn(name: &str, detail: impl Into<String>, fix: &str) -> Self {
        Self {
            name: name.into(),
            ok: true,
            status: "warn",
            detail: detail.into(),
            fix: Some(fix.into()),
        }
    }

    fn new(name: &str, result: Result<String, String>, fix: &str) -> Self {
        let (ok, detail) = match result {
            Ok(detail) => (true, detail),
            Err(detail) => (false, detail),
        };
        Self {
            name: name.into(),
            ok,
            status: if ok { "pass" } else { "fail" },
            detail,
            fix: (!ok).then(|| fix.into()),
        }
    }
}

pub async fn run(json: bool) -> anyhow::Result<bool> {
    let loaded = Settings::load_base().map(|mut loaded| {
        if let Some(name) = &loaded.settings.remote.profile
            && let Ok(profile) =
                swarmy_config::RemoteProfile::read(Path::new(&loaded.settings.state_dir), name)
        {
            profile.apply(&mut loaded.settings);
        }
        loaded
    });
    let mut checks = vec![Check::new(
        "config",
        match &loaded {
            Ok(loaded) => loaded.path.as_ref().map_or_else(
                || Err("no configuration file found".into()),
                |path| {
                    Ok(format!(
                        "{} (valid; environment overrides applied)",
                        path.display()
                    ))
                },
            ),
            // TOML diagnostics can include source values, including secrets.
            Err(_) => Err(invalid_config()),
        },
        "Run swarmy dev up to create .swarmy/config.toml; repair an existing file or its SWARMY_* overrides. See docs/DEV.md.",
    )];
    checks.push(Check::new("keyring", keyring(), "Run swarmy dev up to create a keyring, or install the existing cluster key with chmod 600; set SWARMY_KEYRING for another path."));
    let remote = loaded
        .as_ref()
        .is_ok_and(|loaded| loaded.settings.remote.profile.is_some());
    if !remote {
        for (name, argument) in [
            ("fdbserver", "--version"),
            ("fdbcli", "--version"),
            ("nats-server", "--version"),
            ("weed", "version"),
        ] {
            checks.push(Check::new(name, binary_version(name, argument).await,
            "Run scripts/install-dev-tools.sh. swarmy searches PATH, the install prefix's bin directory, and ~/.local/bin automatically; add another location to PATH if needed."));
        }
    }
    for name in [
        "swarmy-api",
        "swarmy-scheduler",
        "swarmy-worker",
        "swarmy-gateway",
    ] {
        // Companions are exec'd beside the CLI; services honor the caller's PATH.
        let executable = crate::dev::service_binary(name);
        let result = match executable {
            Ok(path) => crate::dev::version_check(&path, name).await,
            Err(error) => Err(error),
        };
        let result = result.map_err(|error| format!("{error:#}"));
        let fix = format!("Reinstall from the CLI checkout: {}", crate::dev::REINSTALL);
        checks.push(match (remote, result) {
            (true, Err(detail)) => Check::warn(
                name,
                format!("{detail}; service binary runs on the remote node"),
                &fix,
            ),
            (_, result) => Check::new(name, result, &fix),
        });
    }

    let providers = if let Ok(loaded) = &loaded {
        checks.extend(remote_checks(loaded).await);
        checks.push(s3_line(&loaded.settings).await);
        if let Some(path) = Path::new(&loaded.settings.credential_file).to_str() {
            let result = if Path::new(path).is_file() {
                Check::new("credential file", Ok(format!("{path} present")), "")
            } else {
                Check::warn(
                    "credential file",
                    format!("{path} absent; store credentials may still be ready"),
                    "Use swarmy auth login or swarmy auth import if a subscription login is needed.",
                )
            };
            checks.push(result);
        }
        api_checks(&mut checks, loaded).await
    } else {
        checks.push(Check::new(
            "API",
            Err("cannot check without valid settings".into()),
            "Repair the configuration and run swarmy doctor again.",
        ));
        Vec::new()
    };
    Ok(report(checks, providers, json))
}

fn report(
    checks: Vec<Check>,
    providers: Vec<crate::provider_report::ProviderRow>,
    json: bool,
) -> bool {
    let ok = checks.iter().all(|check| check.ok);
    if json {
        println!(
            "{}",
            serde_json::json!({"ok": ok, "checks": checks, "providers": providers})
        );
    } else {
        for check in checks {
            match (check.status, check.fix) {
                ("fail", Some(fix)) => println!("fix {}: {}. {fix}", check.name, check.detail),
                ("warn", Some(fix)) => println!("warn {}: {}. {fix}", check.name, check.detail),
                _ => println!("ok {}: {}", check.name, check.detail),
            }
        }
        println!("Providers:");
        for row in providers {
            println!(
                "  {}: credential={} status={} store={} gateway={} {}",
                row.provider,
                row.credential,
                row.status,
                row.store,
                row.gateway,
                row.gateway_reason
            );
        }
    }
    ok
}

async fn remote_checks(loaded: &Loaded) -> Vec<Check> {
    let mut checks = Vec::new();
    if let Some(name) = &loaded.settings.remote.profile {
        let profile =
            match swarmy_config::RemoteProfile::read(Path::new(&loaded.settings.state_dir), name) {
                Ok(profile) => profile,
                Err(error) => {
                    return vec![Check::new(
                        "remote tunnel",
                        Err(format!("{name}: profile unavailable: {error}")),
                        &format!(
                            "Run swarmy remote connect {name}; repair an invalid profile if needed."
                        ),
                    )];
                }
            };
        checks.push(Check::new(
            "remote FoundationDB port",
            profile
                .validate_fdb_port()
                .map(|()| "advertised port preserved".into())
                .map_err(|error| error.to_string()),
            "Disconnect, free the advertised FoundationDB port, and reconnect.",
        ));
        let result = if remote_ssh::healthy(&profile).await {
            Ok(format!(
                "{name}: SSH control master healthy (pid {})",
                profile.pid
            ))
        } else {
            Err(format!("{name}: SSH control master is down"))
        };
        checks.push(Check::new(
            "remote tunnel",
            result,
            &format!("Run swarmy remote connect {name}."),
        ));
    }
    checks
}

fn invalid_config() -> String {
    let environment = ["HOME", "XDG_CONFIG_HOME"]
        .into_iter()
        .filter_map(|key| std::env::var(key).ok().map(|value| (key.to_owned(), value)))
        .collect();
    let path = std::env::current_dir()
        .ok()
        .and_then(|cwd| Settings::discover_from(&cwd, &environment));
    path.map_or_else(
        || "configuration environment overrides are invalid".into(),
        |path| {
            format!(
                "{} or its environment overrides are invalid",
                path.display()
            )
        },
    )
}

async fn binary_version(name: &str, argument: &str) -> Result<String, String> {
    let path = crate::tools::find(name).ok_or_else(|| {
        format!("{name} is missing from the executable search path (including ~/.local/bin)")
    })?;
    let output = timeout(
        Duration::from_secs(5),
        Command::new(&path)
            .arg(argument)
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| format!("{} version check timed out", path.display()))?
    .map_err(|error| format!("{} cannot run: {error}", path.display()))?;
    if !output.status.success() {
        return Err(format!(
            "{} version check failed ({})",
            path.display(),
            output.status
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let version = stdout
        .lines()
        .chain(stderr.lines())
        .find(|line| !line.trim().is_empty())
        .ok_or_else(|| format!("{} did not report a version", path.display()))?;
    Ok(format!("{}: {version}", path.display()))
}

fn keyring() -> Result<String, String> {
    let path = swarmy_config::Keyring::path().map_err(|e| e.to_string())?;
    // Fake-only stacks do not require encryption, but still report absence.
    match swarmy_config::Keyring::read(&path) {
        Ok(_) => Ok(format!("{} present (mode 600)", path.display())),
        Err(swarmy_config::Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(format!(
                "{} absent (required for encrypted credentials)",
                path.display()
            ))
        }
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

async fn s3_line(settings: &Settings) -> Check {
    let profile = settings.remote.profile.as_ref().and_then(|name| {
        swarmy_config::RemoteProfile::read(Path::new(&settings.state_dir), name).ok()
    });
    if let Some(bucket) = profile
        .as_ref()
        .filter(|profile| profile.s3_endpoint.is_empty())
        .and_then(|profile| profile.s3_bucket.as_deref())
    {
        // The client holds no object store credentials; the control plane
        // owns its bucket. Listing it from the laptop would need the same
        // AWS identity the API host already uses.
        return Check::warn(
            "remote S3",
            format!(
                "bucket {bucket}: object storage is verified on the API host, not from the client"
            ),
            "Check the API host's AWS credentials and region if image or volume operations fail.",
        );
    }
    let (result, fix) = s3_check(settings).await;
    let label = if settings.remote.profile.is_some() {
        "remote S3"
    } else {
        "dev stack S3"
    };
    Check::new(label, result, fix)
}

async fn s3_check(settings: &Settings) -> (Result<String, String>, &'static str) {
    let address = url::Url::parse(&settings.s3_endpoint).ok().and_then(|url| {
        url.host_str()
            .map(|host| format!("{host}:{}", url.port_or_known_default().unwrap_or(8333)))
    });
    let result = match address {
        Some(address) if connect(&address).await => Ok(format!("S3 reachable at {address}")),
        Some(address) => Err(format!("S3 unreachable at {address}")),
        None => Err("invalid S3 endpoint".into()),
    };
    (
        result,
        "Run swarmy dev up; check the configured endpoint if the stack is remote.",
    )
}

async fn connect(address: &str) -> bool {
    timeout(
        Duration::from_secs(2),
        tokio::net::TcpStream::connect(address),
    )
    .await
    .is_ok_and(|result| result.is_ok())
}

type Snapshot = swarmy_api_types::DoctorSnapshot;

async fn api_checks(
    checks: &mut Vec<Check>,
    loaded: &Loaded,
) -> Vec<crate::provider_report::ProviderRow> {
    let (client, endpoint) = match crate::api_client::connect() {
        Ok(result) => result,
        Err(error) => {
            checks.push(Check::new(
                "API",
                Err(format!("{error:#}")),
                "Run swarmy dev up or swarmy remote connect NAME; check the API URL and token.",
            ));
            return Vec::new();
        }
    };
    let health = crate::api_client::call(&endpoint, client.health()).await;
    let health = match health {
        Ok(health) => health,
        Err(error) => {
            checks.push(Check::new(
                "API",
                Err(format!("{error:#}")),
                "Run swarmy dev up or swarmy remote connect NAME; check the API URL and token.",
            ));
            return Vec::new();
        }
    };
    let version = health["version"].as_str().unwrap_or("unknown");
    let commit = health["git_commit"].as_str().unwrap_or("unknown");
    let api_version = health["api_version"].as_str().unwrap_or("");
    // Servers older than the documented contract carry no api_version; those
    // still need an exact binary match. Newer servers only need the same major
    // API version, so minor releases do not break existing clients.
    let compatible = if api_version.is_empty() {
        version == swarmy_version::VERSION && commit == swarmy_version::GIT_COMMIT
    } else {
        swarmy_api_types::same_major(api_version, swarmy_api_types::API_VERSION)
    };
    checks.push(Check::new(
        "API",
        if compatible {
            Ok(format!(
                "reachable at {endpoint}; api {api_version} version {version} ({commit})"
            ))
        } else {
            Err(format!(
                "api {api_version} version {version} ({commit}); CLI expects api {}",
                swarmy_api_types::API_VERSION
            ))
        },
        "Reinstall the CLI and API from checkouts with the same major API version.",
    ));
    let snapshot = crate::api_client::call(&endpoint, client.doctor())
        .await
        .map_err(|error| format!("{error:#}"));
    match snapshot {
        Ok(snapshot) => snapshot_checks(checks, snapshot, &loaded.settings),
        Err(error) => {
            checks.push(Check::new(
                "API diagnostics",
                Err(error),
                "Update the API to the CLI version and check the API token.",
            ));
            Vec::new()
        }
    }
}

fn gateway_providers(snapshot: &Snapshot) -> Vec<String> {
    snapshot
        .services
        .iter()
        .filter(|s| s.role == "gateway" && s.alive)
        .flat_map(|s| s.providers.iter().cloned())
        .collect()
}

fn service_checks(checks: &mut Vec<Check>, snapshot: &Snapshot) {
    for role in ["scheduler", "worker", "gateway"] {
        let live: Vec<_> = snapshot
            .services
            .iter()
            .filter(|s| s.role == role && s.alive)
            .collect();
        let result = if live.is_empty() {
            Err(format!("no live {role} heartbeat"))
        } else {
            Ok(format!(
                "{} alive: {}",
                live.len(),
                live.iter()
                    .map(|s| format!("{} ({})", s.instance_id, s.version))
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        };
        checks.push(Check::new(role, result, &format!("Restart swarmy-{role} and inspect its logs; check the API's service heartbeat store.")));
    }
    let mut seen = std::collections::HashSet::new();
    let nodes: Vec<_> = snapshot
        .services
        .iter()
        .rev()
        .filter(|s| s.role == "node" && s.alive && seen.insert(s.instance_id.as_str()))
        .collect();
    let slots: u32 = nodes
        .iter()
        .filter_map(|s| s.capacity.as_ref().map(|c| c.sandboxes))
        .sum();
    checks.push(if nodes.is_empty() {
        Check::warn(
            "nodes",
            "no live nodes; capacity is zero",
            "Start swarmyd on a sandbox node.",
        )
    } else {
        Check::new(
            "nodes",
            Ok(format!("{} alive; {} sandbox slots", nodes.len(), slots)),
            "",
        )
    });
    let images = snapshot.images.join(", ");
    checks.push(
        if snapshot.images.is_empty() || snapshot.default_image.is_none() {
            Check::warn(
                "images",
                format!(
                    "registered: {}; default: {}",
                    if images.is_empty() { "none" } else { &images },
                    snapshot.default_image.as_deref().unwrap_or("unset")
                ),
                "Build and register an image, then configure default_image.",
            )
        } else {
            Check::new(
                "images",
                Ok(format!(
                    "registered: {images}; default: {}",
                    snapshot.default_image.as_deref().unwrap_or_default()
                )),
                "",
            )
        },
    );
    let gateway_providers = gateway_providers(snapshot);
    checks.push(if gateway_providers.is_empty() {
        Check::warn(
            "gateway providers",
            "no providers advertised",
            "Configure a provider credential and restart the gateway.",
        )
    } else {
        Check::new("gateway providers", Ok(gateway_providers.join(", ")), "")
    });
}

fn snapshot_checks(
    checks: &mut Vec<Check>,
    snapshot: Snapshot,
    settings: &Settings,
) -> Vec<crate::provider_report::ProviderRow> {
    service_checks(checks, &snapshot);
    let mut rows = settings
        .catalog()
        .map(|catalog| crate::provider_report::local(&catalog, "absent"))
        .unwrap_or_default();
    let gateway_providers = gateway_providers(&snapshot);
    if let Some(credentials) = snapshot.credentials {
        for credential in credentials {
            let name = format!("credential {}", credential.provider);
            let status = format!("{:?}", credential.status).to_lowercase();
            checks.push(
                if credential.status == swarmy_api_types::CredentialStatus::Ready {
                    Check::new(&name, Ok(format!("present; {status}")), "")
                } else {
                    Check::warn(
                        &name,
                        format!("present; {status}"),
                        "Refresh the credential with swarmy auth login or auth set.",
                    )
                },
            );
            if let Some(row) = rows
                .iter_mut()
                .find(|row| row.provider == credential.provider)
            {
                row.credential = "store".into();
                row.store = "present".into();
                row.status = status;
            }
        }
    } else {
        checks.push(Check::warn(
            "credentials",
            "credential metadata unavailable",
            "Install the cluster keyring on the API host.",
        ));
    }
    for row in &mut rows {
        if row.credential != "store" && row.credential != "not required" {
            checks.push(Check::warn(
                &format!("credential {}", row.provider),
                "no stored credential (ambient credentials are not verified)",
                "Use swarmy auth set or swarmy auth login if this provider is needed.",
            ));
        }
        row.gateway = if gateway_providers.contains(&row.provider) {
            "served"
        } else {
            "unavailable"
        }
        .into();
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn core_services_are_required_and_missing_api_has_no_service_lines() {
        let mut checks = vec![Check::new("API", Err("unreachable".into()), "start API")];
        assert!(!report(std::mem::take(&mut checks), Vec::new(), true));
        let snapshot: Snapshot = serde_json::from_value(json!({
            "services": [{"role":"scheduler", "instance_id":"s1", "version":"0.1.0", "alive":false,
                "providers":[], "capacity":null}], "images":[], "default_image":null,
            "credentials":[]
        }))
        .unwrap();
        let settings = Settings::default();
        snapshot_checks(&mut checks, snapshot, &settings);
        assert!(
            checks
                .iter()
                .any(|check| check.name == "scheduler" && check.status == "fail")
        );
        assert!(
            checks
                .iter()
                .any(|check| check.name == "worker" && check.status == "fail")
        );
        assert!(
            checks
                .iter()
                .any(|check| check.name == "gateway" && check.status == "fail")
        );
    }
}
