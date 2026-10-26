use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

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

pub(crate) async fn run(json: bool) -> anyhow::Result<bool> {
    let loaded = Settings::load_base();
    let target = crate::target::resolve();
    let remote = matches!(target, Ok(swarmy_client::api_client::Api::Tunnel { .. }));
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
        "Run swarmy dev up to create one (.swarmy/config.toml in a checkout, else ~/.config/swarmy/config.toml); repair an existing file or its SWARMY_* overrides. See docs/DEV.md.",
    )];
    checks.push(Check::new("target", match &target {
        Ok(swarmy_client::api_client::Api::Direct { url, .. }) => Ok(format!("API {url}")),
        Ok(swarmy_client::api_client::Api::Tunnel { swarm, host, .. }) => Ok(format!("swarm {swarm} over SSH to {}", host.public_address)),
        Err(error) => Err(error.to_string()),
    }, "Run swarmy swarm ls to list swarms and swarmy swarm use NAME to choose one, or set SWARMY_API_URL and SWARMY_API_TOKEN."));
    checks.push(Check::new("keyring", keyring(), "Run swarmy dev up to create a keyring, or install the existing cluster key with chmod 600; set SWARMY_KEYRING for another path."));
    if !remote {
        for (name, argument) in [
            ("fdbserver", "--version"),
            ("fdbcli", "--version"),
            ("nats-server", "--version"),
            ("weed", "version"),
        ] {
            checks.push(Check::new(name, binary_version(name, argument).await,
            "Run swarmy dev install-tools (scripts/install-dev-tools.sh from a checkout). swarmy searches PATH, the install prefix's bin directory, and ~/.local/bin automatically; add another location to PATH if needed."));
        }
    }
    checks.push(core_binary_check(remote).await);

    let providers = if let Ok(loaded) = &loaded {
        checks.push(s3_line(&loaded.settings, remote).await);
        if let Some(path) = loaded.settings.selection.credential_file.to_str() {
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

async fn core_binary_check(remote: bool) -> Check {
    let source =
        crate::dev::CoreSource::select(std::env::var_os("SWARMY_CORE_BIN").map(PathBuf::from));
    let fix = source.as_ref().map_or_else(
        |_| format!("Reinstall from the CLI checkout: {}", crate::dev::REINSTALL),
        |source| source.fix().unwrap_or_else(|error| format!("{error:#}")),
    );
    let (path, not_downloaded): (anyhow::Result<PathBuf>, bool) = match source {
        Ok(crate::dev::CoreSource::Explicit(path)) => (Ok(path), false),
        Ok(crate::dev::CoreSource::Development) => {
            (crate::dev::service_binary("swarmy-core"), false)
        }
        Ok(crate::dev::CoreSource::Release(release)) => {
            match (
                crate::dev::core_cache(),
                swarmy_cloud::release::Target::host(),
            ) {
                (Ok(cache), Ok(target)) => {
                    match release.cached(&cache, swarmy_cloud::release::Component::Core, target) {
                        Some(dir) => (Ok(dir.join("swarmy-core")), false),
                        None => (Err(anyhow::anyhow!("not downloaded yet")), true),
                    }
                }
                (Err(error), _) => (Err(error), false),
                (_, Err(error)) => (Err(error.into()), false),
            }
        }
        Err(error) => (Err(error), false),
    };
    let result = match path {
        Ok(path) => crate::dev::version_check(&path, "swarmy-core").await,
        Err(error) => Err(error),
    }
    .map_err(|error| format!("{error:#}"));
    if not_downloaded {
        Check::warn(
            "swarmy-core",
            "not downloaded yet",
            "Run swarmy dev up or swarmy core ensure.",
        )
    } else {
        match (remote, result) {
            (true, Err(detail)) => Check::warn(
                "swarmy-core",
                format!("{detail}; service binary runs on the remote node"),
                &fix,
            ),
            (_, result) => Check::new("swarmy-core", result, &fix),
        }
    }
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

async fn s3_line(settings: &Settings, remote: bool) -> Check {
    if remote {
        return Check::warn(
            "remote S3",
            "object store runs on the swarm; verified there, not from the client",
            "Check the swarm's object storage if image or volume operations fail.",
        );
    }
    let (result, fix) = s3_check(settings).await;
    Check::new("dev stack S3", result, fix)
}

async fn s3_check(settings: &Settings) -> (Result<String, String>, &'static str) {
    let address = url::Url::parse(&settings.s3.endpoint).ok().and_then(|url| {
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
    let (client, endpoint) = match crate::target::connect().await {
        Ok(result) => result,
        Err(error) => {
            checks.push(Check::new(
                "API",
                Err(format!("{error:#}")),
                "Check the target with swarmy swarm show, or run swarmy dev up for a local stack; check the API URL and token.",
            ));
            return Vec::new();
        }
    };
    let health = swarmy_client::api_client::call(&endpoint, client.health()).await;
    let health = match health {
        Ok(health) => health,
        Err(error) => {
            checks.push(Check::new(
                "API",
                Err(format!("{error:#}")),
                "Check the target with swarmy swarm show, or run swarmy dev up for a local stack; check the API URL and token.",
            ));
            return Vec::new();
        }
    };
    let version = health.version.as_str();
    let commit = health.git_commit.as_str();
    let api_version = health.api_version.as_str();
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
    let core_fix = concat!(
        "Run swarmy dev up, or upgrade the swarm's nodes; ",
        "run and chat refuse an incompatible core unless --allow-version-mismatch."
    );
    checks.push(match crate::version_guard::core_mismatch(&health) {
        None => Check::new(
            "core version",
            Ok(format!("{} matches this client", swarmy_version::IDENTITY)),
            core_fix,
        ),
        Some(crate::version_guard::CoreMismatch::Commit(message)) => {
            Check::warn("core version", message, core_fix)
        }
        Some(crate::version_guard::CoreMismatch::Incompatible(message)) => {
            Check::new("core version", Err(message), core_fix)
        }
    });
    let snapshot = swarmy_client::api_client::call(&endpoint, client.doctor())
        .await
        .map_err(|error| format!("{error:#}"));
    match snapshot {
        Ok(snapshot) => {
            let providers = swarmy_client::api_client::call(&endpoint, client.providers())
                .await
                .ok();
            let routes = swarmy_client::api_client::call(&endpoint, client.routes())
                .await
                .ok();
            snapshot_checks(
                checks,
                snapshot,
                &loaded.settings,
                providers.as_deref(),
                routes.as_deref(),
            )
        }
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
    let mut providers: Vec<_> = snapshot
        .services
        .iter()
        .filter(|s| s.role == swarmy_api_types::ServiceRole::Gateway && s.alive)
        .flat_map(|s| s.providers.iter().cloned())
        .collect();
    providers.sort();
    providers.dedup();
    providers
}

fn service_checks(checks: &mut Vec<Check>, snapshot: &Snapshot) {
    for (role, kind) in [
        ("scheduler", swarmy_api_types::ServiceRole::Scheduler),
        ("worker", swarmy_api_types::ServiceRole::Worker),
        ("gateway", swarmy_api_types::ServiceRole::Gateway),
    ] {
        let live: Vec<_> = snapshot
            .services
            .iter()
            .filter(|s| s.role == kind && s.alive)
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
        .filter(|s| {
            s.role == swarmy_api_types::ServiceRole::Node
                && s.alive
                && seen.insert(s.instance_id.as_str())
        })
        .collect();
    let slots: u32 = nodes
        .iter()
        .filter_map(|s| s.capacity.as_ref().map(|c| c.sandboxes))
        .sum();
    checks.push(if nodes.is_empty() {
        Check::warn(
            "nodes",
            "no live nodes; capacity is zero",
            "Start `swarmy-core node` on a sandbox node.",
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
                "Run swarmy image pull base-ubuntu --set-default (release clients) or swarmy image build images/base-ubuntu from a checkout, then restart with swarmy dev up.",
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
}

fn snapshot_checks(
    checks: &mut Vec<Check>,
    snapshot: Snapshot,
    settings: &Settings,
    api_providers: Option<&[swarmy_api_types::Provider]>,
    routes: Option<&[swarmy_api_types::Route]>,
) -> Vec<crate::provider_report::ProviderRow> {
    service_checks(checks, &snapshot);
    let served = gateway_providers(&snapshot);
    let mut required = vec![settings.selection.provider.clone()];
    if let Some(routes) = routes {
        required.extend(
            routes
                .iter()
                .filter_map(|route| route.steps.first().map(|step| step.provider.clone())),
        );
    }
    required.sort();
    required.dedup();
    let missing: Vec<_> = required
        .iter()
        .filter(|provider| !served.contains(provider))
        .cloned()
        .collect();
    checks.push(Check::new(
        "gateway providers",
        if missing.is_empty() {
            Ok(served.join(", "))
        } else {
            let available = served.join(", ");
            let available = if available.is_empty() { "none" } else { &available };
            Err(format!("{available}; unserved: {}", missing.join(", ")))
        },
        "Inspect the gateway log for provider discovery errors (swarmy dev logs gateway: .swarmy/dev/logs in a project, ~/.local/state/swarmy/dev/logs otherwise; journalctl -u swarmy-gateway remotely); configure credentials and restart the gateway.",
    ));
    let mut rows = api_providers.map_or_else(
        || {
            settings
                .catalog()
                .map(|catalog| {
                    crate::provider_report::local(
                        catalog
                            .providers()
                            .map(|p| (p.id.clone(), p.env_keys.clone())),
                        "absent",
                    )
                })
                .unwrap_or_default()
        },
        |providers| {
            crate::provider_report::local(
                providers.iter().map(|p| {
                    let keys = crate::provider_report::credential_env_keys(p);
                    (p.id.clone(), keys)
                }),
                "absent",
            )
        },
    );
    if let Some(credentials) = snapshot.credentials {
        for credential in credentials {
            let name = format!("credential {}", credential.provider);
            let status = credential.status.as_str();
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
                status.clone_into(&mut row.status);
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
        row.gateway = if served.contains(&row.provider) {
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
    #![deny(clippy::disallowed_methods)]
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
        snapshot_checks(&mut checks, snapshot, &settings, None, None);
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
