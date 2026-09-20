use std::{path::Path, process::Stdio, time::Duration};

use serde::Serialize;
use swarmy_config::{Loaded, Settings};
use tokio::{net::TcpStream, process::Command, time::timeout};

#[derive(Serialize)]
struct Check {
    name: String,
    ok: bool,
    detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    fix: Option<String>,
}

impl Check {
    fn new(name: &str, result: Result<String, String>, fix: &str) -> Self {
        let (ok, detail) = match result {
            Ok(detail) => (true, detail),
            Err(detail) => (false, detail),
        };
        Self {
            name: name.into(),
            ok,
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
    checks.push(Check::new("libfdb_c", client_library(),
        "Run scripts/install-dev-tools.sh and the printed cargo install command; keep libfdb_c.so (libfdb_c.dylib on macOS) in the directory selected by SWARMY_FDB_LIB_DIR at build time."));
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
        "swarmy-session",
        "swarmy-scheduler",
        "swarmy-worker",
        "swarmy-gateway",
    ] {
        // The companion is exec'd beside the CLI; services honor the caller's PATH.
        let executable = if name == "swarmy-session" {
            Ok(std::env::current_exe()?.with_file_name(name))
        } else {
            crate::dev::service_binary(name)
        };
        let result = match executable {
            Ok(path) => crate::dev::version_check(&path, name).await,
            Err(error) => Err(error),
        };
        checks.push(Check::new(
            name,
            result.map_err(|error| format!("{error:#}")),
            &format!("Reinstall from the CLI checkout: {}", crate::dev::REINSTALL),
        ));
    }

    if let Ok(loaded) = &loaded {
        checks.extend(remote_checks(loaded).await);
        checks.extend(stack(loaded).await);
    } else {
        for name in ["credentials", "dev stack"] {
            checks.push(Check::new(
                name,
                Err("cannot check without valid settings".into()),
                "Repair the configuration and run swarmy doctor again.",
            ));
        }
    }
    let providers = if let Ok(loaded) = &loaded {
        match gateway_providers(&loaded.settings).await {
            Ok(rows) => rows,
            Err(_) => crate::provider_report::local(&loaded.settings.catalog()?, "unavailable"),
        }
    } else {
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
            if let Some(fix) = check.fix {
                println!("fix {}: {}. {fix}", check.name, check.detail);
            } else {
                println!("ok {}: {}", check.name, check.detail);
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
        let result = if crate::remote::ssh::healthy(&profile).await {
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

// Loading the native client and calling its documented no-argument version API
// requires FFI. Keep the library alive until the borrowed function returns.
#[allow(unsafe_code)]
fn client_library() -> Result<String, String> {
    let name = if cfg!(target_os = "macos") {
        "libfdb_c.dylib"
    } else {
        "libfdb_c.so"
    };
    unsafe {
        let library = libloading::Library::new(name)
            .map_err(|error| format!("{name} is not loadable: {error}"))?;
        let version: libloading::Symbol<unsafe extern "C" fn() -> i32> = library
            .get(b"fdb_get_max_api_version\0")
            .map_err(|_| format!("{name} is missing fdb_get_max_api_version"))?;
        let version = version();
        if version < 730 {
            return Err(format!(
                "{name} supports API {version}; API 730 is required"
            ));
        }
        Ok(format!("{name} loadable; maximum API version {version}"))
    }
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

async fn stack(loaded: &Loaded) -> Vec<Check> {
    let settings = &loaded.settings;
    if let Some(name) = &settings.remote.profile
        && swarmy_config::RemoteProfile::read(Path::new(&settings.state_dir), name).is_err()
    {
        return vec![Check::new(
            "remote services",
            Err("cannot check without a tunnel profile".into()),
            &format!("Run swarmy remote connect {name}."),
        )];
    }
    let cluster = Path::new(&settings.fdb_cluster_file);
    if !loaded.root.join(".dev").exists() && !cluster.parent().is_some_and(Path::is_dir) {
        return vec![Check::new(
            "dev stack",
            Ok("not initialized; run swarmy dev up when ready".into()),
            "",
        )];
    }
    let fdb = database_transaction(settings).await;
    let label = if settings.remote.profile.is_some() {
        "remote"
    } else {
        "dev stack"
    };
    let mut checks = vec![Check::new(
        &format!("{label} FoundationDB"),
        fdb,
        "Run swarmy dev up; check fdb_cluster_file if the stack is remote.",
    )];
    checks.push(Check::new(
        &format!("{label} NATS"),
        nats_round_trip(&settings.nats_url).await,
        "Run swarmy dev up; check the configured NATS endpoint and tunnel if remote.",
    ));
    let address = url::Url::parse(&settings.s3_endpoint).ok().and_then(|url| {
        url.host_str()
            .map(|host| format!("{host}:{}", url.port_or_known_default().unwrap_or(8333)))
    });
    let result = match address {
        Some(address) if connect(&address).await => Ok(format!("S3 reachable at {address}")),
        Some(address) => Err(format!("S3 unreachable at {address}")),
        None => Err("invalid S3 endpoint".into()),
    };
    checks.push(Check::new(
        &format!("{label} S3"),
        result,
        "Run swarmy dev up; check the configured endpoint if the stack is remote.",
    ));
    checks
}

async fn connect(address: &str) -> bool {
    timeout(Duration::from_secs(2), TcpStream::connect(address))
        .await
        .is_ok_and(|result| result.is_ok())
}

// Keep the public CLI runnable when libfdb_c is missing, and bound native client retries.
async fn database_transaction(settings: &Settings) -> Result<String, String> {
    if let Some(name) = &settings.remote.profile {
        swarmy_config::RemoteProfile::read(Path::new(&settings.state_dir), name)
            .and_then(|profile| profile.validate_fdb_port())
            .map_err(|error| error.to_string())?;
    }
    let runtime = std::env::current_exe()
        .map_err(|error| error.to_string())?
        .with_file_name("swarmy-session");
    let output = timeout(
        Duration::from_secs(8),
        Command::new(runtime)
            .arg("doctor-fdb")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .status(),
    )
    .await
    .map_err(|_| "FoundationDB transaction timed out after 8s; a reachable coordinator or SSH tunnel does not prove database usability".to_owned())?
    .map_err(|error| format!("cannot start FoundationDB transaction probe: {error}"))?;
    if output.success() {
        Ok("FoundationDB session read transaction succeeded".into())
    } else {
        Err(format!(
            "FoundationDB transaction failed ({output}); check the cluster file, advertised address, and tunnel"
        ))
    }
}

async fn nats_round_trip(endpoint: &str) -> Result<String, String> {
    use futures_util::StreamExt;
    timeout(Duration::from_secs(5), async {
        let client = async_nats::connect(endpoint)
            .await
            .map_err(|_| "NATS connection failed")?;
        let inbox = client.new_inbox();
        let mut subscription = client
            .subscribe(inbox.clone())
            .await
            .map_err(|_| "NATS subscribe failed")?;
        client
            .flush()
            .await
            .map_err(|_| "NATS subscription flush failed")?;
        let payload = ulid::Ulid::generate().to_string();
        client
            .publish(inbox, payload.clone().into())
            .await
            .map_err(|_| "NATS publish failed")?;
        client
            .flush()
            .await
            .map_err(|_| "NATS publish flush failed")?;
        let message = subscription
            .next()
            .await
            .ok_or("NATS subscription closed")?;
        if message.payload.as_ref() != payload.as_bytes() {
            return Err("NATS round trip payload mismatch");
        }
        Ok("NATS publish/subscribe round trip succeeded".to_owned())
    })
    .await
    .map_err(|_| "NATS round trip timed out after 5s".to_owned())?
    .map_err(str::to_owned)
}

async fn gateway_providers(
    settings: &Settings,
) -> Result<Vec<crate::provider_report::ProviderRow>, String> {
    // Keep the front end usable when the native database client cannot load.
    let runtime = std::env::current_exe()
        .map_err(|error| error.to_string())?
        .with_file_name("swarmy-session");
    let output = timeout(
        Duration::from_secs(5),
        Command::new(runtime)
            .arg("doctor-providers")
            .envs(settings.environment())
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| "provider discovery timed out".to_owned())?
    .map_err(|error| format!("cannot start provider discovery: {error}"))?;
    if !output.status.success() {
        return Err(
            "provider discovery failed; check configuration and database connectivity".into(),
        );
    }
    serde_json::from_slice(&output.stdout).map_err(|_| "invalid provider report".into())
}
