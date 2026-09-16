use std::{path::Path, process::Stdio, time::Duration};

use serde::Serialize;
use swarmy_config::{Loaded, Settings};
use swarmy_llm::auth::Credentials;
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
        let executable = std::env::current_exe()?.with_file_name(name);
        checks.push(Check::new(name, if executable.is_file() {
            Ok(executable.display().to_string())
        } else {
            Err(format!("{} is missing", executable.display()))
        }, "Run the cargo install commands in README.md to install the CLI and all three services together."));
    }
    if let Ok(loaded) = &loaded {
        if loaded.settings.provider == "chatgpt" {
            checks.push(credentials(&loaded.settings));
        }
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
    let ok = checks.iter().all(|check| check.ok);
    if json {
        println!("{}", serde_json::json!({"ok": ok, "checks": checks}));
    } else {
        for check in checks {
            if let Some(fix) = check.fix {
                println!("fix {}: {}. {fix}", check.name, check.detail);
            } else {
                println!("ok {}: {}", check.name, check.detail);
            }
        }
    }
    Ok(ok)
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

fn credentials(settings: &Settings) -> Check {
    let path = Path::new(&settings.credential_file);
    let result = std::fs::read(path)
        .map_err(|error| format!("{}: {error}", path.display()))
        .and_then(|bytes| {
            serde_json::from_slice(&bytes)
                .map_err(|_| format!("{} contains invalid JSON", path.display()))
        })
        .and_then(|value| {
            Credentials::from_json(value).map_err(|_| {
                format!(
                    "{} contains invalid ChatGPT OAuth credentials",
                    path.display()
                )
            })
        })
        .map(|_| format!("{} (valid ChatGPT credentials)", path.display()));
    Check::new(
        "credentials",
        result,
        "Run swarmy auth login to save dedicated ChatGPT credentials to the configured credential_file.",
    )
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
    let coordinators = std::fs::read_to_string(cluster).ok().and_then(|text| {
        text.lines()
            .find(|line| !line.trim_start().starts_with('#') && line.contains('@'))
            .and_then(|line| line.rsplit_once('@'))
            .map(|(_, addresses)| {
                addresses
                    .split(',')
                    .map(|address| address.trim().trim_end_matches(":tls").to_owned())
                    .collect::<Vec<_>>()
            })
    });
    let fdb = if let Some(addresses) = coordinators {
        let mut reachable = false;
        for address in addresses {
            reachable |= connect(&address).await;
        }
        if reachable {
            Ok("FoundationDB coordinator reachable".into())
        } else {
            Err("FoundationDB coordinators unreachable".into())
        }
    } else {
        Err(format!(
            "cannot read coordinators from {}",
            cluster.display()
        ))
    };
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
    for (name, endpoint, default_port) in [
        ("NATS", &settings.nats_url, 4222),
        ("S3", &settings.s3_endpoint, 8333),
    ] {
        let address = url::Url::parse(endpoint).ok().and_then(|url| {
            url.host_str().map(|host| {
                format!(
                    "{host}:{}",
                    url.port_or_known_default().unwrap_or(default_port)
                )
            })
        });
        let result = match address {
            Some(address) if connect(&address).await => {
                Ok(format!("{name} reachable at {address}"))
            }
            Some(address) => Err(format!("{name} unreachable at {address}")),
            None => Err(format!("invalid {name} endpoint")),
        };
        checks.push(Check::new(
            &format!("{label} {name}"),
            result,
            "Run swarmy dev up; check the configured endpoint if the stack is remote.",
        ));
    }
    checks
}

async fn connect(address: &str) -> bool {
    timeout(Duration::from_secs(2), TcpStream::connect(address))
        .await
        .is_ok_and(|result| result.is_ok())
}
