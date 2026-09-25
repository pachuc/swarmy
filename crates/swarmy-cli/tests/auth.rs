use serde_json::Value;
use std::{
    fs,
    process::{Command, Output},
};

static NETWORK: std::sync::OnceLock<foundationdb::api::NetworkAutoStop> =
    std::sync::OnceLock::new();

struct Fixture {
    dir: tempfile::TempDir,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    server: Option<std::thread::JoinHandle<()>>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
    }
}
impl Fixture {
    fn new() -> Option<Self> {
        let Ok(cluster) = std::env::var("SWARMY_FDB_CLUSTER_FILE") else {
            eprintln!("skipping auth integration: SWARMY_FDB_CLUSTER_FILE unset");
            return None;
        };
        let Ok(nats) = std::env::var("SWARMY_NATS_URL") else {
            return None;
        };
        let dir = tempfile::tempdir().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        fs::create_dir(dir.path().join(".swarmy")).unwrap();
        let settings = swarmy_config::Settings {
            fdb_cluster_file: cluster.clone(),
            api: swarmy_config::ApiSettings {
                url: Some(endpoint),
                token: "auth-test-token".into(),
                ..Default::default()
            },
            store_directory: format!("auth-test-{}", ulid::Ulid::generate()),
            credential_file: dir.path().join("auth.json").to_string_lossy().into_owned(),
            ..Default::default()
        };
        fs::write(
            dir.path().join(".swarmy/config.toml"),
            settings.to_toml().unwrap(),
        )
        .unwrap();
        let keyring =
            swarmy_config::Keyring::generate_at(&dir.path().join(".swarmy/keyring")).unwrap();
        fs::write(
            dir.path().join("auth.json"),
            include_bytes!("../../swarmy-llm/tests/fixtures/auth.json"),
        )
        .unwrap();
        NETWORK.get_or_init(swarmy_store::boot);
        let directory = settings.store_directory;
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let (ready, started) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async move {
                let store = swarmy_store::Store::open(
                    Some(&cluster),
                    Some(&[directory]),
                    std::sync::Arc::new(swarmy_store::blob::MemoryBlobStore::default()),
                )
                .await
                .unwrap();
                let bus = swarmy_bus::Bus::connect(&nats, swarmy_bus::Config::default())
                    .await
                    .unwrap();
                let mut state = swarmy_api::AppState::new(
                    store,
                    bus,
                    "auth-test-token".into(),
                    swarmy_llm::catalog::Catalog::get().clone(),
                );
                state.credential_keyring = Some(keyring);
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                ready.send(()).unwrap();
                axum::serve(listener, swarmy_api::router(state))
                    .with_graceful_shutdown(async {
                        let _ = stopped.await;
                    })
                    .await
                    .unwrap();
            });
        });
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        Some(Self {
            dir,
            shutdown: Some(shutdown),
            server: Some(server),
        })
    }
    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_swarmy"));
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("SWARMY_") {
                command.env_remove(name);
            }
        }
        command
            .current_dir(self.dir.path())
            .env("HOME", self.dir.path())
            .env("XDG_CONFIG_HOME", self.dir.path().join("config"))
            .args(args);
        command
    }
    fn run(&self, args: &[&str]) -> Output {
        self.command(args).output().unwrap()
    }
    fn success(&self, args: &[&str]) -> String {
        let result = self.run(args);
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let text = String::from_utf8(result.stdout).unwrap();
        assert!(!text.contains("sk-test"));
        text
    }
}

#[test]
fn set_list_check_remove_and_import() {
    let Some(f) = Fixture::new() else {
        return;
    };
    f.success(&[
        "auth",
        "set",
        "anthropic",
        "--api-key",
        "sk-test",
        "--extra",
        "region=us-east-1",
    ]);
    let rows = f.success(&["auth", "ls", "--json"]);
    let row: Value = serde_json::from_str(rows.trim()).unwrap();
    assert_eq!(row["provider"], "anthropic");
    assert_eq!(row["status"], "ready");
    assert_eq!(row["kind"], "api-key");
    f.success(&["auth", "check", "anthropic", "--json"]);
    f.success(&[
        "auth",
        "rm",
        "anthropic",
        row["label"].as_str().unwrap(),
        "--json",
    ]);
    assert!(f.success(&["auth", "ls", "--json"]).is_empty());
    let missing = f.run(&["auth", "check", "anthropic"]);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("does not exist"));
    let original = fs::read(f.dir.path().join("auth.json")).unwrap();
    f.success(&["auth", "import", "--json"]);
    let rows = f.success(&["auth", "ls", "--json"]);
    let row: Value = serde_json::from_str(rows.trim()).unwrap();
    assert_eq!(row["provider"], "chatgpt");
    assert_eq!(row["kind"], "subscription");
    assert_eq!(fs::read(f.dir.path().join("auth.json")).unwrap(), original);
    let check = f.run(&["auth", "check", "chatgpt", "--json"]);
    let row: Value = serde_json::from_slice(&check.stdout).unwrap();
    assert!(row["expires_in_seconds"].is_number());
}

#[test]
fn key_sources_are_exclusive_and_support_files_and_environment() {
    let Some(f) = Fixture::new() else {
        return;
    };
    assert!(!f.run(&["auth", "set", "openai"]).status.success());
    assert!(
        !f.run(&[
            "auth",
            "set",
            "openai",
            "--api-key",
            "sk-test",
            "--from-env"
        ])
        .status
        .success()
    );
    let result = f
        .command(&["auth", "set", "openai", "--from-env", "--json"])
        .env("OPENAI_API_KEY", "sk-test")
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    fs::write(f.dir.path().join("key"), "sk-test\n").unwrap();
    f.success(&["auth", "set", "anthropic", "--file", "key"]);
    assert_eq!(f.success(&["auth", "ls", "--json"]).lines().count(), 2);
}

#[test]
#[cfg(unix)]
fn azure_login_saves_to_cluster_and_missing_cli_reports_login_needed() {
    use std::os::unix::fs::PermissionsExt;
    let Some(f) = Fixture::new() else {
        return;
    };
    let bin = f.dir.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let az = bin.join("az");
    fs::write(&az, r#"#!/bin/sh
[ "$*" = 'account get-access-token --scope https://cognitiveservices.azure.com/.default --output json' ] || exit 1
printf '%s\n' '{"accessToken":"secret-azure-fixture","expiresOn":"2099-01-02T03:04:05Z"}'
"#).unwrap();
    fs::set_permissions(&az, fs::Permissions::from_mode(0o700)).unwrap();
    let original = fs::read(f.dir.path().join("auth.json")).unwrap();
    let result = f
        .command(&["auth", "login", "azure", "--resource", "fixture", "--json"])
        .env("PATH", &bin)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(!String::from_utf8_lossy(&result.stdout).contains("secret-azure-fixture"));
    let row: Value =
        serde_json::from_str(&f.success(&["auth", "check", "azure", "--json"])).unwrap();
    assert_eq!(row["kind"], "subscription");
    assert_eq!(row["status"], "ready");
    assert_eq!(fs::read(f.dir.path().join("auth.json")).unwrap(), original);
    fs::remove_file(az).unwrap();
    let result = f
        .command(&["auth", "login", "azure", "--resource", "fixture"])
        .env("PATH", &bin)
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("requires az on this host"));
}

#[test]
fn two_labels_under_one_provider_are_independent() {
    let Some(f) = Fixture::new() else { return };
    for label in ["primary", "backup"] {
        f.success(&[
            "auth",
            "set",
            "--provider",
            "openai",
            "--label",
            label,
            "--api-key",
            "sk-test",
        ]);
    }
    let rows = f.success(&["auth", "ls", "--json"]);
    let entries: Vec<Value> = rows
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(entries.len(), 2);
    assert!(
        entries
            .iter()
            .any(|row| row["provider"] == "openai" && row["label"] == "primary")
    );
    assert!(
        entries
            .iter()
            .any(|row| row["provider"] == "openai" && row["label"] == "backup")
    );
    f.success(&["auth", "rm", "openai", "primary"]);
    let remaining = f.success(&["auth", "ls", "--json"]);
    let row: Value = serde_json::from_str(remaining.trim()).unwrap();
    assert_eq!(row["label"], "backup");
}

#[test]
fn labelless_set_replaces_the_default_entry() {
    let Some(f) = Fixture::new() else { return };
    for _ in 0..2 {
        f.success(&["auth", "set", "openai", "--api-key", "sk-test"]);
    }
    let rows = f.success(&["auth", "ls", "--json"]);
    let entries: Vec<Value> = rows
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["label"], "default");
}
