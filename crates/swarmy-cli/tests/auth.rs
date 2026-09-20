use serde_json::Value;
use std::{
    fs,
    process::{Command, Output},
};

struct Fixture(tempfile::TempDir);
impl Fixture {
    fn new() -> Option<Self> {
        let Ok(cluster) = std::env::var("SWARMY_FDB_CLUSTER_FILE") else {
            eprintln!("skipping auth integration: SWARMY_FDB_CLUSTER_FILE unset");
            return None;
        };
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join(".swarmy")).unwrap();
        let settings = swarmy_config::Settings {
            fdb_cluster_file: cluster,
            store_directory: format!("auth-test-{}", ulid::Ulid::generate()),
            credential_file: dir.path().join("auth.json").to_string_lossy().into_owned(),
            ..Default::default()
        };
        fs::write(
            dir.path().join(".swarmy/config.toml"),
            settings.to_toml().unwrap(),
        )
        .unwrap();
        swarmy_config::Keyring::generate_at(&dir.path().join(".swarmy/keyring")).unwrap();
        fs::write(
            dir.path().join("auth.json"),
            include_bytes!("../../swarmy-llm/tests/fixtures/auth.json"),
        )
        .unwrap();
        Some(Self(dir))
    }
    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_swarmy"));
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("SWARMY_") {
                command.env_remove(name);
            }
        }
        command
            .current_dir(self.0.path())
            .env("HOME", self.0.path())
            .env("XDG_CONFIG_HOME", self.0.path().join("config"))
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
    assert_eq!(row["kind"], "api_key");
    f.success(&["auth", "check", "anthropic", "--json"]);
    f.success(&["auth", "rm", "anthropic", "--json"]);
    assert!(f.success(&["auth", "ls", "--json"]).is_empty());
    let missing = f.run(&["auth", "check", "anthropic"]);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("does not exist"));
    let original = fs::read(f.0.path().join("auth.json")).unwrap();
    f.success(&["auth", "import", "--json"]);
    let rows = f.success(&["auth", "ls", "--json"]);
    let row: Value = serde_json::from_str(rows.trim()).unwrap();
    assert_eq!(row["provider"], "chatgpt");
    assert_eq!(row["kind"], "oauth");
    assert_eq!(fs::read(f.0.path().join("auth.json")).unwrap(), original);
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
    fs::write(f.0.path().join("key"), "sk-test\n").unwrap();
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
    let bin = f.0.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let az = bin.join("az");
    fs::write(&az, r#"#!/bin/sh
[ "$*" = 'account get-access-token --scope https://cognitiveservices.azure.com/.default --output json' ] || exit 1
printf '%s\n' '{"accessToken":"secret-azure-fixture","expiresOn":"2099-01-02T03:04:05Z"}'
"#).unwrap();
    fs::set_permissions(&az, fs::Permissions::from_mode(0o700)).unwrap();
    let original = fs::read(f.0.path().join("auth.json")).unwrap();
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
    assert_eq!(row["kind"], "oauth");
    assert_eq!(row["status"], "ready");
    assert_eq!(fs::read(f.0.path().join("auth.json")).unwrap(), original);
    fs::remove_file(az).unwrap();
    let result = f
        .command(&["auth", "login", "azure", "--resource", "fixture"])
        .env("PATH", &bin)
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("requires az on this host"));
}
