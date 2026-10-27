#![deny(clippy::disallowed_methods)]
use std::{fs, process::Command};

fn run(dry_run: bool) {
    if !Command::new("python3")
        .args(["-c", "import tomllib"])
        .status()
        .expect("python available")
        .success()
    {
        eprintln!("skipping acceptance script: Python tomllib unavailable");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let bin = tmp.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let curl = bin.join("curl");
    std::os::unix::fs::symlink(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/acceptance-curl"
        ),
        &curl,
    )
    .unwrap();
    let script = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../scripts/acceptance-hetzner-cloud.sh"
    );
    let mut command = Command::new("bash");
    command
        .arg(script)
        .env_clear()
        .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
        .env("HOME", tmp.path().join("home"))
        .env("XDG_CONFIG_HOME", tmp.path().join("config"))
        .env("XDG_DATA_HOME", tmp.path().join("data"))
        .env("XDG_STATE_HOME", tmp.path().join("state"))
        .env("XDG_CACHE_HOME", tmp.path().join("cache"))
        .env("TMPDIR", tmp.path());
    if dry_run {
        command.arg("--dry-run");
    }
    let output = command
        .args(["--swarmy", env!("CARGO_BIN_EXE_swarmy")])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if dry_run {
        assert!(output.status.success(), "{stdout}\n{stderr}");
        for expected in [
            "dry-run plan for control",
            "dry-run plan for workers",
            "dry run complete",
        ] {
            assert!(stdout.contains(expected), "{stdout}\n{stderr}");
        }
    } else {
        assert!(!output.status.success());
        assert!(
            stderr.contains("swarmy cloud login hetzner-cloud"),
            "{stderr}"
        );
    }
    assert!(!tmp.path().join("curl-marker").exists());
    for path in ["home", "config", "data", "state"] {
        assert!(!tmp.path().join(path).exists(), "{path} changed");
    }
}

#[test]
fn dry_run_uses_only_fake_tofu() {
    run(true);
}

#[test]
fn real_run_without_credentials_stops_locally() {
    run(false);
}
