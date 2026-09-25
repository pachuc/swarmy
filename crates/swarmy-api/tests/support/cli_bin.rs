//! Locate the sibling `swarmy` binary for end-to-end CLI tests.
//!
//! Integration test binaries live in `target/<profile>/deps` while cargo
//! places built binaries directly under the profile directory. A per-package
//! `cargo test -p swarmy-api` does not build the `swarmy` binary, so the
//! first call in each test process runs `cargo build -p swarmy-cli --bin
//! swarmy` with the test process's profile (a no-op on a warm target
//! directory). The build uses default features; when testing another feature
//! set, prebuild the binary and set `SWARMY_SKIP_CLI_BUILD=1` to resolve the
//! path without building (the call then panics when the binary is missing).
use std::{path::PathBuf, sync::OnceLock};

static BUILT: OnceLock<PathBuf> = OnceLock::new();

/// Resolve the `swarmy` binary built beside the test profile directory.
///
/// # Panics
///
/// Panics when the test binary path has no parent directories, when the
/// build fails, or when the binary is missing and `SWARMY_SKIP_CLI_BUILD=1`.
#[must_use]
pub fn swarmy() -> PathBuf {
    BUILT
        .get_or_init(|| {
            let current = std::env::current_exe().expect("integration test binary path");
            let profile = current
                .parent()
                .expect("deps directory")
                .parent()
                .expect("target profile directory");
            let binary = profile.join("swarmy");
            if std::env::var_os("SWARMY_SKIP_CLI_BUILD").is_none() {
                build(&binary);
            }
            assert!(
                binary.exists(),
                "missing swarmy binary at {}",
                binary.display()
            );
            binary
        })
        .clone()
}

/// Build `swarmy` in the workspace with the test process's profile. Cargo
/// no-ops when the binary is newer than the sources, so this is cheap on a
/// warm target directory.
fn build(binary: &std::path::Path) {
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .parent()
        .expect("workspace root")
        .to_owned();
    let mut command = std::process::Command::new("cargo");
    command
        .arg("build")
        .arg("-p")
        .arg("swarmy-cli")
        .arg("--bin")
        .arg("swarmy")
        .current_dir(&workspace);
    if !cfg!(debug_assertions) {
        command.arg("--release");
    }
    let status = command.status().expect("cargo build the swarmy binary");
    assert!(
        status.success(),
        "cargo build -p swarmy-cli failed: {status}"
    );
    assert!(
        binary.exists(),
        "build succeeded but {} is missing",
        binary.display()
    );
}
