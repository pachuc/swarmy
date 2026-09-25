//! Locate the sibling `swarmy` binary for end-to-end CLI tests.
//!
//! Integration test binaries live in `<target-dir>/<profile>/deps` while cargo
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
/// When `CARGO_BIN_EXE_swarmy` is present (for example a wrapper build or a
/// caller that prebuilt the binary) it wins; otherwise the profile directory
/// is derived from the test binary's own parent, so `CARGO_TARGET_DIR`,
/// `--target`, and custom `--profile` layouts resolve without assuming
/// `debug` or `release`.
///
/// # Panics
///
/// Panics when the test binary path has no parent directories, when the
/// build fails, or when the binary is missing and `SWARMY_SKIP_CLI_BUILD=1`.
#[must_use]
pub fn swarmy() -> PathBuf {
    BUILT
        .get_or_init(|| {
            if let Some(path) = std::env::var_os("CARGO_BIN_EXE_swarmy") {
                let path = PathBuf::from(path);
                assert!(path.exists(), "missing swarmy binary at {}", path.display());
                return path;
            }
            let current = std::env::current_exe().expect("integration test binary path");
            let profile = current
                .parent()
                .expect("deps directory")
                .parent()
                .expect("target profile directory");
            let binary = profile.join(format!("swarmy{}", std::env::consts::EXE_SUFFIX));
            if std::env::var_os("SWARMY_SKIP_CLI_BUILD").is_none() {
                build(&binary, profile);
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
/// warm target directory. The profile directory basename selects the cargo
/// profile (`debug` builds default, `release` passes `--release`, anything
/// else passes `--profile <name>`); `CARGO_TARGET_DIR` and `CARGO_BUILD_TARGET`
/// flow through the environment to the child cargo invocation.
fn build(binary: &std::path::Path, profile: &std::path::Path) {
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
    match profile
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("debug")
    {
        "debug" => {}
        "release" => {
            command.arg("--release");
        }
        name => {
            command.arg("--profile").arg(name);
        }
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
