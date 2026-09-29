//! Locate sibling binaries for end-to-end tests.
//!
//! Integration test binaries live in `<target-dir>/<profile>/deps` while cargo
//! places built binaries directly under the profile directory. The binaries
//! come from a `cargo build --workspace` step that CI and the runbook run
//! before the test suite, so this helper never builds anything itself: it
//! resolves the path and panics with a rebuild hint when the binary is
//! missing.

use std::{path::PathBuf, sync::OnceLock};

fn profile_dir() -> PathBuf {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let current = std::env::current_exe().expect("integration test binary path");
        current
            .parent()
            .expect("deps directory")
            .parent()
            .expect("target profile directory")
            .to_owned()
    })
    .clone()
}

/// Resolve the sibling binary `name` built beside the test profile directory.
///
/// When `CARGO_BIN_EXE_swarmy` is present (for example a wrapper build or a
/// caller that prebuilt the binary) it wins for the `swarmy` binary;
/// otherwise the profile directory is derived from the test binary's own
/// parent, so `CARGO_TARGET_DIR`, `--target`, and custom `--profile` layouts
/// resolve without assuming `debug` or `release`.
///
/// # Panics
///
/// Panics when the test binary path has no parent directories or when the
/// sibling binary is missing because the workspace was not built first.
#[must_use]
pub(crate) fn bin(name: &str) -> PathBuf {
    if name == "swarmy"
        && let Some(path) = std::env::var_os("CARGO_BIN_EXE_swarmy")
    {
        let path = PathBuf::from(path);
        assert!(path.exists(), "missing swarmy binary at {}", path.display());
        return path;
    }
    let binary = profile_dir().join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    assert!(
        binary.exists(),
        "missing {name} binary at {}; run `cargo build --workspace --locked` first",
        binary.display()
    );
    binary
}
