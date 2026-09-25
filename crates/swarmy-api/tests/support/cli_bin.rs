//! Locate the sibling `swarmy` binary for end-to-end CLI tests.
//!
//! Integration test binaries live in `target/<profile>/deps` while cargo
//! places built binaries directly under the profile directory.
use std::path::PathBuf;

/// Resolve the `swarmy` binary built beside the test profile directory.
///
/// # Panics
///
/// Panics when the test binary path has no parent directories.
#[must_use]
pub fn swarmy() -> PathBuf {
    let current = std::env::current_exe().expect("integration test binary path");
    let profile = current
        .parent()
        .expect("deps directory")
        .parent()
        .expect("target profile directory");
    profile.join("swarmy")
}
