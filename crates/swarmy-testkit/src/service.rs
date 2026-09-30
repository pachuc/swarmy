//! Sibling-binary lookup and child-process guard for service-spawning tests.

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
/// The binaries come from a `cargo build --workspace` step that CI and the
/// runbook run before the test suite, so this helper never builds anything
/// itself: it resolves the path and panics with a rebuild hint when the
/// binary is missing.
///
/// # Panics
///
/// Panics when the test binary path has no parent directories or when the
/// sibling binary is missing because the workspace was not built first.
#[must_use]
pub fn bin(name: &str) -> PathBuf {
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

/// A spawned service killed when the fixture drops, even on panic.
///
/// Suites kept `Vec<Child>` and killed each entry in an explicit cleanup
/// that a panic skipped. Holding this guard in the fixture kills the process
/// on both paths.
pub struct ChildGuard {
    child: Option<tokio::process::Child>,
}

impl ChildGuard {
    /// Guard an already-spawned service process.
    #[must_use]
    pub fn new(child: tokio::process::Child) -> Self {
        Self { child: Some(child) }
    }

    /// The guarded process id, when the platform reports one.
    #[must_use]
    pub fn id(&self) -> Option<u32> {
        self.child.as_ref().and_then(tokio::process::Child::id)
    }

    /// Stop the guarded service and reap it. Later drops are no-ops.
    /// Fixture cleanups call this explicitly so port conflicts fail loudly
    /// in order; the `Drop` backstop still kills whatever remains on panic.
    pub async fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            swarmy_core::ignore_best_effort(child.kill().await, "kill guarded service process");
            swarmy_core::ignore_best_effort(child.wait().await, "reap guarded service process");
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            // Dropping a running service must not hang the test harness, so
            // signal and forget: the OS reaps the process after exit.
            swarmy_core::ignore_best_effort(child.start_kill(), "kill guarded service process");
        }
    }
}
