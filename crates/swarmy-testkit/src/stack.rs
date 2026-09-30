//! Stack gate and cleanup guard for tests that need the dev stack.

use std::sync::{Arc, OnceLock};

use foundationdb::{
    Database,
    directory::{Directory, DirectoryLayer},
    tuple::Subspace,
};
use swarmy_store::blob::BlobStore;

/// Read a required dev-stack setting.
///
/// This is the one stack gate: a missing setting skips the test locally and
/// fails under `CI`, so suites never pass silently without the stack. Call
/// sites write `let Some(value) = require_stack("SWARMY_NATS_URL") else {
/// return; };`.
#[must_use = "check the returned option: missing stack settings skip the test locally"]
pub fn require_stack(name: &str) -> Option<String> {
    swarmy_core::test_support::stack_env(name)
}

/// Boot the `FoundationDB` client exactly once per test process.
pub fn boot_fdb() -> &'static foundationdb::api::NetworkAutoStop {
    static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();
    NETWORK.get_or_init(swarmy_store::boot)
}

/// A unique directory and subject prefix for one test.
#[must_use]
pub fn unique_prefix(tag: &str) -> String {
    format!("{tag}_{}", ulid::Ulid::generate())
}

/// The dev-stack connection settings shared by one test's fixture.
#[derive(Clone, Debug)]
pub struct Stack {
    /// Contents of `SWARMY_FDB_CLUSTER_FILE`.
    pub cluster: String,
    /// Contents of `SWARMY_NATS_URL`.
    pub nats_url: String,
    /// Unique prefix isolating this test's keys and subjects.
    pub prefix: String,
}

impl Stack {
    /// Read the stack settings, returning `None` to skip without the stack.
    /// Boots the `FoundationDB` client as a side effect.
    #[must_use = "check the returned option: missing stack settings skip the test locally"]
    pub fn load(tag: &str) -> Option<Self> {
        let cluster = require_stack("SWARMY_FDB_CLUSTER_FILE")?;
        let nats_url = require_stack("SWARMY_NATS_URL")?;
        boot_fdb();
        Some(Self {
            cluster,
            nats_url,
            prefix: unique_prefix(tag),
        })
    }

    /// Read the stack settings including object storage, for fixtures that
    /// persist blobs outside memory.
    #[must_use = "check the returned option: missing stack settings skip the test locally"]
    pub fn load_with_blobs(tag: &str) -> Option<Self> {
        require_stack("SWARMY_S3_ENDPOINT")?;
        Self::load(tag)
    }

    /// Open a store isolated under this test's prefix with a cleanup guard.
    /// Hold the guard in the fixture: dropping it removes the keys and
    /// streams even when the test panics.
    ///
    /// # Panics
    /// Panics when the store cannot open; fixture setup has no recovery.
    pub async fn open_store(&self, blobs: Arc<dyn BlobStore>) -> (swarmy_store::Store, StackGuard) {
        let store = swarmy_store::Store::open(
            Some(std::path::Path::new(&self.cluster)),
            Some(std::slice::from_ref(&self.prefix)),
            blobs,
        )
        .await
        .expect("fixture store must open");
        let guard = StackGuard::new(self);
        (store, guard)
    }
}

/// Deletes a test's directory prefix and bus streams when it goes out of
/// scope, even when the test panics.
///
/// The explicit [`StackGuard::cleanup`] path runs first on success; the
/// `Drop` implementation repeats it on a joined thread so a panic between
/// setup and cleanup still releases the keys and streams before the test
/// result is recorded. Both paths are idempotent and best effort: cleanup
/// must never fail a test, and panicking in `Drop` while unwinding would
/// abort the test process.
#[derive(Clone)]
pub struct StackGuard {
    cluster: String,
    nats_url: String,
    base: String,
    prefixes: Vec<String>,
    cleaned: bool,
}

impl StackGuard {
    /// Guard one test's stack state. The stack prefix is cleaned along with
    /// any prefixes added with [`StackGuard::register`].
    #[must_use]
    pub fn new(stack: &Stack) -> Self {
        Self {
            cluster: stack.cluster.clone(),
            nats_url: stack.nats_url.clone(),
            base: stack.prefix.clone(),
            prefixes: vec![stack.prefix.clone()],
            cleaned: false,
        }
    }

    /// Register another stream prefix for the same cleanup. The prefix must
    /// extend the guard's own base (`register` panics otherwise), so one
    /// test can only ever clean up its own keys and streams.
    ///
    /// # Panics
    /// Panics when `prefix` is outside the guard's base prefix; fixture
    /// setup has no recovery from a cross-test cleanup registration.
    pub fn register(&mut self, prefix: &str) {
        assert!(
            prefix == self.base || prefix.starts_with(&format!("{}_", self.base)),
            "test prefix {prefix} is outside guard base {}",
            self.base
        );
        if !self.prefixes.contains(&prefix.to_owned()) {
            self.prefixes.push(prefix.to_owned());
        }
    }

    /// Remove the guarded keys and streams. Runs once; the `Drop`
    /// implementation repeats it only when this was never called.
    pub async fn cleanup(&mut self) {
        self.cleaned = true;
        cleanup(&self.cluster, &self.nats_url, &self.prefixes).await;
    }
}

impl Drop for StackGuard {
    fn drop(&mut self) {
        if self.cleaned {
            return;
        }
        let cluster = self.cluster.clone();
        let url = self.nats_url.clone();
        let prefixes = self.prefixes.clone();
        // Join a worker thread so the cleanup runs to completion before the
        // test result is recorded. The worker owns its own runtime: the
        // panicking thread may be a runtime worker whose runtime is tearing
        // down, so blocking it on async work directly could hang.
        let worker = std::thread::spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            runtime.block_on(async {
                // Bound the backstop: a wedged cluster or bus must delay
                // the suite, never hang it.
                swarmy_core::ignore_best_effort(
                    tokio::time::timeout(
                        std::time::Duration::from_secs(60),
                        cleanup(&cluster, &url, &prefixes),
                    )
                    .await,
                    "bound panic-path test cleanup",
                );
            });
        });
        swarmy_core::ignore_best_effort(
            worker.join().map_err(|_| "cleanup thread panicked"),
            "join test cleanup thread",
        );
    }
}

async fn cleanup(cluster: &str, url: &str, prefixes: &[String]) {
    let Ok(database) = Database::new(Some(cluster)) else {
        return;
    };
    for prefix in prefixes {
        let path = vec![prefix.clone()];
        let subspace = Subspace::all().subspace(&(prefix.clone(),));
        swarmy_core::ignore_best_effort(
            database
                .run(|trx, _| {
                    let path = &path;
                    let subspace = &subspace;
                    async move {
                        DirectoryLayer::default()
                            .remove_if_exists(&trx, path)
                            .await?;
                        // Fixtures that probe raw keys isolate under a tuple
                        // subspace instead of a directory; clear it too.
                        let (begin, end) = subspace.range();
                        trx.clear_range(&begin, &end);
                        Ok(())
                    }
                })
                .await,
            "remove test directory prefix",
        );
    }
    let Ok(client) = async_nats::connect(url).await else {
        return;
    };
    let context = async_nats::jetstream::new(client);
    for prefix in prefixes {
        for stream in ["INFER_REQ", "SCHED_RUNNABLE", "TOOL_NODE"] {
            swarmy_core::ignore_best_effort(
                context.delete_stream(format!("{prefix}_{stream}")).await,
                "delete test bus stream",
            );
        }
    }
}
