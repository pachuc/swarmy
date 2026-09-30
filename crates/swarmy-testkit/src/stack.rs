//! Stack gate and cleanup guard for tests that need the dev stack.

use std::sync::OnceLock;

use foundationdb::{
    Database,
    directory::{Directory, DirectoryLayer},
};

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
}

/// Deletes a test's directory prefix and bus streams when it goes out of
/// scope, even when the test panics.
///
/// The explicit [`StackGuard::cleanup`] path runs first on success; the
/// `Drop` implementation repeats it on a detached thread so a panic between
/// setup and cleanup still releases the keys and streams. Both paths are
/// idempotent.
pub struct StackGuard {
    cluster: String,
    nats_url: String,
    prefixes: Vec<String>,
    cleaned: bool,
}

impl StackGuard {
    /// Guard one test's stack state. Registrations added with
    /// [`StackGuard::register`] are cleaned with the initial prefix.
    #[must_use]
    pub fn new(stack: &Stack) -> Self {
        Self {
            cluster: stack.cluster.clone(),
            nats_url: stack.nats_url.clone(),
            prefixes: vec![stack.prefix.clone()],
            cleaned: false,
        }
    }

    /// Register another prefix (for example a second bus subject namespace)
    /// for the same cleanup.
    pub fn register(&mut self, prefix: &str) {
        self.prefixes.push(prefix.to_owned());
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
        // The test's runtime is tearing down, so blocking it here could hang
        // the panic path. A detached thread with its own runtime finishes the
        // best-effort cleanup after the test result is recorded.
        std::thread::spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            runtime.block_on(cleanup(&cluster, &url, &prefixes));
        });
    }
}

async fn cleanup(cluster: &str, url: &str, prefixes: &[String]) {
    let Ok(database) = Database::new(Some(cluster)) else {
        return;
    };
    for prefix in prefixes {
        let path = vec![prefix.clone()];
        swarmy_core::ignore_best_effort(
            database
                .run(|trx, _| {
                    let path = &path;
                    async move {
                        DirectoryLayer::default()
                            .remove_if_exists(&trx, path)
                            .await?;
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
