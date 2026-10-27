//! Shared helpers for integration tests that need the dev stack.
//!
//! Each fixture the suites share has one implementation here: the stack gate
//! (`require_stack`) that skips a test locally and fails it under `CI` when a
//! stack setting is missing; a cleanup guard (`StackGuard`) that releases the
//! test's store keys and bus streams even when the test panics; the
//! [`eventually`] poll helper; the fake-provider `Script` builder (with the
//! `script` feature); sibling-binary lookup (`bin`) with a kill-on-drop
//! `ChildGuard`; the metadata-only `image` fixture; and the docs command
//! walker and a port-free fake backing stack (`FakeStack`).
//!
//! The `stack` feature (on by default) covers everything that touches
//! `FoundationDB`, the store, or the bus. Suites that only need the light
//! helpers disable default features so their test binaries do not link the
//! `FoundationDB` client. The `script` feature adds the fake-provider script
//! builder, which needs swarmy-llm's response types.

mod dev_stack;
mod docs;
mod eventually;
mod gate;
#[cfg(feature = "stack")]
mod image;
pub mod release;
#[cfg(feature = "script")]
mod script;
mod service;
mod settings;
#[cfg(feature = "stack")]
mod stack;
mod subscribers;

pub use dev_stack::FakeStack;
pub use docs::check_docs_commands;
pub use eventually::eventually;
pub use gate::{opt_in_env, optional_env, require_root_host, require_stack};
#[cfg(feature = "stack")]
pub use image::image;
#[cfg(feature = "script")]
pub use script::Script;
pub use service::{ChildGuard, bin};
pub use settings::{require_api_endpoint, stack_settings, test_settings};
#[cfg(feature = "stack")]
pub use stack::{Stack, StackGuard, boot_fdb, unique_prefix};
pub use subscribers::nats_subscribers;

/// Keep test commands away from the developer's own files. Pass these five
/// environment variables to `Command::envs` for every CLI invocation.
#[must_use]
pub fn isolated_home(root: &std::path::Path) -> [(&'static str, std::path::PathBuf); 5] {
    [
        ("HOME", root.to_path_buf()),
        ("XDG_CONFIG_HOME", root.join("config")),
        ("XDG_DATA_HOME", root.join("data")),
        ("XDG_STATE_HOME", root.join("state")),
        ("XDG_CACHE_HOME", root.join("cache")),
    ]
}
