//! Shared helpers for integration tests that need the dev stack.
//!
//! Each fixture the suites share has one implementation here: the stack gate
//! (`require_stack`) that skips a test locally and fails it under `CI` when a
//! stack setting is missing; a cleanup guard (`StackGuard`) that releases the
//! test's store keys and bus streams even when the test panics; the
//! [`eventually`] poll helper; the fake-provider [`Script`] builder;
//! sibling-binary lookup (`bin`) with a kill-on-drop `ChildGuard`; and the
//! metadata-only `image` fixture.
//!
//! The `stack` feature (on by default) covers everything that touches
//! `FoundationDB`, the store, or the bus. Suites that only need the light
//! helpers disable default features so their test binaries do not link the
//! `FoundationDB` client.

mod eventually;
#[cfg(feature = "stack")]
mod image;
mod script;
mod service;
mod settings;
#[cfg(feature = "stack")]
mod stack;

pub use eventually::eventually;
#[cfg(feature = "stack")]
pub use image::image;
pub use script::Script;
pub use service::{ChildGuard, bin};
pub use settings::{require_api_endpoint, stack_settings, test_settings};
#[cfg(feature = "stack")]
pub use stack::{Stack, StackGuard, boot_fdb, require_stack, unique_prefix};
