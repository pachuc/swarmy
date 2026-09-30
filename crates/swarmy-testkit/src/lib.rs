//! Shared helpers for integration tests that need the dev stack.
//!
//! Before this crate, every suite re-implemented the same fixture: check the
//! stack environment, boot the `FoundationDB` client, open a store under a
//! unique directory prefix, connect a bus under a unique subject prefix, and
//! spawn services. The copies drifted (some cleaned up on panic, some did
//! not) and every poll loop hand-rolled its own sleep. This crate owns one
//! implementation: the stack gate, a cleanup guard that runs even when a
//! test panics, an [`eventually`] poll helper, a fake-provider [`Script`]
//! builder, sibling-binary lookup, and the metadata-only image fixture.
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
