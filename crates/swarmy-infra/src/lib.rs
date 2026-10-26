//! Verified `OpenTofu` downloads and per-swarm execution. Cloud credentials reach
//! `OpenTofu` only through its process environment, never through module files.
pub mod contract;
pub mod download;
mod error;
pub mod modules;
pub mod opentofu;
mod runner;

pub use contract::{Bucket, Machine, ModuleOutputs, OutputError, parse_outputs};
pub use error::{Error, Result};
pub use runner::{CredentialEnv, ModuleFile, PlanOutcome, Tofu, VARIABLES_FILE, Workspace};
