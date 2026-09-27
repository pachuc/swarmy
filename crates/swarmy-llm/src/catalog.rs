//! Catalog types re-exported from `swarmy-catalog`.
//!
//! The data and the serde types live in `swarmy-catalog` so crates that only
//! validate model metadata do not compile the provider clients. This module
//! keeps the previous import paths working unchanged.
pub use swarmy_catalog::{
    Api, Catalog, Compat, Cost, CostTier, Limit, ModelInfo, ProviderInfo, ReasoningOptions,
};
