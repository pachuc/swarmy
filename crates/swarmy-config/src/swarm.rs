//! Swarm configuration, durable records, and target selection.
mod record;
mod registry;
mod spec;
mod target;
pub use record::{RecordedHost, SwarmApi, SwarmRecord};
pub use registry::{Registry, Swarm};
pub use spec::{GroupSpec, HostSpec, Lifecycle, Module, Placement, SwarmSpec, validate_swarm_name};
pub use target::{Target, resolve};
