//! Local node control protocol. The Unix socket is accessible only to its owner.
mod api_metadata;
mod gpu;
mod heads;
mod hosting;
mod keeper;
mod memory;
pub mod role;
mod service;
mod tools;
mod upgrade;
mod vol;
mod vol_command;
mod vol_server;

use serde::{Deserialize, Serialize};
use swarmy_core::{
    BlockDevice, ExecOutput, ExecRequest, ExecResult, PauseHandle, RuntimeCaps, Sandbox,
    SandboxSpec,
};

/// One request per connection; exec emits output frames followed by one result.
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Create {
        spec: SandboxSpec,
        disk: BlockDevice,
        /// Caller-acquired writer lease for the disk, owned by this node.
        writer: swarmy_core::WriterOpening,
    },
    Exec {
        sandbox: Sandbox,
        request: ExecRequest,
    },
    Pause(Sandbox),
    Resume {
        handle: PauseHandle,
        writer: swarmy_core::WriterOpening,
    },
    Destroy(Sandbox),
    Checkpoint(Sandbox),
    Capabilities,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Sandbox(Sandbox),
    Paused(PauseHandle),
    Output(ExecOutput),
    Exited(ExecResult),
    Capabilities(RuntimeCaps),
    Destroyed,
    Checkpointed(swarmy_core::ManifestId),
    Error(String),
}
