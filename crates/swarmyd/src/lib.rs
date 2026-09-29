//! Local node control protocol. The Unix socket is accessible only to its owner.
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
    },
    Exec {
        sandbox: Sandbox,
        request: ExecRequest,
    },
    Pause(Sandbox),
    Resume(PauseHandle),
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

/// Failures in node hosting and volume operations: any cause, boxed.
///
/// Callers do not branch on node errors; the daemon entry point renders the
/// chain with anyhow. Message-only failures use [`other`]; failures keeping
/// a cause use [`context`], as anyhow's `.context()` did.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;

/// A site message paired with its cause. `Display` shows the message so logs
/// read the same with or without the chain; the chain carries the cause.
#[derive(Debug)]
struct WithCause {
    message: String,
    source: Error,
}

impl std::fmt::Display for WithCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for WithCause {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// An arbitrary failure with no structured cause to preserve.
pub fn other(message: impl Into<String>) -> Error {
    std::io::Error::other(message.into()).into()
}

/// A site message keeping its cause, as anyhow's `.context()` did.
pub fn context(source: impl Into<Error>, message: impl Into<String>) -> Error {
    Box::new(WithCause {
        message: message.into(),
        source: source.into(),
    })
}
