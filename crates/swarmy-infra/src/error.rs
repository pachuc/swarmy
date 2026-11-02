use crate::{contract::OutputError, download};
use std::{path::PathBuf, process::ExitStatus};
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SWARMY_TOFU_BIN={} does not exist", .0.display())]
    TofuBinMissing(PathBuf),
    #[error("invalid OpenTofu release base URL")]
    BaseUrl(#[source] download::Error),
    #[error("downloading OpenTofu")]
    Download(#[from] download::Error),
    #[error("OpenTofu publishes no build for {os}/{arch}")]
    UnsupportedPlatform { os: String, arch: String },
    #[error("no OpenTofu release {version} at {url} (HTTP 404)")]
    ReleaseNotFound { version: String, url: String },
    #[error("{sums} has no entry for {asset}")]
    MissingChecksum { sums: String, asset: String },
    #[error(
        "{asset} is listed with SHA-256 {listed} but swarmy pins {pinned}; refusing to install it"
    )]
    PinMismatch {
        asset: String,
        listed: String,
        pinned: String,
    },
    #[error("cannot run {}", binary.display())]
    Spawn {
        binary: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{action} {}", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("module file path {0} must be relative and stay inside the working directory")]
    ModulePath(String),
    #[error(
        "credential {name} appears in the OpenTofu variables or module files; credentials are passed only through the environment"
    )]
    SecretInFiles { name: String },
    #[error("tofu {command} failed in {} ({status}): {stderr}", dir.display())]
    Failed {
        command: &'static str,
        dir: PathBuf,
        status: ExitStatus,
        stderr: String,
    },
    #[error("tofu output -json")]
    Output(#[from] OutputError),
    #[error("reading OpenTofu state {}", path.display())]
    State {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("serializing OpenTofu variables")]
    Variables(#[source] serde_json::Error),
}
pub type Result<T> = std::result::Result<T, Error>;
