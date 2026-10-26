//! Generic verified release archive installation.
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    process::ExitStatus,
    time::Duration,
};
use tokio::{fs, io::AsyncWriteExt};

pub const MARKER: &str = ".sha256";
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error("{action} {}", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{} has no parent directory", path.display())]
    NoParent { path: PathBuf },
    #[error(
        "checksum mismatch for {asset}: SHA256SUMS lists {expected} but the download hashes to {actual}; refusing to unpack it (retry; if it persists the release asset is corrupt)"
    )]
    ChecksumMismatch {
        asset: String,
        expected: String,
        actual: String,
    },
    #[error("tar failed unpacking {asset}: {status}")]
    Unpack { asset: String, status: ExitStatus },
    #[error("invalid release member {member} in {asset}")]
    InvalidMember { asset: String, member: String },
    #[error("invalid URL {value}")]
    InvalidUrl {
        value: String,
        #[source]
        source: url::ParseError,
    },
    #[error("{value}: requires https or http on loopback")]
    InsecureUrl { value: String },
}
fn io(action: &'static str, path: &Path, source: std::io::Error) -> Error {
    Error::Io {
        action,
        path: path.to_owned(),
        source,
    }
}
/// Accept HTTPS anywhere, HTTP only on loopback.
/// # Errors
/// Rejects invalid and insecure URLs.
pub fn release_base_url(value: &str) -> Result<url::Url, Error> {
    let base =
        url::Url::parse(value.trim_end_matches('/')).map_err(|source| Error::InvalidUrl {
            value: value.into(),
            source,
        })?;
    let loopback = match base.host() {
        Some(url::Host::Domain("localhost")) => true,
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        _ => false,
    };
    if base.scheme() != "https" && !(base.scheme() == "http" && loopback) {
        return Err(Error::InsecureUrl {
            value: value.into(),
        });
    }
    Ok(base)
}
/// Find a lowercase SHA-256 digest for an asset.
#[must_use]
pub fn expected_digest(sums: &str, asset: &str) -> Option<String> {
    sums.lines().find_map(|line| {
        let (hex, name) = line.split_once(' ')?;
        (hex.len() == 64
            && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
            && (name.strip_prefix(' ') == Some(asset) || name.strip_prefix('*') == Some(asset)))
        .then(|| hex.to_ascii_lowercase())
    })
}
/// Read a complete cache entry's digest.
#[must_use]
pub fn verified_digest(dir: &Path, members: &[&str]) -> Option<String> {
    let digest = std::fs::read_to_string(dir.join(MARKER)).ok()?;
    members
        .iter()
        .all(|member| {
            std::fs::symlink_metadata(dir.join(member)).is_ok_and(|m| m.file_type().is_file())
        })
        .then(|| digest.trim().to_owned())
}
pub struct Install<'a> {
    pub url: url::Url,
    pub asset: &'a str,
    pub sha256: &'a str,
    pub top: Option<&'a str>,
    pub members: &'a [&'a str],
    pub destination: &'a Path,
}
pub struct Fetcher {
    client: reqwest::Client,
}
impl Fetcher {
    /// # Errors
    /// Returns HTTP client configuration errors.
    pub fn new() -> Result<Self, Error> {
        Ok(Self {
            client: reqwest::Client::builder()
                .user_agent(concat!("swarmy/", env!("CARGO_PKG_VERSION")))
                .connect_timeout(Duration::from_secs(30))
                .timeout(Duration::from_mins(15))
                .build()?,
        })
    }
    /// # Errors
    /// Returns request or body failures; 404 is not an error.
    pub async fn get_text(&self, url: url::Url) -> Result<Option<String>, Error> {
        let response = self.client.get(url).send().await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(response.error_for_status()?.text().await?))
    }
    /// Install a verified tarball atomically from a staging directory.
    /// # Errors
    /// Returns download, checksum, archive, or filesystem failures.
    pub async fn install_tarball(&self, install: Install<'_>) -> Result<(), Error> {
        let Install {
            url,
            asset,
            sha256,
            top,
            members,
            destination,
        } = install;
        let parent = destination.parent().ok_or_else(|| Error::NoParent {
            path: destination.to_owned(),
        })?;
        fs::create_dir_all(parent)
            .await
            .map_err(|e| io("creating", parent, e))?;
        let temp = tempfile::Builder::new()
            .prefix(".partial-")
            .tempdir_in(parent)
            .map_err(|e| io("staging", parent, e))?;
        let mut response = self.client.get(url).send().await?.error_for_status()?;
        let archive = temp.path().join(asset);
        let mut file = fs::File::create(&archive)
            .await
            .map_err(|e| io("creating", &archive, e))?;
        let mut hash = Sha256::new();
        while let Some(chunk) = response.chunk().await? {
            hash.update(&chunk);
            file.write_all(&chunk)
                .await
                .map_err(|e| io("writing", &archive, e))?;
        }
        file.flush()
            .await
            .map_err(|e| io("flushing", &archive, e))?;
        drop(file);
        let actual = format!("{:x}", hash.finalize());
        let expected = sha256.to_ascii_lowercase();
        if actual != expected {
            return Err(Error::ChecksumMismatch {
                asset: asset.into(),
                expected,
                actual,
            });
        }
        let extract = temp.path().join("extract");
        fs::create_dir(&extract)
            .await
            .map_err(|e| io("creating", &extract, e))?;
        let status = tokio::process::Command::new("tar")
            .arg("-xzf")
            .arg(&archive)
            .arg("-C")
            .arg(&extract)
            .status()
            .await
            .map_err(|e| io("running tar for", &archive, e))?;
        if !status.success() {
            return Err(Error::Unpack {
                asset: asset.into(),
                status,
            });
        }
        let unpacked = top.map_or_else(|| extract.clone(), |t| extract.join(t));
        for member in members {
            if !fs::symlink_metadata(unpacked.join(member))
                .await
                .is_ok_and(|m| m.file_type().is_file())
            {
                return Err(Error::InvalidMember {
                    asset: asset.into(),
                    member: (*member).into(),
                });
            }
        }
        let marker = unpacked.join(MARKER);
        fs::write(&marker, expected)
            .await
            .map_err(|e| io("writing", &marker, e))?;
        if destination.exists() {
            fs::remove_dir_all(destination)
                .await
                .map_err(|e| io("removing", destination, e))?;
        }
        fs::rename(&unpacked, destination)
            .await
            .map_err(|e| io("renaming", destination, e))?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn checksums() {
        let hex = "a".repeat(64);
        assert_eq!(
            expected_digest(&format!("{hex}  core"), "core"),
            Some(hex.clone())
        );
        assert_eq!(
            expected_digest(&format!("{hex} *core"), "core"),
            Some(hex.clone())
        );
        assert!(expected_digest("ab  core", "core").is_none());
        assert!(expected_digest(&format!("{hex}  other"), "core").is_none());
    }
    #[test]
    fn base_urls() {
        for url in [
            "https://example.com/release",
            "http://127.0.0.1:1234/",
            "http://[::1]:1234",
            "http://localhost:1234/",
        ] {
            assert!(release_base_url(url).is_ok());
        }
        for url in ["http://example.com", "file:///x"] {
            assert!(matches!(
                release_base_url(url),
                Err(Error::InsecureUrl { .. })
            ));
        }
        let error = release_base_url("not a url").unwrap_err();
        assert!(matches!(error, Error::InvalidUrl { .. }));
        assert!(std::error::Error::source(&error).is_some());
    }
    #[test]
    fn marker_needs_members() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(MARKER), "abc").unwrap();
        assert!(verified_digest(dir.path(), &["tofu"]).is_none());
        std::fs::write(dir.path().join("tofu"), b"binary").unwrap();
        assert_eq!(
            verified_digest(dir.path(), &["tofu"]).as_deref(),
            Some("abc")
        );
    }
}
