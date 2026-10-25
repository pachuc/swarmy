//! Verified release assets shared by local and remote provisioning.
use crate::{Error, Result};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{fs, io::AsyncWriteExt};

pub const DEFAULT_BASE_URL: &str = "https://github.com/pachuc/swarmy/releases/download";
pub const BASE_URL_ENV: &str = "SWARMY_RELEASE_BASE_URL";
pub const SUMS: &str = "SHA256SUMS";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    X86_64Linux,
    Aarch64Linux,
}
impl Target {
    #[must_use]
    pub const fn triple(self) -> &'static str {
        match self {
            Self::X86_64Linux => "x86_64-unknown-linux-gnu",
            Self::Aarch64Linux => "aarch64-unknown-linux-gnu",
        }
    }
    /// # Errors
    /// Rejects unsupported target triples.
    pub fn parse(triple: &str) -> Result<Self> {
        match triple {
            "x86_64-unknown-linux-gnu" => Ok(Self::X86_64Linux),
            "aarch64-unknown-linux-gnu" => Ok(Self::Aarch64Linux),
            _ => Err(Error::other(format!(
                "unsupported swarmy-core target {triple}"
            ))),
        }
    }
    /// # Errors
    /// Rejects unsupported node architectures.
    pub fn from_uname(machine: &str) -> Result<Self> {
        match machine.trim() {
            "x86_64" => Ok(Self::X86_64Linux),
            "aarch64" | "arm64" => Ok(Self::Aarch64Linux),
            _ => Err(Error::other(format!(
                "unsupported node architecture {machine}"
            ))),
        }
    }
    /// # Errors
    /// Local core binaries require Linux and a supported architecture.
    pub fn host() -> Result<Self> {
        if !cfg!(target_os = "linux") {
            return Err(Error::other(
                "swarmy-core runs on Linux only; pass --target for a remote node",
            ));
        }
        Self::from_uname(std::env::consts::ARCH)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Component {
    Core,
    Client,
    BaseImage,
}
impl Component {
    #[must_use]
    pub const fn prefix(self) -> &'static str {
        match self {
            Self::Core => "swarmy-core",
            Self::Client => "swarmy",
            Self::BaseImage => "swarmy-image-base-ubuntu",
        }
    }
    #[must_use]
    pub const fn files(self) -> &'static [&'static str] {
        match self {
            Self::Core => &["swarmy-core", "libfdb_c.so"],
            Self::Client => &["swarmy"],
            Self::BaseImage => &["disk.ext4", "recipe.toml"],
        }
    }
    /// What to do when the release has no such asset.
    #[must_use]
    pub const fn missing_hint(self) -> &'static str {
        match self {
            Self::Core | Self::Client => {
                "a development client passes --core-bin PATH or sets SWARMY_CORE_BIN"
            }
            Self::BaseImage => {
                "releases publish base-ubuntu for x86_64-unknown-linux-gnu only; build one from a checkout with swarmy image build images/base-ubuntu"
            }
        }
    }
}
#[derive(Clone, Debug)]
pub struct Release {
    base: url::Url,
    tag: String,
}
impl Release {
    /// # Errors
    /// Rejects unsafe or invalid base URLs.
    pub fn new(base: &str, tag: &str) -> Result<Self> {
        let value = base;
        let base = url::Url::parse(value.trim_end_matches('/')).map_err(|error| {
            Error::context(error, format!("{BASE_URL_ENV}={value}: invalid URL"))
        })?;
        let loopback = match base.host() {
            Some(url::Host::Domain("localhost")) => true,
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            _ => false,
        };
        if base.scheme() != "https" && !(base.scheme() == "http" && loopback) {
            return Err(Error::other(format!(
                "{BASE_URL_ENV}={value}: requires https or http on loopback"
            )));
        }
        Ok(Self {
            base,
            tag: tag.to_owned(),
        })
    }
    /// # Errors
    /// Rejects an invalid environment override.
    pub fn for_client() -> Result<Option<Self>> {
        Self::select(
            std::env::var(BASE_URL_ENV).ok().as_deref(),
            swarmy_version::RELEASE_BUILD,
        )
    }
    /// # Errors
    /// Rejects an invalid base URL.
    pub fn select(base: Option<&str>, release_build: bool) -> Result<Option<Self>> {
        match base.filter(|value| !value.is_empty()) {
            Some(value) => Self::new(value, swarmy_version::RELEASE_TAG).map(Some),
            None if release_build => {
                Self::new(DEFAULT_BASE_URL, swarmy_version::RELEASE_TAG).map(Some)
            }
            None => Ok(None),
        }
    }
    #[must_use]
    pub fn tag(&self) -> &str {
        &self.tag
    }
    #[must_use]
    pub fn asset(component: Component, target: Target) -> String {
        format!("{}-{}.tar.gz", component.prefix(), target.triple())
    }
    #[must_use]
    pub fn unpacked(&self, cache: &Path, component: Component, target: Target) -> PathBuf {
        cache
            .join(&self.tag)
            .join(format!("{}-{}", component.prefix(), target.triple()))
    }
    #[must_use]
    pub fn cached(&self, cache: &Path, component: Component, target: Target) -> Option<PathBuf> {
        let dir = self.unpacked(cache, component, target);
        (dir.join(".sha256").is_file()
            && component
                .files()
                .iter()
                .all(|file| dir.join(file).is_file()))
        .then_some(dir)
    }
    fn url(&self, asset: &str) -> Result<url::Url> {
        Ok(url::Url::parse(&format!(
            "{}/{}/{asset}",
            self.base.as_str().trim_end_matches('/'),
            self.tag
        ))?)
    }
    fn digest(&self, sums: &str, asset: &str, component: Component) -> Result<String> {
        expected_digest(sums, asset).map_err(|_| {
            Error::other(format!(
                "SHA256SUMS for {} has no entry for {asset}; {}",
                self.tag,
                component.missing_hint()
            ))
        })
    }
    /// Fetch SHA256SUMS on every invocation, reusing a verified cache if offline.
    /// # Errors
    /// Returns an error for missing releases, invalid checksums, or failed downloads.
    pub async fn ensure(
        &self,
        cache: &Path,
        component: Component,
        target: Target,
    ) -> Result<PathBuf> {
        let client = reqwest::Client::builder()
            .user_agent(concat!("swarmy/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(30))
            .timeout(Duration::from_mins(15))
            .build()?;
        let asset = Self::asset(component, target);
        let sums_url = self.url(SUMS)?;
        let cached = self.cached(cache, component, target);
        let response = match client.get(sums_url.clone()).send().await {
            Ok(response) => response,
            Err(error) => {
                if let Some(dir) = cached {
                    cloud_err!(
                        "warning: cannot reach {sums_url}: {error}; using the verified copy at {}",
                        dir.display()
                    );
                    return Ok(dir);
                }
                return Err(error.into());
            }
        };
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(Error::other(format!(
                "no swarmy release {} at {} (HTTP 404 for SHA256SUMS); {}",
                self.tag,
                self.base,
                component.missing_hint()
            )));
        }
        let sums = response.error_for_status()?.text().await?;
        let expected = self.digest(&sums, &asset, component)?;
        if let Some(dir) = self.cached(cache, component, target)
            && fs::read_to_string(dir.join(".sha256")).await?.trim() == expected
        {
            return Ok(dir);
        }
        let parent = cache.join(&self.tag);
        fs::create_dir_all(&parent).await?;
        let temp = tempfile::Builder::new()
            .prefix(".partial-")
            .tempdir_in(&parent)?;
        let url = self.url(&asset)?;
        cloud_out!("Downloading {url}");
        let mut response = client.get(url).send().await?.error_for_status()?;
        let archive = temp.path().join(&asset);
        let mut file = fs::File::create(&archive).await?;
        let mut hash = Sha256::new();
        while let Some(chunk) = response.chunk().await? {
            hash.update(&chunk);
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        drop(file);
        let actual = format!("{:x}", hash.finalize());
        if actual != expected {
            return Err(Error::other(format!(
                "checksum mismatch for {asset}: SHA256SUMS lists {expected} but the download hashes to {actual}; refusing to unpack it (retry; if it persists the release asset is corrupt)"
            )));
        }
        let extract = temp.path().join("extract");
        fs::create_dir(&extract).await?;
        let status = tokio::process::Command::new("tar")
            .arg("-xzf")
            .arg(&archive)
            .arg("-C")
            .arg(&extract)
            .status()
            .await?;
        Error::ensure(
            status.success(),
            format!("tar failed unpacking {asset}: {status}"),
        )?;
        let unpacked = extract.join(format!("{}-{}", component.prefix(), target.triple()));
        for member in component.files() {
            Error::ensure(
                fs::symlink_metadata(unpacked.join(member))
                    .await?
                    .file_type()
                    .is_file(),
                format!("invalid release member {member}"),
            )?;
        }
        fs::write(unpacked.join(".sha256"), &expected).await?;
        let destination = self.unpacked(cache, component, target);
        if destination.exists() {
            fs::remove_dir_all(&destination).await?;
        }
        fs::rename(&unpacked, &destination).await?;
        cloud_out!(
            "Verified {asset} ({}) against SHA256SUMS: {}",
            self.tag,
            destination.display()
        );
        Ok(destination)
    }
}

pub(crate) fn expected_digest(sums: &str, asset: &str) -> Result<String> {
    for line in sums.lines() {
        if let Some((hex, name)) = line.split_once(' ')
            && (name.strip_prefix(' ') == Some(asset) || name.strip_prefix('*') == Some(asset))
            && hex.len() == 64
            && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Ok(hex.to_ascii_lowercase());
        }
    }
    Err(Error::other(format!(
        "no valid SHA256SUMS entry for {asset}"
    )))
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use swarmy_testkit::release::{fake_release, serve_files};

    #[test]
    fn base_image_component_names() {
        assert_eq!(
            Release::asset(Component::BaseImage, Target::X86_64Linux),
            "swarmy-image-base-ubuntu-x86_64-unknown-linux-gnu.tar.gz"
        );
        assert_eq!(Component::BaseImage.files(), &["disk.ext4", "recipe.toml"]);
    }
    #[tokio::test]
    async fn ensure_unpacks_a_sparse_base_image() {
        use std::os::unix::fs::{FileExt, MetadataExt};
        use swarmy_testkit::release::{FAKE_IMAGE_RECIPE, image_archive, sha256sums};
        let target = Target::X86_64Linux;
        let top = format!("{}-{}", Component::BaseImage.prefix(), target.triple());
        let asset = Release::asset(Component::BaseImage, target);
        let bytes = image_archive(
            &top,
            16 * 1024 * 1024,
            &[(0, &[1; 4096]), (8 * 1024 * 1024, &[2; 4096])],
            FAKE_IMAGE_RECIPE,
        );
        let sums = sha256sums(&[(&asset, &bytes)]);
        let address = serve_files(vec![
            (format!("/v1/{asset}"), bytes),
            ("/v1/SHA256SUMS".into(), sums.as_bytes().to_vec()),
        ]);
        let release = Release::new(&format!("http://{address}"), "v1").unwrap();
        let cache = tempfile::tempdir().unwrap();
        let dir = release
            .ensure(cache.path(), Component::BaseImage, target)
            .await
            .unwrap();
        let disk = std::fs::File::open(dir.join("disk.ext4")).unwrap();
        let metadata = disk.metadata().unwrap();
        assert_eq!(metadata.len(), 16 * 1024 * 1024);
        assert!(metadata.blocks() * 512 < metadata.len());
        let mut first = [0; 4096];
        disk.read_exact_at(&mut first, 0).unwrap();
        assert_eq!(first, [1; 4096]);
        disk.read_exact_at(&mut first, 8 * 1024 * 1024).unwrap();
        assert_eq!(first, [2; 4096]);
        assert!(dir.join("recipe.toml").is_file());
        let address = serve_files(vec![("/v1/SHA256SUMS".into(), sums.as_bytes().to_vec())]);
        let release = Release::new(&format!("http://{address}"), "v1").unwrap();
        let error = release
            .ensure(cache.path(), Component::BaseImage, Target::Aarch64Linux)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("x86_64-unknown-linux-gnu only"), "{error}");
    }
    #[test]
    fn checksums_and_targets() {
        let hex = "a".repeat(64);
        assert_eq!(
            expected_digest(&format!("{hex}  core"), "core").unwrap(),
            hex
        );
        assert_eq!(
            expected_digest(&format!("{hex} *core"), "core").unwrap(),
            hex
        );
        assert!(expected_digest("ab  core", "core").is_err());
        assert!(expected_digest(&format!("{hex}  other"), "core").is_err());
        for (machine, target) in [
            ("x86_64", Target::X86_64Linux),
            ("aarch64", Target::Aarch64Linux),
            ("arm64", Target::Aarch64Linux),
        ] {
            assert_eq!(Target::from_uname(machine).unwrap(), target);
            assert_eq!(Target::parse(target.triple()).unwrap(), target);
        }
        assert!(Target::from_uname("mips").is_err());
        assert!(Target::parse("unknown").is_err());
    }
    #[test]
    fn base_url_rules() {
        for url in [
            "https://example.com/release",
            "http://127.0.0.1:1234/",
            "http://[::1]:1234",
            "http://localhost:1234/",
        ] {
            assert!(Release::new(url, "v1").is_ok());
        }
        for url in ["http://example.com", "file:///x"] {
            let error = Release::new(url, "v1").unwrap_err();
            assert!(error.to_string().contains(&format!("{BASE_URL_ENV}={url}")));
        }
        let error = Release::new("not a url", "v1").unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("{BASE_URL_ENV}=not a url"))
        );
        assert!(std::error::Error::source(&error).is_some());
        assert_eq!(
            Release::new("http://localhost:1234/base/", "v1")
                .unwrap()
                .url("SHA256SUMS")
                .unwrap()
                .as_str(),
            "http://localhost:1234/base/v1/SHA256SUMS"
        );
        assert!(Release::select(None, false).unwrap().is_none());
        assert!(Release::select(None, true).unwrap().is_some());
        assert!(
            Release::select(Some("http://localhost"), false)
                .unwrap()
                .is_some()
        );
        assert!(Release::select(Some(""), false).unwrap().is_none());
    }
    #[tokio::test]
    async fn download_and_reuse() {
        let target = Target::host().unwrap();
        let files = fake_release("v1", target.triple(), "#!/bin/sh\n");
        let address = serve_files(files);
        let release = Release::new(&format!("http://{address}"), "v1").unwrap();
        let cache = tempfile::tempdir().unwrap();
        let dir = release
            .ensure(cache.path(), Component::Core, target)
            .await
            .unwrap();
        assert!(dir.join("swarmy-core").is_file());
        assert!(dir.join("libfdb_c.so").is_file());
        assert!(dir.join(".sha256").is_file());
        assert_eq!(
            dir,
            release
                .ensure(cache.path(), Component::Core, target)
                .await
                .unwrap()
        );
        let changed = fake_release("v1", target.triple(), "#!/bin/sh\necho changed\n");
        let address = serve_files(changed);
        let release = Release::new(&format!("http://{address}"), "v1").unwrap();
        release
            .ensure(cache.path(), Component::Core, target)
            .await
            .unwrap();
        assert!(
            std::fs::read_to_string(dir.join("swarmy-core"))
                .unwrap()
                .contains("changed")
        );
    }
    #[tokio::test]
    async fn corrupt_and_missing() {
        let target = Target::host().unwrap();
        let mut files = fake_release("v1", target.triple(), "#!/bin/sh\n");
        files[0].1 = b"corrupt".to_vec();
        let address = serve_files(files);
        let release = Release::new(&format!("http://{address}"), "v1").unwrap();
        let cache = tempfile::tempdir().unwrap();
        let error = release
            .ensure(cache.path(), Component::Core, target)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("checksum mismatch")
                && error.contains(&Release::asset(Component::Core, target)),
            "{error}"
        );
        assert!(
            !release
                .unpacked(cache.path(), Component::Core, target)
                .exists()
        );
        assert!(
            std::fs::read_dir(cache.path().join("v1"))
                .unwrap()
                .next()
                .is_none()
        );
        let address = serve_files(vec![]);
        let release = Release::new(&format!("http://{address}"), "v1").unwrap();
        assert!(
            release
                .ensure(cache.path(), Component::Core, target)
                .await
                .unwrap_err()
                .to_string()
                .contains("no swarmy release")
        );
    }
    #[tokio::test]
    async fn offline_uses_complete_cache() {
        let target = Target::host().unwrap();
        let address = serve_files(fake_release("v1", target.triple(), "#!/bin/sh\n"));
        let cache = tempfile::tempdir().unwrap();
        let release = Release::new(&format!("http://{address}"), "v1").unwrap();
        let dir = release
            .ensure(cache.path(), Component::Core, target)
            .await
            .unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let closed = listener.local_addr().unwrap();
        drop(listener);
        let offline = Release::new(&format!("http://{closed}"), "v1").unwrap();
        assert_eq!(
            offline
                .ensure(cache.path(), Component::Core, target)
                .await
                .unwrap(),
            dir
        );
    }
}
