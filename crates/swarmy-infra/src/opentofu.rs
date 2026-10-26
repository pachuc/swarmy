//! Pinned `OpenTofu` release installation.
use crate::{
    Error, Result,
    download::{self, Fetcher, Install},
};
use std::path::{Path, PathBuf};
pub const VERSION: &str = "1.13.2";
pub const DEFAULT_BASE_URL: &str = "https://github.com/opentofu/opentofu/releases/download";
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Platform {
    LinuxAmd64,
    LinuxArm64,
    DarwinAmd64,
    DarwinArm64,
}
impl Platform {
    /// # Errors
    /// Rejects unsupported host combinations.
    pub fn from_os_arch(os: &str, arch: &str) -> Result<Self> {
        match (os, arch) {
            ("linux", "x86_64") => Ok(Self::LinuxAmd64),
            ("linux", "aarch64") => Ok(Self::LinuxArm64),
            ("macos", "x86_64") => Ok(Self::DarwinAmd64),
            ("macos", "aarch64") => Ok(Self::DarwinArm64),
            _ => Err(Error::UnsupportedPlatform {
                os: os.into(),
                arch: arch.into(),
            }),
        }
    }
    /// # Errors
    /// Rejects unsupported hosts.
    pub fn host() -> Result<Self> {
        Self::from_os_arch(std::env::consts::OS, std::env::consts::ARCH)
    }
    #[must_use]
    pub const fn suffix(self) -> &'static str {
        match self {
            Self::LinuxAmd64 => "linux_amd64",
            Self::LinuxArm64 => "linux_arm64",
            Self::DarwinAmd64 => "darwin_amd64",
            Self::DarwinArm64 => "darwin_arm64",
        }
    }
}
#[derive(Clone, Debug)]
pub struct Source {
    base: url::Url,
    version: String,
    pins: Vec<(Platform, String)>,
}
impl Source {
    #[must_use]
    /// # Panics
    /// The hard-coded release base URL is always valid.
    pub fn pinned() -> Self {
        Self::new(DEFAULT_BASE_URL, VERSION)
            .expect("constant release base URL is valid")
            .pin(
                Platform::LinuxAmd64,
                "ac05d9952662c7db28dd57e93f0a68018837bfdf9342384ec0987977c16a9645",
            )
            .pin(
                Platform::LinuxArm64,
                "88e2d9e3f94f97b76ab2faf55ade87578f56aed19c5e45fa139d70e994366d1f",
            )
            .pin(
                Platform::DarwinAmd64,
                "dc6e4eb1d3ed6550c2ef052596f933dbcd8c806957665ea83ae7537147cb3074",
            )
            .pin(
                Platform::DarwinArm64,
                "0d1cdc85f38992055b48e7fb161304a2ddfe2e5ee0ff68ffec9d0349c8bd1b59",
            )
    }
    /// # Errors
    /// Rejects unsafe or invalid release URLs.
    pub fn new(base: &str, version: &str) -> Result<Self> {
        Ok(Self {
            base: download::release_base_url(base).map_err(Error::BaseUrl)?,
            version: version.into(),
            pins: Vec::new(),
        })
    }
    #[must_use]
    pub fn pin(mut self, platform: Platform, sha256: &str) -> Self {
        self.pins.retain(|(p, _)| *p != platform);
        self.pins.push((platform, sha256.to_ascii_lowercase()));
        self
    }
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }
    #[must_use]
    pub fn asset(&self, platform: Platform) -> String {
        format!("tofu_{}_{}.tar.gz", self.version, platform.suffix())
    }
    #[must_use]
    pub fn sums(&self) -> String {
        format!("tofu_{}_SHA256SUMS", self.version)
    }
    fn url(&self, file: &str) -> Result<url::Url> {
        self.base
            .join(&format!(
                "{}/v{}/{file}",
                self.base.path().trim_end_matches('/'),
                self.version
            ))
            .map_err(|source| {
                Error::BaseUrl(download::Error::InvalidUrl {
                    value: file.into(),
                    source,
                })
            })
    }
}
#[must_use]
pub fn install_dir(cache: &Path, version: &str) -> PathBuf {
    cache.join("tofu").join(version)
}
#[must_use]
pub fn plugin_cache_dir(cache: &Path) -> PathBuf {
    cache.join("tofu/plugins")
}
/// # Errors
/// Rejects missing releases, pin mismatches and corrupt downloads.
pub async fn ensure(cache: &Path, source: &Source, platform: Platform) -> Result<PathBuf> {
    let dir = install_dir(cache, source.version());
    let binary = dir.join("tofu");
    let pin = source
        .pins
        .iter()
        .find(|(p, _)| *p == platform)
        .map(|(_, hash)| hash.as_str());
    if pin.is_some_and(|p| download::verified_digest(&dir, &["tofu"]).as_deref() == Some(p)) {
        return Ok(binary);
    }
    let fetcher = Fetcher::new()?;
    let asset = source.asset(platform);
    let sums_url = source.url(&source.sums())?;
    let sums = fetcher
        .get_text(sums_url.clone())
        .await?
        .ok_or_else(|| Error::ReleaseNotFound {
            version: source.version.clone(),
            url: sums_url.to_string(),
        })?;
    let expected =
        download::expected_digest(&sums, &asset).ok_or_else(|| Error::MissingChecksum {
            sums: sums_url.to_string(),
            asset: asset.clone(),
        })?;
    if let Some(pinned) = pin
        && pinned != expected
    {
        return Err(Error::PinMismatch {
            asset,
            listed: expected,
            pinned: pinned.into(),
        });
    }
    if download::verified_digest(&dir, &["tofu"]).as_deref() == Some(&expected) {
        return Ok(binary);
    }
    tracing::info!(
        version = source.version(),
        platform = platform.suffix(),
        "downloading OpenTofu"
    );
    fetcher
        .install_tarball(Install {
            url: source.url(&asset)?,
            asset: &asset,
            sha256: &expected,
            top: None,
            members: &["tofu"],
            destination: &dir,
        })
        .await?;
    Ok(binary)
}
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn platforms() {
        for (os, arch, platform) in [
            ("linux", "x86_64", Platform::LinuxAmd64),
            ("linux", "aarch64", Platform::LinuxArm64),
            ("macos", "x86_64", Platform::DarwinAmd64),
            ("macos", "aarch64", Platform::DarwinArm64),
        ] {
            assert_eq!(Platform::from_os_arch(os, arch).unwrap(), platform);
        }
        assert!(Platform::from_os_arch("windows", "x86_64").is_err());
    }
    #[test]
    fn pinned_urls() {
        let source = Source::pinned();
        assert_eq!(
            source
                .url(&source.asset(Platform::LinuxAmd64))
                .unwrap()
                .as_str(),
            "https://github.com/opentofu/opentofu/releases/download/v1.13.2/tofu_1.13.2_linux_amd64.tar.gz"
        );
        assert_eq!(
            source.url(&source.sums()).unwrap().as_str(),
            "https://github.com/opentofu/opentofu/releases/download/v1.13.2/tofu_1.13.2_SHA256SUMS"
        );
    }
}
