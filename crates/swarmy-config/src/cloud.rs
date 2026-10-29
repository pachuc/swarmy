//! Operator cloud keys live here because machines must be created before a swarm
//! exists. Unlike cluster credentials, these reach `OpenTofu` only through its
//! process environment.
use crate::{Error, create_private_dir, paths::Dirs, write_private};
use serde::{Deserialize, Serialize};
use std::{
    fmt, fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    str::FromStr,
};

pub const FILE: &str = "cloud-credentials.toml";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CloudProvider {
    HetznerCloud,
    HetznerS3,
    Aws,
    Scaleway,
}
impl CloudProvider {
    pub const ALL: [Self; 4] = [
        Self::HetznerCloud,
        Self::HetznerS3,
        Self::Aws,
        Self::Scaleway,
    ];
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::HetznerCloud => "hetzner-cloud",
            Self::HetznerS3 => "hetzner-s3",
            Self::Aws => "aws",
            Self::Scaleway => "scaleway",
        }
    }
    /// OpenTofu environment names in secret field order, followed by the optional field.
    #[must_use]
    pub const fn env_names(self) -> (&'static [&'static str], Option<&'static str>) {
        match self {
            Self::HetznerCloud => (&["HCLOUD_TOKEN"], None),
            Self::HetznerS3 => (&["MINIO_USER", "MINIO_PASSWORD"], None),
            Self::Aws => (
                &["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"],
                Some("AWS_REGION"),
            ),
            Self::Scaleway => (
                &["SCW_ACCESS_KEY", "SCW_SECRET_KEY", "SCW_DEFAULT_PROJECT_ID"],
                None,
            ),
        }
    }
    /// Secret fields read from stdin, in order.
    #[must_use]
    pub const fn secret_fields(self) -> &'static [&'static str] {
        match self {
            Self::HetznerCloud => &["token"],
            Self::HetznerS3 => &["access_key", "secret_key"],
            Self::Aws => &["access_key_id", "secret_access_key"],
            Self::Scaleway => &["access_key", "secret_key", "project_id"],
        }
    }
    /// The optional non-secret field.
    #[must_use]
    pub const fn option_field(self) -> Option<&'static str> {
        match self {
            Self::Aws => Some("region"),
            _ => None,
        }
    }
}
impl fmt::Display for CloudProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}
impl FromStr for CloudProvider {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        match s {
            "hetzner-cloud" => Ok(Self::HetznerCloud),
            "hetzner-s3" => Ok(Self::HetznerS3),
            "aws" => Ok(Self::Aws),
            "scaleway" => Ok(Self::Scaleway),
            _ => Err(Error::UnknownCloudProvider(s.into())),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HetznerCloud {
    pub token: String,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HetznerS3 {
    pub access_key: String,
    pub secret_key: String,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Aws {
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scaleway {
    pub access_key: String,
    pub secret_key: String,
    pub project_id: String,
}
impl fmt::Debug for Scaleway {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scaleway")
            .field("access_key", &"..redacted..")
            .field("secret_key", &"..redacted..")
            .field("project_id", &"..redacted..")
            .finish()
    }
}
impl fmt::Debug for HetznerCloud {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HetznerCloud")
            .field("token", &"..redacted..")
            .finish()
    }
}
impl fmt::Debug for HetznerS3 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HetznerS3")
            .field("access_key", &"..redacted..")
            .field("secret_key", &"..redacted..")
            .finish()
    }
}
impl fmt::Debug for Aws {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Aws")
            .field("access_key_id", &"..redacted..")
            .field("secret_access_key", &"..redacted..")
            .field("region", &self.region)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloudCredential {
    HetznerCloud(HetznerCloud),
    HetznerS3(HetznerS3),
    Aws(Aws),
    Scaleway(Scaleway),
}
impl CloudCredential {
    /// Build and validate, consuming secret values in provider field order.
    /// # Errors
    /// Returns `CloudCredential` for invalid values or count.
    /// # Panics
    /// The checked field count guarantees each expected secret is present.
    pub fn new(
        provider: CloudProvider,
        secrets: Vec<String>,
        option: Option<String>,
    ) -> Result<Self, Error> {
        if secrets.len() != provider.secret_fields().len() {
            return Err(Error::CloudCredential(format!(
                "{provider} expected {} secret fields, got {}",
                provider.secret_fields().len(),
                secrets.len()
            )));
        }
        if option.is_some() && provider.option_field().is_none() {
            return Err(Error::CloudCredential(format!(
                "{provider} does not accept an optional field"
            )));
        }
        let mut secrets = secrets.into_iter();
        let credential = match provider {
            CloudProvider::HetznerCloud => Self::HetznerCloud(HetznerCloud {
                token: secrets.next().expect("count checked"),
            }),
            CloudProvider::HetznerS3 => Self::HetznerS3(HetznerS3 {
                access_key: secrets.next().expect("count checked"),
                secret_key: secrets.next().expect("count checked"),
            }),
            CloudProvider::Aws => Self::Aws(Aws {
                access_key_id: secrets.next().expect("count checked"),
                secret_access_key: secrets.next().expect("count checked"),
                region: option,
            }),
            CloudProvider::Scaleway => Self::Scaleway(Scaleway {
                access_key: secrets.next().expect("count checked"),
                secret_key: secrets.next().expect("count checked"),
                project_id: secrets.next().expect("count checked"),
            }),
        };
        credential.validate()?;
        Ok(credential)
    }
    /// Build from environment lookups using the same validation as `new`.
    /// # Errors
    /// Reports the first missing variable by name, never its value.
    pub fn from_env(
        provider: CloudProvider,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, Error> {
        let (names, option_name) = provider.env_names();
        let secrets = names
            .iter()
            .map(|name| {
                lookup(name)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        Error::CloudCredential(format!(
                            "{provider} needs {name} in the environment"
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let option = option_name
            .and_then(lookup)
            .filter(|value| !value.is_empty());
        Self::new(provider, secrets, option)
    }
    #[must_use]
    pub const fn provider(&self) -> CloudProvider {
        match self {
            Self::HetznerCloud(_) => CloudProvider::HetznerCloud,
            Self::HetznerS3(_) => CloudProvider::HetznerS3,
            Self::Aws(_) => CloudProvider::Aws,
            Self::Scaleway(_) => CloudProvider::Scaleway,
        }
    }
    #[must_use]
    pub fn set_fields(&self) -> Vec<&'static str> {
        let mut fields = self.provider().secret_fields().to_vec();
        if let Self::Aws(a) = self
            && a.region.is_some()
        {
            fields.push("region");
        }
        fields
    }
    #[must_use]
    pub fn env(&self) -> ProviderEnv {
        let values: Vec<String> = match self {
            Self::HetznerCloud(c) => vec![c.token.clone()],
            Self::HetznerS3(c) => vec![c.access_key.clone(), c.secret_key.clone()],
            Self::Aws(c) => vec![c.access_key_id.clone(), c.secret_access_key.clone()],
            Self::Scaleway(c) => vec![
                c.access_key.clone(),
                c.secret_key.clone(),
                c.project_id.clone(),
            ],
        };
        let (names, optional) = self.provider().env_names();
        let mut vars: Vec<_> = names.iter().copied().zip(values).collect();
        if let (Some(name), Self::Aws(c)) = (optional, self)
            && let Some(value) = &c.region
        {
            vars.push((name, value.clone()));
        }
        ProviderEnv { vars }
    }
    fn validate(&self) -> Result<(), Error> {
        let check = |provider: CloudProvider, field: &str, value: &str| -> Result<(), Error> {
            if value.is_empty()
                || value.chars().any(char::is_whitespace)
                || value.chars().any(char::is_control)
            {
                return Err(Error::CloudCredential(format!(
                    "{provider} {field} must be non-empty with no spaces or control characters"
                )));
            }
            Ok(())
        };
        match self {
            Self::HetznerCloud(c) => check(self.provider(), "token", &c.token)?,
            Self::HetznerS3(c) => {
                check(self.provider(), "access_key", &c.access_key)?;
                check(self.provider(), "secret_key", &c.secret_key)?;
            }
            Self::Scaleway(c) => {
                check(self.provider(), "access_key", &c.access_key)?;
                check(self.provider(), "secret_key", &c.secret_key)?;
                check(self.provider(), "project_id", &c.project_id)?;
                if c.project_id.len() != 36
                    || !c.project_id.bytes().enumerate().all(|(i, b)| {
                        if [8, 13, 18, 23].contains(&i) {
                            b == b'-'
                        } else {
                            b.is_ascii_hexdigit()
                        }
                    })
                {
                    return Err(Error::CloudCredential("scaleway project_id must be a UUID like 11111111-2222-3333-4444-555555555555".into()));
                }
            }
            Self::Aws(c) => {
                check(self.provider(), "access_key_id", &c.access_key_id)?;
                check(self.provider(), "secret_access_key", &c.secret_access_key)?;
                if let Some(r) = &c.region
                    && (r.is_empty()
                        || r.len() > 64
                        || !r
                            .bytes()
                            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'))
                {
                    return Err(Error::CloudCredential("aws region must be lowercase letters, digits, and hyphens, like eu-central-1".into()));
                }
            }
        }
        Ok(())
    }
}
/// Environment for one `OpenTofu` run. Debug exposes names, never values.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ProviderEnv {
    vars: Vec<(&'static str, String)>,
}
impl ProviderEnv {
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, &str)> {
        self.vars.iter().map(|(n, v)| (*n, v.as_str()))
    }
    pub fn names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.vars.iter().map(|(n, _)| *n)
    }
}
impl fmt::Debug for ProviderEnv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.names()).finish()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudCredentials {
    #[serde(
        rename = "hetzner-cloud",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    hetzner_cloud: Option<HetznerCloud>,
    #[serde(
        rename = "hetzner-s3",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    hetzner_s3: Option<HetznerS3>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    aws: Option<Aws>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scaleway: Option<Scaleway>,
}
impl CloudCredentials {
    #[must_use]
    pub fn path(dirs: &Dirs) -> PathBuf {
        dirs.data.join(FILE)
    }
    /// A missing file is empty; a file readable by others is rejected.
    /// # Errors
    /// Returns `File` for read/parse/validation failures and `CloudCredential` for loose mode.
    /// Parse errors report a line number but never show the source line, which may contain a secret.
    pub fn load(path: &Path) -> Result<Self, Error> {
        let metadata = match fs::metadata(path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => return Err(file_error(path, e.into())),
        };
        let mode = metadata.permissions().mode();
        if mode & 0o077 != 0 {
            return Err(Error::CloudCredential(format!(
                "{} is readable by other users (mode {:o}); run chmod 600 on it",
                path.display(),
                mode & 0o777
            )));
        }
        let content = fs::read_to_string(path).map_err(|e| file_error(path, e.into()))?;
        let store: Self = toml::from_str(&content).map_err(|e: toml::de::Error| {
            // TOML diagnostics can include the source line, including secret values.
            let start = e.span().map_or(0, |span| span.start.min(content.len()));
            let line = content.as_bytes()[..start]
                .split(|&byte| byte == b'\n')
                .count();
            file_error(
                path,
                Error::CloudCredential(format!(
                    "not valid cloud credentials TOML near line {line} (contents not shown; they may be secrets)"
                )),
            )
        })?;
        for provider in CloudProvider::ALL {
            if let Some(c) = store.get(provider) {
                c.validate().map_err(|e| file_error(path, e))?;
            }
        }
        Ok(store)
    }
    /// Write atomically as a private file, creating the private parent first.
    /// # Errors
    /// Returns encode or filesystem errors.
    pub fn save(&self, path: &Path) -> Result<(), Error> {
        create_private_dir(path.parent().ok_or_else(|| {
            Error::CloudCredential("cloud credentials path has no parent".into())
        })?)?;
        write_private(path, toml::to_string(self)?.as_bytes())
    }
    #[must_use]
    pub fn get(&self, provider: CloudProvider) -> Option<CloudCredential> {
        match provider {
            CloudProvider::HetznerCloud => self
                .hetzner_cloud
                .clone()
                .map(CloudCredential::HetznerCloud),
            CloudProvider::HetznerS3 => self.hetzner_s3.clone().map(CloudCredential::HetznerS3),
            CloudProvider::Aws => self.aws.clone().map(CloudCredential::Aws),
            CloudProvider::Scaleway => self.scaleway.clone().map(CloudCredential::Scaleway),
        }
    }
    pub fn set(&mut self, credential: CloudCredential) {
        match credential {
            CloudCredential::HetznerCloud(c) => self.hetzner_cloud = Some(c),
            CloudCredential::HetznerS3(c) => self.hetzner_s3 = Some(c),
            CloudCredential::Aws(c) => self.aws = Some(c),
            CloudCredential::Scaleway(c) => self.scaleway = Some(c),
        }
    }
    /// Returns whether an entry was removed.
    pub fn remove(&mut self, provider: CloudProvider) -> bool {
        match provider {
            CloudProvider::HetznerCloud => self.hetzner_cloud.take().is_some(),
            CloudProvider::HetznerS3 => self.hetzner_s3.take().is_some(),
            CloudProvider::Aws => self.aws.take().is_some(),
            CloudProvider::Scaleway => self.scaleway.take().is_some(),
        }
    }
    /// # Errors
    /// Returns `CloudNotLoggedIn` if no credential exists.
    pub fn env(&self, provider: CloudProvider) -> Result<ProviderEnv, Error> {
        self.get(provider)
            .map(|c| c.env())
            .ok_or(Error::CloudNotLoggedIn(provider.name()))
    }
}
fn file_error(path: &Path, source: Error) -> Error {
    Error::File {
        path: path.to_path_buf(),
        source: Box::new(source),
    }
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    const TOKEN: &str = "hcloud-test-token-5f2b9c";
    const S3_ACCESS: &str = "s3-access-7c1d";
    const S3_SECRET: &str = "s3-secret-9e4a";
    const AWS_ACCESS: &str = "aws-akid-3b8f";
    const AWS_SECRET: &str = "aws-secret-6d2e";
    const SCW_ACCESS: &str = "SCWEXAMPLEACCESS0001";
    const SCW_SECRET: &str = "scw-secret-4f1a";
    const SCW_PROJECT: &str = "11111111-2222-3333-4444-555555555555";
    fn entry(p: CloudProvider, secrets: &[&str], option: Option<&str>) -> CloudCredential {
        CloudCredential::new(
            p,
            secrets.iter().map(|s| (*s).into()).collect(),
            option.map(str::to_owned),
        )
        .unwrap()
    }
    #[test]
    fn environment_credential_round_trip() {
        for (provider, values) in [
            (CloudProvider::HetznerCloud, vec![("HCLOUD_TOKEN", TOKEN)]),
            (
                CloudProvider::Aws,
                vec![
                    ("AWS_ACCESS_KEY_ID", AWS_ACCESS),
                    ("AWS_SECRET_ACCESS_KEY", AWS_SECRET),
                ],
            ),
            (
                CloudProvider::Aws,
                vec![
                    ("AWS_ACCESS_KEY_ID", AWS_ACCESS),
                    ("AWS_SECRET_ACCESS_KEY", AWS_SECRET),
                    ("AWS_REGION", "eu-central-1"),
                ],
            ),
        ] {
            let credential = CloudCredential::from_env(provider, |name| {
                values
                    .iter()
                    .find(|(key, _)| *key == name)
                    .map(|(_, value)| (*value).into())
            })
            .unwrap();
            assert_eq!(credential.env().iter().collect::<Vec<_>>(), values);
        }
        let error = CloudCredential::from_env(CloudProvider::HetznerCloud, |_| None)
            .unwrap_err()
            .to_string();
        assert!(error.contains("HCLOUD_TOKEN"));
        assert!(!error.contains(TOKEN));
    }
    #[test]
    fn round_trip_and_modes() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("data/swarmy/cloud-credentials.toml");
        let mut store = CloudCredentials::default();
        store.set(entry(CloudProvider::HetznerCloud, &[TOKEN], None));
        store.set(entry(
            CloudProvider::HetznerS3,
            &[S3_ACCESS, S3_SECRET],
            None,
        ));
        store.set(entry(
            CloudProvider::Aws,
            &[AWS_ACCESS, AWS_SECRET],
            Some("eu-central-1"),
        ));
        store.set(entry(
            CloudProvider::Scaleway,
            &[SCW_ACCESS, SCW_SECRET, SCW_PROJECT],
            None,
        ));
        store.save(&path).unwrap();
        assert_eq!(CloudCredentials::load(&path).unwrap(), store);
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(path.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(
            CloudCredentials::load(&path)
                .unwrap_err()
                .to_string()
                .contains("chmod 600")
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        assert!(
            CloudCredentials::load(&path)
                .unwrap_err()
                .to_string()
                .contains("chmod 600")
        );
        store.save(&path).unwrap();
        assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 1);
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    #[test]
    fn missing_and_bad_contents() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("missing/store");
        assert_eq!(
            CloudCredentials::load(&path).unwrap(),
            CloudCredentials::default()
        );
        assert!(!path.parent().unwrap().exists());
        for contents in [
            "[digitalocean]\nkey = 'x'\n[hetzner-cloud]\ntoken = 'hcloud-test-token-5f2b9c'",
            "[aws]\nregion2 = 'x'\n[hetzner-cloud]\ntoken = 'hcloud-test-token-5f2b9c'",
            "[hetzner-cloud]\ntoken = ''",
            "[hetzner-cloud]\ntokn = 'hcloud-test-token-5f2b9c'",
            "[hetzner-cloud]\ntoken = hcloud-test-token-5f2b9c",
            "hetzner-cloud = 'hcloud-test-token-5f2b9c'",
        ] {
            let path = tmp.path().join("bad");
            fs::write(&path, contents).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            let error = CloudCredentials::load(&path).unwrap_err();
            let mut cause: Option<&dyn std::error::Error> = Some(&error);
            while let Some(current) = cause {
                assert!(!current.to_string().contains(TOKEN));
                cause = current.source();
            }
            match error {
                Error::File { source, .. } => {
                    assert!(matches!(*source, Error::CloudCredential(_)));
                }
                e => panic!("unexpected error: {e}"),
            }
        }
    }
    #[test]
    fn mapping_and_redaction() {
        let hc = entry(CloudProvider::HetznerCloud, &[TOKEN], None);
        let s3 = entry(CloudProvider::HetznerS3, &[S3_ACCESS, S3_SECRET], None);
        let scw = entry(
            CloudProvider::Scaleway,
            &[SCW_ACCESS, SCW_SECRET, SCW_PROJECT],
            None,
        );
        let aws = entry(
            CloudProvider::Aws,
            &[AWS_ACCESS, AWS_SECRET],
            Some("eu-central-1"),
        );
        assert_eq!(
            hc.env().iter().collect::<Vec<_>>(),
            vec![("HCLOUD_TOKEN", TOKEN)]
        );
        assert_eq!(
            s3.env().iter().collect::<Vec<_>>(),
            vec![("MINIO_USER", S3_ACCESS), ("MINIO_PASSWORD", S3_SECRET)]
        );
        assert_eq!(
            aws.env().iter().collect::<Vec<_>>(),
            vec![
                ("AWS_ACCESS_KEY_ID", AWS_ACCESS),
                ("AWS_SECRET_ACCESS_KEY", AWS_SECRET),
                ("AWS_REGION", "eu-central-1")
            ]
        );
        assert_eq!(
            scw.env().iter().collect::<Vec<_>>(),
            vec![
                ("SCW_ACCESS_KEY", SCW_ACCESS),
                ("SCW_SECRET_KEY", SCW_SECRET),
                ("SCW_DEFAULT_PROJECT_ID", SCW_PROJECT)
            ]
        );
        assert_eq!(scw.set_fields(), ["access_key", "secret_key", "project_id"]);
        let aws_without_region = entry(CloudProvider::Aws, &[AWS_ACCESS, AWS_SECRET], None);
        assert_eq!(
            aws_without_region.env().iter().collect::<Vec<_>>(),
            vec![
                ("AWS_ACCESS_KEY_ID", AWS_ACCESS),
                ("AWS_SECRET_ACCESS_KEY", AWS_SECRET)
            ]
        );
        assert_eq!(hc.set_fields(), ["token"]);
        assert_eq!(s3.set_fields(), ["access_key", "secret_key"]);
        assert_eq!(
            aws.set_fields(),
            ["access_key_id", "secret_access_key", "region"]
        );
        assert_eq!(
            aws_without_region.set_fields(),
            ["access_key_id", "secret_access_key"]
        );
        for env in [&hc.env(), &s3.env(), &aws.env(), &scw.env()] {
            let debug = format!("{env:?}");
            for secret in [
                TOKEN,
                S3_ACCESS,
                S3_SECRET,
                AWS_ACCESS,
                AWS_SECRET,
                SCW_ACCESS,
                SCW_SECRET,
                SCW_PROJECT,
            ] {
                assert!(!debug.contains(secret));
            }
        }
        assert!(format!("{aws:?}").contains("eu-central-1"));
        for c in [hc, s3, aws, scw] {
            let debug = format!("{c:?}");
            for secret in [
                TOKEN,
                S3_ACCESS,
                S3_SECRET,
                AWS_ACCESS,
                AWS_SECRET,
                SCW_ACCESS,
                SCW_SECRET,
                SCW_PROJECT,
            ] {
                assert!(!debug.contains(secret));
            }
            assert!(debug.contains("..redacted.."));
        }
    }
    #[test]
    fn credential_validation() {
        assert_eq!(
            CloudCredential::new(
                CloudProvider::Scaleway,
                vec![SCW_ACCESS.into(), SCW_SECRET.into(), "not-a-uuid".into()],
                None
            )
            .unwrap_err()
            .to_string(),
            "cloud credentials: scaleway project_id must be a UUID like 11111111-2222-3333-4444-555555555555"
        );
        assert_eq!(
            CloudCredential::new(
                CloudProvider::Scaleway,
                vec![SCW_ACCESS.into(), SCW_SECRET.into()],
                None
            )
            .unwrap_err()
            .to_string(),
            "cloud credentials: scaleway expected 3 secret fields, got 2"
        );
        for (p, secret, field) in [
            (CloudProvider::HetznerCloud, "", "token"),
            (CloudProvider::HetznerCloud, "a b", "token"),
            (CloudProvider::HetznerCloud, "a\tb", "token"),
        ] {
            assert_eq!(
                CloudCredential::new(p, vec![secret.into()], None)
                    .unwrap_err()
                    .to_string(),
                format!(
                    "cloud credentials: {p} {field} must be non-empty with no spaces or control characters"
                )
            );
        }
        assert_eq!(
            CloudCredential::new(
                CloudProvider::Aws,
                vec![AWS_ACCESS.into(), AWS_SECRET.into()],
                Some("EU Central".into())
            )
            .unwrap_err()
            .to_string(),
            "cloud credentials: aws region must be lowercase letters, digits, and hyphens, like eu-central-1"
        );
        assert_eq!(
            CloudCredential::new(
                CloudProvider::HetznerCloud,
                vec![TOKEN.into()],
                Some("x".into())
            )
            .unwrap_err()
            .to_string(),
            "cloud credentials: hetzner-cloud does not accept an optional field"
        );
        assert_eq!(
            CloudCredential::new(CloudProvider::HetznerS3, vec![S3_ACCESS.into()], None)
                .unwrap_err()
                .to_string(),
            "cloud credentials: hetzner-s3 expected 2 secret fields, got 1"
        );
    }
    #[test]
    fn names_replace_and_remove() {
        let mut store = CloudCredentials::default();
        assert!(matches!(
            store.env(CloudProvider::Aws),
            Err(Error::CloudNotLoggedIn("aws"))
        ));
        for p in CloudProvider::ALL {
            assert_eq!(p.name().parse::<CloudProvider>().unwrap(), p);
            assert_eq!(p.to_string(), p.name());
        }
        assert_eq!(
            "digitalocean"
                .parse::<CloudProvider>()
                .unwrap_err()
                .to_string(),
            "unknown cloud provider \"digitalocean\"; expected one of hetzner-cloud, hetzner-s3, aws, scaleway"
        );
        store.set(entry(CloudProvider::HetznerCloud, &[TOKEN], None));
        store.set(entry(
            CloudProvider::Aws,
            &[AWS_ACCESS, AWS_SECRET],
            Some("eu-central-1"),
        ));
        store.set(entry(CloudProvider::Aws, &[AWS_ACCESS, AWS_SECRET], None));
        assert_eq!(
            store.get(CloudProvider::Aws).unwrap().set_fields(),
            ["access_key_id", "secret_access_key"]
        );
        assert!(store.remove(CloudProvider::Aws));
        assert!(!store.remove(CloudProvider::Aws));
        let debug = format!("{store:?}");
        assert!(debug.contains("..redacted.."));
        assert!(!debug.contains(TOKEN));
        assert!(store.get(CloudProvider::HetznerCloud).is_some());
        assert!(
            format!("{:?}", store.env(CloudProvider::HetznerCloud).unwrap())
                .contains("HCLOUD_TOKEN")
        );
    }
}
