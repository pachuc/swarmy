use std::{str::FromStr, sync::Arc};

use aws_credential_types::provider::ProvideCredentials;
use object_store::{
    CredentialProvider, ObjectStore,
    aws::{AmazonS3Builder, AwsCredential},
    path::Path,
    prefix::PrefixStore,
};
use serde::{Deserialize, Serialize};

use crate::{Error, Settings};

#[derive(Debug)]
struct DefaultAwsCredentials {
    region: String,
    chain:
        tokio::sync::OnceCell<aws_config::default_provider::credentials::DefaultCredentialsChain>,
}

#[async_trait::async_trait]
impl CredentialProvider for DefaultAwsCredentials {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let chain = self
            .chain
            .get_or_init(|| async {
                aws_config::default_provider::credentials::DefaultCredentialsChain::builder()
                    .region(aws_config::Region::new(self.region.clone()))
                    .build()
                    .await
            })
            .await;
        let credentials =
            chain
                .provide_credentials()
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "S3 credentials",
                    source: Box::new(error),
                })?;
        Ok(Arc::new(AwsCredential {
            key_id: credentials.access_key_id().to_owned(),
            secret_key: credentials.secret_access_key().to_owned(),
            token: credentials.session_token().map(str::to_owned),
        }))
    }
}

/// An exact object namespace, with no empty or relative path segments.
/// Empty selects the whole bucket. Parsing never trims or encodes the value.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ObjectPrefix(Path);

impl ObjectPrefix {
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl FromStr for ObjectPrefix {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || {
            Error::S3Namespace(
                "prefix must have no leading/trailing slash, empty or dot segments, or control characters",
            )
        };
        if value.starts_with('/') || value.ends_with('/') {
            return Err(invalid());
        }
        Ok(Self(Path::parse(value).map_err(|_| invalid())?))
    }
}

impl TryFrom<String> for ObjectPrefix {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<ObjectPrefix> for String {
    fn from(value: ObjectPrefix) -> Self {
        value.0.to_string()
    }
}

impl Settings {
    pub(crate) fn s3_namespace(&self) -> Result<(&str, ObjectPrefix), Error> {
        let (bucket, prefix) = if let Some((bucket, prefix)) = self.s3_bucket.split_once('/') {
            if !self.s3_prefix.as_str().is_empty() {
                return Err(Error::S3Namespace(
                    "set either a legacy bucket/prefix or s3_prefix, not both",
                ));
            }
            if prefix.is_empty() {
                return Err(Error::S3Namespace(
                    "legacy bucket/prefix has an empty prefix",
                ));
            }
            (bucket, prefix.parse()?)
        } else {
            (self.s3_bucket.as_str(), self.s3_prefix.clone())
        };
        if bucket.is_empty() {
            return Err(Error::S3Namespace("bucket must not be empty"));
        }
        Ok((bucket, prefix))
    }

    fn s3_mode(&self) -> (bool, bool) {
        (
            self.s3_endpoint.is_empty(),
            self.s3_access_key.is_empty() && self.s3_secret_key.is_empty(),
        )
    }

    fn s3_builder(&self, bucket: &str) -> AmazonS3Builder {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(&self.s3_region);
        let (regional, default_credentials) = self.s3_mode();
        if regional {
            builder = builder.with_virtual_hosted_style_request(true);
        } else {
            builder = builder
                .with_endpoint(&self.s3_endpoint)
                .with_allow_http(true)
                .with_virtual_hosted_style_request(false);
        }
        if default_credentials {
            builder = builder.with_credentials(Arc::new(DefaultAwsCredentials {
                region: self.s3_region.clone(),
                chain: tokio::sync::OnceCell::new(),
            }));
        } else {
            builder = builder
                .with_access_key_id(&self.s3_access_key)
                .with_secret_access_key(&self.s3_secret_key);
        }
        builder
    }

    /// Build the shared S3 client with namespace-relative request keys and
    /// listing names. A legacy bucket/prefix keeps its exact object locations.
    /// # Errors
    /// Rejects invalid or ambiguous namespaces and invalid S3 client settings.
    pub fn object_store(&self) -> Result<Arc<dyn ObjectStore>, Error> {
        let (bucket, prefix) = self.s3_namespace()?;
        if self.s3_bucket.contains('/') {
            tracing::warn!(
                "s3_bucket = bucket/prefix is deprecated; set s3_bucket and s3_prefix separately"
            );
        }
        let store = self.s3_builder(bucket).build()?;
        if prefix.as_str().is_empty() {
            Ok(Arc::new(store))
        } else {
            // Use the validated path directly: Path::from would encode some
            // characters and could change where legacy objects live.
            Ok(Arc::new(PrefixStore::new(store, prefix.0)))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn regional_and_custom_modes_build_without_network() {
        let mut settings = Settings::default();
        assert_eq!(settings.s3_mode(), (false, false));
        let custom = settings.s3_builder("bucket").build().unwrap();
        let debug = format!("{custom:?}");
        assert!(debug.contains("http://127.0.0.1:8333/bucket"));
        assert!(debug.contains("StaticCredentialProvider"));
        settings.s3_endpoint.clear();
        settings.s3_access_key.clear();
        settings.s3_secret_key.clear();
        assert_eq!(settings.s3_mode(), (true, true));
        let regional = settings.s3_builder("bucket").build().unwrap();
        let debug = format!("{regional:?}");
        assert!(debug.contains("https://bucket.s3.us-east-1.amazonaws.com"));
        assert!(debug.contains("DefaultAwsCredentials"));
    }

    #[test]
    fn prefixes_are_validated_without_normalization() {
        for value in ["", "run", "runs/nested", "spaces and %2F/雪"] {
            assert_eq!(value.parse::<ObjectPrefix>().unwrap().as_str(), value);
        }
        for value in [
            "/", "/run", "run/", "a//b", ".", "..", "a/./b", "a/../b", "a\n",
        ] {
            assert!(value.parse::<ObjectPrefix>().is_err(), "{value:?}");
            let encoded = toml::to_string(&BTreeMap::from([("s3_prefix", value)])).unwrap();
            assert!(toml::from_str::<Settings>(&encoded).is_err());
            assert!(
                Settings::default()
                    .apply_environment(&BTreeMap::from([("SWARMY_S3_PREFIX".into(), value.into())]))
                    .is_err()
            );
        }
    }

    #[test]
    fn prefix_round_trips_and_environment_overrides_file() {
        let mut settings: Settings = toml::from_str("s3_prefix = 'file/nested'").unwrap();
        settings
            .apply_environment(&BTreeMap::from([(
                "SWARMY_S3_PREFIX".into(),
                "env/nested".into(),
            )]))
            .unwrap();
        assert_eq!(settings.s3_prefix.as_str(), "env/nested");
        let decoded: Settings = toml::from_str(&settings.to_toml().unwrap()).unwrap();
        assert_eq!(decoded.s3_prefix, settings.s3_prefix);
        let mut exported = Settings::default();
        exported.apply_environment(&settings.environment()).unwrap();
        assert_eq!(exported.s3_prefix, settings.s3_prefix);
        exported
            .apply_environment(&BTreeMap::from([(
                "SWARMY_S3_PREFIX".into(),
                String::new(),
            )]))
            .unwrap();
        assert_eq!(exported.s3_prefix, ObjectPrefix::default());
    }

    #[test]
    fn legacy_namespace_is_exact_and_ambiguity_is_rejected() {
        let mut settings = Settings {
            s3_bucket: "bucket/run/nested".into(),
            ..Settings::default()
        };
        let (bucket, prefix) = settings.s3_namespace().unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(prefix.as_str(), "run/nested");
        assert!(settings.object_store().is_ok());
        settings.s3_prefix = "explicit".parse().unwrap();
        assert!(settings.object_store().is_err());
        settings.s3_prefix = ObjectPrefix::default();
        for value in [
            "",
            "/run",
            "bucket/",
            "bucket/a//b",
            "bucket/a/../b",
            "bucket/a/",
        ] {
            settings.s3_bucket = value.into();
            assert!(settings.object_store().is_err(), "{value}");
        }
    }
}
