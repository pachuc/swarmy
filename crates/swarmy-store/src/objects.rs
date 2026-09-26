//! Shared object storage clients built from settings.
//!
//! Every service using a metadata namespace must use the same object
//! namespace. This constructor lives next to the store so the client binary
//! never links an object storage implementation.
use std::sync::Arc;

use aws_credential_types::provider::ProvideCredentials;
use object_store::{
    CredentialProvider, ObjectStore,
    aws::{AmazonS3Builder, AwsCredential},
    prefix::PrefixStore,
};
use swarmy_config::Settings;

use crate::blob::BlobError;

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

fn regional_mode(settings: &Settings) -> (bool, bool) {
    (
        settings.s3_endpoint.is_empty(),
        settings.s3_access_key.is_empty() && settings.s3_secret_key.is_empty(),
    )
}

fn builder(settings: &Settings, bucket: &str) -> AmazonS3Builder {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(&settings.s3_region);
    let (regional, default_credentials) = regional_mode(settings);
    if regional {
        builder = builder.with_virtual_hosted_style_request(true);
    } else {
        builder = builder
            .with_endpoint(&settings.s3_endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false);
    }
    if default_credentials {
        builder = builder.with_credentials(Arc::new(DefaultAwsCredentials {
            region: settings.s3_region.clone(),
            chain: tokio::sync::OnceCell::new(),
        }));
    } else {
        builder = builder
            .with_access_key_id(&settings.s3_access_key)
            .with_secret_access_key(&settings.s3_secret_key);
    }
    builder
}

/// Build the shared S3 client with namespace-relative request keys and
/// listing names. A legacy bucket/prefix keeps its exact object locations.
/// # Errors
/// Rejects invalid or ambiguous namespaces and invalid S3 client settings.
pub fn from_settings(settings: &Settings) -> Result<Arc<dyn ObjectStore>, BlobError> {
    let (bucket, prefix) = settings.s3_namespace()?;
    if settings.s3_bucket.contains('/') {
        tracing::warn!(
            "s3_bucket = bucket/prefix is deprecated; set s3_bucket and s3_prefix separately"
        );
    }
    let store = builder(settings, bucket).build()?;
    if prefix.as_str().is_empty() {
        Ok(Arc::new(store))
    } else {
        // Parse instead of converting: Path::from would encode some
        // characters and could change where legacy objects live. The prefix
        // was already validated, so this cannot fail in practice.
        let path = object_store::path::Path::parse(prefix.as_str()).map_err(|error| {
            object_store::Error::Generic {
                store: "S3 namespace",
                source: Box::new(error),
            }
        })?;
        Ok(Arc::new(PrefixStore::new(store, path)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regional_and_custom_modes_build_without_network() {
        let mut settings = Settings::default();
        assert_eq!(regional_mode(&settings), (false, false));
        let custom = builder(&settings, "bucket").build().unwrap();
        let debug = format!("{custom:?}");
        assert!(debug.contains("http://127.0.0.1:8333/bucket"));
        assert!(debug.contains("StaticCredentialProvider"));
        settings.s3_endpoint.clear();
        settings.s3_access_key.clear();
        settings.s3_secret_key.clear();
        assert_eq!(regional_mode(&settings), (true, true));
        let regional = builder(&settings, "bucket").build().unwrap();
        let debug = format!("{regional:?}");
        assert!(debug.contains("https://bucket.s3.us-east-1.amazonaws.com"));
        assert!(debug.contains("DefaultAwsCredentials"));
    }

    #[test]
    fn bucket_profile_uses_laptop_credentials_and_region() {
        let settings = Settings {
            s3_endpoint: String::new(),
            s3_bucket: "bucket".into(),
            s3_region: "eu-west-1".into(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            ..Settings::default()
        };
        let regional = format!("{:?}", from_settings(&settings).unwrap());
        assert!(regional.contains("https://bucket.s3.eu-west-1.amazonaws.com"));
        assert!(regional.contains("DefaultAwsCredentials"));
    }

    #[test]
    fn legacy_namespace_builds_and_ambiguity_is_rejected() {
        let mut settings = Settings {
            s3_bucket: "bucket/run/nested".into(),
            ..Settings::default()
        };
        assert!(from_settings(&settings).is_ok());
        settings.s3_prefix = "explicit".parse().unwrap();
        assert!(from_settings(&settings).is_err());
        for value in [
            "",
            "/run",
            "bucket/",
            "bucket/a//b",
            "bucket/a/../b",
            "bucket/a/",
        ] {
            settings.s3_bucket = value.into();
            settings.s3_prefix = swarmy_config::ObjectPrefix::default();
            assert!(from_settings(&settings).is_err(), "{value}");
        }
    }
}
