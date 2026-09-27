//! Shared object storage clients built from settings.
//!
//! Every service using a metadata namespace must use the same object
//! namespace. This constructor lives next to the store so the client binary
//! never links an object storage implementation.
use std::collections::HashMap;
use std::sync::Arc;

use object_store::aws::AmazonS3ConfigKey;
use object_store::{ObjectStore, aws::AmazonS3Builder, prefix::PrefixStore};
use swarmy_config::Settings;

use crate::blob::BlobError;

fn regional_mode(settings: &Settings) -> (bool, bool) {
    (
        settings.s3_endpoint.is_empty(),
        settings.s3_access_key.is_empty() && settings.s3_secret_key.is_empty(),
    )
}

fn finish(base: AmazonS3Builder, settings: &Settings, bucket: &str) -> AmazonS3Builder {
    let mut builder = base
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
    if !default_credentials {
        builder = builder
            .with_access_key_id(&settings.s3_access_key)
            .with_secret_access_key(&settings.s3_secret_key);
    }
    builder
}

fn builder(settings: &Settings, bucket: &str) -> AmazonS3Builder {
    // Skip non-UTF-8 variables as `AmazonS3Builder::from_env` does;
    // `std::env::vars` would panic on them.
    let env = std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
        .collect();
    builder_with_env(settings, bucket, &env)
}

/// Use one path for live credentials and isolated test environments. The
/// underlying S3 builder also reads web identity settings during `build()`.
fn builder_with_env(
    settings: &Settings,
    bucket: &str,
    env: &HashMap<String, String>,
) -> AmazonS3Builder {
    let (_, default_credentials) = regional_mode(settings);
    let mut base = AmazonS3Builder::new();
    if default_credentials {
        for (key, value) in env {
            if key.starts_with("AWS_")
                && let Ok(config_key) = key.to_ascii_lowercase().parse::<AmazonS3ConfigKey>()
            {
                base = base.with_config(config_key, value);
            }
        }
    }
    finish(base, settings, bucket)
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

    fn hostile_env() -> HashMap<String, String> {
        HashMap::from([
            ("AWS_ACCESS_KEY_ID".into(), "hostile-key".into()),
            ("AWS_SECRET_ACCESS_KEY".into(), "hostile-secret".into()),
            ("AWS_SESSION_TOKEN".into(), "hostile-token".into()),
            (
                "AWS_ENDPOINT_URL".into(),
                "https://hostile.example.invalid".into(),
            ),
            ("AWS_ALLOW_HTTP".into(), "true".into()),
        ])
    }

    #[test]
    fn settings_keys_ignore_a_hostile_shell() {
        // The dev stack carries static keys in settings. A shell's session
        // token, endpoint, or allow-http flag must not leak into that client.
        let settings = Settings::default();
        assert_eq!(regional_mode(&settings), (false, false));
        let custom = builder_with_env(&settings, "bucket", &hostile_env())
            .build()
            .unwrap();
        let debug = format!("{custom:?}");
        assert!(debug.contains("http://127.0.0.1:8333/bucket"));
        assert!(debug.contains("StaticCredentialProvider"));
        for leaked in [
            "hostile-key",
            "hostile-secret",
            "hostile-token",
            "hostile.example.invalid",
        ] {
            assert!(!debug.contains(leaked), "shell value leaked: {leaked}");
        }
    }

    #[test]
    fn no_keys_selects_the_instance_metadata_provider() {
        // object_store reads AWS_WEB_IDENTITY_TOKEN_FILE and AWS_ROLE_ARN
        // directly from the process environment during build(), even with
        // an explicit map. In that environment the web identity provider wins.
        if std::env::var_os("AWS_WEB_IDENTITY_TOKEN_FILE").is_some()
            && std::env::var_os("AWS_ROLE_ARN").is_some()
        {
            return;
        }
        // The nodes carry no static keys: the client must fall back to the
        // instance-metadata provider for the instance role.
        let mut settings = Settings::default();
        settings.s3_endpoint.clear();
        settings.s3_access_key.clear();
        settings.s3_secret_key.clear();
        assert_eq!(regional_mode(&settings), (true, true));
        let regional = builder_with_env(&settings, "bucket", &HashMap::new())
            .build()
            .unwrap();
        let debug = format!("{regional:?}");
        assert!(debug.contains("https://bucket.s3.us-east-1.amazonaws.com"));
        assert!(!debug.contains("StaticCredentialProvider"));
        assert!(
            debug.contains("InstanceCredentialProvider"),
            "expected the instance-metadata provider, got: {debug}"
        );
    }

    #[test]
    fn no_keys_honours_static_keys_from_the_environment() {
        // Same branch with static keys in the environment map: the map is
        // honoured, proving the no-keys tests above exercise the real
        // environment path rather than a stub.
        let mut settings = Settings::default();
        settings.s3_endpoint.clear();
        settings.s3_access_key.clear();
        settings.s3_secret_key.clear();
        let env = HashMap::from([
            ("AWS_ACCESS_KEY_ID".into(), "env-key".into()),
            ("AWS_SECRET_ACCESS_KEY".into(), "env-secret".into()),
        ]);
        let regional = builder_with_env(&settings, "bucket", &env).build().unwrap();
        let debug = format!("{regional:?}");
        assert!(debug.contains("https://bucket.s3.us-east-1.amazonaws.com"));
        assert!(debug.contains("StaticCredentialProvider"));
    }

    #[test]
    fn bucket_profile_uses_settings_region_without_network() {
        // The S3 builder checks web identity directly in the process shell.
        if std::env::var_os("AWS_WEB_IDENTITY_TOKEN_FILE").is_some()
            && std::env::var_os("AWS_ROLE_ARN").is_some()
        {
            return;
        }
        let settings = Settings {
            s3_endpoint: String::new(),
            s3_bucket: "bucket".into(),
            s3_region: "eu-west-1".into(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            ..Settings::default()
        };
        let regional = builder_with_env(&settings, "bucket", &HashMap::new())
            .build()
            .unwrap();
        let debug = format!("{regional:?}");
        assert!(debug.contains("https://bucket.s3.eu-west-1.amazonaws.com"));
        assert!(
            debug.contains("InstanceCredentialProvider"),
            "expected the instance-metadata provider, got: {debug}"
        );
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
