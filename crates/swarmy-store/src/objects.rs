//! Shared object storage clients built from settings.
//!
//! Every service using a metadata namespace must use the same object
//! namespace. This constructor lives next to the store so the client binary
//! never links an object storage implementation.
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::aws::AmazonS3ConfigKey;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, aws::AmazonS3Builder, path::Path,
    prefix::PrefixStore,
};
use swarmy_config::{BucketSpec, Settings};

use crate::blob::BlobError;

fn regional_mode(settings: &Settings) -> (bool, bool) {
    (
        settings.s3.endpoint.is_empty(),
        settings.s3.access_key.is_empty() && settings.s3.secret_key.is_empty(),
    )
}

fn finish(base: AmazonS3Builder, settings: &Settings, bucket: &str) -> AmazonS3Builder {
    let mut builder = base
        .with_bucket_name(bucket)
        .with_region(&settings.s3.region);
    let (regional, default_credentials) = regional_mode(settings);
    if regional {
        builder = builder.with_virtual_hosted_style_request(true);
    } else {
        builder = builder
            .with_endpoint(&settings.s3.endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false);
    }
    if !default_credentials {
        builder = builder
            .with_access_key_id(&settings.s3.access_key)
            .with_secret_access_key(&settings.s3.secret_key);
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
/// listing names.
/// # Errors
/// Rejects invalid or ambiguous namespaces and invalid S3 client settings.
pub fn from_settings(settings: &Settings) -> Result<Arc<dyn ObjectStore>, BlobError> {
    let (bucket, prefix) = settings.s3_namespace()?;
    let store = builder(settings, bucket).build()?;
    let store: Arc<dyn ObjectStore> = Arc::new(store);
    let store = maybe_unconditional(store, settings.s3.conditional_create);
    if prefix.as_str().is_empty() {
        Ok(store)
    } else {
        // Parse instead of converting so namespace characters stay exact.
        let path = object_store::path::Path::parse(prefix.as_str()).map_err(|error| {
            object_store::Error::Generic {
                store: "S3 namespace",
                source: Box::new(error),
            }
        })?;
        Ok(Arc::new(PrefixStore::new(store, path)))
    }
}

/// Build the shared S3 client from one bucket description instead of service
/// settings. Static keys and the custom endpoint become the client
/// credentials; the instance-role source falls back to the instance-metadata
/// provider. Tests exercise the same object operations through this
/// constructor that the node services reach through settings.
/// # Errors
/// Rejects invalid S3 client settings.
pub fn from_bucket_spec(
    spec: &BucketSpec,
    fallback_region: &str,
    conditional_create: bool,
) -> Result<Arc<dyn ObjectStore>, BlobError> {
    let mut owned = spec.clone();
    owned.resolve_region(fallback_region);
    let mut settings = Settings::default();
    owned.apply_to_settings(&mut settings);
    settings.s3.conditional_create = conditional_create;
    from_settings(&settings)
}

/// Downgrade create-only PUTs to plain PUTs for providers that reject the
/// `If-None-Match: *` header. The objects are content-addressed, so a plain
/// PUT overwriting identical bytes is safe.
fn maybe_unconditional(
    store: Arc<dyn ObjectStore>,
    conditional_create: bool,
) -> Arc<dyn ObjectStore> {
    if conditional_create {
        store
    } else {
        Arc::new(UnconditionalStore { inner: store })
    }
}

/// An [`ObjectStore`] that turns create-only PUTs into plain PUTs.
#[derive(Debug)]
pub struct UnconditionalStore {
    inner: Arc<dyn ObjectStore>,
}

impl UnconditionalStore {
    /// Wrap `inner`, downgrading every create-only PUT to a plain PUT.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

impl std::fmt::Display for UnconditionalStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "unconditional({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for UnconditionalStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let opts = if opts.mode == PutMode::Create {
            PutOptions::default()
        } else {
            opts
        };
        self.inner.put_opts(location, payload, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
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
        settings.s3.endpoint.clear();
        settings.s3.access_key.clear();
        settings.s3.secret_key.clear();
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
    fn bucket_path_is_rejected() {
        let mut settings = Settings::default();
        settings.s3.bucket = "bucket/run/nested".into();
        assert!(from_settings(&settings).is_err());
    }

    #[test]
    fn unconditional_store_downgrades_only_create_puts() {
        use object_store::memory::InMemory;
        use object_store::{PutMode, PutOptions, path::Path};
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let store = UnconditionalStore { inner };
            let path = Path::from("downgrade/object");
            // A create-only PUT becomes a plain PUT, so repeats overwrite.
            for expected in ["first", "second"] {
                store
                    .put_opts(
                        &path,
                        expected.as_bytes().to_vec().into(),
                        PutMode::Create.into(),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    store.get(&path).await.unwrap().bytes().await.unwrap(),
                    expected.as_bytes()
                );
            }
            // Other modes pass through unchanged.
            let options = PutOptions::default();
            assert_eq!(options.mode, PutMode::Overwrite);
            store
                .put_opts(&path, "third".as_bytes().to_vec().into(), options)
                .await
                .unwrap();
            assert_eq!(
                store.get(&path).await.unwrap().bytes().await.unwrap(),
                "third"
            );
            assert!(format!("{store}").starts_with("unconditional("));
        });
    }

    #[test]
    fn conditional_create_false_selects_the_unconditional_store() {
        let mut settings = Settings::default();
        settings.s3.conditional_create = false;
        let debug = format!("{:?}", from_settings(&settings).unwrap());
        assert!(debug.contains("UnconditionalStore"), "{debug}");
        settings.s3.conditional_create = true;
        let debug = format!("{:?}", from_settings(&settings).unwrap());
        assert!(!debug.contains("UnconditionalStore"), "{debug}");
    }

    #[tokio::test]
    async fn bucket_spec_with_static_keys_round_trips_objects() {
        // Runs the same object operations through the new bucket description
        // with static keys and a custom endpoint. Skips without the dev stack.
        if swarmy_core::test_support::stack_env_os("SWARMY_S3_ENDPOINT").is_none() {
            return;
        }
        let loaded = swarmy_config::Settings::load().unwrap().settings;
        let spec = BucketSpec {
            endpoint: loaded.s3.endpoint.clone(),
            region: loaded.s3.region.clone(),
            bucket: loaded.s3.bucket.clone(),
            prefix: swarmy_config::ObjectPrefix::default(),
            credentials: swarmy_config::BucketCredentials::StaticKeys {
                access_key: loaded.s3.access_key.clone(),
                secret_key: loaded.s3.secret_key.clone(),
            },
        };
        for conditional_create in [true, false] {
            let store = from_bucket_spec(&spec, &loaded.s3.region, conditional_create).unwrap();
            let scope = format!("bucket-spec-test-{}", ulid::Ulid::generate());
            let path = object_store::path::Path::from(format!("{scope}/object"));
            store.put(&path, "payload".into()).await.unwrap();
            assert_eq!(
                store.get(&path).await.unwrap().bytes().await.unwrap(),
                "payload"
            );
            assert_eq!(store.head(&path).await.unwrap().location, path);
            // A create-only PUT of identical bytes stays safe in both modes.
            let result = store
                .put_opts(&path, "payload".into(), PutMode::Create.into())
                .await;
            if conditional_create {
                assert!(
                    matches!(result, Err(object_store::Error::AlreadyExists { .. })),
                    "unexpected {result:?}"
                );
            } else {
                result.unwrap();
            }
            store.delete(&path).await.unwrap();
            assert!(matches!(
                store.head(&path).await,
                Err(object_store::Error::NotFound { .. })
            ));
        }
    }
}
